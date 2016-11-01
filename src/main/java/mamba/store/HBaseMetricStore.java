package mamba.store;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import mamba.aggregators.AggregatorUtils;
import mamba.aggregators.Function;
import mamba.aggregators.TimelineMetricAggregator;
import mamba.aggregators.TimelineMetricAggregatorFactory;
import mamba.discovery.TimelineMetricMetadataKey;
import mamba.discovery.TimelineMetricMetadataManager;
import mamba.entity.TimelinePutResponse;
import mamba.function.SeriesAggregateFunction;
import mamba.function.TimelineMetricsSeriesAggregateFunction;
import mamba.function.TimelineMetricsSeriesAggregateFunctionFactory;
import mamba.metrics.*;
import mamba.query.Condition;
import mamba.query.ConditionBuilder;
import mamba.query.TopNCondition;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static mamba.store.TimelineMetricConfiguration.*;

/**
 * Created by dongbin on 2016/10/10.
 */
public class HBaseMetricStore implements MetricStore {

    static final Log LOG = LogFactory.getLog(HBaseMetricStore.class);

    private static volatile boolean isInitialized = false;

    private final TimelineMetricConfiguration configuration;

    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    private PhoenixHBaseAccessor hBaseAccessor;

    private TimelineMetricMetadataManager metricMetadataManager;

    private Integer defaultTopNHostsLimit;

    /**
     * Construct the service.
     */
    public HBaseMetricStore(TimelineMetricConfiguration configuration) {
        this.configuration = configuration;
    }

    static Map<Long, Double> updateValuesAsRate(Map<Long, Double> metricValues, boolean isDiff) {
        Long prevTime = null;
        Double prevVal = null;
        long step;
        Double diff;

        for (Iterator<Map.Entry<Long, Double>> it = metricValues.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Long, Double> timeValueEntry = it.next();
            Long currTime = timeValueEntry.getKey();
            Double currVal = timeValueEntry.getValue();

            if (prevTime != null) {
                step = currTime - prevTime;
                diff = currVal - prevVal;
                Double rate = isDiff ? diff : (diff / TimeUnit.MILLISECONDS.toSeconds(step));
                timeValueEntry.setValue(rate);
            } else {
                it.remove();
            }

            prevTime = currTime;
            prevVal = currVal;
        }

        return metricValues;
    }

    static Multimap<String, List<Function>> parseMetricNamesToAggregationFunctions(List<String> metricNames) {
        Multimap<String, List<Function>> metricsFunctions = ArrayListMultimap.create();

        for (String metricName : metricNames) {
            Function function = Function.DEFAULT_VALUE_FUNCTION;
            String cleanMetricName = metricName;

            try {
                function = Function.fromMetricName(metricName);
                int functionStartIndex = metricName.indexOf("._");
                if (functionStartIndex > 0) {
                    cleanMetricName = metricName.substring(0, functionStartIndex);
                }
            } catch (Function.FunctionFormatException ffe) {
                // unknown function so
                // fallback to VALUE, and fullMetricName
            }

            List<Function> functionsList = new ArrayList<>();
            functionsList.add(function);
            metricsFunctions.put(cleanMetricName, functionsList);
        }

        return metricsFunctions;
    }

    @PostConstruct
    protected void serviceInit() throws Exception {
        initializeSubsystem(configuration.getHbaseConf(), configuration.getMetricsConf());
    }

    private synchronized void initializeSubsystem(Configuration hbaseConf,
                                                  Configuration metricsConf) {
        if (!isInitialized) {
            hBaseAccessor = new PhoenixHBaseAccessor(hbaseConf, metricsConf);
            // Initialize schema
            hBaseAccessor.initMetricSchema();
            // Initialize metadata from store
            metricMetadataManager = new TimelineMetricMetadataManager(hBaseAccessor, metricsConf);
            metricMetadataManager.initializeMetadata();
            // Initialize policies before TTL update
            hBaseAccessor.initPoliciesAndTTL();

            String whitelistFile = metricsConf.get(TIMELINE_METRICS_WHITELIST_FILE, "");
            if (!StringUtils.isEmpty(whitelistFile)) {
                AggregatorUtils.populateMetricWhitelistFromFile(whitelistFile);
            }

            defaultTopNHostsLimit = Integer.parseInt(metricsConf.get(DEFAULT_TOPN_HOSTS_LIMIT, "20"));
            if (Boolean.parseBoolean(metricsConf.get(USE_GROUPBY_AGGREGATOR_QUERIES, "true"))) {
                LOG.info("Using group by aggregators for aggregating host and cluster metrics.");
            }

            // Start the cluster aggregator second
            TimelineMetricAggregator secondClusterAggregator =
                    TimelineMetricAggregatorFactory.createTimelineClusterAggregatorSecond(hBaseAccessor, metricsConf, metricMetadataManager);
            scheduleAggregatorThread(secondClusterAggregator);

            // Start the minute cluster aggregator
            TimelineMetricAggregator minuteClusterAggregator =
                    TimelineMetricAggregatorFactory.createTimelineClusterAggregatorMinute(hBaseAccessor, metricsConf);
            scheduleAggregatorThread(minuteClusterAggregator);

            // Start the hourly cluster aggregator
            TimelineMetricAggregator hourlyClusterAggregator =
                    TimelineMetricAggregatorFactory.createTimelineClusterAggregatorHourly(hBaseAccessor, metricsConf);
            scheduleAggregatorThread(hourlyClusterAggregator);

            // Start the daily cluster aggregator
            TimelineMetricAggregator dailyClusterAggregator =
                    TimelineMetricAggregatorFactory.createTimelineClusterAggregatorDaily(hBaseAccessor, metricsConf);
            scheduleAggregatorThread(dailyClusterAggregator);

            // Start the minute host aggregator
            TimelineMetricAggregator minuteHostAggregator =
                    TimelineMetricAggregatorFactory.createTimelineMetricAggregatorMinute(hBaseAccessor, metricsConf);
            scheduleAggregatorThread(minuteHostAggregator);

            // Start the hourly host aggregator
            TimelineMetricAggregator hourlyHostAggregator =
                    TimelineMetricAggregatorFactory.createTimelineMetricAggregatorHourly(hBaseAccessor, metricsConf);
            scheduleAggregatorThread(hourlyHostAggregator);

            // Start the daily host aggregator
            TimelineMetricAggregator dailyHostAggregator =
                    TimelineMetricAggregatorFactory.createTimelineMetricAggregatorDaily(hBaseAccessor, metricsConf);
            scheduleAggregatorThread(dailyHostAggregator);

            if (!configuration.isTimelineMetricsServiceWatcherDisabled()) {
                int initDelay = configuration.getTimelineMetricsServiceWatcherInitDelay();
                int delay = configuration.getTimelineMetricsServiceWatcherDelay();
                // Start the watchdog
                executorService.scheduleWithFixedDelay(
                        new MetricStoreWatcher(this, configuration), initDelay, delay,
                        TimeUnit.SECONDS);
                LOG.info("Started watchdog for timeline metrics store with initial " +
                        "delay = " + initDelay + ", delay = " + delay);
            }

            isInitialized = true;
        }

    }

    @Override
    public TimelineMetrics getTimelineMetrics(List<String> metricNames,
                                              List<String> hostnames, String applicationId, String instanceId,
                                              Long startTime, Long endTime, Precision precision, Integer limit,
                                              boolean groupedByHosts, TopNConfig topNConfig, String seriesAggregateFunction) throws SQLException, IOException {

        if (metricNames == null || metricNames.isEmpty()) {
            throw new IllegalArgumentException("No metric name filter specified.");
        }
        if ((startTime == null && endTime != null)
                || (startTime != null && endTime == null)) {
            throw new IllegalArgumentException("Open ended query not supported ");
        }
        if (limit != null && limit > PhoenixHBaseAccessor.RESULTSET_LIMIT) {
            throw new IllegalArgumentException("Limit too big");
        }

        TimelineMetricsSeriesAggregateFunction seriesAggrFunctionInstance = null;
        if (!StringUtils.isEmpty(seriesAggregateFunction)) {
            SeriesAggregateFunction func = SeriesAggregateFunction.getFunction(seriesAggregateFunction);
            seriesAggrFunctionInstance = TimelineMetricsSeriesAggregateFunctionFactory.newInstance(func);
        }

        Multimap<String, List<Function>> metricFunctions =
                parseMetricNamesToAggregationFunctions(metricNames);

        ConditionBuilder conditionBuilder = new ConditionBuilder(new ArrayList<String>(metricFunctions.keySet()))
                .hostnames(hostnames)
                .appId(applicationId)
                .instanceId(instanceId)
                .startTime(startTime)
                .endTime(endTime)
                .precision(precision)
                .limit(limit)
                .grouped(groupedByHosts);

        if (topNConfig != null) {
            if (TopNCondition.isTopNHostCondition(metricNames, hostnames) || TopNCondition.isTopNMetricCondition(metricNames, hostnames)) {
                conditionBuilder.topN(topNConfig.getTopN());
                conditionBuilder.isBottomN(topNConfig.getIsBottomN());
                Function.ReadFunction readFunction = Function.ReadFunction.getFunction(topNConfig.getTopNFunction());
                Function function = new Function(readFunction, null);
                conditionBuilder.topNFunction(function);
            } else {
                LOG.info("Invalid Input for TopN query. Ignoring TopN Request.");
            }
        } else if (startTime != null && hostnames != null && hostnames.size() > defaultTopNHostsLimit) {
            // if (timeseries query AND hostnames passed AND size(hostnames) > limit)
            LOG.info("Requesting data for more than " + defaultTopNHostsLimit + " Hosts. " +
                    "Defaulting to Top " + defaultTopNHostsLimit);
            conditionBuilder.topN(defaultTopNHostsLimit);
            conditionBuilder.isBottomN(false);
        }

        Condition condition = conditionBuilder.build();

        TimelineMetrics metrics;

        if (hostnames == null || hostnames.isEmpty()) {
            metrics = hBaseAccessor.getAggregateMetricRecords(condition, metricFunctions);
        } else {
            metrics = hBaseAccessor.getMetricRecords(condition, metricFunctions);
        }

        metrics = postProcessMetrics(metrics);

        if (metrics.getMetrics().size() == 0) {
            return metrics;
        }

        return seriesAggregateMetrics(seriesAggrFunctionInstance, metrics);
    }

    private TimelineMetrics postProcessMetrics(TimelineMetrics metrics) {
        List<TimelineMetric> metricsList = metrics.getMetrics();

        for (TimelineMetric metric : metricsList) {
            String name = metric.getMetricName();
            if (name.contains("._rate")) {
                updateValuesAsRate(metric.getMetricValues(), false);
            } else if (name.contains("._diff")) {
                updateValuesAsRate(metric.getMetricValues(), true);
            }
        }

        return metrics;
    }

    private TimelineMetrics seriesAggregateMetrics(TimelineMetricsSeriesAggregateFunction seriesAggrFuncInstance,
                                                   TimelineMetrics metrics) {
        if (seriesAggrFuncInstance != null) {
            TimelineMetric appliedMetric = seriesAggrFuncInstance.apply(metrics);
            metrics.setMetrics(Collections.singletonList(appliedMetric));
        }
        return metrics;
    }

    @Override
    public TimelinePutResponse putMetrics(TimelineMetrics metrics) throws SQLException, IOException {
        TimelinePutResponse response = new TimelinePutResponse();
        hBaseAccessor.insertMetricRecordsWithMetadata(metricMetadataManager, metrics, false);
        return response;
    }

    @Override
    public TimelinePutResponse putContainerMetrics(List<ContainerMetric> metrics)
            throws SQLException, IOException {
        hBaseAccessor.insertContainerMetrics(metrics);
        return new TimelinePutResponse();
    }

    @Override
    public Map<String, List<TimelineMetricMetadata>> getTimelineMetricMetadata() throws SQLException, IOException {
        Map<TimelineMetricMetadataKey, TimelineMetricMetadata> metadata =
                metricMetadataManager.getMetadataCache();

        // Group Metadata by AppId
        Map<String, List<TimelineMetricMetadata>> metadataByAppId = new HashMap<>();
        for (TimelineMetricMetadata metricMetadata : metadata.values()) {
            List<TimelineMetricMetadata> metadataList = metadataByAppId.get(metricMetadata.getAppId());
            if (metadataList == null) {
                metadataList = new ArrayList<>();
                metadataByAppId.put(metricMetadata.getAppId(), metadataList);
            }

            metadataList.add(metricMetadata);
        }

        return metadataByAppId;
    }

    @Override
    public Map<String, Set<String>> getHostAppsMetadata() throws SQLException, IOException {
        return metricMetadataManager.getHostedAppsCache();
    }

    private void scheduleAggregatorThread(TimelineMetricAggregator aggregator) {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        if (!aggregator.isDisabled()) {
            executorService.scheduleAtFixedRate(aggregator,
                    0l,
                    aggregator.getSleepIntervalMillis(),
                    TimeUnit.MILLISECONDS);
        }
    }
}
