package mamba.aggregators;


import mamba.discovery.TimelineMetricMetadataKey;
import mamba.discovery.TimelineMetricMetadataManager;
import mamba.metrics.TimelineMetricMetadata;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.*;

import static mamba.store.TimelineMetricConfiguration.CLUSTER_AGGREGATOR_APP_IDS;
import static mamba.store.TimelineMetricConfiguration.HOST_APP_ID;

/**
 * Created by dongbin on 2016/10/10.
 */
public class TimelineMetricAppAggregator {

    private static final Log LOG = LogFactory.getLog(TimelineMetricAppAggregator.class);
    // Lookup to check candidacy of an app
    private final List<String> appIdsToAggregate;
    private final Map<String, Set<String>> hostedAppsMap;
    Map<TimelineClusterMetric, MetricClusterAggregate> aggregateClusterMetrics;
    TimelineMetricMetadataManager metadataManagerInstance;

    public TimelineMetricAppAggregator(TimelineMetricMetadataManager metadataManager,
                                       Configuration metricsConf) {
        appIdsToAggregate = getAppIdsForHostAggregation(metricsConf);
        hostedAppsMap = metadataManager.getHostedAppsCache();
        metadataManagerInstance = metadataManager;
        LOG.info("AppIds configured for aggregation: " + appIdsToAggregate);
    }

    /**
     * Lifecycle method to initialize aggregation cycle.
     */
    public void init() {
        LOG.debug("Initializing aggregation cycle.");
        aggregateClusterMetrics = new HashMap<TimelineClusterMetric, MetricClusterAggregate>();
    }

    /**
     * Lifecycle method to indicate end of aggregation cycle.
     */
    public void cleanup() {
        LOG.debug("Cleanup aggregated data.");
        aggregateClusterMetrics = null;
    }

    /**
     * Calculate aggregates if the clusterMetric is a Host metric for recorded
     * apps that are housed by this host.
     *
     * @param clusterMetric @TimelineClusterMetric Host / App metric
     * @param hostname      This is the hostname from which this clusterMetric originated.
     * @param metricValue   The metric value for this metric.
     */
    public void processTimelineClusterMetric(TimelineClusterMetric clusterMetric,
                                             String hostname, Double metricValue) {

        String appId = clusterMetric.getAppId();
        if (appId == null) {
            return; // No real use case except tests
        }

        // If metric is a host metric and host has apps on it
        if (appId.equalsIgnoreCase(HOST_APP_ID)) {
            // Candidate metric, update app aggregates
            if (hostedAppsMap.containsKey(hostname)) {
                updateAppAggregatesFromHostMetric(clusterMetric, hostname, metricValue);
            }
        } else {
            // Build the hostedapps map if not a host metric
            // Check app candidacy for host aggregation
            if (appIdsToAggregate.contains(appId)) {
                Set<String> appIds = hostedAppsMap.get(hostname);
                if (appIds == null) {
                    appIds = new HashSet<>();
                    hostedAppsMap.put(hostname, appIds);
                }
                if (!appIds.contains(appId)) {
                    appIds.add(appId);
                    LOG.info("Adding appId to hosted apps: appId = " +
                            clusterMetric.getAppId() + ", hostname = " + hostname);
                }
            }
        }
    }

    /**
     * Build a cluster app metric from a host metric
     */
    private void updateAppAggregatesFromHostMetric(TimelineClusterMetric clusterMetric,
                                                   String hostname, Double metricValue) {

        if (aggregateClusterMetrics == null) {
            LOG.error("Aggregation requested without init call.");
            return;
        }

        TimelineMetricMetadataKey appKey = new TimelineMetricMetadataKey(clusterMetric.getMetricName(), HOST_APP_ID);
        Set<String> apps = hostedAppsMap.get(hostname);
        for (String appId : apps) {
            if (appIdsToAggregate.contains(appId)) {

                appKey.setAppId(appId);
                TimelineMetricMetadata appMetadata = metadataManagerInstance.getMetadataCacheValue(appKey);
                if (appMetadata == null) {
                    TimelineMetricMetadataKey key = new TimelineMetricMetadataKey(clusterMetric.getMetricName(), HOST_APP_ID);
                    TimelineMetricMetadata hostMetricMetadata = metadataManagerInstance.getMetadataCacheValue(key);

                    if (hostMetricMetadata != null) {
                        TimelineMetricMetadata timelineMetricMetadata = new TimelineMetricMetadata(clusterMetric.getMetricName(),
                                appId, hostMetricMetadata.getUnits(), hostMetricMetadata.getType(), hostMetricMetadata.getSeriesStartTime(),
                                hostMetricMetadata.isSupportsAggregates());
                        metadataManagerInstance.putIfModifiedTimelineMetricMetadata(timelineMetricMetadata);
                    }
                }

                // Add a new cluster aggregate metric if none exists
                TimelineClusterMetric appTimelineClusterMetric =
                        new TimelineClusterMetric(clusterMetric.getMetricName(),
                                appId,
                                clusterMetric.getInstanceId(),
                                clusterMetric.getTimestamp(),
                                clusterMetric.getType()
                        );

                MetricClusterAggregate clusterAggregate = aggregateClusterMetrics.get(appTimelineClusterMetric);

                if (clusterAggregate == null) {
                    clusterAggregate = new MetricClusterAggregate(metricValue, 1, null, metricValue, metricValue);
                    aggregateClusterMetrics.put(appTimelineClusterMetric, clusterAggregate);
                } else {
                    clusterAggregate.updateSum(metricValue);
                    clusterAggregate.updateNumberOfHosts(1);
                    clusterAggregate.updateMax(metricValue);
                    clusterAggregate.updateMin(metricValue);
                }
            }

        }
    }

    /**
     * Return current copy of aggregated data.
     */
    public Map<TimelineClusterMetric, MetricClusterAggregate> getAggregateClusterMetrics() {
        return aggregateClusterMetrics;
    }

    private List<String> getAppIdsForHostAggregation(Configuration metricsConf) {
        String appIds = metricsConf.get(CLUSTER_AGGREGATOR_APP_IDS);
        if (!StringUtils.isEmpty(appIds)) {
            return Arrays.asList(StringUtils.stripAll(appIds.split(",")));
        }
        return Collections.emptyList();
    }
}
