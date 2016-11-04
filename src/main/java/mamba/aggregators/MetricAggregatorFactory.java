package mamba.aggregators;

import mamba.aggregators.v2.MetricClusterAggregator;
import mamba.discovery.MetricMetadataManager;
import mamba.store.PhoenixHBaseAccessor;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;

import static java.util.concurrent.TimeUnit.SECONDS;
import static mamba.query.PhoenixTransactSQL.*;
import static mamba.store.MetricConfiguration.*;

/**
 * Created by dongbin on 2016/10/10.
 */
public class MetricAggregatorFactory {

    private static final String HOST_AGGREGATE_MINUTE_CHECKPOINT_FILE =
            "timeline-metrics-host-aggregator-checkpoint";
    private static final String HOST_AGGREGATE_HOURLY_CHECKPOINT_FILE =
            "timeline-metrics-host-aggregator-hourly-checkpoint";
    private static final String HOST_AGGREGATE_DAILY_CHECKPOINT_FILE =
            "timeline-metrics-host-aggregator-daily-checkpoint";
    private static final String CLUSTER_AGGREGATOR_CHECKPOINT_FILE =
            "timeline-metrics-cluster-aggregator-checkpoint";
    private static final String CLUSTER_AGGREGATOR_MINUTE_CHECKPOINT_FILE =
            "timeline-metrics-cluster-aggregator-minute-checkpoint";
    private static final String CLUSTER_AGGREGATOR_HOURLY_CHECKPOINT_FILE =
            "timeline-metrics-cluster-aggregator-hourly-checkpoint";
    private static final String CLUSTER_AGGREGATOR_DAILY_CHECKPOINT_FILE =
            "timeline-metrics-cluster-aggregator-daily-checkpoint";

    private static boolean useGroupByAggregator(Configuration metricsConf) {
        return Boolean.parseBoolean(metricsConf.get(USE_GROUPBY_AGGREGATOR_QUERIES, "true"));
    }

    /**
     * Minute based aggregation for hosts.
     * Interval : 5 mins
     */
    public static MetricAggregator createTimelineMetricAggregatorMinute(PhoenixHBaseAccessor hBaseAccessor, Configuration metricsConf) {

        String checkpointDir = metricsConf.get(
                TIMELINE_METRICS_AGGREGATOR_CHECKPOINT_DIR, DEFAULT_CHECKPOINT_LOCATION);
        String checkpointLocation = FilenameUtils.concat(checkpointDir,
                HOST_AGGREGATE_MINUTE_CHECKPOINT_FILE);
        long sleepIntervalMillis = SECONDS.toMillis(metricsConf.getLong
                (HOST_AGGREGATOR_MINUTE_SLEEP_INTERVAL, 300l));  // 5 mins

        int checkpointCutOffMultiplier = metricsConf.getInt
                (HOST_AGGREGATOR_MINUTE_CHECKPOINT_CUTOFF_MULTIPLIER, 3);
        String hostAggregatorDisabledParam = HOST_AGGREGATOR_MINUTE_DISABLED;

        String inputTableName = METRICS_RECORD_TABLE_NAME;
        String outputTableName = METRICS_AGGREGATE_MINUTE_TABLE_NAME;

        if (useGroupByAggregator(metricsConf)) {
            return new mamba.aggregators.v2.MetricHostAggregator(
                    "MetricHostAggregatorMinute",
                    hBaseAccessor, metricsConf,
                    checkpointLocation,
                    sleepIntervalMillis,
                    checkpointCutOffMultiplier,
                    hostAggregatorDisabledParam,
                    inputTableName,
                    outputTableName,
                    120000l
            );
        }

        return new MetricHostAggregator(
                "MetricHostAggregatorMinute",
                hBaseAccessor, metricsConf,
                checkpointLocation,
                sleepIntervalMillis,
                checkpointCutOffMultiplier,
                hostAggregatorDisabledParam,
                inputTableName,
                outputTableName,
                120000l);
    }

    /**
     * Hourly aggregation for hosts.
     * Interval : 1 hour
     */
    public static MetricAggregator createTimelineMetricAggregatorHourly
    (PhoenixHBaseAccessor hBaseAccessor, Configuration metricsConf) {

        String checkpointDir = metricsConf.get(
                TIMELINE_METRICS_AGGREGATOR_CHECKPOINT_DIR, DEFAULT_CHECKPOINT_LOCATION);
        String checkpointLocation = FilenameUtils.concat(checkpointDir,
                HOST_AGGREGATE_HOURLY_CHECKPOINT_FILE);
        long sleepIntervalMillis = SECONDS.toMillis(metricsConf.getLong
                (HOST_AGGREGATOR_HOUR_SLEEP_INTERVAL, 3600l));

        int checkpointCutOffMultiplier = metricsConf.getInt
                (HOST_AGGREGATOR_HOUR_CHECKPOINT_CUTOFF_MULTIPLIER, 2);
        String hostAggregatorDisabledParam = HOST_AGGREGATOR_HOUR_DISABLED;

        String inputTableName = METRICS_AGGREGATE_MINUTE_TABLE_NAME;
        String outputTableName = METRICS_AGGREGATE_HOURLY_TABLE_NAME;

        if (useGroupByAggregator(metricsConf)) {
            return new mamba.aggregators.v2.MetricHostAggregator(
                    "MetricHostAggregatorHourly",
                    hBaseAccessor, metricsConf,
                    checkpointLocation,
                    sleepIntervalMillis,
                    checkpointCutOffMultiplier,
                    hostAggregatorDisabledParam,
                    inputTableName,
                    outputTableName,
                    3600000l
            );
        }

        return new MetricHostAggregator(
                "MetricHostAggregatorHourly",
                hBaseAccessor, metricsConf,
                checkpointLocation,
                sleepIntervalMillis,
                checkpointCutOffMultiplier,
                hostAggregatorDisabledParam,
                inputTableName,
                outputTableName,
                3600000l);
    }

    /**
     * Daily aggregation for hosts.
     * Interval : 1 day
     */
    public static MetricAggregator createTimelineMetricAggregatorDaily
    (PhoenixHBaseAccessor hBaseAccessor, Configuration metricsConf) {

        String checkpointDir = metricsConf.get(
                TIMELINE_METRICS_AGGREGATOR_CHECKPOINT_DIR, DEFAULT_CHECKPOINT_LOCATION);
        String checkpointLocation = FilenameUtils.concat(checkpointDir,
                HOST_AGGREGATE_DAILY_CHECKPOINT_FILE);
        long sleepIntervalMillis = SECONDS.toMillis(metricsConf.getLong
                (HOST_AGGREGATOR_DAILY_SLEEP_INTERVAL, 86400l));

        int checkpointCutOffMultiplier = metricsConf.getInt
                (HOST_AGGREGATOR_DAILY_CHECKPOINT_CUTOFF_MULTIPLIER, 1);
        String hostAggregatorDisabledParam = HOST_AGGREGATOR_DAILY_DISABLED;

        String inputTableName = METRICS_AGGREGATE_HOURLY_TABLE_NAME;
        String outputTableName = METRICS_AGGREGATE_DAILY_TABLE_NAME;

        if (useGroupByAggregator(metricsConf)) {
            return new mamba.aggregators.v2.MetricHostAggregator(
                    "MetricHostAggregatorDaily",
                    hBaseAccessor, metricsConf,
                    checkpointLocation,
                    sleepIntervalMillis,
                    checkpointCutOffMultiplier,
                    hostAggregatorDisabledParam,
                    inputTableName,
                    outputTableName,
                    3600000l
            );
        }

        return new MetricHostAggregator(
                "MetricHostAggregatorDaily",
                hBaseAccessor, metricsConf,
                checkpointLocation,
                sleepIntervalMillis,
                checkpointCutOffMultiplier,
                hostAggregatorDisabledParam,
                inputTableName,
                outputTableName,
                3600000l);
    }

    /**
     * Second aggregation for cluster.
     * Interval : 2 mins
     * Timeslice : 30 sec
     */
    public static MetricAggregator createTimelineClusterAggregatorSecond(
            PhoenixHBaseAccessor hBaseAccessor, Configuration metricsConf,
            MetricMetadataManager metadataManager) {

        String checkpointDir = metricsConf.get(
                TIMELINE_METRICS_AGGREGATOR_CHECKPOINT_DIR, DEFAULT_CHECKPOINT_LOCATION);

        String checkpointLocation = FilenameUtils.concat(checkpointDir,
                CLUSTER_AGGREGATOR_CHECKPOINT_FILE);

        long sleepIntervalMillis = SECONDS.toMillis(metricsConf.getLong
                (CLUSTER_AGGREGATOR_SECOND_SLEEP_INTERVAL, 120l));

        long timeSliceIntervalMillis = SECONDS.toMillis(metricsConf.getInt
                (CLUSTER_AGGREGATOR_TIMESLICE_INTERVAL, 30));

        int checkpointCutOffMultiplier =
                metricsConf.getInt(CLUSTER_AGGREGATOR_SECOND_CHECKPOINT_CUTOFF_MULTIPLIER, 2);

        String inputTableName = METRICS_RECORD_TABLE_NAME;
        String outputTableName = METRICS_CLUSTER_AGGREGATE_TABLE_NAME;
        String aggregatorDisabledParam = CLUSTER_AGGREGATOR_SECOND_DISABLED;

        // Second based aggregation have added responsibility of time slicing
        return new MetricClusterAggregatorSecond(
                "ClusterAggregatorSecond",
                metadataManager,
                hBaseAccessor, metricsConf,
                checkpointLocation,
                sleepIntervalMillis,
                checkpointCutOffMultiplier,
                aggregatorDisabledParam,
                inputTableName,
                outputTableName,
                120000l,
                timeSliceIntervalMillis
        );
    }

    /**
     * Minute aggregation for cluster.
     * Interval : 5 mins
     */
    public static MetricAggregator createTimelineClusterAggregatorMinute(
            PhoenixHBaseAccessor hBaseAccessor, Configuration metricsConf) {

        String checkpointDir = metricsConf.get(
                TIMELINE_METRICS_AGGREGATOR_CHECKPOINT_DIR, DEFAULT_CHECKPOINT_LOCATION);

        String checkpointLocation = FilenameUtils.concat(checkpointDir,
                CLUSTER_AGGREGATOR_MINUTE_CHECKPOINT_FILE);

        long sleepIntervalMillis = SECONDS.toMillis(metricsConf.getLong
                (CLUSTER_AGGREGATOR_MINUTE_SLEEP_INTERVAL, 300l));

        int checkpointCutOffMultiplier = metricsConf.getInt
                (CLUSTER_AGGREGATOR_MINUTE_CHECKPOINT_CUTOFF_MULTIPLIER, 2);

        String inputTableName = METRICS_CLUSTER_AGGREGATE_TABLE_NAME;
        String outputTableName = METRICS_CLUSTER_AGGREGATE_MINUTE_TABLE_NAME;
        String aggregatorDisabledParam = CLUSTER_AGGREGATOR_MINUTE_DISABLED;

        if (useGroupByAggregator(metricsConf)) {
            return new MetricClusterAggregator(
                    "ClusterAggregatorMinute",
                    hBaseAccessor, metricsConf,
                    checkpointLocation,
                    sleepIntervalMillis,
                    checkpointCutOffMultiplier,
                    aggregatorDisabledParam,
                    inputTableName,
                    outputTableName,
                    120000l
            );
        }

        return new mamba.aggregators.MetricClusterAggregator(
                "ClusterAggregatorMinute",
                hBaseAccessor, metricsConf,
                checkpointLocation,
                sleepIntervalMillis,
                checkpointCutOffMultiplier,
                aggregatorDisabledParam,
                inputTableName,
                outputTableName,
                120000l
        );
    }

    /**
     * Hourly aggregation for cluster.
     * Interval : 1 hour
     */
    public static MetricAggregator createTimelineClusterAggregatorHourly(
            PhoenixHBaseAccessor hBaseAccessor, Configuration metricsConf) {

        String checkpointDir = metricsConf.get(
                TIMELINE_METRICS_AGGREGATOR_CHECKPOINT_DIR, DEFAULT_CHECKPOINT_LOCATION);

        String checkpointLocation = FilenameUtils.concat(checkpointDir,
                CLUSTER_AGGREGATOR_HOURLY_CHECKPOINT_FILE);

        long sleepIntervalMillis = SECONDS.toMillis(metricsConf.getLong
                (CLUSTER_AGGREGATOR_HOUR_SLEEP_INTERVAL, 3600l));

        int checkpointCutOffMultiplier = metricsConf.getInt
                (CLUSTER_AGGREGATOR_HOUR_CHECKPOINT_CUTOFF_MULTIPLIER, 2);

        String inputTableName = METRICS_CLUSTER_AGGREGATE_TABLE_NAME;
        String outputTableName = METRICS_CLUSTER_AGGREGATE_HOURLY_TABLE_NAME;
        String aggregatorDisabledParam = CLUSTER_AGGREGATOR_HOUR_DISABLED;

        if (useGroupByAggregator(metricsConf)) {
            return new MetricClusterAggregator(
                    "ClusterAggregatorHourly",
                    hBaseAccessor, metricsConf,
                    checkpointLocation,
                    sleepIntervalMillis,
                    checkpointCutOffMultiplier,
                    aggregatorDisabledParam,
                    inputTableName,
                    outputTableName,
                    120000l
            );
        }

        return new mamba.aggregators.MetricClusterAggregator(
                "ClusterAggregatorHourly",
                hBaseAccessor, metricsConf,
                checkpointLocation,
                sleepIntervalMillis,
                checkpointCutOffMultiplier,
                aggregatorDisabledParam,
                inputTableName,
                outputTableName,
                120000l
        );
    }

    /**
     * Daily aggregation for cluster.
     * Interval : 1 day
     */
    public static MetricAggregator createTimelineClusterAggregatorDaily(
            PhoenixHBaseAccessor hBaseAccessor, Configuration metricsConf) {

        String checkpointDir = metricsConf.get(
                TIMELINE_METRICS_AGGREGATOR_CHECKPOINT_DIR, DEFAULT_CHECKPOINT_LOCATION);

        String checkpointLocation = FilenameUtils.concat(checkpointDir,
                CLUSTER_AGGREGATOR_DAILY_CHECKPOINT_FILE);

        long sleepIntervalMillis = SECONDS.toMillis(metricsConf.getLong
                (CLUSTER_AGGREGATOR_DAILY_SLEEP_INTERVAL, 86400l));

        int checkpointCutOffMultiplier = metricsConf.getInt
                (CLUSTER_AGGREGATOR_DAILY_CHECKPOINT_CUTOFF_MULTIPLIER, 1);

        String inputTableName = METRICS_CLUSTER_AGGREGATE_HOURLY_TABLE_NAME;
        String outputTableName = METRICS_CLUSTER_AGGREGATE_DAILY_TABLE_NAME;
        String aggregatorDisabledParam = CLUSTER_AGGREGATOR_DAILY_DISABLED;

        if (useGroupByAggregator(metricsConf)) {
            return new MetricClusterAggregator(
                    "ClusterAggregatorDaily",
                    hBaseAccessor, metricsConf,
                    checkpointLocation,
                    sleepIntervalMillis,
                    checkpointCutOffMultiplier,
                    aggregatorDisabledParam,
                    inputTableName,
                    outputTableName,
                    120000l
            );
        }

        return new mamba.aggregators.MetricClusterAggregator(
                "ClusterAggregatorDaily",
                hBaseAccessor, metricsConf,
                checkpointLocation,
                sleepIntervalMillis,
                checkpointCutOffMultiplier,
                aggregatorDisabledParam,
                inputTableName,
                outputTableName,
                120000l
        );
    }
}
