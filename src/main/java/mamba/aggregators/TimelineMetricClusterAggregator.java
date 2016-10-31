package mamba.aggregators;


import mamba.query.Condition;
import mamba.query.DefaultCondition;
import mamba.store.PhoenixHBaseAccessor;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static mamba.query.PhoenixTransactSQL.*;

/**
 * Created by dongbin on 2016/10/10.
 */
public class TimelineMetricClusterAggregator extends AbstractTimelineAggregator {
    private final TimelineMetricReadHelper readHelper = new TimelineMetricReadHelper(true);
    private final boolean isClusterPrecisionInputTable;

    public TimelineMetricClusterAggregator(String aggregatorName,
                                           PhoenixHBaseAccessor hBaseAccessor,
                                           Configuration metricsConf,
                                           String checkpointLocation,
                                           Long sleepIntervalMillis,
                                           Integer checkpointCutOffMultiplier,
                                           String hostAggregatorDisabledParam,
                                           String inputTableName,
                                           String outputTableName,
                                           Long nativeTimeRangeDelay) {
        super(aggregatorName, hBaseAccessor, metricsConf, checkpointLocation,
                sleepIntervalMillis, checkpointCutOffMultiplier,
                hostAggregatorDisabledParam, inputTableName, outputTableName,
                nativeTimeRangeDelay);
        isClusterPrecisionInputTable = inputTableName.equals(METRICS_CLUSTER_AGGREGATE_TABLE_NAME);
    }

    @Override
    protected Condition prepareMetricQueryCondition(long startTime, long endTime) {
        Condition condition = new DefaultCondition(null, null, null, null, startTime,
                endTime, null, null, true);
        condition.setNoLimit();
        condition.setFetchSize(resultsetFetchSize);
        String sqlStr = String.format(GET_CLUSTER_AGGREGATE_TIME_SQL, getQueryHint(startTime), tableName);
        // HOST_COUNT vs METRIC_COUNT
        if (isClusterPrecisionInputTable) {
            sqlStr = String.format(GET_CLUSTER_AGGREGATE_SQL, getQueryHint(startTime), tableName);
        }

        condition.setStatement(sqlStr);
        condition.addOrderByColumn("METRIC_NAME");
        condition.addOrderByColumn("APP_ID");
        condition.addOrderByColumn("INSTANCE_ID");
        condition.addOrderByColumn("SERVER_TIME");
        return condition;
    }

    @Override
    protected void aggregate(ResultSet rs, long startTime, long endTime) throws IOException, SQLException {
        Map<TimelineClusterMetric, MetricHostAggregate> hostAggregateMap = aggregateMetricsFromResultSet(rs, endTime);

        LOG.info("Saving " + hostAggregateMap.size() + " metric aggregates.");
        hBaseAccessor.saveClusterTimeAggregateRecords(hostAggregateMap, outputTableName);
    }

    private Map<TimelineClusterMetric, MetricHostAggregate> aggregateMetricsFromResultSet(ResultSet rs, long endTime)
            throws IOException, SQLException {

        TimelineClusterMetric existingMetric = null;
        MetricHostAggregate hostAggregate = null;
        Map<TimelineClusterMetric, MetricHostAggregate> hostAggregateMap =
                new HashMap<TimelineClusterMetric, MetricHostAggregate>();

        while (rs.next()) {
            TimelineClusterMetric currentMetric = readHelper.fromResultSet(rs);

            MetricClusterAggregate currentHostAggregate =
                    isClusterPrecisionInputTable ?
                            readHelper.getMetricClusterAggregateFromResultSet(rs) :
                            readHelper.getMetricClusterTimeAggregateFromResultSet(rs);

            if (existingMetric == null) {
                // First row
                existingMetric = currentMetric;
                currentMetric.setTimestamp(endTime);
                hostAggregate = new MetricHostAggregate();
                hostAggregateMap.put(currentMetric, hostAggregate);
            }

            if (existingMetric.equalsExceptTime(currentMetric)) {
                // Recalculate totals with current metric
                updateAggregatesFromHost(hostAggregate, currentHostAggregate);

            } else {
                // Switched over to a new metric - save existing
                hostAggregate = new MetricHostAggregate();
                currentMetric.setTimestamp(endTime);
                updateAggregatesFromHost(hostAggregate, currentHostAggregate);
                hostAggregateMap.put(currentMetric, hostAggregate);
                existingMetric = currentMetric;
            }

        }

        return hostAggregateMap;
    }

    private void updateAggregatesFromHost(MetricHostAggregate agg, MetricClusterAggregate currentClusterAggregate) {
        agg.updateMax(currentClusterAggregate.getMax());
        agg.updateMin(currentClusterAggregate.getMin());
        agg.updateSum(currentClusterAggregate.getSum());
        agg.updateNumberOfSamples(currentClusterAggregate.getNumberOfHosts());
    }
}
