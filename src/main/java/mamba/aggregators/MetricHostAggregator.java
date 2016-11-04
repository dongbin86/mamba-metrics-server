package mamba.aggregators;


import mamba.metrics.Metric;
import mamba.query.Condition;
import mamba.query.DefaultCondition;
import mamba.store.PhoenixHBaseAccessor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static mamba.query.PhoenixTransactSQL.GET_METRIC_AGGREGATE_ONLY_SQL;

/**
 * Created by dongbin on 2016/10/10.
 */
public class MetricHostAggregator extends AbstractAggregator {
    private static final Log LOG = LogFactory.getLog(MetricHostAggregator.class);
    MetricReadHelper readHelper = new MetricReadHelper(false);

    public MetricHostAggregator(String aggregatorName,
                                PhoenixHBaseAccessor hBaseAccessor,
                                Configuration metricsConf,
                                String checkpointLocation,
                                Long sleepIntervalMillis,
                                Integer checkpointCutOffMultiplier,
                                String hostAggregatorDisabledParam,
                                String tableName,
                                String outputTableName,
                                Long nativeTimeRangeDelay) {
        super(aggregatorName, hBaseAccessor, metricsConf, checkpointLocation,
                sleepIntervalMillis, checkpointCutOffMultiplier, hostAggregatorDisabledParam,
                tableName, outputTableName, nativeTimeRangeDelay);
    }

    @Override
    protected void aggregate(ResultSet rs, long startTime, long endTime) throws IOException, SQLException {

        Map<Metric, MetricHostAggregate> hostAggregateMap = aggregateMetricsFromResultSet(rs, endTime);

        LOG.info("Saving " + hostAggregateMap.size() + " metric aggregates.");
        hBaseAccessor.saveHostAggregateRecords(hostAggregateMap, outputTableName);
    }

    @Override
    protected Condition prepareMetricQueryCondition(long startTime, long endTime) {
        Condition condition = new DefaultCondition(null, null, null, null, startTime,
                endTime, null, null, true);
        condition.setNoLimit();
        condition.setFetchSize(resultsetFetchSize);
        condition.setStatement(String.format(GET_METRIC_AGGREGATE_ONLY_SQL,
                getQueryHint(startTime), tableName));
        // Retaining order of the row-key avoids client side merge sort.
        condition.addOrderByColumn("METRIC_NAME");
        condition.addOrderByColumn("HOSTNAME");
        condition.addOrderByColumn("SERVER_TIME");
        condition.addOrderByColumn("APP_ID");
        condition.addOrderByColumn("INSTANCE_ID");
        return condition;
    }

    private Map<Metric, MetricHostAggregate> aggregateMetricsFromResultSet(ResultSet rs, long endTime)
            throws IOException, SQLException {
        Metric existingMetric = null;
        MetricHostAggregate hostAggregate = null;
        Map<Metric, MetricHostAggregate> hostAggregateMap = new HashMap<Metric, MetricHostAggregate>();


        while (rs.next()) {
            Metric currentMetric =
                    readHelper.getTimelineMetricKeyFromResultSet(rs);
            MetricHostAggregate currentHostAggregate =
                    readHelper.getMetricHostAggregateFromResultSet(rs);

            if (existingMetric == null) {
                // First row
                existingMetric = currentMetric;
                currentMetric.setTimestamp(endTime);
                hostAggregate = new MetricHostAggregate();
                hostAggregateMap.put(currentMetric, hostAggregate);
            }

            if (existingMetric.equalsExceptTime(currentMetric)) {
                // Recalculate totals with current metric
                hostAggregate.updateAggregates(currentHostAggregate);
            } else {
                // Switched over to a new metric - save existing - create new aggregate
                currentMetric.setTimestamp(endTime);
                hostAggregate = new MetricHostAggregate();
                hostAggregate.updateAggregates(currentHostAggregate);
                hostAggregateMap.put(currentMetric, hostAggregate);
                existingMetric = currentMetric;
            }
        }
        return hostAggregateMap;
    }


}
