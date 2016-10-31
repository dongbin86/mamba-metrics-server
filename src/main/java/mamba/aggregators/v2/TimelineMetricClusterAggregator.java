package mamba.aggregators.v2;


import mamba.aggregators.AbstractTimelineAggregator;
import mamba.query.Condition;
import mamba.query.EmptyCondition;
import mamba.store.PhoenixHBaseAccessor;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import static mamba.query.PhoenixTransactSQL.GET_AGGREGATED_APP_METRIC_GROUPBY_SQL;
import static mamba.query.PhoenixTransactSQL.METRICS_CLUSTER_AGGREGATE_TABLE_NAME;

/**
 * Created by dongbin on 2016/10/10.
 */
public class TimelineMetricClusterAggregator extends AbstractTimelineAggregator {
    private final String aggregateColumnName;

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

        if (inputTableName.equals(METRICS_CLUSTER_AGGREGATE_TABLE_NAME)) {
            aggregateColumnName = "HOSTS_COUNT";
        } else {
            aggregateColumnName = "METRIC_COUNT";
        }
    }

    @Override
    protected Condition prepareMetricQueryCondition(long startTime, long endTime) {
        EmptyCondition condition = new EmptyCondition();
        condition.setDoUpdate(true);

    /*
    UPSERT INTO METRIC_AGGREGATE_HOURLY (METRIC_NAME, APP_ID, INSTANCE_ID,
    SERVER_TIME, UNITS, METRIC_SUM, METRIC_COUNT, METRIC_MAX, METRIC_MIN)
    SELECT METRIC_NAME, APP_ID, INSTANCE_ID, MAX(SERVER_TIME), UNITS,
    SUM(METRIC_SUM), SUM(HOSTS_COUNT), MAX(METRIC_MAX), MIN(METRIC_MIN)
    FROM METRIC_AGGREGATE WHERE SERVER_TIME >= 1441155600000 AND
    SERVER_TIME < 1441159200000 GROUP BY METRIC_NAME, APP_ID, INSTANCE_ID, UNITS;
     */

        condition.setStatement(String.format(GET_AGGREGATED_APP_METRIC_GROUPBY_SQL,
                getQueryHint(startTime), outputTableName, endTime, aggregateColumnName, tableName,
                startTime, endTime));

        if (LOG.isDebugEnabled()) {
            LOG.debug("Condition: " + condition.toString());
        }

        return condition;
    }

    @Override
    protected void aggregate(ResultSet rs, long startTime, long endTime) throws IOException, SQLException {
        LOG.info("Aggregated cluster metrics for " + outputTableName +
                ", with startTime = " + new Date(startTime) +
                ", endTime = " + new Date(endTime));
    }
}
