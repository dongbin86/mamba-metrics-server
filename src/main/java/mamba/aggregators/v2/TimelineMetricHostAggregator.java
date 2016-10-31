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

import static mamba.query.PhoenixTransactSQL.GET_AGGREGATED_HOST_METRIC_GROUPBY_SQL;

/**
 * Created by dongbin on 2016/10/10.
 */
public class TimelineMetricHostAggregator extends AbstractTimelineAggregator {

    public TimelineMetricHostAggregator(String aggregatorName,
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

        LOG.info("Aggregated host metrics for " + outputTableName +
                ", with startTime = " + new Date(startTime) +
                ", endTime = " + new Date(endTime));
    }

    @Override
    protected Condition prepareMetricQueryCondition(long startTime, long endTime) {
        EmptyCondition condition = new EmptyCondition();
        condition.setDoUpdate(true);

        condition.setStatement(String.format(GET_AGGREGATED_HOST_METRIC_GROUPBY_SQL,
                getQueryHint(startTime), outputTableName, endTime, tableName, startTime, endTime));

        if (LOG.isDebugEnabled()) {
            LOG.debug("Condition: " + condition.toString());
        }

        return condition;
    }
}