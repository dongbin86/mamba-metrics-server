package mamba.aggregators;

import mamba.query.Condition;
import mamba.query.PhoenixTransactSQL;
import mamba.store.MetricConfiguration;
import mamba.store.PhoenixHBaseAccessor;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Created by dongbin on 2016/10/10.
 */
public abstract class AbstractAggregator implements MetricAggregator {

    protected final PhoenixHBaseAccessor hBaseAccessor;
    protected final Logger LOG;
    protected final long checkpointDelayMillis;
    protected final Integer resultsetFetchSize;
    // Explicitly name aggregators for logging needs
    private final String aggregatorName;
    protected Configuration metricsConf;
    protected String tableName;
    protected String outputTableName;
    protected Long nativeTimeRangeDelay;
    private String checkpointLocation;
    private Long sleepIntervalMillis;
    private Integer checkpointCutOffMultiplier;
    private String aggregatorDisableParam;

    AbstractAggregator(String aggregatorName,
                       PhoenixHBaseAccessor hBaseAccessor,
                       Configuration metricsConf) {
        this.aggregatorName = aggregatorName;
        this.hBaseAccessor = hBaseAccessor;
        this.metricsConf = metricsConf;
        this.checkpointDelayMillis = SECONDS.toMillis(metricsConf.getInt(MetricConfiguration.AGGREGATOR_CHECKPOINT_DELAY, 120));
        this.resultsetFetchSize = metricsConf.getInt(MetricConfiguration.RESULTSET_FETCH_SIZE, 2000);
        this.LOG = LoggerFactory.getLogger(aggregatorName);
    }

    public AbstractAggregator(String aggregatorName,
                              PhoenixHBaseAccessor hBaseAccessor,
                              Configuration metricsConf,
                              String checkpointLocation,
                              Long sleepIntervalMillis,
                              Integer checkpointCutOffMultiplier,
                              String aggregatorDisableParam,
                              String tableName,
                              String outputTableName,
                              Long nativeTimeRangeDelay) {
        this(aggregatorName, hBaseAccessor, metricsConf);
        this.checkpointLocation = checkpointLocation;
        this.sleepIntervalMillis = sleepIntervalMillis;
        this.checkpointCutOffMultiplier = checkpointCutOffMultiplier;
        this.aggregatorDisableParam = aggregatorDisableParam;
        this.tableName = tableName;
        this.outputTableName = outputTableName;
        this.nativeTimeRangeDelay = nativeTimeRangeDelay;
    }
    /**
     * @referenceTime 上一次的checkpoint时间
     * @aggregatorPeriod checkpoint周期
     * */
    public static long getRoundedCheckPointTimeMillis(long referenceTime, long aggregatorPeriod) {
        return referenceTime - (referenceTime % aggregatorPeriod);
    }

    public static long getRoundedAggregateTimeMillis(long aggregatorPeriod) {
        long currentTime = System.currentTimeMillis();
        return currentTime - (currentTime % aggregatorPeriod);
    }

    /**
     * 假设现成调度周期是100s，那么这里得到的时间相当月当前时间戳去掉后两位的零头
     */

    @Override
    public void run() {
        LOG.info("Started Metrics aggregator thread @ " + new Date());
        Long SLEEP_INTERVAL = getSleepIntervalMillis();
        runOnce(SLEEP_INTERVAL);
    }

    /**
     * Access relaxed for tests
     */
    public void runOnce(Long SLEEP_INTERVAL) {

        long currentTime = System.currentTimeMillis();
        long lastCheckPointTime = readLastCheckpointSavingOnFirstRun(currentTime);

        if (lastCheckPointTime != -1) {
            LOG.info("Last check point time: " + lastCheckPointTime + ", lagBy: "
                    + ((currentTime - lastCheckPointTime) / 1000)
                    + " seconds.");

            boolean success = doWork(lastCheckPointTime, lastCheckPointTime + SLEEP_INTERVAL);

            if (success) {
                try {
                    saveCheckPoint(lastCheckPointTime + SLEEP_INTERVAL);
                } catch (IOException io) {
                    LOG.warn("Error saving checkpoint, restarting aggregation at " +
                            "previous checkpoint.");
                }
            }
        }/**虽然是根据周期调度，但是这里要通过checkpoint时间，决定要不要做聚合，因为太频繁的聚合没有意义，比如分钟级别的聚合，20秒又过来调度了
         要不要做聚合，其实不需要

         那么聚合的时候呢，还需要有一个起始时间和终止时间，还要解决两次聚合操作之间重复聚合某一段数据的问题


         那么这里呢，lastCheckPointTime就是要聚合的数据的开始时间，聚合多久的数据呢，就是这个调度周期，如果没问题的话，下次聚合时间就是
         lastCheckPointTime + SLEEP_INTERVAL

         doWork的过程呢，基本上就是拼装sql然后，读出来结果，把一段时间内的数据全部捞进来，然后聚合，聚合玩完之后全部写入到聚合表里，这样相当于
         离线异步完成聚合，那么查询的效率就会大大提升。
         */
    }

    private long readLastCheckpointSavingOnFirstRun(long currentTime) {
        long lastCheckPointTime = -1;

        try {
            lastCheckPointTime = readCheckPoint();
            /**
             * 1.checkpoint目录保存的是上一次做checkpoint的时间信息，时间戳，解析出来这个上次的时间戳
             * */
            if (lastCheckPointTime != -1) {
                LOG.info("Last Checkpoint read : " + new Date(lastCheckPointTime));
                if (isLastCheckPointTooOld(currentTime, lastCheckPointTime)) {
                    LOG.warn("Last Checkpoint is too old, discarding last checkpoint. " +
                            "lastCheckPointTime = " + new Date(lastCheckPointTime));
                    lastCheckPointTime = getRoundedAggregateTimeMillis(getSleepIntervalMillis()) - getSleepIntervalMillis();

                    /**得到一个接近理论上上一个周期的checkpoint时间*/
                    LOG.info("Saving checkpoint time. " + new Date((lastCheckPointTime)));
                    saveCheckPoint(lastCheckPointTime);

                } else {

                    if (lastCheckPointTime > 0) {
                        lastCheckPointTime = getRoundedCheckPointTimeMillis(lastCheckPointTime, getSleepIntervalMillis());
                        LOG.info("Rounded off checkpoint : " + new Date(lastCheckPointTime));
                    }/**根据当前的checkpoint周期，重启判断上次的checkpoint时间是否合理，如果距离上次时间太短了，那么这一轮就不做checkpoint*/

                    if (isLastCheckPointTooYoung(lastCheckPointTime)) {
                        LOG.info("Last checkpoint too recent for aggregation. Sleeping for 1 cycle.");
                        return -1; //Skip Aggregation this time around
                    }
                }
            } else {
        /*
          No checkpoint. Save current rounded checkpoint and sleep for 1 cycle.
         */
                LOG.info("No checkpoint found");
                long firstCheckPoint = getRoundedAggregateTimeMillis(getSleepIntervalMillis());
                LOG.info("Saving checkpoint time. " + new Date((firstCheckPoint)));
                saveCheckPoint(firstCheckPoint);
            }
        } catch (IOException io) {
            LOG.warn("Unable to write last checkpoint time. Resuming sleep.", io);
        }
        return lastCheckPointTime;
    }

    private boolean isLastCheckPointTooOld(long currentTime, long checkpoint) {
        // first checkpoint is saved checkpointDelayMillis in the past,
        // so here we also need to take it into account
        return checkpoint != -1 &&
                ((currentTime - checkpoint) > getCheckpointCutOffIntervalMillis());
    }

    private boolean isLastCheckPointTooYoung(long checkpoint) {
        return checkpoint != -1 &&
                ((getRoundedAggregateTimeMillis(getSleepIntervalMillis()) <= checkpoint));
    }

    protected long readCheckPoint() {
        try {
            File checkpoint = new File(getCheckpointLocation());
            if (checkpoint.exists()) {
                String contents = FileUtils.readFileToString(checkpoint);
                if (contents != null && !contents.isEmpty()) {
                    return Long.parseLong(contents);
                }
            }
        } catch (IOException io) {
            LOG.debug("", io);
        }
        return -1;
    }

    protected void saveCheckPoint(long checkpointTime) throws IOException {
        File checkpoint = new File(getCheckpointLocation());
        if (!checkpoint.exists()) {
            boolean done = checkpoint.createNewFile();
            if (!done) {
                throw new IOException("Could not create checkpoint at location, " +
                        getCheckpointLocation());
            }
        }
        FileUtils.writeStringToFile(checkpoint, String.valueOf(checkpointTime));
    }

    /**
     * Read metrics written during the time interval and save the sum and total
     * in the aggregate table.
     *
     * @param startTime Sample start time
     * @param endTime   Sample end time
     */
    public boolean doWork(long startTime, long endTime) {
        LOG.info("Start aggregation cycle @ " + new Date() + ", " +
                "startTime = " + new Date(startTime) + ", endTime = " + new Date(endTime));

        boolean success = true;
        Condition condition = prepareMetricQueryCondition(startTime, endTime);

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            conn = hBaseAccessor.getConnection();
            // FLUME 2. aggregate and ignore the instance
            stmt = PhoenixTransactSQL.prepareGetMetricsSqlStmt(conn, condition);

            LOG.debug("Query issued @: " + new Date());
            if (condition.doUpdate()) {
                int rows = stmt.executeUpdate();
                conn.commit();
                LOG.info(rows + " row(s) updated.");
            } else {
                rs = stmt.executeQuery();
            }
            LOG.debug("Query returned @: " + new Date());

            aggregate(rs, startTime, endTime);
            LOG.info("End aggregation cycle @ " + new Date());

        } catch (SQLException | IOException e) {
            LOG.error("Exception during aggregating metrics.", e);
            success = false;
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    // Ignore
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    // Ignore
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException sql) {
                    // Ignore
                }
            }
        }

        LOG.info("End aggregation cycle @ " + new Date());
        return success;
    }

    protected abstract Condition prepareMetricQueryCondition(long startTime, long endTime);

    protected abstract void aggregate(ResultSet rs, long startTime, long endTime) throws IOException, SQLException;

    public Long getSleepIntervalMillis() {
        return sleepIntervalMillis;
    }

    public void setSleepIntervalMillis(Long sleepIntervalMillis) {
        this.sleepIntervalMillis = sleepIntervalMillis;
    }

    protected Integer getCheckpointCutOffMultiplier() {
        return checkpointCutOffMultiplier;
    }

    protected Long getCheckpointCutOffIntervalMillis() {
        return getCheckpointCutOffMultiplier() * getSleepIntervalMillis();
    }

    public boolean isDisabled() {
        return metricsConf.getBoolean(aggregatorDisableParam, false);
    }

    protected String getQueryHint(Long startTime) {
        StringBuilder sb = new StringBuilder();
        sb.append("/*+ ");
        sb.append("NATIVE_TIME_RANGE(");
        sb.append(startTime - nativeTimeRangeDelay);
        sb.append(") ");
        if (hBaseAccessor.isSkipBlockCacheForAggregatorsEnabled()) {
            sb.append("NO_CACHE ");
        }
        sb.append("*/");
        return sb.toString();
    }

    protected String getCheckpointLocation() {
        return checkpointLocation;
    }


    public static void main(String[] args) {
        long aggregatorPeriod = 100L;

        long currentTime = System.currentTimeMillis();
        System.out.println(currentTime);

        System.out.println(currentTime % aggregatorPeriod);
        System.out.println(currentTime - (currentTime % aggregatorPeriod));
    }
}
