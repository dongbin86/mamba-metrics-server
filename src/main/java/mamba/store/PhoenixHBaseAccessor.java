package mamba.store;

import com.google.common.collect.Multimap;
import mamba.aggregators.*;
import mamba.discovery.MetricMetadataKey;
import mamba.discovery.MetricMetadataManager;
import mamba.metrics.*;
import mamba.query.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.phoenix.exception.PhoenixIOException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.SECONDS;
import static mamba.query.PhoenixTransactSQL.*;
import static mamba.store.MetricConfiguration.*;

/**
 * Created by dongbin on 2016/10/10.
 */
public class PhoenixHBaseAccessor {

    static final int PHOENIX_MAX_MUTATION_STATE_SIZE = 50000;
    // Default stale data allowance set to 3 minutes, 2 minutes more than time
    // it was collected. Also 2 minutes is the default aggregation interval at
    // cluster and host levels.
    static final long DEFAULT_OUT_OF_BAND_TIME_ALLOWANCE = 300000;
    static final String HSTORE_COMPACTION_CLASS_KEY =
            "hbase.hstore.defaultengine.compactionpolicy.class";
    static final String FIFO_COMPACTION_POLICY_CLASS =
            "org.apache.hadoop.hbase.regionserver.compactions.FIFOCompactionPolicy";
    static final String DEFAULT_COMPACTION_POLICY_CLASS =
            "org.apache.hadoop.hbase.regionserver.compactions.ExploringCompactionPolicy";
    static final String BLOCKING_STORE_FILES_KEY =
            "hbase.hstore.blockingStoreFiles";
    private static final Log LOG = LogFactory.getLog(PhoenixHBaseAccessor.class);
    /**
     * 22 metrics for 2hours in SECONDS (10 second data)
     * => Reasonable upper bound on the limit such that our Precision calculation for a given time range makes sense.
     */
    private static final int METRICS_PER_MINUTE = 22;
    private static final int POINTS_PER_MINUTE = 6;
    private static final MetricReadHelper TIMELINE_METRIC_READ_HELPER = new MetricReadHelper();
    public static int RESULTSET_LIMIT = (int) TimeUnit.HOURS.toMinutes(2) * METRICS_PER_MINUTE * POINTS_PER_MINUTE;
    private static ObjectMapper mapper = new ObjectMapper();
    private static TypeReference<TreeMap<Long, Double>> metricValuesTypeRef = new TypeReference<TreeMap<Long, Double>>() {
    };
    private final Configuration hbaseConf;
    private final Configuration metricsConf;
    private final RetryCounterFactory retryCounterFactory;
    private final PhoenixConnectionProvider dataSource;
    private final long outOfBandTimeAllowance;
    private final int cacheSize;
    private final boolean cacheEnabled;
    private final BlockingQueue<Metrics> insertCache;
    private final int cacheCommitInterval;
    private final boolean skipBlockCacheForAggregatorsEnabled;
    private final String timelineMetricsTablesDurability;
    private final String timelineMetricsPrecisionTableDurability;
    private ScheduledExecutorService scheduledExecutorService;
    private MetricsCacheCommitterThread metricsCommiterThread;
    private HashMap<String, String> tableTTL = new HashMap<>();

    public PhoenixHBaseAccessor(Configuration hbaseConf,
                                Configuration metricsConf) {
        this(hbaseConf, metricsConf, new DefaultPhoenixDataSource(hbaseConf));
    }

    PhoenixHBaseAccessor(Configuration hbaseConf,
                         Configuration metricsConf,
                         PhoenixConnectionProvider dataSource) {
        this.hbaseConf = hbaseConf;
        this.metricsConf = metricsConf;
        RESULTSET_LIMIT = metricsConf.getInt(GLOBAL_RESULT_LIMIT, RESULTSET_LIMIT);
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        } catch (ClassNotFoundException e) {
            LOG.error("Phoenix client jar not found in the classpath.", e);
            throw new IllegalStateException(e);
        }
        this.dataSource = dataSource;
        this.retryCounterFactory = new RetryCounterFactory(metricsConf.getInt(GLOBAL_MAX_RETRIES, 10),
                (int) SECONDS.toMillis(metricsConf.getInt(GLOBAL_RETRY_INTERVAL, 3)));
        this.outOfBandTimeAllowance = metricsConf.getLong(OUT_OFF_BAND_DATA_TIME_ALLOWANCE,
                DEFAULT_OUT_OF_BAND_TIME_ALLOWANCE);
        this.cacheEnabled = Boolean.valueOf(metricsConf.get(TIMELINE_METRICS_CACHE_ENABLED, "true"));
        this.cacheSize = Integer.valueOf(metricsConf.get(TIMELINE_METRICS_CACHE_SIZE, "150"));
        this.cacheCommitInterval = Integer.valueOf(metricsConf.get(TIMELINE_METRICS_CACHE_COMMIT_INTERVAL, "3"));
        this.insertCache = new ArrayBlockingQueue<Metrics>(cacheSize);/**线程安全的容器*/
        this.skipBlockCacheForAggregatorsEnabled = metricsConf.getBoolean(AGGREGATORS_SKIP_BLOCK_CACHE, false);
        this.timelineMetricsTablesDurability = metricsConf.get(TIMELINE_METRICS_AGGREGATE_TABLES_DURABILITY, "");
        this.timelineMetricsPrecisionTableDurability = metricsConf.get(TIMELINE_METRICS_PRECISION_TABLE_DURABILITY, "");

        tableTTL.put(METRICS_RECORD_TABLE_NAME, metricsConf.get(PRECISION_TABLE_TTL, String.valueOf(1 * 86400)));  // 1 day
        tableTTL.put(CONTAINER_METRICS_TABLE_NAME, metricsConf.get(CONTAINER_METRICS_TTL, String.valueOf(30 * 86400)));  // 30 days
        tableTTL.put(METRICS_AGGREGATE_MINUTE_TABLE_NAME, metricsConf.get(HOST_MINUTE_TABLE_TTL, String.valueOf(7 * 86400))); //7 days
        tableTTL.put(METRICS_AGGREGATE_HOURLY_TABLE_NAME, metricsConf.get(HOST_HOUR_TABLE_TTL, String.valueOf(30 * 86400))); //30 days
        tableTTL.put(METRICS_AGGREGATE_DAILY_TABLE_NAME, metricsConf.get(HOST_DAILY_TABLE_TTL, String.valueOf(365 * 86400))); //1 year
        tableTTL.put(METRICS_CLUSTER_AGGREGATE_TABLE_NAME, metricsConf.get(CLUSTER_SECOND_TABLE_TTL, String.valueOf(7 * 86400))); //7 days
        tableTTL.put(METRICS_CLUSTER_AGGREGATE_MINUTE_TABLE_NAME, metricsConf.get(CLUSTER_MINUTE_TABLE_TTL, String.valueOf(30 * 86400))); //30 days
        tableTTL.put(METRICS_CLUSTER_AGGREGATE_HOURLY_TABLE_NAME, metricsConf.get(CLUSTER_HOUR_TABLE_TTL, String.valueOf(365 * 86400))); //1 year
        tableTTL.put(METRICS_CLUSTER_AGGREGATE_DAILY_TABLE_NAME, metricsConf.get(CLUSTER_DAILY_TABLE_TTL, String.valueOf(730 * 86400))); //2 years

        if (cacheEnabled) {
            LOG.debug("Initialising and starting metrics cache committer thread...");
            metricsCommiterThread = new MetricsCacheCommitterThread(this);
            scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
            scheduledExecutorService.scheduleWithFixedDelay(metricsCommiterThread, 0, cacheCommitInterval, TimeUnit.SECONDS);
        }
    }

    private static Metric getLastTimelineMetricFromResultSet(ResultSet rs)
            throws SQLException, IOException {
        Metric metric = TIMELINE_METRIC_READ_HELPER.getTimelineMetricCommonsFromResultSet(rs);
        metric.setMetricValues(readLastMetricValueFromJSON(rs.getString("METRICS")));
        return metric;
    }

    private static TreeMap<Long, Double> readLastMetricValueFromJSON(String json)
            throws IOException {
        TreeMap<Long, Double> values = readMetricFromJSON(json);
        Long lastTimeStamp = values.lastKey();

        TreeMap<Long, Double> valueMap = new TreeMap<Long, Double>();
        valueMap.put(lastTimeStamp, values.get(lastTimeStamp));
        return valueMap;
    }

    @SuppressWarnings("unchecked")
    public static TreeMap<Long, Double> readMetricFromJSON(String json) throws IOException {
        return mapper.readValue(json, metricValuesTypeRef);
    }

    public boolean isInsertCacheEmpty() {
        return insertCache.isEmpty();
    }

    public void commitMetricsFromCache() {
        LOG.debug("Clearing metrics cache");
        List<Metrics> metricsArray = new ArrayList<Metrics>(insertCache.size());
        while (!insertCache.isEmpty()) {
            metricsArray.add(insertCache.poll());
        }
        if (metricsArray.size() > 0) {
            commitMetrics(metricsArray);
        }
    }

    public void commitMetrics(Metrics metrics) {
        commitMetrics(Collections.singletonList(metrics));
    }

    public void commitMetrics(Collection<Metrics> metricsCollection) {
        LOG.debug("Committing metrics to store");
        Connection conn = null;
        PreparedStatement metricRecordStmt = null;
        long currentTime = System.currentTimeMillis();

        try {
            conn = getConnection();
            metricRecordStmt = conn.prepareStatement(String.format(
                    UPSERT_METRICS_SQL, METRICS_RECORD_TABLE_NAME));

            /**
             * "UPSERT INTO %s " +
             "(METRIC_NAME, HOSTNAME, APP_ID, INSTANCE_ID, SERVER_TIME, START_TIME, " +
             "UNITS, " +
             "METRIC_SUM, " +
             "METRIC_MAX, " +
             "METRIC_MIN, " +
             "METRIC_COUNT, " +
             "METRICS) VALUES " +
             "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
             * */
            for (Metrics metrics : metricsCollection) {
                for (Metric metric : metrics.getMetrics()) {
                    /*if (Math.abs(currentTime - metric.getStartTime()) > outOfBandTimeAllowance) {
                        // 默认5分钟之前的数据是不接收的，这一点很重要！！！
                        LOG.debug("Discarding out of band timeseries, currentTime = "
                                + currentTime + ", startTime = " + metric.getStartTime()
                                + ", hostname = " + metric.getHostName());
                        continue;
                    }*//**2分钟内的才会执行插入，2分钟前的数据则直接丢弃*/

                    metricRecordStmt.clearParameters();

                    if (LOG.isTraceEnabled()) {
                        LOG.trace("host: " + metric.getHostName() + ", " +
                                "metricName = " + metric.getMetricName() + ", " +
                                "values: " + metric.getMetricValues());
                    }
                    double[] aggregates = AggregatorUtils.calculateAggregates(
                            metric.getMetricValues());
                    metricRecordStmt.setString(1, metric.getMetricName());
                    metricRecordStmt.setString(2, metric.getHostName());
                    metricRecordStmt.setString(3, metric.getAppId());
                    metricRecordStmt.setString(4, metric.getInstanceId());
                    metricRecordStmt.setLong(5, currentTime);/**SERVER_TIME 系统时间，即入库时间*/
                    metricRecordStmt.setLong(6, metric.getStartTime());
                    metricRecordStmt.setString(7, metric.getUnits());
                    metricRecordStmt.setDouble(8, aggregates[0]);
                    metricRecordStmt.setDouble(9, aggregates[1]);
                    metricRecordStmt.setDouble(10, aggregates[2]);
                    metricRecordStmt.setLong(11, (long) aggregates[3]);
                    String json = TimelineUtils.dumpTimelineRecordtoJSON(metric.getMetricValues());


                    System.out.println(json);


                    metricRecordStmt.setString(12, json);

                    try {
                        metricRecordStmt.executeUpdate();
                    } catch (SQLException sql) {
                        LOG.error("Failed on insert records to store.", sql);
                    }
                }
            }

            // commit() blocked if HBase unavailable
            conn.commit();
        } catch (Exception exception) {
            exception.printStackTrace();
        } finally {
            if (metricRecordStmt != null) {
                try {
                    metricRecordStmt.close();
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
    }

    private Connection getConnectionRetryingOnException()
            throws SQLException, InterruptedException {
        RetryCounter retryCounter = retryCounterFactory.create();
        while (true) {
            try {
                return getConnection();
            } catch (SQLException e) {
                if (!retryCounter.shouldRetry()) {
                    LOG.error("HBaseAccessor getConnection failed after "
                            + retryCounter.getMaxAttempts() + " attempts");
                    throw e;
                }
            }
            retryCounter.sleepUntilNextRetry();
        }
    }

    /**
     * Get JDBC connection to HBase store. Assumption is that the hbase
     * configuration is present on the classpath and loaded by the caller into
     * the Configuration object.
     * Phoenix already caches the HConnection between the client and HBase
     * cluster.
     *
     * @return @java.sql.Connection
     */
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    /**
     * Unit test purpose only for now.
     *
     * @return @HBaseAdmin
     * @throws java.io.IOException
     */
    HBaseAdmin getHBaseAdmin() throws IOException {
        return dataSource.getHBaseAdmin();
    }

    protected void initMetricSchema() {
        Connection conn = null;
        Statement stmt = null;

        String encoding = metricsConf.get(HBASE_ENCODING_SCHEME, DEFAULT_ENCODING);
        String compression = metricsConf.get(HBASE_COMPRESSION_SCHEME, DEFAULT_TABLE_COMPRESSION);


        try {
            LOG.info("Initializing metrics schema...");
            conn = getConnectionRetryingOnException();
            stmt = conn.createStatement();

            // Metadata
            String metadataSql = String.format(CREATE_METRICS_METADATA_TABLE_SQL,
                    encoding, compression);
            stmt.executeUpdate(metadataSql);
            String hostedAppSql = String.format(CREATE_HOSTED_APPS_METADATA_TABLE_SQL,
                    encoding, compression);
            stmt.executeUpdate(hostedAppSql);

            // Container Metrics
            stmt.executeUpdate(String.format(CREATE_CONTAINER_METRICS_TABLE_SQL,
                    encoding, tableTTL.get(CONTAINER_METRICS_TABLE_NAME), compression));

            // Host level
            String precisionSql = String.format(CREATE_METRICS_TABLE_SQL,
                    encoding, tableTTL.get(METRICS_RECORD_TABLE_NAME), compression);
            String splitPoints = metricsConf.get(PRECISION_TABLE_SPLIT_POINTS);
            if (!StringUtils.isEmpty(splitPoints)) {
                precisionSql += getSplitPointsStr(splitPoints);
            }
            stmt.executeUpdate(precisionSql);

            String hostMinuteAggregrateSql = String.format(CREATE_METRICS_AGGREGATE_TABLE_SQL,
                    METRICS_AGGREGATE_MINUTE_TABLE_NAME, encoding,
                    tableTTL.get(METRICS_AGGREGATE_MINUTE_TABLE_NAME),
                    compression);
            splitPoints = metricsConf.get(AGGREGATE_TABLE_SPLIT_POINTS);
            if (!StringUtils.isEmpty(splitPoints)) {
                hostMinuteAggregrateSql += getSplitPointsStr(splitPoints);
            }
            stmt.executeUpdate(hostMinuteAggregrateSql);

            stmt.executeUpdate(String.format(CREATE_METRICS_AGGREGATE_TABLE_SQL,
                    METRICS_AGGREGATE_HOURLY_TABLE_NAME, encoding,
                    tableTTL.get(METRICS_AGGREGATE_HOURLY_TABLE_NAME),
                    compression));
            stmt.executeUpdate(String.format(CREATE_METRICS_AGGREGATE_TABLE_SQL,
                    METRICS_AGGREGATE_DAILY_TABLE_NAME, encoding,
                    tableTTL.get(METRICS_AGGREGATE_DAILY_TABLE_NAME),
                    compression));

            // Cluster level
            String aggregateSql = String.format(CREATE_METRICS_CLUSTER_AGGREGATE_TABLE_SQL,
                    METRICS_CLUSTER_AGGREGATE_TABLE_NAME, encoding,
                    tableTTL.get(METRICS_CLUSTER_AGGREGATE_TABLE_NAME),
                    compression);
            splitPoints = metricsConf.get(AGGREGATE_TABLE_SPLIT_POINTS);
            if (!StringUtils.isEmpty(splitPoints)) {
                aggregateSql += getSplitPointsStr(splitPoints);
            }
            stmt.executeUpdate(aggregateSql);
            stmt.executeUpdate(String.format(CREATE_METRICS_CLUSTER_AGGREGATE_GROUPED_TABLE_SQL,
                    METRICS_CLUSTER_AGGREGATE_MINUTE_TABLE_NAME, encoding,
                    tableTTL.get(METRICS_CLUSTER_AGGREGATE_MINUTE_TABLE_NAME),
                    compression));
            stmt.executeUpdate(String.format(CREATE_METRICS_CLUSTER_AGGREGATE_GROUPED_TABLE_SQL,
                    METRICS_CLUSTER_AGGREGATE_HOURLY_TABLE_NAME, encoding,
                    tableTTL.get(METRICS_CLUSTER_AGGREGATE_HOURLY_TABLE_NAME),
                    compression));
            stmt.executeUpdate(String.format(CREATE_METRICS_CLUSTER_AGGREGATE_GROUPED_TABLE_SQL,
                    METRICS_CLUSTER_AGGREGATE_DAILY_TABLE_NAME, encoding,
                    tableTTL.get(METRICS_CLUSTER_AGGREGATE_DAILY_TABLE_NAME),
                    compression));


            conn.commit();

            LOG.info("Metrics schema initialized.");
        } catch (SQLException | InterruptedException sql) {
            LOG.error("Error creating Metrics Schema in HBase using Phoenix.", sql);
            throw new MetricsSystemInitializationException(
                    "Error creating Metrics Schema in HBase using Phoenix.", sql);
        } finally {
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
                } catch (SQLException e) {
                    // Ignore
                }
            }
        }
    }

    protected void initPoliciesAndTTL() {
        boolean enableNormalizer = hbaseConf.getBoolean("hbase.normalizer.enabled", false);
        boolean enableFifoCompaction = metricsConf.getBoolean("timeline.metrics.hbase.fifo.compaction.enabled", true);

        HBaseAdmin hBaseAdmin = null;
        try {
            hBaseAdmin = dataSource.getHBaseAdmin();
        } catch (IOException e) {
            LOG.warn("Unable to initialize HBaseAdmin for setting policies.", e);
        }

        if (hBaseAdmin != null) {
            for (String tableName : PHOENIX_TABLES) {
                try {
                    boolean modifyTable = false;
                    HTableDescriptor tableDescriptor = hBaseAdmin.getTableDescriptor(tableName.getBytes());

                    if (enableNormalizer ^ tableDescriptor.isNormalizationEnabled()){
                        tableDescriptor.setNormalizationEnabled(enableNormalizer);
                        LOG.info("Normalizer set to " + enableNormalizer + " for " + tableName);
                        modifyTable = true;
                    }

                    if (METRICS_RECORD_TABLE_NAME.equals(tableName)) {
                        if (!timelineMetricsPrecisionTableDurability.isEmpty()) {
                            LOG.info("Setting WAL option " + timelineMetricsPrecisionTableDurability + " for table : " + tableName);
                            boolean validDurability = true;
                            if ("SKIP_WAL".equals(timelineMetricsPrecisionTableDurability)) {
                                tableDescriptor.setDurability(Durability.SKIP_WAL);
                            } else if ("SYNC_WAL".equals(timelineMetricsPrecisionTableDurability)) {
                                tableDescriptor.setDurability(Durability.SYNC_WAL);
                            } else if ("ASYNC_WAL".equals(timelineMetricsPrecisionTableDurability)) {
                                tableDescriptor.setDurability(Durability.ASYNC_WAL);
                            } else if ("FSYNC_WAL".equals(timelineMetricsPrecisionTableDurability)) {
                                tableDescriptor.setDurability(Durability.FSYNC_WAL);
                            } else {
                                LOG.info("Unknown value for " + TIMELINE_METRICS_PRECISION_TABLE_DURABILITY + " : " + timelineMetricsPrecisionTableDurability);
                                validDurability = false;
                            }
                            if (validDurability) {
                                modifyTable = true;
                            }
                        }
                    } else {
                        if (!timelineMetricsTablesDurability.isEmpty()) {
                            LOG.info("Setting WAL option " + timelineMetricsTablesDurability + " for table : " + tableName);
                            boolean validDurability = true;
                            if ("SKIP_WAL".equals(timelineMetricsTablesDurability)) {
                                tableDescriptor.setDurability(Durability.SKIP_WAL);
                            } else if ("SYNC_WAL".equals(timelineMetricsTablesDurability)) {
                                tableDescriptor.setDurability(Durability.SYNC_WAL);
                            } else if ("ASYNC_WAL".equals(timelineMetricsTablesDurability)) {
                                tableDescriptor.setDurability(Durability.ASYNC_WAL);
                            } else if ("FSYNC_WAL".equals(timelineMetricsTablesDurability)) {
                                tableDescriptor.setDurability(Durability.FSYNC_WAL);
                            } else {
                                LOG.info("Unknown value for " + TIMELINE_METRICS_AGGREGATE_TABLES_DURABILITY + " : " + timelineMetricsTablesDurability);
                                validDurability = false;
                            }
                            if (validDurability) {
                                modifyTable = true;
                            }
                        }
                    }

                    Map<String, String> config = tableDescriptor.getConfiguration();
                    if (enableFifoCompaction &&
                            !FIFO_COMPACTION_POLICY_CLASS.equals(config.get(HSTORE_COMPACTION_CLASS_KEY))) {
                        tableDescriptor.setConfiguration(HSTORE_COMPACTION_CLASS_KEY,
                                FIFO_COMPACTION_POLICY_CLASS);
                        LOG.info("Setting config property " + HSTORE_COMPACTION_CLASS_KEY +
                                " = " + FIFO_COMPACTION_POLICY_CLASS + " for " + tableName);
                        // Need to set blockingStoreFiles to 1000 for FIFO
                        int blockingStoreFiles = hbaseConf.getInt(HBASE_BLOCKING_STORE_FILES, 1000);
                        if (blockingStoreFiles < 1000) {
                            blockingStoreFiles = 1000;
                        }
                        tableDescriptor.setConfiguration(BLOCKING_STORE_FILES_KEY, String.valueOf(blockingStoreFiles));
                        LOG.info("Setting config property " + BLOCKING_STORE_FILES_KEY +
                                " = " + blockingStoreFiles + " for " + tableName);
                        modifyTable = true;
                    }
                    // Set back original policy if fifo disabled
                    if (!enableFifoCompaction &&
                            FIFO_COMPACTION_POLICY_CLASS.equals(config.get(HSTORE_COMPACTION_CLASS_KEY))) {
                        tableDescriptor.setConfiguration(HSTORE_COMPACTION_CLASS_KEY,
                                DEFAULT_COMPACTION_POLICY_CLASS);
                        LOG.info("Setting config property " + HSTORE_COMPACTION_CLASS_KEY +
                                " = " + DEFAULT_COMPACTION_POLICY_CLASS + " for " + tableName);

                        int blockingStoreFiles = hbaseConf.getInt(HBASE_BLOCKING_STORE_FILES, 300);
                        if (blockingStoreFiles > 300) {
                            LOG.warn("HBase blocking store file set too high without FIFO " +
                                    "Compaction policy enabled, restoring low value = 300.");
                            blockingStoreFiles = 300;
                        }
                        tableDescriptor.setConfiguration(BLOCKING_STORE_FILES_KEY, String.valueOf(blockingStoreFiles));
                        LOG.info("Setting config property " + BLOCKING_STORE_FILES_KEY +
                                " = " + blockingStoreFiles + " for " + tableName);
                        modifyTable = true;
                    }
                    // Change TTL setting to match user configuration
                    HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
                    if (columnFamilies != null) {
                        for (HColumnDescriptor family : columnFamilies) {
                            String ttlValue = family.getValue("TTL");
                            if (StringUtils.isEmpty(ttlValue) ||
                                    !ttlValue.trim().equals(tableTTL.get(tableName))) {
                                family.setValue("TTL", tableTTL.get(tableName));
                                LOG.info("Setting TTL on table: " + tableName + " to : " +
                                        tableTTL.get(tableName) + " seconds.");
                                modifyTable = true;
                            }
                        }
                    }

                    // Persist only if anything changed
                    if (modifyTable) {
                        hBaseAdmin.modifyTable(tableName.getBytes(), tableDescriptor);
                    }

                } catch (IOException e) {
                    LOG.error("Failed setting policies for " + tableName, e);
                }
            }
            try {
                hBaseAdmin.close();
            } catch (IOException e) {
                LOG.warn("Exception on HBaseAdmin close.", e);
            }
        }
    }

    protected String getSplitPointsStr(String splitPoints) {
        if (StringUtils.isEmpty(splitPoints.trim())) {
            return "";
        }
        String[] points = splitPoints.split(",");
        if (points.length > 0) {
            StringBuilder sb = new StringBuilder(" SPLIT ON ");
            sb.append("(");
            for (String point : points) {
                sb.append("'");
                sb.append(point.trim());
                sb.append("'");
                sb.append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append(")");
            return sb.toString();
        }
        return "";
    }

    public void insertContainerMetrics(List<ContainerMetric> metrics)
            throws SQLException, IOException {
        Connection conn = getConnection();
        PreparedStatement metricRecordStmt = null;

        try {
            metricRecordStmt = conn.prepareStatement(
                    String.format(UPSERT_CONTAINER_METRICS_SQL, CONTAINER_METRICS_TABLE_NAME));
            for (ContainerMetric metric : metrics) {
                metricRecordStmt.clearParameters();
                metricRecordStmt.setString(1, ContainerId.fromString(metric.getContainerId())
                        .getApplicationAttemptId().getApplicationId().toString());
                metricRecordStmt.setString(2, metric.getContainerId());
                metricRecordStmt.setTimestamp(3, new Timestamp(metric.getStartTime()));
                metricRecordStmt.setTimestamp(4, new Timestamp(metric.getFinishTime()));
                metricRecordStmt.setLong(5, metric.getFinishTime() - metric.getStartTime());
                metricRecordStmt.setString(6, metric.getHostName());
                metricRecordStmt.setInt(7, metric.getExitCode());
                metricRecordStmt.setLong(8, metric.getLocalizationDuration());
                metricRecordStmt.setLong(9, metric.getLaunchDuration());
                metricRecordStmt.setDouble(10, (double) metric.getPmemLimit() / 1024);
                metricRecordStmt.setDouble(11,
                        ((double) metric.getPmemLimit() / 1024) * (metric.getFinishTime()
                                - metric.getStartTime()));
                metricRecordStmt.setDouble(12, (double) metric.getVmemLimit() / 1024);
                metricRecordStmt.setDouble(13, (double) metric.getPmemUsedMin() / 1024);
                metricRecordStmt.setDouble(14, (double) metric.getPmemUsedMax() / 1024);
                metricRecordStmt.setDouble(15, (double) metric.getPmemUsedAvg() / 1024);
                metricRecordStmt.setDouble(16, (double) metric.getPmem50Pct() / 1024);
                metricRecordStmt.setDouble(17, (double) metric.getPmem75Pct() / 1024);
                metricRecordStmt.setDouble(18, (double) metric.getPmem90Pct() / 1024);
                metricRecordStmt.setDouble(19, (double) metric.getPmem95Pct() / 1024);
                metricRecordStmt.setDouble(20, (double) metric.getPmem99Pct() / 1024);
                metricRecordStmt.setDouble(21, (double) metric.getPmemLimit() / 1024
                        - (double) metric.getPmemUsedMax() / 1024);
                metricRecordStmt.setDouble(22, ((double) metric.getPmemLimit() / 1024
                        - (double) metric.getPmemUsedMax() / 1024) * (metric.getFinishTime()
                        - metric.getStartTime()));

                try {
                    metricRecordStmt.executeUpdate();
                } catch (SQLException sql) {
                    LOG.error("Failed on insert records to store.", sql);
                }
            }

            conn.commit();
        } finally {
            if (metricRecordStmt != null) {
                try {
                    metricRecordStmt.close();
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
    }

    public void insertMetricRecordsWithMetadata(MetricMetadataManager metadataManager,
                                                Metrics metrics, boolean skipCache) throws SQLException, IOException {
        List<Metric> timelineMetrics = metrics.getMetrics();
        if (timelineMetrics == null || timelineMetrics.isEmpty()) {
            LOG.debug("Empty metrics insert request.");
            return;
        }
        for (Metric tm : timelineMetrics) {

            if (CollectionUtils.isNotEmpty(AggregatorUtils.whitelistedMetrics) &&
                    !AggregatorUtils.whitelistedMetrics.contains(tm.getMetricName())) {
                continue;
            }/**
             聚合工具白名单中如果没有这个东西，那么对不起，不保留元数据
             */

            // Write to metadata cache on successful write to store
            if (metadataManager != null) {
                metadataManager.putIfModifiedTimelineMetricMetadata(
                        metadataManager.getTimelineMetricMetadata(tm));

                metadataManager.putIfModifiedHostedAppsMetadata(
                        tm.getHostName(), tm.getAppId());
            }/**保存元数据*/
        }

        if (!skipCache && cacheEnabled) {
            LOG.info("Adding metrics to cache");
            if (insertCache.size() >= cacheSize) {
                commitMetricsFromCache();
            }
            try {
                insertCache.put(metrics); // blocked while the queue is full
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            /**
             * 这里的cache刷写机制是非线程安全的可能多个线程同时进去flush流程，同时写入缓存和flush两个流程可能导致oom
             * poll的过程中，不断写入缓存，那么拷贝不会停止，从而导致oom
             *
             * */
        } else {
            System.out.println("直接插入，关闭cache，牺牲一部分性能................");
            LOG.debug("Skipping metrics cache");
            commitMetrics(metrics);
        }
    }

    public void insertMetricRecords(Metrics metrics, boolean skipCache) throws SQLException, IOException {
        insertMetricRecordsWithMetadata(null, metrics, skipCache);
    }

    public void insertMetricRecords(Metrics metrics) throws SQLException, IOException {
        insertMetricRecords(metrics, false);
    }

    @SuppressWarnings("unchecked")
    public Metrics getMetricRecords(
            final Condition condition, Multimap<String, List<Function>> metricFunctions)
            throws SQLException, IOException {

        validateConditionIsNotEmpty(condition);

        Connection conn = getConnection();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        Metrics metrics = new Metrics();

        try {
            //get latest
            if (condition.isPointInTime()) {
                getLatestMetricRecords(condition, conn, metrics);
            } else {
                if (condition.getEndTime() >= condition.getStartTime()) {
                    stmt = PhoenixTransactSQL.prepareGetMetricsSqlStmt(conn, condition);
                    rs = stmt.executeQuery();
                    while (rs.next()) {
                        appendMetricFromResultSet(metrics, condition, metricFunctions, rs);
                    }
                } else {
                    LOG.warn("Skipping metrics query because endTime < startTime");
                }
            }

        } catch (PhoenixIOException pioe) {
            Throwable pioe2 = pioe.getCause();
            // Need to find out if this is exception "Could not find hash cache
            // for joinId" or another PhoenixIOException
            if (pioe2 instanceof PhoenixIOException &&
                    pioe2.getCause() instanceof DoNotRetryIOException) {
                String className = null;
                for (StackTraceElement ste : pioe2.getCause().getStackTrace()) {
                    className = ste.getClassName();
                }

                if (className != null && className.equals("HashJoinRegionScanner")) {
                    LOG.error("The cache might have expired and have been removed. Try to" +
                            " increase the cache size by setting bigger value for " +
                            "phoenix.coprocessor.maxMetaDataCacheSize in ams-hbase-site config." +
                            " Falling back to sort-merge join algorithm.");
                    PhoenixTransactSQL.setSortMergeJoinEnabled(true);
                }
            }
            throw pioe;
        } catch (RuntimeException ex) {
            // We need to find out if this is a real IO exception
            // or exception "maxStamp is smaller than minStamp"
            // which is thrown in hbase TimeRange.java
            Throwable io = ex.getCause();
            String className = null;
            if (io != null) {
                for (StackTraceElement ste : io.getStackTrace()) {
                    className = ste.getClassName();
                }
            }
            if (className != null && className.equals("TimeRange")) {
                // This is "maxStamp is smaller than minStamp" exception
                // Log error and return empty metrics
                LOG.debug(io);
                return new Metrics();
            } else {
                throw ex;
            }

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

        LOG.debug("Metrics records size: " + metrics.getMetrics().size());
        return metrics;
    }

    /**
     * Apply aggregate function to the result if supplied else get precision
     * or aggregate data with default function applied.
     */
    private void appendMetricFromResultSet(Metrics metrics, Condition condition,
                                           Multimap<String, List<Function>> metricFunctions,
                                           ResultSet rs) throws SQLException, IOException {
        String metricName = rs.getString("METRIC_NAME");
        Collection<List<Function>> functionList = findMetricFunctions(metricFunctions, metricName);

        for (List<Function> functions : functionList) {
            // Apply aggregation function if present
            if ((functions != null && !functions.isEmpty())) {
                if (functions.size() > 1) {
                    throw new IllegalArgumentException("Multiple aggregate functions not supported.");
                }
                for (Function f : functions) {
                    if (f.getReadFunction() == Function.ReadFunction.VALUE) {
                        getTimelineMetricsFromResultSet(metrics, f, condition, rs);
                    } else {
                        SingleValuedMetric metric =
                                TIMELINE_METRIC_READ_HELPER.getAggregatedTimelineMetricFromResultSet(rs, f);

                        if (condition.isGrouped()) {
                            metrics.addOrMergeTimelineMetric(metric);
                        } else {
                            metrics.getMetrics().add(metric.getTimelineMetric());
                        }
                    }
                }
            } else {
                // No aggregation requested
                // Execution never goes here, function always contain at least 1 element
                getTimelineMetricsFromResultSet(metrics, null, condition, rs);
            }
        }
    }

    private void getTimelineMetricsFromResultSet(Metrics metrics, Function f, Condition condition, ResultSet rs) throws SQLException, IOException {
        if (condition.getPrecision().equals(Precision.SECONDS)) {
            Metric metric = TIMELINE_METRIC_READ_HELPER.getTimelineMetricFromResultSet(rs);
            if (f != null && f.getSuffix() != null) { //Case : Requesting "._rate" for precision data
                metric.setMetricName(metric.getMetricName() + f.getSuffix());
            }
            if (condition.isGrouped()) {
                metrics.addOrMergeTimelineMetric(metric);
            } else {
                metrics.getMetrics().add(metric);
            }

        } else {
            SingleValuedMetric metric =
                    TIMELINE_METRIC_READ_HELPER.getAggregatedTimelineMetricFromResultSet(rs, f);
            if (condition.isGrouped()) {
                metrics.addOrMergeTimelineMetric(metric);
            } else {
                metrics.getMetrics().add(metric.getTimelineMetric());
            }
        }
    }

    private void getLatestMetricRecords(Condition condition, Connection conn,
                                        Metrics metrics) throws SQLException, IOException {

        validateConditionIsNotEmpty(condition);

        PreparedStatement stmt = PhoenixTransactSQL.prepareGetLatestMetricSqlStmt(conn, condition);
        ResultSet rs = null;
        try {
            rs = stmt.executeQuery();
            while (rs.next()) {
                Metric metric = getLastTimelineMetricFromResultSet(rs);
                metrics.getMetrics().add(metric);
            }
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    // Ignore
                }
            }
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    /**
     * Get metrics aggregated across hosts.
     *
     * @param condition @Condition
     * @return @Metrics
     * @throws java.sql.SQLException
     */
    public Metrics getAggregateMetricRecords(final Condition condition,
                                                     Multimap<String, List<Function>> metricFunctions) throws SQLException {

        validateConditionIsNotEmpty(condition);

        Connection conn = getConnection();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        Metrics metrics = new Metrics();

        try {
            //get latest
            if (condition.isPointInTime()) {
                getLatestAggregateMetricRecords(condition, conn, metrics, metricFunctions);
            } else {
                stmt = PhoenixTransactSQL.prepareGetAggregateSqlStmt(conn, condition);

                rs = stmt.executeQuery();
                while (rs.next()) {
                    appendAggregateMetricFromResultSet(metrics, condition, metricFunctions, rs);
                }
            }
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

        LOG.debug("Aggregate records size: " + metrics.getMetrics().size());
        return metrics;
    }

    private void appendAggregateMetricFromResultSet(Metrics metrics,
                                                    Condition condition, Multimap<String, List<Function>> metricFunctions,
                                                    ResultSet rs) throws SQLException {

        String metricName = rs.getString("METRIC_NAME");
        Collection<List<Function>> functionList = findMetricFunctions(metricFunctions, metricName);

        for (List<Function> functions : functionList) {
            for (Function aggregateFunction : functions) {
                SingleValuedMetric metric;

                if (condition.getPrecision() == Precision.MINUTES
                        || condition.getPrecision() == Precision.HOURS
                        || condition.getPrecision() == Precision.DAYS) {
                    metric = getAggregateTimelineMetricFromResultSet(rs, aggregateFunction, false);
                } else {
                    metric = getAggregateTimelineMetricFromResultSet(rs, aggregateFunction, true);
                }

                if (condition.isGrouped()) {
                    metrics.addOrMergeTimelineMetric(metric);
                } else {
                    metrics.getMetrics().add(metric.getTimelineMetric());
                }
            }
        }

    }

    private void getLatestAggregateMetricRecords(Condition condition,
                                                 Connection conn, Metrics metrics,
                                                 Multimap<String, List<Function>> metricFunctions) throws SQLException {

        PreparedStatement stmt = null;
        SplitByMetricNamesCondition splitCondition =
                new SplitByMetricNamesCondition(condition);

        for (String metricName : splitCondition.getOriginalMetricNames()) {

            splitCondition.setCurrentMetric(metricName);
            stmt = PhoenixTransactSQL.prepareGetLatestAggregateMetricSqlStmt(conn, splitCondition);
            ResultSet rs = null;
            try {
                rs = stmt.executeQuery();
                while (rs.next()) {
                    Collection<List<Function>> functionList = findMetricFunctions(metricFunctions, metricName);
                    for (List<Function> functions : functionList) {
                        if (functions != null) {
                            for (Function f : functions) {
                                SingleValuedMetric metric =
                                        getAggregateTimelineMetricFromResultSet(rs, f, true);

                                if (condition.isGrouped()) {
                                    metrics.addOrMergeTimelineMetric(metric);
                                } else {
                                    metrics.getMetrics().add(metric.getTimelineMetric());
                                }
                            }
                        } else {
                            SingleValuedMetric metric =
                                    getAggregateTimelineMetricFromResultSet(rs, new Function(), true);
                            metrics.getMetrics().add(metric.getTimelineMetric());
                        }
                    }
                }
            } finally {
                if (rs != null) {
                    try {
                        rs.close();
                    } catch (SQLException e) {
                        // Ignore
                    }
                }
                if (stmt != null) {
                    stmt.close();
                }
            }
        }
    }

    private SingleValuedMetric getAggregateTimelineMetricFromResultSet(ResultSet rs,
                                                                               Function f, boolean useHostCount) throws SQLException {

        String countColumnName = "METRIC_COUNT";
        if (useHostCount) {
            countColumnName = "HOSTS_COUNT";
        }

        SingleValuedMetric metric = new SingleValuedMetric(
                rs.getString("METRIC_NAME") + f.getSuffix(),
                rs.getString("APP_ID"),
                rs.getString("INSTANCE_ID"),
                null,
                rs.getLong("SERVER_TIME"),
                rs.getLong("SERVER_TIME"),
                rs.getString("UNITS")
        );

        double value;
        switch (f.getReadFunction()) {
            case AVG:
                value = rs.getDouble("METRIC_SUM") / rs.getInt(countColumnName);
                break;
            case MIN:
                value = rs.getDouble("METRIC_MIN");
                break;
            case MAX:
                value = rs.getDouble("METRIC_MAX");
                break;
            case SUM:
                value = rs.getDouble("METRIC_SUM");
                break;
            default:
                value = rs.getDouble("METRIC_SUM") / rs.getInt(countColumnName);
                break;
        }

        metric.setSingleTimeseriesValue(rs.getLong("SERVER_TIME"), value);

        return metric;
    }

    private void validateConditionIsNotEmpty(Condition condition) {
        if (condition.isEmpty()) {
            throw new IllegalArgumentException("No filter criteria specified.");
        }
    }

    private Collection<List<Function>> findMetricFunctions(Multimap<String, List<Function>> metricFunctions,
                                                           String metricName) {
        if (metricFunctions.containsKey(metricName)) {
            return metricFunctions.get(metricName);
        }

        for (String metricNameEntry : metricFunctions.keySet()) {
            String metricRegEx = metricNameEntry.replace("%", ".*");
            if (metricName.matches(metricRegEx)) {
                return metricFunctions.get(metricNameEntry);
            }
        }

        return null;
    }

    public void saveHostAggregateRecords(Map<Metric, MetricHostAggregate> hostAggregateMap,
                                         String phoenixTableName) throws SQLException {

        if (hostAggregateMap == null || hostAggregateMap.isEmpty()) {
            LOG.debug("Empty aggregate records.");
            return;
        }

        Connection conn = getConnection();
        PreparedStatement stmt = null;

        long start = System.currentTimeMillis();
        int rowCount = 0;

        try {
            stmt = conn.prepareStatement(
                    String.format(UPSERT_AGGREGATE_RECORD_SQL, phoenixTableName));

            for (Map.Entry<Metric, MetricHostAggregate> metricAggregate :
                    hostAggregateMap.entrySet()) {

                Metric metric = metricAggregate.getKey();
                MetricHostAggregate hostAggregate = metricAggregate.getValue();

                rowCount++;
                stmt.clearParameters();
                stmt.setString(1, metric.getMetricName());
                stmt.setString(2, metric.getHostName());
                stmt.setString(3, metric.getAppId());
                stmt.setString(4, metric.getInstanceId());
                stmt.setLong(5, metric.getTimestamp());
                stmt.setString(6, metric.getType());
                stmt.setDouble(7, hostAggregate.getSum());
                stmt.setDouble(8, hostAggregate.getMax());
                stmt.setDouble(9, hostAggregate.getMin());
                stmt.setDouble(10, hostAggregate.getNumberOfSamples());

                try {
                    stmt.executeUpdate();
                } catch (SQLException sql) {
                    LOG.error(sql);
                }

                if (rowCount >= PHOENIX_MAX_MUTATION_STATE_SIZE - 1) {
                    conn.commit();
                    rowCount = 0;
                }

            }

            conn.commit();

        } finally {
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

        long end = System.currentTimeMillis();

        if ((end - start) > 60000l) {
            LOG.info("Time to save map: " + (end - start) + ", " +
                    "thread = " + Thread.currentThread().getClass());
        }
    }

    /**
     * Save Metric aggregate records.
     *
     * @throws java.sql.SQLException
     */
    public void saveClusterAggregateRecords(Map<ClusterMetric, MetricClusterAggregate> records)
            throws SQLException {

        if (records == null || records.isEmpty()) {
            LOG.debug("Empty aggregate records.");
            return;
        }

        long start = System.currentTimeMillis();
        String sqlStr = String.format(UPSERT_CLUSTER_AGGREGATE_SQL, METRICS_CLUSTER_AGGREGATE_TABLE_NAME);
        Connection conn = getConnection();
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement(sqlStr);
            int rowCount = 0;

            for (Map.Entry<ClusterMetric, MetricClusterAggregate>
                    aggregateEntry : records.entrySet()) {
                ClusterMetric clusterMetric = aggregateEntry.getKey();
                MetricClusterAggregate aggregate = aggregateEntry.getValue();

                if (LOG.isTraceEnabled()) {
                    LOG.trace("clusterMetric = " + clusterMetric + ", " +
                            "aggregate = " + aggregate);
                }

                rowCount++;
                stmt.clearParameters();
                stmt.setString(1, clusterMetric.getMetricName());
                stmt.setString(2, clusterMetric.getAppId());
                stmt.setString(3, clusterMetric.getInstanceId());
                stmt.setLong(4, clusterMetric.getTimestamp());
                stmt.setString(5, clusterMetric.getType());
                stmt.setDouble(6, aggregate.getSum());
                stmt.setInt(7, aggregate.getNumberOfHosts());
                stmt.setDouble(8, aggregate.getMax());
                stmt.setDouble(9, aggregate.getMin());

                try {
                    stmt.executeUpdate();
                } catch (SQLException sql) {
                    // we have no way to verify it works!!!
                    LOG.error(sql);
                }

                if (rowCount >= PHOENIX_MAX_MUTATION_STATE_SIZE - 1) {
                    conn.commit();
                    rowCount = 0;
                }
            }

            conn.commit();

        } finally {
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
        long end = System.currentTimeMillis();
        if ((end - start) > 60000l) {
            LOG.info("Time to save: " + (end - start) + ", " +
                    "thread = " + Thread.currentThread().getName());
        }
    }


    /**
     * Save Metric aggregate records.
     *
     * @throws java.sql.SQLException
     */
    public void saveClusterTimeAggregateRecords(Map<ClusterMetric, MetricHostAggregate> records,
                                                String tableName) throws SQLException {
        if (records == null || records.isEmpty()) {
            LOG.debug("Empty aggregate records.");
            return;
        }

        long start = System.currentTimeMillis();

        Connection conn = getConnection();
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement(String.format(UPSERT_CLUSTER_AGGREGATE_TIME_SQL, tableName));
            int rowCount = 0;

            for (Map.Entry<ClusterMetric, MetricHostAggregate> aggregateEntry : records.entrySet()) {
                ClusterMetric clusterMetric = aggregateEntry.getKey();
                MetricHostAggregate aggregate = aggregateEntry.getValue();

                if (LOG.isTraceEnabled()) {
                    LOG.trace("clusterMetric = " + clusterMetric + ", " +
                            "aggregate = " + aggregate);
                }

                rowCount++;
                stmt.clearParameters();
                stmt.setString(1, clusterMetric.getMetricName());
                stmt.setString(2, clusterMetric.getAppId());
                stmt.setString(3, clusterMetric.getInstanceId());
                stmt.setLong(4, clusterMetric.getTimestamp());
                stmt.setString(5, clusterMetric.getType());
                stmt.setDouble(6, aggregate.getSum());
                stmt.setLong(7, aggregate.getNumberOfSamples());
                stmt.setDouble(8, aggregate.getMax());
                stmt.setDouble(9, aggregate.getMin());

                try {
                    stmt.executeUpdate();
                } catch (SQLException sql) {
                    // we have no way to verify it works!!!
                    LOG.error(sql);
                }

                if (rowCount >= PHOENIX_MAX_MUTATION_STATE_SIZE - 1) {
                    conn.commit();
                    rowCount = 0;
                }
            }

            conn.commit();

        } finally {
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
        long end = System.currentTimeMillis();
        if ((end - start) > 60000l) {
            LOG.info("Time to save: " + (end - start) + ", " +
                    "thread = " + Thread.currentThread().getName());
        }
    }

    /**
     * Provide skip block cache hint for aggregator queries.
     */
    public boolean isSkipBlockCacheForAggregatorsEnabled() {
        return skipBlockCacheForAggregatorsEnabled;
    }

    /**
     * One time save of metadata when discovering topology during aggregation.
     *
     * @throws java.sql.SQLException
     */
    public void saveHostAppsMetadata(Map<String, Set<String>> hostedApps) throws SQLException {
        Connection conn = getConnection();
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement(UPSERT_HOSTED_APPS_METADATA_SQL);
            int rowCount = 0;

            for (Map.Entry<String, Set<String>> hostedAppsEntry : hostedApps.entrySet()) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("HostedAppsMetadata: " + hostedAppsEntry);
                }

                stmt.clearParameters();
                stmt.setString(1, hostedAppsEntry.getKey());
                stmt.setString(2, StringUtils.join(hostedAppsEntry.getValue(), ","));
                try {
                    stmt.executeUpdate();
                    rowCount++;
                } catch (SQLException sql) {
                    LOG.error("Error saving hosted apps metadata.", sql);
                }
            }

            conn.commit();
            LOG.info("Saved " + rowCount + " hosted apps metadata records.");

        } finally {
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
    }

    /**
     * Save metdata on updates.
     *
     * @param metricMetadata @Collection<@MetricMetadata>
     * @throws java.sql.SQLException
     */
    public void saveMetricMetadata(Collection<MetricMetadata> metricMetadata) throws SQLException {
        if (metricMetadata.isEmpty()) {
            LOG.info("No metadata records to save.");
            return;
        }

        Connection conn = getConnection();
        PreparedStatement stmt = null;

        try {
            stmt = conn.prepareStatement(UPSERT_METADATA_SQL);
            /**
             * UPSERT INTO METRICS_METADATA (METRIC_NAME, APP_ID, UNITS, TYPE, " +
             "START_TIME, SUPPORTS_AGGREGATION) " +
             "VALUES (?, ?, ?, ?, ?, ?)
             * */
            int rowCount = 0;

            for (MetricMetadata metadata : metricMetadata) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("MetricMetadata: metricName = " + metadata.getMetricName()
                            + ", appId = " + metadata.getAppId()
                            + ", seriesStartTime = " + metadata.getSeriesStartTime()
                    );
                }

                stmt.clearParameters();
                stmt.setString(1, metadata.getMetricName());
                stmt.setString(2, metadata.getAppId());
                stmt.setString(3, metadata.getUnits());
                stmt.setString(4, metadata.getType());
                stmt.setLong(5, metadata.getSeriesStartTime());
                stmt.setBoolean(6, metadata.isSupportsAggregates());

                try {
                    stmt.executeUpdate();
                    rowCount++;
                } catch (SQLException sql) {
                    LOG.error("Error saving metadata.", sql);
                }
            }

            conn.commit();
            LOG.info("Saved " + rowCount + " metadata records.");

        } finally {
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
    }

    public Map<String, Set<String>> getHostedAppsMetadata() throws SQLException {
        Map<String, Set<String>> hostedAppMap = new HashMap<>();
        Connection conn = getConnection();
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = conn.prepareStatement(GET_HOSTED_APPS_METADATA_SQL);
            rs = stmt.executeQuery();
            /**
             * SELECT " +
             "HOSTNAME, APP_IDS FROM HOSTED_APPS_METADATA
             * */
            while (rs.next()) {
                hostedAppMap.put(rs.getString("HOSTNAME"),
                        new HashSet<>(Arrays.asList(StringUtils.split(rs.getString("APP_IDS"), ","))));
            }

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

        return hostedAppMap;
    }

    // No filter criteria support for now.
    public Map<MetricMetadataKey, MetricMetadata> getTimelineMetricMetadata() throws SQLException {
        Map<MetricMetadataKey, MetricMetadata> metadataMap = new HashMap<>();
        Connection conn = getConnection();
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            stmt = conn.prepareStatement(GET_METRIC_METADATA_SQL);
            /**
             * SELECT " +
             "METRIC_NAME, APP_ID, UNITS, TYPE, START_TIME, " +
             "SUPPORTS_AGGREGATION FROM METRICS_METADATA
             * */
            rs = stmt.executeQuery();

            while (rs.next()) {
                String metricName = rs.getString("METRIC_NAME");
                String appId = rs.getString("APP_ID");
                MetricMetadata metadata = new MetricMetadata(
                        metricName,
                        appId,
                        rs.getString("UNITS"),
                        rs.getString("TYPE"),
                        rs.getLong("START_TIME"),
                        rs.getBoolean("SUPPORTS_AGGREGATION")
                );

                MetricMetadataKey key = new MetricMetadataKey(metricName, appId);
                metadata.setIsPersisted(true); // Always true on retrieval
                metadataMap.put(key, metadata);
            }

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

        return metadataMap;
    }
}
