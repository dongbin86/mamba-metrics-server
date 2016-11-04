package mamba.discovery;

import mamba.metrics.MetricMetadata;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.SQLException;
import java.util.*;

/**
 * Created by dongbin on 2016/10/10.
 */
public class MetricMetadataSync implements Runnable {
    private static final Log LOG = LogFactory.getLog(MetricMetadataSync.class);

    private final MetricMetadataManager cacheManager;

    public MetricMetadataSync(MetricMetadataManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    @Override
    public void run() {
        List<MetricMetadata> metadataToPersist = new ArrayList<>();
        // Find all entries to persist
        for (MetricMetadata metadata : cacheManager.getMetadataCache().values()) {
            if (!metadata.isPersisted()) {
                metadataToPersist.add(metadata);
            }
        }/**扫描所有还没有持久化的metricdata*/
        boolean markSuccess = false;
        if (!metadataToPersist.isEmpty()) {
            try {
                cacheManager.persistMetadata(metadataToPersist);/**写入hbase的METRICS_METADATA表*/
                /**
                 * UPSERT INTO METRICS_METADATA (METRIC_NAME, APP_ID, UNITS, TYPE, " +
                 "START_TIME, SUPPORTS_AGGREGATION) " +
                 "VALUES (?, ?, ?, ?, ?, ?)
                 * */
/**
 * "CREATE TABLE IF NOT EXISTS METRICS_METADATA " +
 "(METRIC_NAME VARCHAR, " +
 "APP_ID VARCHAR, " +
 "UNITS CHAR(20), " +
 "TYPE CHAR(20), " +
 "START_TIME UNSIGNED_LONG, " +
 "SUPPORTS_AGGREGATION BOOLEAN " +
 "CONSTRAINT pk PRIMARY KEY (METRIC_NAME, APP_ID)) " +
 "DATA_BLOCK_ENCODING='%s', COMPRESSION='%s'"
 *
 *
 * metricName,appId,units,start_time,support_aggregation,
 *
 *
 *
 * */

                 markSuccess = true;
            } catch (SQLException e) {
                LOG.warn("Error persisting metadata.", e);
            }
        }
        // Mark corresponding entries as persisted to skip on next run
        if (markSuccess) {
            for (MetricMetadata metadata : metadataToPersist) {
                MetricMetadataKey key = new MetricMetadataKey(
                        metadata.getMetricName(), metadata.getAppId()
                );

                // Mark entry as being persisted
                metadata.setIsPersisted(true);
                // Update cache
                cacheManager.getMetadataCache().put(key, metadata);
            }
        }/**重新标记已经持久化*/
        // Sync hosted apps data is needed
        if (cacheManager.syncHostedAppsMetadata()) {
            Map<String, Set<String>> persistedData = null;
            try {
                persistedData = cacheManager.getPersistedHostedAppsData(); /**从表里把数据全部捞出来*/
            } catch (SQLException e) {
                LOG.warn("Failed on fetching hosted apps data from store.", e);
                return; // Something wrong with store
            }


            /**
             * "CREATE TABLE IF NOT EXISTS HOSTED_APPS_METADATA " +
             "(HOSTNAME VARCHAR, APP_IDS VARCHAR, " +
             "CONSTRAINT pk PRIMARY KEY (HOSTNAME))" +
             "DATA_BLOCK_ENCODING='%s', COMPRESSION='%s'"
             * */
            Map<String, Set<String>> cachedData = cacheManager.getHostedAppsCache();  /**
             本地缓存
             */
            Map<String, Set<String>> dataToSync = new HashMap<>();

            if (cachedData != null && !cachedData.isEmpty()) {

                for (Map.Entry<String, Set<String>> cacheEntry : cachedData.entrySet()) {
                    // No persistence / stale data in store
                    if (persistedData == null || persistedData.isEmpty() ||
                            !persistedData.containsKey(cacheEntry.getKey()) ||
                            !persistedData.get(cacheEntry.getKey()).containsAll(cacheEntry.getValue())) {
                        dataToSync.put(cacheEntry.getKey(), cacheEntry.getValue());
                    }/**
                     把已经存储好的hosts-app关系捞出来，然后本地缓存的这些跟存储好的做下比对，
                     存过的不需要再存了，没存的要挑出来，存储
                     */
                }
                try {
                    cacheManager.persistHostedAppsMetadata(dataToSync);
                    cacheManager.markSuccessOnSyncHostedAppsMetadata();

                } catch (SQLException e) {
                    LOG.warn("Error persisting hosted apps metadata.", e);
                }
            }

        }
    }
}
