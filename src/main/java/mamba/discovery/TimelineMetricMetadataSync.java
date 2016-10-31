package mamba.discovery;

import mamba.metrics.TimelineMetricMetadata;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.SQLException;
import java.util.*;

/**
 * Created by dongbin on 2016/10/10.
 */
public class TimelineMetricMetadataSync implements Runnable {
    private static final Log LOG = LogFactory.getLog(TimelineMetricMetadataSync.class);

    private final TimelineMetricMetadataManager cacheManager;

    public TimelineMetricMetadataSync(TimelineMetricMetadataManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    @Override
    public void run() {
        List<TimelineMetricMetadata> metadataToPersist = new ArrayList<>();
        // Find all entries to persist
        for (TimelineMetricMetadata metadata : cacheManager.getMetadataCache().values()) {
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
                markSuccess = true;
            } catch (SQLException e) {
                LOG.warn("Error persisting metadata.", e);
            }
        }
        // Mark corresponding entries as persisted to skip on next run
        if (markSuccess) {
            for (TimelineMetricMetadata metadata : metadataToPersist) {
                TimelineMetricMetadataKey key = new TimelineMetricMetadataKey(
                        metadata.getMetricName(), metadata.getAppId()
                );

                // Mark entry as being persisted
                metadata.setIsPersisted(true);
                // Update cache
                cacheManager.getMetadataCache().put(key, metadata);
            }
        }/**重新标记已经持久化，不看时间戳吗？*/
        // Sync hosted apps data is needed
        if (cacheManager.syncHostedAppsMetadata()) {
            Map<String, Set<String>> persistedData = null;
            try {
                persistedData = cacheManager.getPersistedHostedAppsData();
            } catch (SQLException e) {
                LOG.warn("Failed on fetching hosted apps data from store.", e);
                return; // Something wrong with store
            }

            Map<String, Set<String>> cachedData = cacheManager.getHostedAppsCache();
            Map<String, Set<String>> dataToSync = new HashMap<>();
            if (cachedData != null && !cachedData.isEmpty()) {
                for (Map.Entry<String, Set<String>> cacheEntry : cachedData.entrySet()) {
                    // No persistence / stale data in store
                    if (persistedData == null || persistedData.isEmpty() ||
                            !persistedData.containsKey(cacheEntry.getKey()) ||
                            !persistedData.get(cacheEntry.getKey()).containsAll(cacheEntry.getValue())) {
                        dataToSync.put(cacheEntry.getKey(), cacheEntry.getValue());
                    }
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
