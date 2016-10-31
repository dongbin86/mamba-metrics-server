package mamba.discovery;

import mamba.metrics.MetadataException;
import mamba.metrics.TimelineMetric;
import mamba.metrics.TimelineMetricMetadata;
import mamba.store.PhoenixHBaseAccessor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static mamba.store.TimelineMetricConfiguration.*;

/**
 * Created by dongbin on 2016/10/10.
 */
public class TimelineMetricMetadataManager {
    private static final Log LOG = LogFactory.getLog(TimelineMetricMetadataManager.class);
    // Cache all metadata on retrieval
    private final Map<TimelineMetricMetadataKey, TimelineMetricMetadata> METADATA_CACHE = new ConcurrentHashMap<>();
    // Map to lookup apps on a host
    private final Map<String, Set<String>> HOSTED_APPS_MAP = new ConcurrentHashMap<>();
    // Single thread to sync back new writes to the store
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    // Sync only when needed
    AtomicBoolean SYNC_HOSTED_APPS_METADATA = new AtomicBoolean(false);
    private boolean isDisabled = false;
    private PhoenixHBaseAccessor hBaseAccessor;
    private Configuration metricsConf;

    public TimelineMetricMetadataManager(PhoenixHBaseAccessor hBaseAccessor,
                                         Configuration metricsConf) {
        this.hBaseAccessor = hBaseAccessor;
        this.metricsConf = metricsConf;
    }

    /**
     * Initialize Metadata from the store
     */
    public void initializeMetadata() {
        if (metricsConf.getBoolean(DISABLE_METRIC_METADATA_MGMT, false)) {
            isDisabled = true;
        } else {
            // Schedule the executor to sync to store
            executorService.scheduleWithFixedDelay(new TimelineMetricMetadataSync(this),
                    metricsConf.getInt(METRICS_METADATA_SYNC_INIT_DELAY, 120), // 2 minutes
                    metricsConf.getInt(METRICS_METADATA_SYNC_SCHEDULE_DELAY, 300), // 5 minutes
                    TimeUnit.SECONDS);
            // Read from store and initialize map
            try {
                Map<TimelineMetricMetadataKey, TimelineMetricMetadata> metadata =
                        hBaseAccessor.getTimelineMetricMetadata();

                LOG.info("Retrieved " + metadata.size() + ", metadata objects from store.");
                // Store in the cache
                METADATA_CACHE.putAll(metadata);

                Map<String, Set<String>> hostedAppData = hBaseAccessor.getHostedAppsMetadata();

                LOG.info("Retrieved " + hostedAppData.size() + " host objects from store.");
                HOSTED_APPS_MAP.putAll(hostedAppData);

            } catch (SQLException e) {
                LOG.warn("Exception loading metric metadata", e);
            }
        }
    }

    public Map<TimelineMetricMetadataKey, TimelineMetricMetadata> getMetadataCache() {
        return METADATA_CACHE;
    }

    public TimelineMetricMetadata getMetadataCacheValue(TimelineMetricMetadataKey key) {
        return METADATA_CACHE.get(key);
    }

    public Map<String, Set<String>> getHostedAppsCache() {
        return HOSTED_APPS_MAP;
    }

    public boolean syncHostedAppsMetadata() {
        return SYNC_HOSTED_APPS_METADATA.get();
    }

    public void markSuccessOnSyncHostedAppsMetadata() {
        SYNC_HOSTED_APPS_METADATA.set(false);
    }

    /**
     * Update value in metadata cache
     *
     * @param metadata @TimelineMetricMetadata
     */
    public void putIfModifiedTimelineMetricMetadata(TimelineMetricMetadata metadata) {
        TimelineMetricMetadataKey key = new TimelineMetricMetadataKey(
                metadata.getMetricName(), metadata.getAppId());

        TimelineMetricMetadata metadataFromCache = METADATA_CACHE.get(key);

        if (metadataFromCache != null) {
            try {
                if (metadataFromCache.needsToBeSynced(metadata)) {
                    metadata.setIsPersisted(false); // Set the flag to ensure sync to store on next run
                    METADATA_CACHE.put(key, metadata);
                }
            } catch (MetadataException e) {
                LOG.warn("Error inserting Metadata in cache.", e);
            }

        } else {
            METADATA_CACHE.put(key, metadata);
        }
    }

    /**
     * Update value in hosted apps cache
     *
     * @param hostname Host name
     * @param appId    Application Id
     */
    public void putIfModifiedHostedAppsMetadata(String hostname, String appId) {
        Set<String> apps = HOSTED_APPS_MAP.get(hostname);
        if (apps == null) {
            apps = new HashSet<>();
            HOSTED_APPS_MAP.put(hostname, apps);
        }

        if (!apps.contains(appId)) {
            apps.add(appId);
            SYNC_HOSTED_APPS_METADATA.set(true);
        }
    }

    public void persistMetadata(Collection<TimelineMetricMetadata> metadata) throws SQLException {
        hBaseAccessor.saveMetricMetadata(metadata);
    }

    public void persistHostedAppsMetadata(Map<String, Set<String>> hostedApps) throws SQLException {
        hBaseAccessor.saveHostAppsMetadata(hostedApps);
    }

    public TimelineMetricMetadata getTimelineMetricMetadata(TimelineMetric timelineMetric) {
        return new TimelineMetricMetadata(
                timelineMetric.getMetricName(),
                timelineMetric.getAppId(),
                timelineMetric.getUnits(),
                timelineMetric.getType(),
                timelineMetric.getStartTime(),
                true
        );
    }

    /**
     * Fetch hosted apps from store
     *
     * @throws java.sql.SQLException
     */
    Map<String, Set<String>> getPersistedHostedAppsData() throws SQLException {
        return hBaseAccessor.getHostedAppsMetadata();
    }

    public boolean isDisabled() {
        return isDisabled;
    }
}
