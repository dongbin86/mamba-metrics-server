package mamba.cache;


import mamba.metrics.Metric;
import mamba.metrics.Metrics;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by sanbing on 10/10/16.
 */
public class MetricsCache {

    private final MetricHolder metricHolder = new MetricHolder();
    private static final Log LOG = LogFactory.getLog(MetricsCache.class);
    public static final int MAX_RECS_PER_NAME_DEFAULT = 10000;
    public static final int MAX_EVICTION_TIME_MILLIS = 59000; // ~ 1 min
    private final int maxRecsPerName;
    private final int maxEvictionTimeInMillis;
    private boolean skipCounterTransform = true;
    private final Map<String, Double> counterMetricLastValue = new HashMap<String, Double>();

    public MetricsCache(int maxRecsPerName, int maxEvictionTimeInMillis) {
        this(maxRecsPerName, maxEvictionTimeInMillis, false);
    }

    public MetricsCache(int maxRecsPerName, int maxEvictionTimeInMillis,
                        boolean skipCounterTransform) {
        this.maxRecsPerName = maxRecsPerName;
        this.maxEvictionTimeInMillis = maxEvictionTimeInMillis;
        this.skipCounterTransform = skipCounterTransform;
    }

    class MetricWrapper {
        private long timeDiff = -1;
        private long oldestTimestamp = -1;
        private Metric metric;

        MetricWrapper(Metric metric) {
            this.metric = metric;
            this.oldestTimestamp = metric.getStartTime();
        }

        private void updateTimeDiff(long timestamp) {
            if (oldestTimestamp != -1 && timestamp > oldestTimestamp) {
                timeDiff = timestamp - oldestTimestamp;
            } else {
                oldestTimestamp = timestamp;
            }
        }

        public synchronized void putMetric(Metric metric) {
            TreeMap<Long, Double> metricValues = this.metric.getMetricValues();
            if (metricValues.size() > maxRecsPerName) {
                // remove values for eldest maxEvictionTimeInMillis
                long newEldestTimestamp = oldestTimestamp + maxEvictionTimeInMillis;
                TreeMap<Long, Double> metricsSubSet =
                        new TreeMap<>(metricValues.tailMap(newEldestTimestamp));
                if (metricsSubSet.isEmpty()) {
                    oldestTimestamp = metric.getStartTime();
                    this.metric.setStartTime(metric.getStartTime());
                } else {
                    Long newStartTime = metricsSubSet.firstKey();
                    oldestTimestamp = newStartTime;
                    this.metric.setStartTime(newStartTime);
                }
                this.metric.setMetricValues(metricsSubSet);
                LOG.warn("Metrics cache overflow. Values for metric " +
                        metric.getMetricName() + " older than " + newEldestTimestamp +
                        " were removed to clean up the cache.");
            }
            this.metric.addMetricValues(metric.getMetricValues());
            updateTimeDiff(metric.getStartTime());
        }

        public synchronized long getTimeDiff() {
            return timeDiff;
        }

        public synchronized Metric getMetric() {
            return metric;
        }
    }

    // TODO: Add weighted eviction
    class MetricHolder extends ConcurrentSkipListMap<String, MetricWrapper> {
        private static final long serialVersionUID = 2L;
        // To avoid duplication at the end of the buffer and beginning of the next
        // segment of values
        private Map<String, Long> endOfBufferTimestamps = new HashMap<String, Long>();

        public Metric evict(String metricName) {
            MetricWrapper metricWrapper = this.get(metricName);

            if (metricWrapper == null
                    || metricWrapper.getTimeDiff() < getMaxEvictionTimeInMillis()) {
                return null;
            }

            Metric metric = metricWrapper.getMetric();
            this.remove(metricName);

            return metric;
        }

        public Metrics evictAll() {
            List<Metric> metricList = new ArrayList<Metric>();

            for (Iterator<Entry<String, MetricWrapper>> it = this.entrySet().iterator(); it.hasNext(); ) {
                Entry<String, MetricWrapper> cacheEntry = it.next();
                MetricWrapper metricWrapper = cacheEntry.getValue();
                if (metricWrapper != null) {
                    Metric metric = cacheEntry.getValue().getMetric();
                    metricList.add(metric);
                }
                it.remove();
            }
            Metrics metrics = new Metrics();
            metrics.setMetrics(metricList);
            return metrics;
        }

        public void put(String metricName, Metric timelineMetric) {
            if (isDuplicate(timelineMetric)) {
                return;
            }
            MetricWrapper metric = this.get(metricName);
            if (metric == null) {
                this.put(metricName, new MetricWrapper(timelineMetric));
            } else {
                metric.putMetric(timelineMetric);
            }
            // Buffer last ts value
            endOfBufferTimestamps.put(metricName, timelineMetric.getStartTime());
        }

        /**
         * Test whether last buffered timestamp is same as the newly received.
         *
         * @param metric @Metric
         * @return true/false
         */
        private boolean isDuplicate(Metric metric) {
            return endOfBufferTimestamps.containsKey(metric.getMetricName())
                    && endOfBufferTimestamps.get(metric.getMetricName()).equals(metric.getStartTime());
        }
    }

    public Metric getTimelineMetric(String metricName) {
        if (metricHolder.containsKey(metricName)) {
            return metricHolder.evict(metricName);
        }

        return null;
    }

    public Metrics getAllMetrics() {
        return metricHolder.evictAll();
    }

    /**
     * Getter method to help testing eviction
     *
     * @return @int
     */
    public int getMaxEvictionTimeInMillis() {
        return maxEvictionTimeInMillis;
    }

    public void putTimelineMetric(Metric metric) {
        metricHolder.put(metric.getMetricName(), metric);
    }

    private void transformMetricValuesToDerivative(Metric metric) {
        String metricName = metric.getMetricName();
        double firstValue = metric.getMetricValues().size() > 0
                ? metric.getMetricValues().entrySet().iterator().next().getValue() : 0;
        Double value = counterMetricLastValue.get(metricName);
        double previousValue = value != null ? value : firstValue;
        Map<Long, Double> metricValues = metric.getMetricValues();
        TreeMap<Long, Double> newMetricValues = new TreeMap<Long, Double>();
        for (Map.Entry<Long, Double> entry : metricValues.entrySet()) {
            newMetricValues.put(entry.getKey(), entry.getValue() - previousValue);
            previousValue = entry.getValue();
        }
        metric.setMetricValues(newMetricValues);
        counterMetricLastValue.put(metricName, previousValue);
    }

    public void putTimelineMetric(Metric metric, boolean isCounter) {
        if (isCounter && !skipCounterTransform) {
            transformMetricValuesToDerivative(metric);
        }
        putTimelineMetric(metric);
    }
}
