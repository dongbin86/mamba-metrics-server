package mamba.store;

import mamba.metrics.Metric;
import mamba.metrics.Metrics;
import mamba.metrics.Precision;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ExitUtil;

import java.util.Collections;
import java.util.concurrent.*;

/**
 * Created by dongbin on 2016/10/10.
 */
public class MetricStoreWatcher implements Runnable {

    private static final Log LOG = LogFactory.getLog(MetricStoreWatcher.class);
    private static final String FAKE_METRIC_NAME = "MetricStoreWatcher.FakeMetric";
    private static final String FAKE_HOSTNAME = "fakehostname";
    private static final String FAKE_APP_ID = "timeline_metric_store_watcher";

    private static int failures = 0;
    private final MetricConfiguration configuration;

    private MetricStore metricStore;

    //used to call metricStore blocking methods with timeout
    private ExecutorService executor = Executors.newSingleThreadExecutor();


    public MetricStoreWatcher(MetricStore metricStore,
                              MetricConfiguration configuration) {
        this.metricStore = metricStore;
        this.configuration = configuration;
    }

    @Override
    public void run() {
        if (checkMetricStore()) {
            failures = 0;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Successfully got metrics from MetricStore");
            }
        } else {
            LOG.info("Failed to get metrics from MetricStore, attempt = " + failures);
            failures++;
        }

        if (failures >= configuration.getTimelineMetricsServiceWatcherMaxFailures()) {
            String msg = "Error getting metrics from MetricStore. " +
                    "Shutting down by MetricStoreWatcher.";
            LOG.fatal(msg);
            ExitUtil.terminate(-1, msg);
        }

    }

    /**
     * Checks MetricStore functionality by adding and getting
     * a fake metric to/from HBase
     *
     * @return if check was successful
     */
    private boolean checkMetricStore() {
        final long startTime = System.currentTimeMillis();
        final int delay = configuration.getTimelineMetricsServiceWatcherDelay();
        final int timeout = configuration.getTimelineMetricsServiceWatcherTimeout();

        Metric fakeMetric = new Metric();
        fakeMetric.setMetricName(FAKE_METRIC_NAME);
        fakeMetric.setHostName(FAKE_HOSTNAME);
        fakeMetric.setAppId(FAKE_APP_ID);
        fakeMetric.setStartTime(startTime);
        fakeMetric.setTimestamp(startTime);
        fakeMetric.getMetricValues().put(startTime, 0.0);

        final Metrics metrics = new Metrics();
        metrics.setMetrics(Collections.singletonList(fakeMetric));

        Callable<Metric> task = new Callable<Metric>() {
            public Metric call() throws Exception {
                metricStore.putMetrics(metrics);
                Metrics metrics = metricStore.getTimelineMetrics(
                        Collections.singletonList(FAKE_METRIC_NAME), Collections.singletonList(FAKE_HOSTNAME),
                        FAKE_APP_ID, null, startTime - delay * 2 * 1000,
                        startTime + delay * 2 * 1000, Precision.SECONDS, 1, true, null, null);
                return metrics.getMetrics().get(0);
            }
        };

        Future<Metric> future = executor.submit(task);
        Metric metric = null;
        try {
            metric = future.get(timeout, TimeUnit.SECONDS);
            // Phoenix might throw RuntimeExeption's
        } catch (Exception e) {
            return false;
        } finally {
            future.cancel(true);
        }

        return metric != null;
    }

}
