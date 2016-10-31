package mamba.loadsimulator.data;

import mamba.loadsimulator.util.RandomMetricsProvider;
import mamba.loadsimulator.util.TimeStampProvider;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by sanbing on 10/10/16.
 */
public class HostMetricsGenerator {

    private Map<String, RandomMetricsProvider> metricDataProviders = new HashMap<String, RandomMetricsProvider>();
    private final TimeStampProvider tsp;
    private final ApplicationInstance id;

    public HostMetricsGenerator(ApplicationInstance id,
                                TimeStampProvider timeStamps,
                                Map<String, RandomMetricsProvider> metricDataProviders) {
        this.id = id;
        this.tsp = timeStamps;
        this.metricDataProviders = metricDataProviders;
    }

    public AppMetrics createMetrics() {
        long[] timestamps = tsp.timestampsForNextInterval();
        AppMetrics appMetrics = new AppMetrics(id, timestamps[0]);

        for (Map.Entry<String, RandomMetricsProvider> entry : metricDataProviders.entrySet()) {
            String metricName = entry.getKey();
            RandomMetricsProvider metricData = entry.getValue();

            Metric metric = appMetrics.createMetric(metricName);
            for (long timestamp : timestamps) {
                metric.putMetric(timestamp, String.valueOf(metricData.next()));
            }
            appMetrics.addMetric(metric);
        }

        return appMetrics;
    }

}
