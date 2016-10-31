package mamba.loadsimulator.data;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Created by sanbing on 10/10/16.
 */
public class AppMetrics {

    private final Collection<Metric> metrics = new ArrayList<Metric>();
    private final transient ApplicationInstance applicationId;
    private final transient long startTime;

    public AppMetrics(ApplicationInstance applicationId, long startTime) {
        this.applicationId = applicationId;
        this.startTime = startTime;
    }

    public Metric createMetric(String metricName) {
        return new Metric(applicationId, metricName, startTime);
    }

    public void addMetric(Metric metric) {
        metrics.add(metric);
    }

}
