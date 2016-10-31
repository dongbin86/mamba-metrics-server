package mamba.loadsimulator.data;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by sanbing on 10/10/16.
 */
public class Metric {

    private String instanceid;
    private String hostname;
    private Map<String, String> metrics = new LinkedHashMap<String, String>();
    private String starttime;
    private String appid;
    private String metricname;

    // i don't like this ctor, but it has to be public for json deserialization
    public Metric() {
    }

    public Metric(ApplicationInstance app, String metricName, long startTime) {
        this.hostname = app.getHostName();
        this.appid = app.getAppId().getId();
        this.instanceid = app.getInstanceId();
        this.metricname = metricName;
        this.starttime = Long.toString(startTime);
    }

    public void putMetric(long timestamp, String value) {
        metrics.put(Long.toString(timestamp), value);
    }

    public String getInstanceid() {
        return instanceid;
    }

    public String getHostname() {
        return hostname;
    }

    public Map<String, String> getMetrics() {
        return metrics;
    }

    public String getStarttime() {
        return starttime;
    }

    public String getAppid() {
        return appid;
    }

    public String getMetricname() {
        return metricname;
    }
}
