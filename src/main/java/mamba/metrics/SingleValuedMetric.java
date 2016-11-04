package mamba.metrics;

/**
 * @author dongbin  @Date: 10/28/16
 */
public class SingleValuedMetric {

    private Long timestamp;
    private Double value;
    private String metricName;
    private String appId;
    private String instanceId;
    private String hostName;
    private Long startTime;
    private String type;

    public void setSingleTimeseriesValue(Long timestamp, Double value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public SingleValuedMetric(String metricName, String appId,
                              String instanceId, String hostName,
                              long timestamp, long startTime, String type) {
        this.metricName = metricName;
        this.appId = appId;
        this.instanceId = instanceId;
        this.hostName = hostName;
        this.timestamp = timestamp;
        this.startTime = startTime;
        this.type = type;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public long getStartTime() {
        return startTime;
    }

    public String getType() {
        return type;
    }

    public Double getValue() {
        return value;
    }

    public String getMetricName() {
        return metricName;
    }

    public String getAppId() {
        return appId;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public String getHostName() {
        return hostName;
    }

    public boolean equalsExceptTime(Metric metric) {
        if (!metricName.equals(metric.getMetricName())) return false;
        if (hostName != null ? !hostName.equals(metric.getHostName()) : metric.getHostName() != null)
            return false;
        if (appId != null ? !appId.equals(metric.getAppId()) : metric.getAppId() != null)
            return false;
        if (instanceId != null ? !instanceId.equals(metric.getInstanceId()) : metric.getInstanceId() != null)
            return false;

        return true;
    }

    public Metric getTimelineMetric() {
        Metric metric = new Metric();
        metric.setMetricName(this.metricName);
        metric.setAppId(this.appId);
        metric.setHostName(this.hostName);
        metric.setType(this.type);
        metric.setInstanceId(this.instanceId);
        metric.setStartTime(this.startTime);
        metric.setTimestamp(this.timestamp);
        metric.getMetricValues().put(timestamp, value);
        return metric;
    }

}
