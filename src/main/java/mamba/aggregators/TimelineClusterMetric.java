package mamba.aggregators;

/**
 * Created by dongbin on 2016/10/10.
 */
public class TimelineClusterMetric {
    private String metricName;
    private String appId;
    private String instanceId;
    private long timestamp;
    private String type;

    public TimelineClusterMetric(String metricName, String appId, String instanceId,
                                 long timestamp, String type) {
        this.metricName = metricName;
        this.appId = appId;
        this.instanceId = instanceId;
        this.timestamp = timestamp;
        this.type = type;
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

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TimelineClusterMetric that = (TimelineClusterMetric) o;

        if (timestamp != that.timestamp) return false;
        if (appId != null ? !appId.equals(that.appId) : that.appId != null)
            return false;
        if (instanceId != null ? !instanceId.equals(that.instanceId) : that.instanceId != null)
            return false;
        if (!metricName.equals(that.metricName)) return false;

        return true;
    }

    public boolean equalsExceptTime(TimelineClusterMetric metric) {
        if (!metricName.equals(metric.metricName)) return false;
        if (!appId.equals(metric.appId)) return false;
        if (instanceId != null ? !instanceId.equals(metric.instanceId) : metric.instanceId != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = metricName.hashCode();
        result = 31 * result + (appId != null ? appId.hashCode() : 0);
        result = 31 * result + (instanceId != null ? instanceId.hashCode() : 0);
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "TimelineClusterMetric{" +
                "metricName='" + metricName + '\'' +
                ", appId='" + appId + '\'' +
                ", instanceId='" + instanceId + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

}
