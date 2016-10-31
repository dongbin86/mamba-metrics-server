package mamba.metrics;


import java.util.Map;
import java.util.TreeMap;

/**
 * @author dongbin  @Date: 10/28/16
 */
public class TimelineMetric implements Comparable<TimelineMetric> {


    private String metricName;
    private String appId;
    private String instanceId;
    private String hostName;
    private long timestamp;
    private long startTime;
    private String type;
    private String units;
    private TreeMap<Long, Double> metricValues = new TreeMap<Long, Double>();

    // default
    public TimelineMetric() {

    }

    // copy constructor
    public TimelineMetric(TimelineMetric metric) {
        setMetricName(metric.getMetricName());
        setType(metric.getType());
        setUnits(metric.getUnits());
        setTimestamp(metric.getTimestamp());
        setAppId(metric.getAppId());
        setInstanceId(metric.getInstanceId());
        setHostName(metric.getHostName());
        setStartTime(metric.getStartTime());
        setMetricValues(new TreeMap<Long, Double>(metric.getMetricValues()));
    }


    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }


    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }


    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }


    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }


    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }


    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }


    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }


    public String getUnits() {
        return units;
    }

    public void setUnits(String units) {
        this.units = units;
    }


    public TreeMap<Long, Double> getMetricValues() {
        return metricValues;
    }

    public void setMetricValues(TreeMap<Long, Double> metricValues) {
        this.metricValues = metricValues;
    }

    public void addMetricValues(Map<Long, Double> metricValues) {
        this.metricValues.putAll(metricValues);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TimelineMetric metric = (TimelineMetric) o;

        if (!metricName.equals(metric.metricName)) return false;
        if (hostName != null ? !hostName.equals(metric.hostName) : metric.hostName != null)
            return false;
        if (appId != null ? !appId.equals(metric.appId) : metric.appId != null)
            return false;
        if (instanceId != null ? !instanceId.equals(metric.instanceId) : metric.instanceId != null)
            return false;
        if (timestamp != metric.timestamp) return false;
        if (startTime != metric.startTime) return false;

        return true;
    }

    public boolean equalsExceptTime(TimelineMetric metric) {
        if (!metricName.equals(metric.metricName)) return false;
        if (hostName != null ? !hostName.equals(metric.hostName) : metric.hostName != null)
            return false;
        if (appId != null ? !appId.equals(metric.appId) : metric.appId != null)
            return false;
        if (instanceId != null ? !instanceId.equals(metric.instanceId) : metric.instanceId != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = metricName.hashCode();
        result = 31 * result + (appId != null ? appId.hashCode() : 0);
        result = 31 * result + (instanceId != null ? instanceId.hashCode() : 0);
        result = 31 * result + (hostName != null ? hostName.hashCode() : 0);
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

    @Override
    public int compareTo(TimelineMetric other) {
        if (timestamp > other.timestamp) {
            return -1;
        } else if (timestamp < other.timestamp) {
            return 1;
        } else {
            return metricName.compareTo(other.metricName);
        }
    }

    @Override
    public String toString() {
        return "TimelineMetric{" +
                "metricName='" + metricName + '\'' +
                ", appId='" + appId + '\'' +
                ", instanceId='" + instanceId + '\'' +
                ", hostName='" + hostName + '\'' +
                ", timestamp=" + timestamp +
                ", startTime=" + startTime +
                ", type='" + type + '\'' +
                ", units='" + units + '\'' +
                ", metricValues=" + metricValues +
                '}';
    }
}
