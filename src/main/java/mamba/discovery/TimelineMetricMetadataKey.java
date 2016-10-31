package mamba.discovery;
/**
 * Created by dongbin on 2016/10/10.
 */
public class TimelineMetricMetadataKey {

    String metricName;

    String appId;
    /**
     * 用于标记metrics的key,用两个字段
     * */
    public TimelineMetricMetadataKey(String metricName, String appId) {
        this.metricName = metricName;
        this.appId = appId;
    }

    public String getMetricName() {
        return metricName;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TimelineMetricMetadataKey that = (TimelineMetricMetadataKey) o;

        if (!metricName.equals(that.metricName)) return false;
        return !(appId != null ? !appId.equals(that.appId) : that.appId != null);

    }

    @Override
    public int hashCode() {
        int result = metricName.hashCode();
        result = 31 * result + (appId != null ? appId.hashCode() : 0);
        return result;
    }

}
