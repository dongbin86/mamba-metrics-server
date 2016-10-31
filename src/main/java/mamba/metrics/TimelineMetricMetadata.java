package mamba.metrics;

import org.codehaus.jackson.annotate.JsonIgnore;

/**
 * Created by sanbing on 10/10/16.
 */
public class TimelineMetricMetadata {


    private String metricName;
    private String appId;
    private String units;
    private String type = "UNDEFINED";
    private Long seriesStartTime;
    boolean supportsAggregates = true;
    // Serialization ignored helper flag
    boolean isPersisted = false;

    // Placeholder to add more type later
    public enum MetricType {
        GAUGE,
        COUNTER,
        UNDEFINED
    }

    // Default constructor
    public TimelineMetricMetadata() {
    }

    public TimelineMetricMetadata(String metricName, String appId, String units,
                                  String type, Long seriesStartTime,
                                  boolean supportsAggregates) {
        this.metricName = metricName;
        this.appId = appId;
        this.units = units;
        this.type = type;
        this.seriesStartTime = seriesStartTime;
        this.supportsAggregates = supportsAggregates;
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    // This is the key for the webservice hence ignored.
    //@XmlElement(name = "appid")
    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getUnits() {
        return units;
    }

    public void setUnits(String units) {
        this.units = units;
    }


    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }


    public Long getSeriesStartTime() {
        return seriesStartTime;
    }

    public void setSeriesStartTime(Long seriesStartTime) {
        this.seriesStartTime = seriesStartTime;
    }


    public boolean isSupportsAggregates() {
        return supportsAggregates;
    }

    public void setSupportsAggregates(boolean supportsAggregates) {
        this.supportsAggregates = supportsAggregates;
    }

    @JsonIgnore
    public boolean isPersisted() {
        return isPersisted;
    }

    public void setIsPersisted(boolean isPersisted) {
        this.isPersisted = isPersisted;
    }

    public boolean needsToBeSynced(TimelineMetricMetadata metadata) throws MetadataException {
        if (!this.metricName.equals(metadata.getMetricName()) ||
                !this.appId.equals(metadata.getAppId())) {
            throw new MetadataException("Unexpected argument: metricName = " +
                    metadata.getMetricName() + ", appId = " + metadata.getAppId());
        }

        // Series start time should never change
        return (this.units != null && !this.units.equals(metadata.getUnits())) ||
                (this.type != null && !this.type.equals(metadata.getType())) ||
                //!this.lastRecordedTime.equals(metadata.getLastRecordedTime()) || // TODO: support
                !this.supportsAggregates == metadata.isSupportsAggregates();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TimelineMetricMetadata that = (TimelineMetricMetadata) o;

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
