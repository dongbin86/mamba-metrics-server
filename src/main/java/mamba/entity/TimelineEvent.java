package mamba.entity;


import java.util.HashMap;
import java.util.Map;

/**
 * @author dongbin  @Date: 10/28/16
 */
public class TimelineEvent implements Comparable<TimelineEvent> {

    private long timestamp;
    private String eventType;
    private HashMap<String, Object> eventInfo = new HashMap<String, Object>();

    public TimelineEvent() {
    }


    public long getTimestamp() {
        return timestamp;
    }


    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }


    public String getEventType() {
        return eventType;
    }


    public void setEventType(String eventType) {
        this.eventType = eventType;
    }


    public Map<String, Object> getEventInfo() {
        return eventInfo;
    }


    public HashMap<String, Object> getEventInfoJAXB() {
        return eventInfo;
    }


    public void addEventInfo(String key, Object value) {
        this.eventInfo.put(key, value);
    }


    public void addEventInfo(Map<String, Object> eventInfo) {
        this.eventInfo.putAll(eventInfo);
    }


    public void setEventInfo(Map<String, Object> eventInfo) {
        if (eventInfo != null && !(eventInfo instanceof HashMap)) {
            this.eventInfo = new HashMap<String, Object>(eventInfo);
        } else {
            this.eventInfo = (HashMap<String, Object>) eventInfo;
        }
    }

    @Override
    public int compareTo(TimelineEvent other) {
        if (timestamp > other.timestamp) {
            return -1;
        } else if (timestamp < other.timestamp) {
            return 1;
        } else {
            return eventType.compareTo(other.eventType);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        TimelineEvent event = (TimelineEvent) o;

        if (timestamp != event.timestamp)
            return false;
        if (!eventType.equals(event.eventType))
            return false;
        if (eventInfo != null ? !eventInfo.equals(event.eventInfo) :
                event.eventInfo != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + eventType.hashCode();
        result = 31 * result + (eventInfo != null ? eventInfo.hashCode() : 0);
        return result;
    }

}
