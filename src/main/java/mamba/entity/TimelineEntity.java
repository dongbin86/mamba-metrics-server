package mamba.entity;

import java.util.*;

/**
 * @author dongbin  @Date: 10/28/16
 */
public class TimelineEntity implements Comparable<TimelineEntity> {

    private String entityType;
    private String entityId;
    private Long startTime;
    private List<TimelineEvent> events = new ArrayList<TimelineEvent>();
    private HashMap<String, Set<String>> relatedEntities =
            new HashMap<String, Set<String>>();
    private HashMap<String, Set<Object>> primaryFilters =
            new HashMap<String, Set<Object>>();
    private HashMap<String, Object> otherInfo =
            new HashMap<String, Object>();
    private String domainId;

    public TimelineEntity() {

    }


    public String getEntityType() {
        return entityType;
    }


    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }


    public String getEntityId() {
        return entityId;
    }


    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }


    public Long getStartTime() {
        return startTime;
    }


    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }


    public List<TimelineEvent> getEvents() {
        return events;
    }


    public void addEvent(TimelineEvent event) {
        events.add(event);
    }


    public void addEvents(List<TimelineEvent> events) {
        this.events.addAll(events);
    }


    public void setEvents(List<TimelineEvent> events) {
        this.events = events;
    }


    public Map<String, Set<String>> getRelatedEntities() {
        return relatedEntities;
    }


    public HashMap<String, Set<String>> getRelatedEntitiesJAXB() {
        return relatedEntities;
    }


    public void addRelatedEntity(String entityType, String entityId) {
        Set<String> thisRelatedEntity = relatedEntities.get(entityType);
        if (thisRelatedEntity == null) {
            thisRelatedEntity = new HashSet<String>();
            relatedEntities.put(entityType, thisRelatedEntity);
        }
        thisRelatedEntity.add(entityId);
    }


    public void addRelatedEntities(Map<String, Set<String>> relatedEntities) {
        for (Map.Entry<String, Set<String>> relatedEntity : relatedEntities.entrySet()) {
            Set<String> thisRelatedEntity =
                    this.relatedEntities.get(relatedEntity.getKey());
            if (thisRelatedEntity == null) {
                this.relatedEntities.put(
                        relatedEntity.getKey(), relatedEntity.getValue());
            } else {
                thisRelatedEntity.addAll(relatedEntity.getValue());
            }
        }
    }


    public void setRelatedEntities(
            Map<String, Set<String>> relatedEntities) {
        if (relatedEntities != null && !(relatedEntities instanceof HashMap)) {
            this.relatedEntities = new HashMap<String, Set<String>>(relatedEntities);
        } else {
            this.relatedEntities = (HashMap<String, Set<String>>) relatedEntities;
        }
    }


    public Map<String, Set<Object>> getPrimaryFilters() {
        return primaryFilters;
    }


    public HashMap<String, Set<Object>> getPrimaryFiltersJAXB() {
        return primaryFilters;
    }


    public void addPrimaryFilter(String key, Object value) {
        Set<Object> thisPrimaryFilter = primaryFilters.get(key);
        if (thisPrimaryFilter == null) {
            thisPrimaryFilter = new HashSet<Object>();
            primaryFilters.put(key, thisPrimaryFilter);
        }
        thisPrimaryFilter.add(value);
    }


    public void addPrimaryFilters(Map<String, Set<Object>> primaryFilters) {
        for (Map.Entry<String, Set<Object>> primaryFilter : primaryFilters.entrySet()) {
            Set<Object> thisPrimaryFilter =
                    this.primaryFilters.get(primaryFilter.getKey());
            if (thisPrimaryFilter == null) {
                this.primaryFilters.put(
                        primaryFilter.getKey(), primaryFilter.getValue());
            } else {
                thisPrimaryFilter.addAll(primaryFilter.getValue());
            }
        }
    }


    public void setPrimaryFilters(Map<String, Set<Object>> primaryFilters) {
        if (primaryFilters != null && !(primaryFilters instanceof HashMap)) {
            this.primaryFilters = new HashMap<String, Set<Object>>(primaryFilters);
        } else {
            this.primaryFilters = (HashMap<String, Set<Object>>) primaryFilters;
        }
    }


    public Map<String, Object> getOtherInfo() {
        return otherInfo;
    }


    public HashMap<String, Object> getOtherInfoJAXB() {
        return otherInfo;
    }


    public void addOtherInfo(String key, Object value) {
        this.otherInfo.put(key, value);
    }


    public void addOtherInfo(Map<String, Object> otherInfo) {
        this.otherInfo.putAll(otherInfo);
    }


    public void setOtherInfo(Map<String, Object> otherInfo) {
        if (otherInfo != null && !(otherInfo instanceof HashMap)) {
            this.otherInfo = new HashMap<String, Object>(otherInfo);
        } else {
            this.otherInfo = (HashMap<String, Object>) otherInfo;
        }
    }


    public String getDomainId() {
        return domainId;
    }


    public void setDomainId(String domainId) {
        this.domainId = domainId;
    }

    @Override
    public int hashCode() {
        // generated by eclipse
        final int prime = 31;
        int result = 1;
        result = prime * result + ((entityId == null) ? 0 : entityId.hashCode());
        result =
                prime * result + ((entityType == null) ? 0 : entityType.hashCode());
        result = prime * result + ((events == null) ? 0 : events.hashCode());
        result = prime * result + ((otherInfo == null) ? 0 : otherInfo.hashCode());
        result =
                prime * result
                        + ((primaryFilters == null) ? 0 : primaryFilters.hashCode());
        result =
                prime * result
                        + ((relatedEntities == null) ? 0 : relatedEntities.hashCode());
        result = prime * result + ((startTime == null) ? 0 : startTime.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        // generated by eclipse
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TimelineEntity other = (TimelineEntity) obj;
        if (entityId == null) {
            if (other.entityId != null)
                return false;
        } else if (!entityId.equals(other.entityId))
            return false;
        if (entityType == null) {
            if (other.entityType != null)
                return false;
        } else if (!entityType.equals(other.entityType))
            return false;
        if (events == null) {
            if (other.events != null)
                return false;
        } else if (!events.equals(other.events))
            return false;
        if (otherInfo == null) {
            if (other.otherInfo != null)
                return false;
        } else if (!otherInfo.equals(other.otherInfo))
            return false;
        if (primaryFilters == null) {
            if (other.primaryFilters != null)
                return false;
        } else if (!primaryFilters.equals(other.primaryFilters))
            return false;
        if (relatedEntities == null) {
            if (other.relatedEntities != null)
                return false;
        } else if (!relatedEntities.equals(other.relatedEntities))
            return false;
        if (startTime == null) {
            if (other.startTime != null)
                return false;
        } else if (!startTime.equals(other.startTime))
            return false;
        return true;
    }

    @Override
    public int compareTo(TimelineEntity other) {
        int comparison = entityType.compareTo(other.entityType);
        if (comparison == 0) {
            long thisStartTime =
                    startTime == null ? Long.MIN_VALUE : startTime;
            long otherStartTime =
                    other.startTime == null ? Long.MIN_VALUE : other.startTime;
            if (thisStartTime > otherStartTime) {
                return -1;
            } else if (thisStartTime < otherStartTime) {
                return 1;
            } else {
                return entityId.compareTo(other.entityId);
            }
        } else {
            return comparison;
        }
    }
}
