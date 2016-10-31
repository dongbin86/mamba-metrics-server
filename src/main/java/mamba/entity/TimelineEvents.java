package mamba.entity;


import java.util.ArrayList;
import java.util.List;

/**
 * @author dongbin  @Date: 10/28/16
 */
public class TimelineEvents {

    private List<EventsOfOneEntity> allEvents =
            new ArrayList<EventsOfOneEntity>();

    public TimelineEvents() {

    }


    public List<EventsOfOneEntity> getAllEvents() {
        return allEvents;
    }


    public void addEvent(EventsOfOneEntity eventsOfOneEntity) {
        allEvents.add(eventsOfOneEntity);
    }


    public void addEvents(List<EventsOfOneEntity> allEvents) {
        this.allEvents.addAll(allEvents);
    }

    public void setEvents(List<EventsOfOneEntity> allEvents) {
        this.allEvents.clear();
        this.allEvents.addAll(allEvents);
    }

    public static class EventsOfOneEntity {

        private String entityId;
        private String entityType;
        private List<TimelineEvent> events = new ArrayList<TimelineEvent>();

        public EventsOfOneEntity() {

        }


        public String getEntityId() {
            return entityId;
        }


        public void setEntityId(String entityId) {
            this.entityId = entityId;
        }


        public String getEntityType() {
            return entityType;
        }


        public void setEntityType(String entityType) {
            this.entityType = entityType;
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

    }
}
