package mamba.entity;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;
import java.util.SortedSet;

/**
 * @author dongbin  @Date: 10/28/16
 */
public interface TimelineReader {


    final long DEFAULT_LIMIT = 100;

    TimelineEntities getEntities(String entityType,
                                 Long limit, Long windowStart, Long windowEnd, String fromId, Long fromTs,
                                 NameValuePair primaryFilter, Collection<NameValuePair> secondaryFilters,
                                 EnumSet<Field> fieldsToRetrieve) throws IOException;

    TimelineEntity getEntity(String entityId, String entityType, EnumSet<Field>
            fieldsToRetrieve) throws IOException;

    TimelineEvents getEntityTimelines(String entityType,
                                      SortedSet<String> entityIds, Long limit, Long windowStart,
                                      Long windowEnd, Set<String> eventTypes) throws IOException;


    enum Field {
        EVENTS,
        RELATED_ENTITIES,
        PRIMARY_FILTERS,
        OTHER_INFO,
        LAST_EVENT_ONLY
    }
}
