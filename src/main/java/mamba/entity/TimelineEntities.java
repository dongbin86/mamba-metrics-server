package mamba.entity;

import java.util.ArrayList;
import java.util.List;

/**
 * @author dongbin  @Date: 10/28/16
 */
public class TimelineEntities {

    private List<TimelineEntity> entities =
            new ArrayList<TimelineEntity>();

    public TimelineEntities() {

    }


    public List<TimelineEntity> getEntities() {
        return entities;
    }


    public void addEntity(TimelineEntity entity) {
        entities.add(entity);
    }


    public void addEntities(List<TimelineEntity> entities) {
        this.entities.addAll(entities);
    }


    public void setEntities(List<TimelineEntity> entities) {
        this.entities = entities;
    }
}
