package mamba.entity;

import java.io.IOException;

/**
 * @author dongbin  @Date: 10/28/16
 */
public interface TimelineWriter {

    TimelinePutResponse put(TimelineEntities data) throws IOException;
}
