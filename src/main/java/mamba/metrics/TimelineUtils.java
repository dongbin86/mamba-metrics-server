package mamba.metrics;

import mamba.util.JsonProvider;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;


/**
 * @author dongbin  @Date: 10/29/16
 */
public class TimelineUtils {

    private static ObjectMapper mapper;

    static {
        mapper = new ObjectMapper();
        JsonProvider.configObjectMapper(mapper);
    }

    public static String dumpTimelineRecordtoJSON(Object o)
            throws JsonGenerationException, JsonMappingException, IOException {
        return dumpTimelineRecordtoJSON(o, false);
    }

    public static String dumpTimelineRecordtoJSON(Object o, boolean pretty)
            throws JsonGenerationException, JsonMappingException, IOException {
        if (pretty) {
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(o);
        } else {
            return mapper.writeValueAsString(o);
        }
    }


}
