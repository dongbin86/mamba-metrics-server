package mamba.loadsimulator.util;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonMethod;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

import java.io.IOException;

/**
 * Created by sanbing on 10/10/16.
 */
public class Json {

    private ObjectMapper myObjectMapper;

    /**
     * Creates default Json ObjectMapper that maps fields.
     */
    public Json() {
        this(false);
    }

    /**
     * Creates a Json ObjectMapper that maps fields and optionally pretty prints the
     * serialized objects.
     *
     * @param pretty a flag - if true the output will be pretty printed.
     */
    public Json(boolean pretty) {
        myObjectMapper = new ObjectMapper();
        myObjectMapper.setVisibility(JsonMethod.FIELD, JsonAutoDetect.Visibility.ANY);
        if (pretty) {
            myObjectMapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
        }
    }

    public String serialize(Object o) throws IOException {
        return myObjectMapper.writeValueAsString(o);
    }

    public <T> T deserialize(String content, Class<T> paramClass) throws IOException {
        return myObjectMapper.readValue(content, paramClass);
    }
}
