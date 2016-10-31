package mamba.util;

import org.codehaus.jackson.jaxrs.JacksonJaxbJsonProvider;
import org.codehaus.jackson.map.AnnotationIntrospector;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.xc.JaxbAnnotationIntrospector;

import javax.ws.rs.core.MediaType;

/**
 * @author dongbin  @Date: 10/29/16
 */
public class JsonProvider extends JacksonJaxbJsonProvider {

    public JsonProvider() {
        super();
    }

    @Override
    public ObjectMapper locateMapper(Class<?> type, MediaType mediaType) {
        ObjectMapper mapper = super.locateMapper(type, mediaType);
        configObjectMapper(mapper);
        return mapper;
    }

    public static void configObjectMapper(ObjectMapper mapper) {
        AnnotationIntrospector introspector = new JaxbAnnotationIntrospector();
        mapper.setAnnotationIntrospector(introspector);
        mapper.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
    }
}
