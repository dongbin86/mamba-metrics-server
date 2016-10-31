package mamba.entity;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.map.ObjectWriter;

import java.io.IOException;

/**
 * @author dongbin  @Date: 10/28/16
 */
public class GenericObjectMapper {


    public static final ObjectReader OBJECT_READER;
    public static final ObjectWriter OBJECT_WRITER;
    private static final byte[] EMPTY_BYTES = new byte[0];

    static {
        ObjectMapper mapper = new ObjectMapper();
        OBJECT_READER = mapper.reader(Object.class);
        OBJECT_WRITER = mapper.writer();
    }


    public static byte[] write(Object o) throws IOException {
        if (o == null) {
            return EMPTY_BYTES;
        }
        return OBJECT_WRITER.writeValueAsBytes(o);
    }


    public static Object read(byte[] b) throws IOException {
        return read(b, 0);
    }


    public static Object read(byte[] b, int offset) throws IOException {
        if (b == null || b.length == 0) {
            return null;
        }
        return OBJECT_READER.readValue(b, offset, b.length - offset);
    }


    public static byte[] writeReverseOrderedLong(long l) {
        byte[] b = new byte[8];
        return writeReverseOrderedLong(l, b, 0);
    }

    public static byte[] writeReverseOrderedLong(long l, byte[] b, int offset) {
        b[offset] = (byte) (0x7f ^ ((l >> 56) & 0xff));
        for (int i = offset + 1; i < offset + 7; i++) {
            b[i] = (byte) (0xff ^ ((l >> 8 * (7 - i)) & 0xff));
        }
        b[offset + 7] = (byte) (0xff ^ (l & 0xff));
        return b;
    }


    public static long readReverseOrderedLong(byte[] b, int offset) {
        long l = b[offset] & 0xff;
        for (int i = 1; i < 8; i++) {
            l = l << 8;
            l = l | (b[offset + i] & 0xff);
        }
        return l ^ 0x7fffffffffffffffl;
    }
}
