package io.dogy.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ByteUtil {

    public static String toString(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static byte[] toBytes(String value) {
        if (value == null) {
            return null;
        }
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public static Long toLong(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return Longs.fromByteArray(bytes);
    }

    public static byte[] toBytes(Long value) {
        if (value == null) {
            return null;
        }
        return Longs.toByteArray(value);
    }

    public static byte[] toBytes(float value) {
        byte[] bytes = new byte[8];
        ByteBuffer.wrap(bytes).putFloat(value);
        return bytes;
    }

    public static Float toFloat(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return ByteBuffer.wrap(bytes).getFloat();
    }

    public static Integer toInteger(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return Ints.fromByteArray(bytes);
    }

    public static byte[] toBytes(Integer value) {
        if (value == null) {
            return null;
        }
        return Ints.toByteArray(value);
    }

    public static boolean toBoolean(byte[] bytes) {
        if (bytes == null) {
            return false;
        }
        if (bytes.length != 1) {
            throw new IllegalArgumentException("Array has wrong size: " + bytes.length);
        }
        return bytes[0] != (byte) 0;
    }

    public static byte[] toBytes(Boolean value) {
        if (value == null) {
            return null;
        }
        return new byte[]{value ? (byte) 1 : 0};
    }

    public static byte[] toBytes(Object object) throws JsonProcessingException {
        if (object == null) {
            return null;
        }
        return Util.OBJECT_MAPPER.writeValueAsBytes(object);
    }

    public static <T> T toPOJO(byte[] bytes, Class<T> clazz) throws IOException {
        if (bytes == null) {
            return null;
        }
        return Util.OBJECT_MAPPER.readValue(bytes, clazz);
    }

    public static <T> T toPOJO(byte[] bytes, TypeReference<T> type) throws IOException {
        if (bytes == null) {
            return null;
        }
        return Util.OBJECT_MAPPER.readValue(bytes, type);
    }

    public static <T> T toPOJO(byte[] bytes, JavaType type) throws IOException {
        if (bytes == null) {
            return null;
        }
        return Util.OBJECT_MAPPER.readValue(bytes, type);
    }

}
