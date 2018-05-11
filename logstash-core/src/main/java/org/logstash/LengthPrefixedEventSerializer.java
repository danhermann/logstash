package org.logstash;

import org.joda.time.DateTime;
import org.jruby.*;
import org.jruby.ext.bigdecimal.RubyBigDecimal;
import org.jruby.java.proxies.ArrayJavaProxy;
import org.jruby.java.proxies.ConcreteJavaProxy;
import org.jruby.java.proxies.MapJavaProxy;
import org.logstash.ext.JrubyTimestampExtLibrary;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.logstash.LengthPrefixedEventSerializer.LongSerializer.LONG_WIDTH;
import static org.logstash.LengthPrefixedEventSerializer.LongSerializer.longFromBytes;
import static org.logstash.LengthPrefixedEventSerializer.LongSerializer.longToBytes;

public final class LengthPrefixedEventSerializer {

    private static final byte VERSION = 1;
    private static final String METADATA = "@metadata";
    static Map<Class, Byte> TYPE_TO_BYTE = new HashMap<>();
    static Map<Byte, SerializationFunction<?>> SERIALIZERS = new HashMap<>();
    static Map<Byte, DeserializationFunction> DESERIALIZERS = new HashMap<>();
    private ByteArrayOutputStream serializationBuffer = new ByteArrayOutputStream(10240);

    static {
        TYPE_TO_BYTE.put(RubyString.class, (byte) 1);
        TYPE_TO_BYTE.put(RubyNil.class, (byte) 2);
        TYPE_TO_BYTE.put(RubySymbol.class, (byte) 3);
        TYPE_TO_BYTE.put(RubyFixnum.class, (byte) 4);
        TYPE_TO_BYTE.put(JrubyTimestampExtLibrary.RubyTimestamp.class, (byte) 5);
        TYPE_TO_BYTE.put(RubyFloat.class, (byte) 6);
        TYPE_TO_BYTE.put(ConvertedMap.class, (byte) 7);
        TYPE_TO_BYTE.put(ConvertedList.class, (byte) 8);
        TYPE_TO_BYTE.put(RubyBoolean.class, (byte) 9);
        TYPE_TO_BYTE.put(RubyBignum.class, (byte) 10);
        TYPE_TO_BYTE.put(RubyBigDecimal.class, (byte) 11);
        TYPE_TO_BYTE.put(String.class, (byte) 12);
        TYPE_TO_BYTE.put(Float.class, (byte) 13);
        TYPE_TO_BYTE.put(Double.class, (byte) 14);
        TYPE_TO_BYTE.put(BigInteger.class, (byte) 15);
        TYPE_TO_BYTE.put(BigDecimal.class, (byte) 16);
        TYPE_TO_BYTE.put(Long.class, (byte) 17);
        TYPE_TO_BYTE.put(Integer.class, (byte) 18);
        TYPE_TO_BYTE.put(Boolean.class, (byte) 19);
        TYPE_TO_BYTE.put(Timestamp.class, (byte) 20);
        TYPE_TO_BYTE.put(RubyTime.class, (byte) 21);
        TYPE_TO_BYTE.put(DateTime.class, (byte) 22);
        TYPE_TO_BYTE.put(RubyHash.class, (byte) 23);
        TYPE_TO_BYTE.put(Map.class, (byte) 24);
        TYPE_TO_BYTE.put(HashMap.class, (byte) 24);
        TYPE_TO_BYTE.put(List.class, (byte) 25);
        TYPE_TO_BYTE.put(ArrayJavaProxy.class, (byte) 26);
        TYPE_TO_BYTE.put(ConcreteJavaProxy.class, (byte) 27);
        TYPE_TO_BYTE.put(MapJavaProxy.class, (byte) 28);
        TYPE_TO_BYTE.put(RubyArray.class, (byte) 29);

        /*
        SERIALIZERS.put((byte)1, RubyStringSerializer::serialize);
        DESERIALIZERS.put((byte)1, RubyStringSerializer::deserialize);

        SERIALIZERS.put((byte)12, StringSerializer::serialize);
        DESERIALIZERS.put((byte)12, StringSerializer::deserialize);
        */
    }

    public byte[] serialize(Event e) {
        serializationBuffer.reset();
        serializationBuffer.write(VERSION); // header
        MapSerializer.serialize(e.getData(), serializationBuffer);
        MapSerializer.serialize(e.getMetadata(), serializationBuffer); // does this serialize metadata twice?
        return serializationBuffer.toByteArray();
    }

    public Event deserialize(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return new Event();
        }

        // header
        if (bytes[0] != VERSION) {
            throw new IllegalStateException("Unknown serialization version: " + bytes[0]);
        }

        // data map
        DeserializationResult result = MapSerializer.deserialize(bytes, 1);
        Map<String, Object> dataMap = (Map<String, Object>) result.object;

        // metadata map
        result = MapSerializer.deserialize(bytes, result.pos);
        Map<String, Object> metadataMap = (Map<String, Object>) result.object;

        dataMap.put(METADATA, metadataMap);
        return new Event(dataMap);
    }

    static void serialize(Object object, ByteArrayOutputStream bos) {
        byte typeCode = TYPE_TO_BYTE.get(object.getClass());
        if (typeCode == (byte) 1) {
            RubyStringSerializer.serialize((RubyString) object, bos);
        } else if (typeCode == (byte) 4) {
            RubyFixNumSerializer.serialize((RubyFixnum) object, bos);
        } else if (typeCode == (byte) 5) {
            RubyTimestampSerializer.serialize((JrubyTimestampExtLibrary.RubyTimestamp) object, bos);
        } else if (typeCode == (byte) 12) {
            StringSerializer.serialize((String) object, bos);
        } else if (typeCode == (byte) 17) {
            LongSerializer.serialize((Long) object, bos);
        } else if (typeCode == (byte) 18) {
            IntegerSerializer.serialize((Integer) object, bos);
        } else if (typeCode == (byte) 20) {
            TimestampSerializer.serialize((Timestamp) object, bos);
        } else if (typeCode == (byte) 24) {
            MapSerializer.serialize((Map) object, bos);
        } else {
            throw new IllegalStateException("Unknown type: " + object.getClass().getName());
        }
    }

    static DeserializationResult deserialize(byte[] bytes, int pos) {
        byte typeCode = bytes[pos];
        DeserializationResult result;
        if (typeCode == (byte) 1) {
            result = RubyStringSerializer.deserialize(bytes, pos);
        } else if (typeCode == (byte) 4) {
            result = RubyFixNumSerializer.deserialize(bytes, pos);
        } else if (typeCode == (byte) 5) {
            result = RubyTimestampSerializer.deserialize(bytes, pos);
        } else if (typeCode == (byte) 12) {
            result = StringSerializer.deserialize(bytes, pos);
        } else if (typeCode == (byte) 17) {
            result = LongSerializer.deserialize(bytes, pos);
        } else if (typeCode == (byte) 18) {
            result = IntegerSerializer.deserialize(bytes, pos);
        } else if (typeCode == (byte) 20) {
            result = TimestampSerializer.deserialize(bytes, pos);
        } else if (typeCode == (byte) 24) {
            result = MapSerializer.deserialize(bytes, pos);
        } else {
            throw new IllegalStateException("Unknown type code: " + typeCode);
        }

        return result;
    }

    static class MapSerializer {

        static DeserializationResult deserialize(byte[] bytes, int pos) {
            if (bytes[pos] != TYPE_TO_BYTE.get(Map.class)) {
                throw new IllegalStateException(
                        String.format("Invalid type code '%d' for serializer '%s'",
                                bytes[pos], MethodHandles.lookup().lookupClass().getName()));
            }

            // get map size as 32-bit integer
            int size = IntegerSerializer.intFromBytes(bytes, pos + 1);

            Map map = new HashMap();
            DeserializationResult keyResult, valueResult = new DeserializationResult(null, pos + 5);
            for (int k = 0; k < size; k++) {
                keyResult = LengthPrefixedEventSerializer.deserialize(bytes, valueResult.pos);
                valueResult = LengthPrefixedEventSerializer.deserialize(bytes, keyResult.pos);
                map.put(keyResult.object, valueResult.object);
            }

            return new DeserializationResult(map, size > 0 ? valueResult.pos : pos + 5);
        }

        static void serialize(Map<?,?> map, ByteArrayOutputStream bos) {
            try {
                bos.write(TYPE_TO_BYTE.get(Map.class));
                bos.write(IntegerSerializer.intToBytes(map.size()));
                for (Map.Entry entry : map.entrySet()) {
                    LengthPrefixedEventSerializer.serialize(entry.getKey(), bos);
                    LengthPrefixedEventSerializer.serialize(entry.getValue(), bos);
                }
            } catch (IOException e) {
                // ByteArrayOutputStream never throws IOException
            }
        }

    }

    static class RubyStringSerializer {

        // PoC only -- doesn't track encoding
        static DeserializationResult deserialize(byte[] bytes, int pos) {
            if (bytes[pos] != TYPE_TO_BYTE.get(RubyString.class)) {
                throw new IllegalStateException(
                        String.format("Invalid type code '%d' for serializer '%s'",
                                bytes[pos], MethodHandles.lookup().lookupClass().getName()));
            }

            // get string length as 32-bit integer
            int length = IntegerSerializer.intFromBytes(bytes, pos + 1);

            if ((pos + length + 5) > bytes.length) {
                throw new IllegalStateException(
                        String.format("Byte array too short in serializer '%s'",
                                MethodHandles.lookup().lookupClass().getName()));
            }

            try {
                String str = new String(bytes, pos + 5, length, "UTF-8");
                return new DeserializationResult(
                        new RubyString(RubyUtil.RUBY, RubyString.createStringClass(RubyUtil.RUBY), str),
                        pos + length + 5);
            } catch (UnsupportedEncodingException e) {
                throw new IllegalStateException("Unable to deserialize Ruby string", e);
            }
        }

        static void serialize(RubyString s, ByteArrayOutputStream bos) {
            try {
                bos.write(TYPE_TO_BYTE.get(RubyString.class));
                byte[] bytes = s.asJavaString().getBytes("UTF-8");
                bos.write(IntegerSerializer.intToBytes(bytes.length));
                bos.write(bytes);
            } catch (UnsupportedEncodingException e) {
                throw new IllegalStateException("Unable to serialize Ruby string", e);
            } catch (IOException e) {
                // ByteArrayOutputStream never throws IOException
            }
        }

    }

    static class StringSerializer {

        // always de/serializes to/from UTF-8
        // use a codec if a different output encoding is desired

        static DeserializationResult deserialize(byte[] bytes, int pos) {
            if (bytes[pos] != TYPE_TO_BYTE.get(String.class)) {
                throw new IllegalStateException(
                        String.format("Invalid type code '%d' for serializer '%s'",
                                bytes[pos], MethodHandles.lookup().lookupClass().getName()));
            }

            // get string length as 32-bit integer
            int length = IntegerSerializer.intFromBytes(bytes, pos + 1);

            if ((pos + length + 5) > bytes.length) {
                throw new IllegalStateException(
                        String.format("Byte array too short in serializer '%s'",
                                MethodHandles.lookup().lookupClass().getName()));
            }

            try {
                String str = new String(bytes, pos + 5, length, "UTF-8");
                return new DeserializationResult(str, pos + length + 5);
            } catch (UnsupportedEncodingException e) {
                throw new IllegalStateException("Unable to deserialize string", e);
            }
        }

        static void serialize(String s, ByteArrayOutputStream bos) {
            try {
                bos.write(TYPE_TO_BYTE.get(String.class));
                byte[] bytes = s.getBytes("UTF-8");
                bos.write(IntegerSerializer.intToBytes(bytes.length));
                bos.write(bytes);
            } catch (UnsupportedEncodingException e) {
                throw new IllegalStateException("Unable to serialize string", e);
            } catch (IOException e) {
                // ByteArrayOutputStream never throws IOException
            }
        }

    }

    static class LongSerializer {

        static final int LONG_WIDTH = 9; // 8 bytes plus type code

        static DeserializationResult deserialize(byte[] bytes, int pos) {
            if (bytes[pos] != TYPE_TO_BYTE.get(Long.class)) {
                throw new IllegalStateException(
                        String.format("Invalid type code '%d' for serializer '%s'",
                                bytes[pos], MethodHandles.lookup().lookupClass().getName()));
            }

            if ((pos + LONG_WIDTH) > bytes.length) {
                throw new IllegalStateException(
                        String.format("Byte array too short in serializer '%s'",
                                MethodHandles.lookup().lookupClass().getName()));
            }

            long longVal = longFromBytes(bytes, pos + 1);

            return new DeserializationResult(longVal, pos + LONG_WIDTH);
        }

        static void serialize(Long l, ByteArrayOutputStream bos) {
            bos.write(TYPE_TO_BYTE.get(Long.class));
            long longVal = l.longValue();
            try {
                bos.write(longToBytes(longVal));
            } catch (IOException e) {
                // ByteArrayOutputStream never throws IOException
            }
        }

        static long longFromBytes(byte[] bytes, int offset) {
            return (long) ((bytes[offset] & 0xFF)) << 56 |
                    (long) ((bytes[offset + 1] & 0xFF)) << 48 |
                    (long) ((bytes[offset + 2] & 0xFF)) << 40 |
                    (long) ((bytes[offset + 3] & 0xFF)) << 32 |
                    (long) ((bytes[offset + 4] & 0xFF)) << 24 |
                    (long) ((bytes[offset + 5] & 0xFF)) << 16 |
                    (long) ((bytes[offset + 6] & 0xFF)) << 8 |
                    (long) (bytes[offset + 7] & 0xFF);
        }

        static byte[] longToBytes(long longVal) {
            return new byte[]{
                    (byte) (longVal >>> 56),
                    (byte) (longVal >>> 48),
                    (byte) (longVal >>> 40),
                    (byte) (longVal >>> 32),
                    (byte) (longVal >>> 24),
                    (byte) (longVal >>> 16),
                    (byte) (longVal >>> 8),
                    (byte) longVal};
        }
    }

    static class IntegerSerializer {

        static final int INTEGER_WIDTH = 5; // 4 bytes plus type code

        static DeserializationResult deserialize(byte[] bytes, int pos) {
            if (bytes[pos] != TYPE_TO_BYTE.get(Integer.class)) {
                throw new IllegalStateException(
                        String.format("Invalid type code '%d' for serializer '%s'",
                                bytes[pos], MethodHandles.lookup().lookupClass().getName()));
            }

            if ((pos + INTEGER_WIDTH) > bytes.length) {
                throw new IllegalStateException(
                        String.format("Byte array too short in serializer '%s'",
                                MethodHandles.lookup().lookupClass().getName()));
            }

            int intVal = intFromBytes(bytes, pos + 1);

            return new DeserializationResult(intVal, pos + INTEGER_WIDTH);
        }

        static void serialize(Integer i, ByteArrayOutputStream bos) {
            bos.write(TYPE_TO_BYTE.get(Integer.class));
            try {
                bos.write(intToBytes(i));
            } catch (IOException e) {
                // ByteArrayOutputStream never throws IOException
            }
        }

        static byte[] intToBytes(int intVal) {
            return new byte[]{
                    (byte) (intVal >>> 24),
                    (byte) (intVal >>> 16),
                    (byte) (intVal >>> 8),
                    (byte) intVal
            };
        }

        static int intFromBytes(byte[] bytes, int offset) {
            return ((bytes[offset] & 0xFF)) << 24 |
                    ((bytes[offset + 1] & 0xFF)) << 16 |
                    ((bytes[offset + 2] & 0xFF)) << 8 |
                    (bytes[offset + 3] & 0xFF);
        }
    }

    static class TimestampSerializer {

        // org.logstash.Timestamp wraps a org.joda.time.DateTime instance that is always
        // created with UTC_CHRONOLOGY so serializing the Long iMillis value from the
        // wrapped DateTime instance is sufficient

        static DeserializationResult deserialize(byte[] bytes, int pos) {
            if (bytes[pos] != TYPE_TO_BYTE.get(Timestamp.class)) {
                throw new IllegalStateException(
                        String.format("Invalid type code '%d' for serializer '%s'",
                                bytes[pos], MethodHandles.lookup().lookupClass().getName()));
            }

            if ((pos + LONG_WIDTH) > bytes.length) {
                throw new IllegalStateException(
                        String.format("Byte array too short in serializer '%s'",
                                MethodHandles.lookup().lookupClass().getName()));
            }

            long millis = longFromBytes(bytes, pos + 1);
            Timestamp t = new Timestamp(millis);
            return new DeserializationResult(t, pos + LONG_WIDTH);
        }

        static void serialize(Timestamp t, ByteArrayOutputStream bos) {
            bos.write(TYPE_TO_BYTE.get(Timestamp.class));
            try {
                bos.write(longToBytes(t.getTime().getMillis()));
            } catch (IOException e) {
                // ByteArrayOutputStream never throws IOException
            }
        }


    }

    static class RubyTimestampSerializer {

        // RubyTimestamp wraps a org.logstash.Timestamp so it is de/serialized the same

        static DeserializationResult deserialize(byte[] bytes, int pos) {
            if (bytes[pos] != TYPE_TO_BYTE.get(JrubyTimestampExtLibrary.RubyTimestamp.class)) {
                throw new IllegalStateException(
                        String.format("Invalid type code '%d' for serializer '%s'",
                                bytes[pos], MethodHandles.lookup().lookupClass().getName()));
            }

            if ((pos + LONG_WIDTH) > bytes.length) {
                throw new IllegalStateException(
                        String.format("Byte array too short in serializer '%s'",
                                MethodHandles.lookup().lookupClass().getName()));
            }

            long millis = longFromBytes(bytes, pos + 1);
            Timestamp t = new Timestamp(millis);
            return new DeserializationResult(
                    JrubyTimestampExtLibrary.RubyTimestamp.newRubyTimestamp(RubyUtil.RUBY, t),
                    pos + LONG_WIDTH);
        }

        static void serialize(JrubyTimestampExtLibrary.RubyTimestamp t, ByteArrayOutputStream bos) {
            bos.write(TYPE_TO_BYTE.get(JrubyTimestampExtLibrary.RubyTimestamp.class));
            try {
                bos.write(longToBytes(t.getTimestamp().getTime().getMillis()));
            } catch (IOException e) {
                // ByteArrayOutputStream never throws IOException
            }
        }


    }

    static class RubyFixNumSerializer {

        static final int RUBY_FIXNUM_WIDTH = 9; // 8 bytes plus type code

        static DeserializationResult deserialize(byte[] bytes, int pos) {
            if (bytes[pos] != TYPE_TO_BYTE.get(RubyFixnum.class)) {
                throw new IllegalStateException(
                        String.format("Invalid type code '%d' for serializer '%s'",
                                bytes[pos], MethodHandles.lookup().lookupClass().getName()));
            }

            if ((pos + RUBY_FIXNUM_WIDTH) > bytes.length) {
                throw new IllegalStateException(
                        String.format("Byte array too short in serializer '%s'",
                                MethodHandles.lookup().lookupClass().getName()));
            }

            long longVal = longFromBytes(bytes, pos + 1);
            return new DeserializationResult(
                    RubyFixnum.newFixnum(RubyUtil.RUBY, longVal), pos + RUBY_FIXNUM_WIDTH);
        }

        static void serialize(RubyFixnum f, ByteArrayOutputStream bos) {
            bos.write(TYPE_TO_BYTE.get(RubyFixnum.class));
            long longVal = f.getLongValue();
            try {
                bos.write(longToBytes(longVal));
            } catch (IOException e) {
                // ByteArrayOutputStream never throws IOException
            }
        }
    }

}

@FunctionalInterface
interface DeserializationFunction {
    DeserializationResult deserialize(byte[] bytes, int pos);
}

@FunctionalInterface
interface SerializationFunction<Q> {
    void serialize(Q obj, ByteArrayOutputStream bos);
}

class DeserializationResult {
    Object object;
    int pos;

    DeserializationResult(Object object, int pos) {
        this.object = object;
        this.pos = pos;
    }
}