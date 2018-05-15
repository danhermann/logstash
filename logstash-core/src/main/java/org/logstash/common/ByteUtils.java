package org.logstash.common;

/**
 * Utility functions for converting various types to and from byte array
 * representations.
 */
public final class ByteUtils {

    public static long longFromBytes(byte[] bytes) {
        return longFromBytes(bytes, 0);
    }

    public static long longFromBytes(byte[] bytes, int offset) {
        return (long) ((bytes[offset] & 0xFF)) << 56 |
                (long) ((bytes[offset + 1] & 0xFF)) << 48 |
                (long) ((bytes[offset + 2] & 0xFF)) << 40 |
                (long) ((bytes[offset + 3] & 0xFF)) << 32 |
                (long) ((bytes[offset + 4] & 0xFF)) << 24 |
                (long) ((bytes[offset + 5] & 0xFF)) << 16 |
                (long) ((bytes[offset + 6] & 0xFF)) << 8 |
                (long) (bytes[offset + 7] & 0xFF);
    }

    public static byte[] longToBytes(long longVal) {
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

    public static byte[] intToBytes(int intVal) {
        return new byte[]{
                (byte) (intVal >>> 24),
                (byte) (intVal >>> 16),
                (byte) (intVal >>> 8),
                (byte) intVal
        };
    }

    public static int intFromBytes(byte[] bytes) {
        return intFromBytes(bytes, 0);
    }

    public static int intFromBytes(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF)) << 24 |
                ((bytes[offset + 1] & 0xFF)) << 16 |
                ((bytes[offset + 2] & 0xFF)) << 8 |
                (bytes[offset + 3] & 0xFF);
    }

}
