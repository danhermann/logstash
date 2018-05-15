package org.logstash.common;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeUnit;

/**
 * Created by andrewvc on 12/23/16.
 */
public class Util {
    // Modified from http://stackoverflow.com/a/11009612/11105

    public static MessageDigest defaultMessageDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static String digest(String base) {
        MessageDigest digest = defaultMessageDigest();
        byte[] hash = digest.digest(base.getBytes(StandardCharsets.UTF_8));
        return bytesToHexString(hash);
    }

    public static String bytesToHexString(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();

        for (byte aHash : bytes) {
            String hex = Integer.toHexString(0xff & aHash);
            if (hex.length() == 1) hexString.append('0');
            hexString.append(hex);
        }

        return hexString.toString();
    }

    public static double nanosToMillis(double nanos) {
        return nanos / 1e6;
    }
}
