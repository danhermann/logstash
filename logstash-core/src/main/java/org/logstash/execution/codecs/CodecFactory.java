package org.logstash.execution.codecs;

import org.logstash.execution.Codec;

import java.util.concurrent.ConcurrentHashMap;

public class CodecFactory {

    // eagerly initialize singleton
    private static final CodecFactory INSTANCE = new CodecFactory();

    private ConcurrentHashMap<String, Class> codecMap = new ConcurrentHashMap<>();

    private CodecFactory() {
        // singleton class
    }

    public static CodecFactory getInstance() {
        return INSTANCE;
    }

    public void addCodec(String name, Class codecClass) {
        codecMap.put(name, codecClass);
    }

    public Codec getCodec(String name, String defaultName) {
        return null;
    }
}
