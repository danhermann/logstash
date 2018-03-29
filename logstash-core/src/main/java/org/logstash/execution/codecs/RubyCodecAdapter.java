package org.logstash.execution.codecs;

import org.logstash.Event;
import org.logstash.execution.Codec;
import org.logstash.execution.PluginConfigSpec;

import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;

public class RubyCodecAdapter implements Codec {

    @Override
    public int decode(byte[] input, Map<String, Object>[] events) {
        return 0;
    }

    @Override
    public int decode(byte[] input, int offset, int length, Map<String, Object>[] events) {
        return 0;
    }

    @Override
    public void encode(Event event, OutputStream output) {

    }

    @Override
    public Map<String, Object>[] flush() {
        return new Map[0];
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        return null;
    }
}
