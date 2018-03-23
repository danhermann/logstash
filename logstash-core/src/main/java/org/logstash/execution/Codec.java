package org.logstash.execution;

import org.logstash.Event;

import java.io.OutputStream;
import java.util.Map;

public interface Codec extends LsPlugin {
    int decode(byte[] input, Map<String, Object>[] events);
    int decode(byte[] input, int offset, int length, Map<String, Object>[] events);
    void encode(Event event, OutputStream output);
    Map<String, Object>[] flush();
}
