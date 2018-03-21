package org.logstash.execution.codecs;

import org.logstash.Event;
import org.logstash.execution.Codec;
import org.logstash.execution.LsConfiguration;
import org.logstash.execution.LsContext;
import org.logstash.execution.PluginConfigSpec;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Line implements Codec {

    static final PluginConfigSpec<String> CHARSET_CONFIG =
            LsConfiguration.stringSetting("charset", "UTF-8");

    static final PluginConfigSpec<String> DELIMITER_CONFIG =
            LsConfiguration.stringSetting("delimiter", System.lineSeparator());

    static final PluginConfigSpec<String> FORMAT_CONFIG =
            LsConfiguration.stringSetting("format");

    // not sure of the preferred method (if any) for arrays of generic types
    @SuppressWarnings({"unchecked"})
    private static Map<String, Object>[] EMPTY_MAP =
            (HashMap<String, Object>[]) Array.newInstance(new HashMap<String, Object>().getClass(), 0);

    static String MESSAGE_FIELD = "message";

    private String delimiter;
    private Charset charset;
    private String remainder = "";

    public Line(final LsConfiguration configuration, final LsContext context) {
        delimiter = configuration.get(DELIMITER_CONFIG);
        charset = Charset.forName(configuration.get(CHARSET_CONFIG));
    }

    @Override
    public int decode(byte[] input, int offset, int length, Map<String, Object>[] events) {
        String s = remainder.concat(new String(input, offset, length, charset));
        String[] lines = s.split(delimiter, events.length + 1);
        int numEvents = Math.max(lines.length, events.length);
        for (int k = 0; k < numEvents; k++) {
            setEvent(events, k, lines[k]);
        }
        remainder = (lines.length == events.length + 1) ? lines[events.length] : "";

        return numEvents;
    }

    @Override
    public Map<String, Object>[] flush() {
        if (remainder.length() == 0) {
            return EMPTY_MAP;
        } else {
            String[] lines = remainder.split(delimiter, -1);
            @SuppressWarnings({"unchecked"})
            HashMap<String, Object>[] events = (HashMap<String, Object>[]) new Object[lines.length];
            for (int k = 0; k < lines.length; k++) {
                setEvent(events, k, lines[k]);
            }
            return events;
        }
    }

    private static void setEvent(Map<String, Object>[] events, int index, String message) {
        Map<String, Object> event;
        if (events[index] == null) {
            event = new HashMap<>();
            events[index] = event;
        } else {
            event = events[index];
        }
        event.clear();
        event.put(MESSAGE_FIELD, message);
    }


    @Override
    public void encode(Event event, OutputStream output) {
        // doesn't honor the format setting, yet
        try {
            output.write(event.toJson().getBytes(charset));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        return Arrays.asList(CHARSET_CONFIG, DELIMITER_CONFIG, FORMAT_CONFIG);
    }
}
