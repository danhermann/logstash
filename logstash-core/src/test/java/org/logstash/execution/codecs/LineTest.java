package org.logstash.execution.codecs;

import org.junit.Test;
import org.logstash.execution.LsConfiguration;

import java.lang.reflect.Array;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class LineTest {

    @Test
    public void testDefaultDelimiter() {
        Line line = new Line(new LsConfiguration(Collections.EMPTY_MAP), null);
        String[] inputStrings = {"foo", "bar", "baz"};
        String input = String.join(System.lineSeparator(), inputStrings);
        Map<String, Object>[] events =
                (HashMap<String, Object>[]) Array.newInstance(new HashMap<String, Object>().getClass(), inputStrings.length);

        int num = line.decode(input.getBytes(), 0, input.length(), events);
        assertEquals(inputStrings.length, num);
        for (int k = 0; k < num; k++) {
            assertEquals(inputStrings[k], events[k].get(Line.MESSAGE_FIELD));
        }
    }
}
