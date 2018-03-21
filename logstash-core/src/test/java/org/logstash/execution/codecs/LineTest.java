package org.logstash.execution.codecs;

import org.junit.Test;
import org.logstash.Event;
import org.logstash.execution.LsConfiguration;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Array;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class LineTest {

    @Test
    public void testDecodeDefaultDelimiter() {
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

        events = line.flush();
        assertEquals(0, events.length);
    }

    @Test
    public void testDecodeCustomDelimiter() {
        String delimiter = "z";
        int expectedEventCount = 3;
        Line line = new Line(new LsConfiguration(Collections.singletonMap("delimiter", delimiter)), null);
        String input = "foozbarzbaz";
        Map<String, Object>[] events =
                (HashMap<String, Object>[]) Array.newInstance(new HashMap<String, Object>().getClass(), expectedEventCount + 1);

        int num = line.decode(input.getBytes(), 0, input.length(), events);
        assertEquals(expectedEventCount, num);

        events = line.flush();
        assertEquals(0, events.length);
    }

    @Test
    public void testDecodeWithTrailingDelimiter() {
        Line line = new Line(new LsConfiguration(Collections.EMPTY_MAP), null);
        int expectedEventCount = 3;
        String input = "foo\nbar\nbaz\n";
        Map<String, Object>[] events =
                (HashMap<String, Object>[]) Array.newInstance(new HashMap<String, Object>().getClass(), expectedEventCount + 1);
        int num = line.decode(input.getBytes(), 0, input.length(), events);
        assertEquals(expectedEventCount, num);

        events = line.flush();
        assertEquals(0, events.length);
    }

    @Test
    public void testDecodeOnDelimiterOnly() {
        String delimiter = "z";
        int expectedEventCount = 1;
        Line line = new Line(new LsConfiguration(Collections.singletonMap("delimiter", delimiter)), null);
        String input = "z";
        Map<String, Object>[] events =
                (HashMap<String, Object>[]) Array.newInstance(new HashMap<String, Object>().getClass(), expectedEventCount + 1);

        int num = line.decode(input.getBytes(), 0, input.length(), events);
        assertEquals(expectedEventCount, num);

        events = line.flush();
        assertEquals(0, events.length);
    }

    @Test
    public void testDecodeWithMulticharDelimiter() {
        String delimiter = "xyz";
        int expectedEventCount = 3;
        Line line = new Line(new LsConfiguration(Collections.singletonMap("delimiter", delimiter)), null);
        String input = "axyzbxyzc";
        Map<String, Object>[] events =
                (HashMap<String, Object>[]) Array.newInstance(new HashMap<String, Object>().getClass(), expectedEventCount + 1);

        int num = line.decode(input.getBytes(), 0, input.length(), events);
        assertEquals(expectedEventCount, num);

        events = line.flush();
        assertEquals(0, events.length);
    }

    @Test
    public void testDecodeWithMulticharTrailingDelimiter() {
        String delimiter = "xyz";
        int expectedEventCount = 3;
        Line line = new Line(new LsConfiguration(Collections.singletonMap("delimiter", delimiter)), null);
        String input = "fooxyzbarxyzbazxyz";
        Map<String, Object>[] events =
                (HashMap<String, Object>[]) Array.newInstance(new HashMap<String, Object>().getClass(), expectedEventCount + 1);
        int num = line.decode(input.getBytes(), 0, input.length(), events);
        assertEquals(expectedEventCount, num);

        events = line.flush();
        assertEquals(0, events.length);
    }

    @Test
    public void testEncode() {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Line line = new Line(new LsConfiguration(Collections.emptyMap()), null);
        Event e = new Event();
        e.setField("myfield1", "myvalue1");
        e.setField("myfield2", 42L);
        line.encode(e, outputStream);
        e.setField("myfield1", "myvalue2");
        e.setField("myfield2", 43L);
        line.encode(e, outputStream);

        String delimiter = System.lineSeparator();
        String resultingString = outputStream.toString();
        // first delimiter should occur at the halfway point of the string
        assertEquals(resultingString.indexOf(delimiter), (resultingString.length() / 2) - delimiter.length());
        // second delimiter should occur at end of string
        assertEquals(resultingString.lastIndexOf(delimiter), resultingString.length() - delimiter.length());
    }

    @Test
    public void testEncodeWithCustomDelimiter() {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        String delimiter = "xyz";
        Line line = new Line(new LsConfiguration(Collections.singletonMap("delimiter", delimiter)), null);
        Event e = new Event();
        e.setField("myfield1", "myvalue1");
        e.setField("myfield2", 42L);
        line.encode(e, outputStream);
        e.setField("myfield1", "myvalue2");
        e.setField("myfield2", 43L);
        line.encode(e, outputStream);

        String resultingString = outputStream.toString();
        // first delimiter should occur at the halfway point of the string
        assertEquals(resultingString.indexOf(delimiter), (resultingString.length() / 2) - delimiter.length());
        // second delimiter should occur at end of string
        assertEquals(resultingString.lastIndexOf(delimiter), resultingString.length() - delimiter.length());
    }

}
