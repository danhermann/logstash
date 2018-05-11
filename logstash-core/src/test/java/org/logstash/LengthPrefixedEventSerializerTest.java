package org.logstash;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.jruby.RubyFixnum;
import org.jruby.RubyString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.logstash.ext.JrubyTimestampExtLibrary;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(JUnitParamsRunner.class)
public class LengthPrefixedEventSerializerTest {

    @Test
    public void testEventRoundtrip() {
        Map<String, Object> map = new HashMap<>();
        map.put("longField1", 1L);
        map.put("intField1", 42);
        map.put("stringField1", "foo");
        map.put("timestampField1", new Timestamp(1));
        map.put("@version", RubyUtil.RUBY.newString("1"));
        Event e = new Event(ConvertedMap.newFromMap(map));
        e.setTimestamp(new Timestamp(5));

        LengthPrefixedEventSerializer serializer = new LengthPrefixedEventSerializer();

        byte[] serialized = serializer.serialize(e);
        Event e2 = serializer.deserialize(serialized);
        assertEquals(e, e2);
    }

    @Test
    @Parameters({"-1", "1", "0", "9223372036854775807", "-9223372036854775808"})
    public void testLongs(Long l) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(10);
        LengthPrefixedEventSerializer.serialize(l, bos);
        Object roundtrip = LengthPrefixedEventSerializer.deserialize(bos.toByteArray(), 0).object;

        assertEquals(l, roundtrip);
    }

    @Test
    @Parameters({"-1", "1", "0", "9223372036854775807", "-9223372036854775808"})
    public void testRubyFixnums(Long l) {
        RubyFixnum rf = RubyFixnum.newFixnum(RubyUtil.RUBY, l);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(10);
        LengthPrefixedEventSerializer.serialize(l, bos);
        Object roundtrip = LengthPrefixedEventSerializer.deserialize(bos.toByteArray(), 0).object;

        assertEquals(l, roundtrip);
    }

    @Test
    @Parameters({"-1", "1", "0", "2147483647", "-2147483648"})
    public void testIntegers(Integer i) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(10);
        LengthPrefixedEventSerializer.serialize(i, bos);
        Object roundtrip = LengthPrefixedEventSerializer.deserialize(bos.toByteArray(), 0).object;

        assertEquals(i, roundtrip);
    }

    @Test
    @Parameters({"2005-03-25", "19700101"})
    public void testTimestampStrings(String timestampString) {
        Timestamp t = new Timestamp(timestampString);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(10);
        LengthPrefixedEventSerializer.serialize(t, bos);
        Object roundtrip = LengthPrefixedEventSerializer.deserialize(bos.toByteArray(), 0).object;

        assertEquals(t, roundtrip);
    }

    @Test
    @Parameters({"0", "1", "9223372017129599999", "-9223372017043200000"})
    public void testTimestampMillis(Long millis) {
        Timestamp t2 = new Timestamp(millis);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(101);
        LengthPrefixedEventSerializer.serialize(t2, bos);
        Object roundtrip = LengthPrefixedEventSerializer.deserialize(bos.toByteArray(), 0).object;

        assertEquals(t2, roundtrip);
    }

    @Test
    @Parameters({"0", "1", "9223372017129599999", "-9223372017043200000"})
    public void testRubyTimestampMillis(Long millis) {
        Timestamp t2 = new Timestamp(millis);
        JrubyTimestampExtLibrary.RubyTimestamp ts =
                JrubyTimestampExtLibrary.RubyTimestamp.newRubyTimestamp(RubyUtil.RUBY, t2);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(10);
        LengthPrefixedEventSerializer.serialize(ts, bos);
        Object roundtrip = LengthPrefixedEventSerializer.deserialize(bos.toByteArray(), 0).object;

        assertEquals(ts, roundtrip);
    }

    @Test
    @Parameters({"foo", ""})
    public void testStrings(String s) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(10);
        LengthPrefixedEventSerializer.serialize(s, bos);
        Object roundtrip = LengthPrefixedEventSerializer.deserialize(bos.toByteArray(), 0).object;

        assertEquals(s, roundtrip);
    }

    @Test
    @Parameters({"foo", ""})
    public void testRubyStrings(String s) {
        RubyString rs = new RubyString(RubyUtil.RUBY, RubyString.createStringClass(RubyUtil.RUBY), s);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(10);
        LengthPrefixedEventSerializer.serialize(rs, bos);
        Object roundtrip = LengthPrefixedEventSerializer.deserialize(bos.toByteArray(), 0).object;

        assertEquals(rs, roundtrip);
    }

    @Test
    public void testMaps() {
        Map<String, Object> map = new HashMap<>();
        map.put("longField1", 1L);
        map.put("intField1", 42);
        map.put("stringField1", "foo");
        map.put("timestampField1", new Timestamp(1));

        ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
        LengthPrefixedEventSerializer.serialize(map, bos);
        Object roundtrip = LengthPrefixedEventSerializer.deserialize(bos.toByteArray(), 0).object;

        assertEquals(map, roundtrip);
    }



}


