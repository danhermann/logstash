package org.logstash;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertEquals;

@RunWith(JUnitParamsRunner.class)
public class LengthPrefixedEventSerializerTest {

    @Test
    public void testRoundtrip() {
        Event e = new Event();
        e.setField("longField1", 1L);
        e.setField("intField1", 42);
        e.setField("stringField1", "foo");
        e.setField("timestampField1", new Timestamp(1));
        e.setTimestamp(new Timestamp(5));

        LengthPrefixedEventSerializer serializer = new LengthPrefixedEventSerializer();

        byte[] serialized = serializer.serialize(e);
        Event e2 = serializer.deserialize(serialized);
        /*
        e2.setField("longField1", 1L);
        e2.setField("intField1", 42);
        e2.setField("stringField1", "foo");
        e2.setField("timestampField1", new Timestamp(1));
        e2.setTimestamp(new Timestamp(5));
        */

        assertEquals(e, e2);

        //LengthPrefixedEventSerializer.TimestampSerializer;

    }


    @Test
    @Parameters({"-1", "1", "0", "9223372036854775807", "-9223372036854775808"})
    public void testLongs(Long l) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(10);
        LengthPrefixedEventSerializer
                .LongSerializer
                .serialize(l, bos);
        Object roundtrip = LengthPrefixedEventSerializer
                .LongSerializer
                .deserialize(bos.toByteArray(), 0).object;

        assertEquals(l, roundtrip);
    }

    @Test
    @Parameters({"-1", "1", "0", "2147483647", "-2147483648"})
    public void testIntegers(Integer i) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(10);
        LengthPrefixedEventSerializer
                .IntegerSerializer
                .serialize(i, bos);
        Object roundtrip = LengthPrefixedEventSerializer
                .IntegerSerializer
                .deserialize(bos.toByteArray(), 0).object;

        assertEquals(i, roundtrip);
    }

    @Test
    @Parameters({"2005-03-25", "19700101"})
    public void testTimestampStrings(String timestampString) {
        Timestamp t = new Timestamp(timestampString);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(10);
        LengthPrefixedEventSerializer
                .TimestampSerializer
                .serialize(t, bos);
        Object roundtrip = LengthPrefixedEventSerializer
                .TimestampSerializer
                .deserialize(bos.toByteArray(), 0).object;

        assertEquals(t, roundtrip);
    }

    @Test
    @Parameters({"0", "1", "9223372017129599999", "-9223372017043200000"})
    public void testTimestampMillis(Long millis) {
        Timestamp t2 = new Timestamp(millis);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(10);
        LengthPrefixedEventSerializer
                .TimestampSerializer
                .serialize(t2, bos);
        Object roundtrip = LengthPrefixedEventSerializer
                .TimestampSerializer
                .deserialize(bos.toByteArray(), 0).object;

        assertEquals(t2, roundtrip);
    }

    @Test
    @Parameters({"foo"})
    public void testStrings(String s) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(10);
        LengthPrefixedEventSerializer
                .StringSerializer
                .serialize(s, bos);
        Object roundtrip = LengthPrefixedEventSerializer
                .StringSerializer
                .deserialize(bos.toByteArray(), 0).object;

        assertEquals(s, roundtrip);
    }


}


