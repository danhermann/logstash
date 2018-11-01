package org.logstash.plugins.ingestnode;

import org.jruby.RubyString;
import org.junit.Assert;
import org.junit.Test;
import org.logstash.ConvertedList;
import org.logstash.ConvertedMap;
import org.logstash.Event;
import org.logstash.RubyUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IngestNodeFilterTest {

    @Test
    public void testAppendProcessor() throws Exception {

        String json =

                "{" +
                        "    \"processors\": [" +
                        "      {" +
                        "        \"append\": {" +
                        "          \"field\": \"my_field\"," +
                        "          \"value\": [\"my_value2\"]" +
                        "        }" +
                        "      }" +
                        "    ]" +
                        "  }";

        Event e1 = new Event();
        e1.setField("my_field", "my_value1");
        Event e2 = IngestNodeFilter.filter(json, e1);
        compareEventsExcludingFields(e1, e2, new String[]{"my_field"});
        List<RubyString> rubyStrings = new ArrayList<>();
        rubyStrings.add(RubyString.newString(RubyUtil.RUBY, "my_value1"));
        rubyStrings.add(RubyString.newString(RubyUtil.RUBY, "my_value2"));
        ConvertedList expected = ConvertedList.newFromList(rubyStrings);
        Assert.assertEquals(expected, e2.getUnconvertedField("my_field"));
    }

    @Test
    public void testBytesProcessor() throws Exception {

        String json =

                "{" +
                        "    \"processors\": [" +
                        "      {" +
                        "        \"bytes\": {" +
                        "          \"field\": \"my_field\"," +
                        "          \"target_field\": \"my_field2\"" +
                        "        }" +
                        "      }" +
                        "    ]" +
                        "  }";

        Event e1 = new Event();
        e1.setField("my_field", "1kb");
        Event e2 = IngestNodeFilter.filter(json, e1);
        compareEventsExcludingFields(e1, e2, new String[]{"my_field2"});
        Assert.assertEquals(1024L, e2.getField("my_field2"));
    }

    @Test
    public void testConvertProcessor() throws Exception {

        String json =

                "{" +
                        "    \"processors\": [" +
                        "      {" +
                        "        \"convert\": {" +
                        "          \"field\": \"my_field\"," +
                        "          \"type\": \"long\"," +
                        "          \"target_field\": \"my_field2\"" +
                        "        }" +
                        "      }" +
                        "    ]" +
                        "  }";

        Event e1 = new Event();
        e1.setField("my_field", "1024");
        Event e2 = IngestNodeFilter.filter(json, e1);
        compareEventsExcludingFields(e1, e2, new String[]{"my_field2"});
        Assert.assertEquals(1024L, e2.getField("my_field2"));
    }

    @Test
    public void testDateProcessor() throws Exception {

        String json =

                "{" +
                        "    \"processors\": [" +
                        "      {" +
                        "        \"date\": {" +
                        "          \"field\": \"my_field\"," +
                        "          \"formats\": [\"MM/dd/yyyy HH:mm:ss\"]," +
                        "          \"target_field\": \"my_field2\"" +
                        "        }" +
                        "      }" +
                        "    ]" +
                        "  }";

        Event e1 = new Event();
        e1.setField("my_field", "08/14/1991 13:45:55");
        Event e2 = IngestNodeFilter.filter(json, e1);
        compareEventsExcludingFields(e1, e2, new String[]{"my_field2"});
        RubyString expected = RubyString.newString(RubyUtil.RUBY, "1991-08-14T13:45:55.000Z");
        Assert.assertEquals(expected, e2.getUnconvertedField("my_field2"));
    }

    @Test
    public void testDateIndexNameProcessor() throws Exception {

        String json =

                "{" +
                        "    \"processors\": [" +
                        "      {" +
                        "        \"date_index_name\": {" +
                        "          \"field\": \"my_field\"," +
                        "          \"index_name_prefix\": \"myindex-\"," +
                        "          \"index_name_format\": \"yyyy-MM-dd\"," +
                        "          \"date_formats\": [\"MM/dd/yyyy HH:mm:ss\"]," +
                        "          \"date_rounding\": \"d\"" +
                        "        }" +
                        "      }" +
                        "    ]" +
                        "  }";

        Event e1 = new Event();
        e1.setField("my_field", "08/14/1991 13:45:55");
        Event e2 = IngestNodeFilter.filter(json, e1);
        compareEventsExcludingFields(e1, e2, new String[]{"my_field2", "_index"});
        RubyString expected = RubyString.newString(RubyUtil.RUBY, "<myindex-{1991-08-14||/d{yyyy-MM-dd|UTC}}>");
        Assert.assertEquals(expected, e2.getUnconvertedField("_index"));
    }

    @Test
    public void testDotExpanderProcessor() throws Exception {

        String json =

                "{" +
                        "    \"processors\": [" +
                        "      {" +
                        "        \"dot_expander\": {" +
                        "          \"field\": \"my_field1.my_field2\"" +
                        "        }" +
                        "      }" +
                        "    ]" +
                        "  }";

        Event e1 = new Event();
        e1.setField("my_field1.my_field2", "foo");
        Event e2 = IngestNodeFilter.filter(json, e1);
        compareEventsExcludingFields(e1, e2, new String[]{"my_field1", "my_field1.my_field2"});
        Assert.assertEquals("foo", e2.getField("[my_field1][my_field2]"));
    }

    @Test
    public void testFailProcessor() throws Exception {

        String json =

                "{" +
                        "    \"processors\": [" +
                        "      {" +
                        "        \"fail\": {" +
                        "          \"message\": \"custom error message\"" +
                        "        }" +
                        "      }" +
                        "    ]" +
                        "  }";

        try {
            IngestNodeFilter.filter(json, new Event());
            Assert.fail("Exception should have been thrown");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("custom error message"));
        }
    }

    @Test
    public void testForeachProcessor() throws Exception {

        String json =

                "{" +
                        "    \"processors\": [" +
                        "      {" +
                        "        \"foreach\": {" +
                        "          \"field\": \"my_array_field\"," +
                        "          \"processor\": {" +
                        "            \"lowercase\": {" +
                        "              \"field\": \"_ingest._value\"" +
                        "            }" +
                        "          }" +
                        "        }" +
                        "      }" +
                        "    ]" +
                        "  }";

        Event e1 = new Event();
        List<String> strings = new ArrayList<>(Arrays.asList("FOO", "BAR", "BAZ"));
        List<String> expected = strings.stream().map(String::toLowerCase).collect(Collectors.toList());
        e1.setField("my_array_field", strings);
        Event e2 = IngestNodeFilter.filter(json, e1);
        compareEventsExcludingFields(e1, e2, new String[]{"my_array_field", "_value"});
        Assert.assertEquals(expected, e2.getField("my_array_field"));
    }

    @Test
    public void testGrokProcessor() throws Exception {

        String json =

                "{" +
                        "    \"processors\": [" +
                        "      {" +
                        "        \"grok\": {" +
                        "          \"field\": \"my_field\"," +
                        "          \"patterns\": [\"{NUMBER:duration} %{IP:client}\"]" +
                        "        }" +
                        "      }" +
                        "    ]" +
                        "  }";

        Event e1 = new Event();
        e1.setField("my_field", "3.44 55.3.244.1");
        Event e2 = IngestNodeFilter.filter(json, e1);
        compareEventsExcludingFields(e1, e2, new String[]{"my_field", "duration", "client"});
        Assert.assertEquals(3.44, e2.getField("duration"));
        Assert.assertEquals("55.3.244.1", e2.getField("client"));
    }

    @Test
    public void testGsubProcessor() throws Exception {

        String json =

                "{" +
                        "    \"processors\": [" +
                        "      {" +
                        "        \"gsub\": {" +
                        "          \"field\": \"my_field\"," +
                        "          \"pattern\": \"\\\\.\"," +
                        "          \"replacement\": \"-\"" +
                        "        }" +
                        "      }" +
                        "    ]" +
                        "  }";

        Event e1 = new Event();
        String value = "800.555.1234";
        e1.setField("my_field", value);
        Event e2 = IngestNodeFilter.filter(json, e1);
        compareEventsExcludingFields(e1, e2, new String[]{"my_field"});
        Assert.assertEquals(value.replace(".", "-"), e2.getField("my_field"));
    }

    @Test
    public void testJoinProcessor() throws Exception {

        String json =

                "{" +
                        "    \"processors\": [" +
                        "      {" +
                        "        \"join\": {" +
                        "          \"field\": \"my_array_field\"," +
                        "          \"target_field\": \"my_field2\"," +
                        "          \"separator\": \"=\"" +
                        "        }" +
                        "      }" +
                        "    ]" +
                        "  }";

        Event e1 = new Event();
        List<String> strings = new ArrayList<>(Arrays.asList("FOO", "BAR", "BAZ"));
        String expected = String.join("=", strings);
        e1.setField("my_array_field", strings);
        Event e2 = IngestNodeFilter.filter(json, e1);
        compareEventsExcludingFields(e1, e2, new String[]{"my_array_field", "my_field2"});
        Assert.assertEquals(expected, e2.getField("my_field2"));
    }

    @Test
    public void testJsonProcessor() throws Exception {

        String json =

                "{" +
                        "    \"processors\": [" +
                        "      {" +
                        "        \"json\": {" +
                        "          \"field\": \"my_raw_json\"," +
                        "          \"target_field\": \"my_parsed_json\"" +
                        "        }" +
                        "      }" +
                        "    ]" +
                        "  }";

        Event e1 = new Event();
        String myRawJson = "{\"foo\": 2000}";
        e1.setField("my_raw_json", myRawJson);
        Event e2 = IngestNodeFilter.filter(json, e1);
        compareEventsExcludingFields(e1, e2, new String[]{"my_parsed_json"});
        Assert.assertEquals(2000L, e2.getField("[my_parsed_json][foo]"));
    }

    @Test
    public void testKvProcessor() throws Exception {

        String json =

                "{" +
                        "    \"processors\": [" +
                        "      {" +
                        "        \"kv\": {" +
                        "          \"field\": \"my_kv_field\"," +
                        "          \"field_split\": \" \"," +
                        "          \"value_split\": \"=\"" +
                        "        }" +
                        "      }" +
                        "    ]" +
                        "  }";

        Event e1 = new Event();
        String myRawKv = "ip=1.2.3.4 error=REFUSED foo=bar";
        e1.setField("my_kv_field", myRawKv);
        Event e2 = IngestNodeFilter.filter(json, e1);
        compareEventsExcludingFields(e1, e2, new String[]{"ip", "error", "foo"});
        Assert.assertEquals("1.2.3.4", e2.getField("[ip]"));
        Assert.assertEquals("REFUSED", e2.getField("[error]"));
        Assert.assertEquals("bar", e2.getField("[foo]"));
    }

    @Test
    public void testLowercaseProcessor() throws Exception {

        String json =

                "{" +
                        "    \"processors\": [" +
                        "      {" +
                        "        \"lowercase\": {" +
                        "          \"field\": \"my_field1\"," +
                        "          \"target_field\": \"my_field2\"," +
                        "          \"ignore_missing\": false" +
                        "        }" +
                        "      }" +
                        "    ]" +
                        "  }";

        Event e1 = new Event();
        String value = "FOO";
        e1.setField("my_field1", value);
        Event e2 = IngestNodeFilter.filter(json, e1);
        compareEventsExcludingFields(e1, e2, new String[]{"my_field2"});
        Assert.assertEquals(value, e1.getField("my_field1"));
        Assert.assertEquals(value.toLowerCase(), e2.getField("my_field2"));
    }

    @Test
    public void testRemoveProcessor() throws Exception {

        String json =

                "{" +
                        "    \"processors\": [" +
                        "      {" +
                        "        \"remove\": {" +
                        "          \"field\": \"my_field1\"," +
                        "          \"ignore_missing\": false" +
                        "        }" +
                        "      }" +
                        "    ]" +
                        "  }";

        Event e1 = new Event();
        String value = "FOO";
        e1.setField("my_field1", value);
        e1.setField("my_field2", value);
        Event e2 = IngestNodeFilter.filter(json, e1);
        compareEventsExcludingFields(e1, e2, new String[]{"my_field1"});
        Assert.assertNull(e2.getField("my_field1"));
    }

    @Test
    public void testRenameProcessor() throws Exception {

        String json =

                "{" +
                        "    \"processors\": [" +
                        "      {" +
                        "        \"rename\": {" +
                        "          \"field\": \"my_field1\"," +
                        "          \"target_field\": \"my_field2\"," +
                        "          \"ignore_missing\": false" +
                        "        }" +
                        "      }" +
                        "    ]" +
                        "  }";

        Event e1 = new Event();
        e1.setField("my_field1", "foo");
        Event e2 = IngestNodeFilter.filter(json, e1);
        compareEventsExcludingFields(e1, e2, new String[]{"my_field1", "my_field2"});
        Assert.assertEquals(e1.getField("my_field1"), e2.getField("my_field2"));
    }

    @Test
    public void testScriptProcessorWithPainless() throws Exception {

        String json =

                "{" +
                        "    \"processors\": [" +
                        "      {" +
                        "         \"script\": {" +
                        "            \"lang\": \"painless\"," +
                        "            \"source\": \"ctx.field_a_plus_b_times_c = (ctx.field_a + ctx.field_b) * params.param_c\"," +
                        "            \"params\": {" +
                        "               \"param_c\": 10" +
                        "             }" +
                        "          }" +
                        "       }" +
                        "    ]" +
                        "  }";

        Event e1 = new Event();
        e1.setField("hostname", "FOO");
        Event e2 = IngestNodeFilter.filter(json, e1);
        compareEventsExcludingFields(e1, e2, new String[]{"host", "hostname"});
        Assert.assertEquals(e1.getField("hostname"), "FOO");
        Assert.assertEquals(e2.getField("host"), "foo");
    }

    @Test
    public void testSetProcessor() throws Exception {

        String json =

                "{" +
                        "    \"processors\": [" +
                        "      {" +
                        "        \"set\": {" +
                        "          \"field\": \"my_field1\"," +
                        "          \"value\": 3.1415" +
                        "        }" +
                        "      }" +
                        "    ]" +
                        "  }";

        Event e1 = new Event();
        e1.setField("my_field1", "foo");
        e1.setField("my_field2", "foo");
        Event e2 = IngestNodeFilter.filter(json, e1);
        compareEventsExcludingFields(e1, e2, new String[]{"my_field1"});
        Assert.assertEquals(3.1415, e2.getField("my_field1"));
    }

    @Test
    public void testSplitProcessor() throws Exception {

        String json =

                "{" +
                        "    \"processors\": [" +
                        "      {" +
                        "        \"split\": {" +
                        "          \"field\": \"my_field1\"," +
                        "          \"separator\": \"\\\\s+\"," +
                        "          \"target_field\": \"my_field2\"" +
                        "        }" +
                        "      }" +
                        "    ]" +
                        "  }";

        Event e1 = new Event();
        e1.setField("my_field1", "foo   bar baz");
        List<String> expected = new ArrayList<>(Arrays.asList("foo", "bar", "baz"));
        Event e2 = IngestNodeFilter.filter(json, e1);
        compareEventsExcludingFields(e1, e2, new String[]{"my_field2"});
        Assert.assertEquals(expected, e2.getField("my_field2"));
    }

    @Test
    public void testSortProcessor() throws Exception {

        String json =

                "{" +
                        "    \"processors\": [" +
                        "      {" +
                        "        \"sort\": {" +
                        "          \"field\": \"my_field1\"," +
                        "          \"target_field\": \"my_field2\"" +
                        "        }" +
                        "      }" +
                        "    ]" +
                        "  }";

        String[] strings = new String[]{"foo", "bar", "baz"};
        List<String> stringList = new ArrayList<>(Arrays.asList(strings));
        Arrays.sort(strings);
        List<String> expected = Arrays.asList(strings);
        Event e1 = new Event();
        e1.setField("my_field1", stringList);
        Event e2 = IngestNodeFilter.filter(json, e1);
        compareEventsExcludingFields(e1, e2, new String[]{"my_field2"});
        Assert.assertEquals(expected, e2.getField("my_field2"));
    }

    @Test
    public void testTrimProcessor() throws Exception {

        String json =

                "{" +
                        "    \"processors\": [" +
                        "      {" +
                        "        \"trim\": {" +
                        "          \"field\": \"my_field1\"," +
                        "          \"target_field\": \"my_field2\"" +
                        "        }" +
                        "      }" +
                        "    ]" +
                        "  }";

        String value = "   foo ";
        Event e1 = new Event();
        e1.setField("my_field1", value);
        Event e2 = IngestNodeFilter.filter(json, e1);
        compareEventsExcludingFields(e1, e2, new String[]{"my_field2"});
        Assert.assertEquals(value.trim(), e2.getField("my_field2"));
    }

    @Test
    public void testUppercaseProcessor() throws Exception {

        String json =

                "{" +
                        "    \"processors\": [" +
                        "      {" +
                        "        \"uppercase\": {" +
                        "          \"field\": \"my_field1\"," +
                        "          \"target_field\": \"my_field2\"," +
                        "          \"ignore_missing\": false" +
                        "        }" +
                        "      }" +
                        "    ]" +
                        "  }";

        Event e1 = new Event();
        String value = "Foo bar baz";
        e1.setField("my_field1", value);
        Event e2 = IngestNodeFilter.filter(json, e1);
        compareEventsExcludingFields(e1, e2, new String[]{"my_field2"});
        Assert.assertEquals(value, e1.getField("my_field1"));
        Assert.assertEquals(value.toUpperCase(), e2.getField("my_field2"));
    }

    @Test
    public void testUrlDecodeProcessor() throws Exception {

        String json =

                "{" +
                        "    \"processors\": [" +
                        "      {" +
                        "        \"urldecode\": {" +
                        "          \"field\": \"my_field1\"," +
                        "          \"target_field\": \"my_field2\"" +
                        "        }" +
                        "      }" +
                        "    ]" +
                        "  }";
        String encodedUrl = "https%3A%2F%2Fwww.google.com";
        String expectedUrl = "https://www.google.com";
        Event e1 = new Event();
        e1.setField("my_field1", encodedUrl);
        Event e2 = IngestNodeFilter.filter(json, e1);
        compareEventsExcludingFields(e1, e2, new String[]{"my_field2"});
        Assert.assertEquals(expectedUrl, e2.getField("my_field2"));
    }

    private static void compareEventsExcludingFields(Event e1, Event e2, String[] excludedFields) {
        compareOneWayEventsExcludingFields(e1, e2, excludedFields);
        compareOneWayEventsExcludingFields(e2, e1, excludedFields);
    }

    private static void compareOneWayEventsExcludingFields(Event e1, Event e2, String[] excludedFields) {
        List<String> exclFields = Arrays.asList(excludedFields);
        ConvertedMap m = e2.getData();
        for (Map.Entry<String, Object> entry : e1.getData().entrySet()) {
            if (!exclFields.contains(entry.getKey())) {
                Assert.assertEquals(entry.getValue(), m.get(entry.getKey()));
            }
        }

        m = e2.getMetadata();
        for (Map.Entry<String, Object> entry : e1.getMetadata().entrySet()) {
            if (!exclFields.contains(entry.getKey())) {
                Assert.assertEquals(entry.getValue(), m.get(entry.getKey()));
            }
        }
    }
}
