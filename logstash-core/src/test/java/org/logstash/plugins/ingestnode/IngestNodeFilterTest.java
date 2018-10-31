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
import java.util.List;
import java.util.Map;

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
    public void testLowercaseProcessor() throws Exception {

        String json =

                "{" +
                        "    \"processors\": [" +
                        "      {" +
                        "        \"lowercase\": {" +
                        "          \"field\": \"hostname\"," +
                        "          \"target_field\": \"host\"," +
                        "          \"ignore_missing\": false" +
                        "        }" +
                        "      }" +
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
    public void testRenameProcessor() throws Exception {

        String json =

                "{" +
                        "    \"processors\": [" +
                        "      {" +
                        "        \"rename\": {" +
                        "          \"field\": \"hostname\"," +
                        "          \"target_field\": \"host\"," +
                        "          \"ignore_missing\": false" +
                        "        }" +
                        "      }" +
                        "    ]" +
                        "  }";

        Event e1 = new Event();
        e1.setField("hostname", "foo");
        Event e2 = IngestNodeFilter.filter(json, e1);
        compareEventsExcludingFields(e1, e2, new String[]{"host", "hostname"});
        Assert.assertEquals(e1.getField("hostname"), e2.getField("host"));
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
