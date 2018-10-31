package org.logstash.plugins.ingestnode;

import org.junit.Test;

public class IngestNodeFilterTest {

    @Test
    public void test() {

        String json =

                "{" +
                        "    \"processors\": [" +
                        "      {" +
                        "        \"rename\": {" +
                        "          \"field\": \"hostname\"," +
                        "          \"target_field\": \"host\"," +
                        "          \"ignore_missing\": true" +
                        "        }" +
                        "      }" +
                        "    ]" +
                        "  }";

        IngestNodeFilter.test(json);
    }
}
