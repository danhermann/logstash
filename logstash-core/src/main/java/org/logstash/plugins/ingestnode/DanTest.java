package org.logstash.plugins.ingestnode;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.common.RenameProcessor;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DanTest {

    public static void test() {
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

        try {
            Pipeline.Factory factory = new Pipeline.Factory();
            BytesReference b = new BytesArray(json);

            Map<String, Object> pipelineConfig = XContentHelper.convertToMap(b, false, XContentType.JSON).v2();
            Map<String, Processor.Factory> processorFactories = new HashMap<>();
            processorFactories.put("rename", new RenameProcessor.Factory(getScriptService()));
            //pipelineConfig.put(Pipeline.PROCESSORS_KEY, processorFactories);
            Pipeline pipeline = factory.create("my_pipeline_id", pipelineConfig, processorFactories);

            IngestDocument doc = getIngestDocument();

            pipeline.execute(doc);
            System.out.println("========= ingest pipeline succeeded: " + new String(doc.getFieldValueAsBytes("host")));

        } catch (Exception e) {
            System.out.println("Error in dantest\n" + e);
            e.printStackTrace();
        }


    }

    private static ScriptService getScriptService() {
        ScriptModule m = new ScriptModule(Settings.EMPTY, null);
        return m.getScriptService();
    }

    private static IngestDocument getIngestDocument() {
        Map<String, Object> ingestMetadata = new HashMap<>();
        ingestMetadata.put("timestamp", ZonedDateTime.now(ZoneOffset.UTC));

        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("hostname", "foo");

        return new IngestDocument(sourceAndMetadata, ingestMetadata);
    }
}
