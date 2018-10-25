package org.logstash.plugins.ingestnode;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.Processor;

import java.util.HashMap;
import java.util.Map;

public class DanTest {

    public static void test() {
        String json = "{}";
        try {
            XContentParser parser = JsonXContent.jsonXContent
                    .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
            //parser.

            final Map<String, Processor.Factory> processorFactories = new HashMap<>();
            Pipeline.Factory factory = new Pipeline.Factory();
            XContentBuilder xcb = XContentBuilder.builder(JsonXContent.jsonXContent);
            new Text(json).bytes();

            BytesReference b = BytesReference.bytes(xcb);
            Map<String, Object> pipelineConfig = XContentHelper.convertToMap(b, false, XContentType.JSON).v2();
            Pipeline pipeline = factory.create("my_pipeline_id", pipelineConfig, processorFactories);
            pipeline.execute(null);

        } catch (Exception e) {
            System.out.println("Error in dantest\n" + e);
        }


    }
}
