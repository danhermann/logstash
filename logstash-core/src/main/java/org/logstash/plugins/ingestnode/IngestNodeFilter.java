package org.logstash.plugins.ingestnode;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.env.Environment;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.logstash.Event;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.function.BiFunction;

public class IngestNodeFilter {

    //Collection<PluginConfigSpec<?>> configSchema();

    private Pipeline pipeline;

    public IngestNodeFilter(String json) {
        this.pipeline = getPipeline(json);
    }

    public Collection<Event> filter(Collection<Event> e) throws Exception {
        List<Event> events = new ArrayList<>();
        for (Event evt : e) {
            IngestDocument doc = IngestMarshaller.toDocument(evt);
            pipeline.execute(doc);
            events.add(IngestMarshaller.toEvent(doc));
        }
        return events;
    }

    private static Pipeline getPipeline(String json) {
        Pipeline pipeline = null;
        try {
            Pipeline.Factory factory = new Pipeline.Factory();
            BytesReference b = new BytesArray(json);

            Map<String, Object> pipelineConfig = XContentHelper.convertToMap(b, false, XContentType.JSON).v2();
            IngestCommonPlugin ingestCommonPlugin = new IngestCommonPlugin();
            Map<String, Processor.Factory> processorFactories = ingestCommonPlugin.getProcessors(getParameters());
            pipeline = factory.create("my_pipeline_id", pipelineConfig, processorFactories);
        } catch (Exception e) {
            System.out.println("Error building pipeline\n" + e);
            e.printStackTrace();
        }
        return pipeline;
    }

    private static Processor.Parameters getParameters() {
        final ThreadPool threadPool = new ThreadPool(getSettings(), new ExecutorBuilder[0]);

        BiFunction<Long, Runnable, ScheduledFuture<?>> scheduler =
                (delay, command) -> threadPool.schedule(TimeValue.timeValueMillis(delay), ThreadPool.Names.GENERIC, command);
        Processor.Parameters parameters = new Processor.Parameters(getEnvironment(), getScriptService(), null, null, null, scheduler);
        return parameters;
    }

    private static Settings getSettings() {
        Settings s = Settings.builder()
                .put("path.home", "/")
                .put("node.name", "foo")
                .build();

        return s;
    }

    private static Environment getEnvironment() {

        return new Environment(getSettings(), null);
    }

    private static ScriptService getScriptService() {
        List<ScriptPlugin> scriptPlugins = new ArrayList<>();
        ScriptModule m = new ScriptModule(Settings.EMPTY, scriptPlugins);
        return m.getScriptService();
    }
}
