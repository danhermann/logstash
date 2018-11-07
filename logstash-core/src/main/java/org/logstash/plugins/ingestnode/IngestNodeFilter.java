package org.logstash.plugins.ingestnode;

import com.google.common.annotations.VisibleForTesting;
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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.function.BiFunction;

public class IngestNodeFilter {

    //Collection<PluginConfigSpec<?>> configSchema();
    static final String PIPELINE_DEFINITIONS = "pipeline_definitions";
    static final String PRIMARY_PIPELINE = "primary_pipeline";

    private Map<String, Pipeline> pipelines;
    private Pipeline primaryPipeline;

    public IngestNodeFilter(Map<String, Object> settings) throws IOException {
        this(new FileInputStream((String)settings.get(PIPELINE_DEFINITIONS)), (String)settings.get(PRIMARY_PIPELINE));
    }

    @VisibleForTesting
    IngestNodeFilter(InputStream pipelineDefinitions, String primaryPipelineName) throws IOException {
        List<IngestNodePipeline> ingestNodePipelines = IngestNodePipeline.createFrom(pipelineDefinitions);

        if (ingestNodePipelines.size() == 0) {
            throw new IllegalStateException("No pipeline definitions found");
        }

        this.pipelines = new Hashtable<>();
        for (IngestNodePipeline p : ingestNodePipelines) {
            pipelines.put(p.getName(), getPipeline(p.getName(), p.toIngestNodeFormat()));
        }

        String resolvedPrimaryPipelineName = primaryPipelineName == null
                ? ingestNodePipelines.get(0).getName()
                : primaryPipelineName;
        primaryPipeline = pipelines.get(resolvedPrimaryPipelineName);
        if (primaryPipeline == null) {
            throw new IllegalStateException(
                    String.format("Could not find primary pipeline '%s'", resolvedPrimaryPipelineName));
        }
    }


    public Collection<Event> filter(Collection<Event> e) throws Exception {
        List<Event> events = new ArrayList<>();
        for (Event evt : e) {
            IngestDocument doc = IngestMarshaller.toDocument(evt);
            primaryPipeline.execute(doc);
            events.add(IngestMarshaller.toEvent(doc));
        }
        return events;
    }

    private static Pipeline getPipeline(String pipelineId, String json) {
        Pipeline pipeline = null;
        try {
            Pipeline.Factory factory = new Pipeline.Factory();
            BytesReference b = new BytesArray(json);

            Map<String, Object> pipelineConfig = XContentHelper.convertToMap(b, false, XContentType.JSON).v2();
            IngestCommonPlugin ingestCommonPlugin = new IngestCommonPlugin();
            Map<String, Processor.Factory> processorFactories = ingestCommonPlugin.getProcessors(getParameters());
            pipeline = factory.create(pipelineId, pipelineConfig, processorFactories);
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
