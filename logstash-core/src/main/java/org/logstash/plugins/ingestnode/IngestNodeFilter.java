package org.logstash.plugins.ingestnode;

import com.google.common.annotations.VisibleForTesting;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.painless.PainlessScriptEngine;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.script.IngestConditionalScript;
import org.elasticsearch.script.IngestScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.function.BiFunction;

public class IngestNodeFilter implements PipelineProvider {

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
            IngestDocument result = primaryPipeline.execute(doc);
            if (result != null) {
                events.add(IngestMarshaller.toEvent(result));
            } else {
                evt.cancel();
                events.add(evt);
            }
        }
        return events;
    }

    @Override
    public Pipeline getPipelineByName(String name) {
        return pipelines.get(name);
    }

    private Pipeline getPipeline(String pipelineId, String json) {
        Pipeline pipeline = null;
        try {
            BytesReference b = new BytesArray(json);
            Map<String, Object> pipelineConfig = XContentHelper.convertToMap(b, false, XContentType.JSON).v2();
            pipeline = Pipeline.create(pipelineId, pipelineConfig, getProcessorFactories(), getScriptService());
        } catch (Exception e) {
            System.out.println("Error building pipeline\n" + e);
            e.printStackTrace();
        }
        return pipeline;
    }

    private Map<String, Processor.Factory> getProcessorFactories() {
        IngestCommonPlugin ingestCommonPlugin = new IngestCommonPlugin();
        Map<String, Processor.Factory> defaultFactories = ingestCommonPlugin.getProcessors(getParameters());
        Map<String, Processor.Factory> overriddenFactories = new HashMap<>(defaultFactories);
        overriddenFactories.put(PipelineProcessor.TYPE, new PipelineProcessor.Factory(this));
        return Collections.unmodifiableMap(overriddenFactories);
    }

    private static Processor.Parameters getParameters() {
        final ThreadPool threadPool = new ThreadPool(getSettings());

        BiFunction<Long, Runnable, ScheduledFuture<?>> scheduler =
                (delay, command) -> threadPool.schedule(TimeValue.timeValueMillis(delay), ThreadPool.Names.GENERIC, command);
        return new Processor.Parameters(getEnvironment(), getScriptService(), null, null, null, scheduler, null);
    }

    private static Settings getSettings() {
        return Settings.builder()
                .put("path.home", "/")
                .put("node.name", "foo")
                .build();
    }

    private static Environment getEnvironment() {

        return new Environment(getSettings(), null);
    }

    private static ScriptService getScriptService() {
        Map<String, ScriptEngine> engines = new HashMap<>();
        engines.put(PainlessScriptEngine.NAME, new PainlessScriptEngine(getSettings(), scriptContexts()));
        return new ScriptService(getSettings(), engines, ScriptModule.CORE_CONTEXTS);
    }

    private static Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        Map<ScriptContext<?>, List<Whitelist>> contexts = new HashMap<>();
        contexts.put(IngestScript.CONTEXT, Whitelist.BASE_WHITELISTS);
        contexts.put(IngestConditionalScript.CONTEXT, Whitelist.BASE_WHITELISTS);
        return contexts;
    }

}
