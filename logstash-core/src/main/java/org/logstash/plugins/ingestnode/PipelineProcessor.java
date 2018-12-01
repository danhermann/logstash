package org.logstash.plugins.ingestnode;

import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.Processor;

import java.util.Map;

public class PipelineProcessor extends AbstractProcessor {

    public static final String TYPE = "pipeline";

    private String pipelineName;
    private PipelineProvider pipelineProvider;

    private PipelineProcessor(String tag, String pipelineName, PipelineProvider pipelineProvider) {
        super(tag);
        this.pipelineProvider = pipelineProvider;
        this.pipelineName = pipelineName;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        Pipeline pipeline = pipelineProvider.getPipelineByName(pipelineName);
        if (pipeline == null) {
            throw new IllegalStateException(String.format("Could not find pipeline '%s'", pipelineName));
        }

        return pipeline.execute(ingestDocument);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        private PipelineProvider pipelineProvider;

        public Factory(PipelineProvider p) {
            this.pipelineProvider = p;
        }

        @Override
        public PipelineProcessor create(Map<String, Processor.Factory> registry, String processorTag,
                                        Map<String, Object> config) {
            String pipeline = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "name");
            return new PipelineProcessor(processorTag, pipeline, pipelineProvider);
        }
    }

}
