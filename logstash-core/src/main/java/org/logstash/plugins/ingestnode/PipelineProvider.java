package org.logstash.plugins.ingestnode;

import org.elasticsearch.ingest.Pipeline;

public interface PipelineProvider {

    Pipeline getPipelineByName(String name);
}
