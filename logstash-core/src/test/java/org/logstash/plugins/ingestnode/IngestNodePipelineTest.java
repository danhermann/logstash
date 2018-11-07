package org.logstash.plugins.ingestnode;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class IngestNodePipelineTest {

    @Test
    public void testMultiplePipelines() throws IOException {
        try (final InputStream src = getClass().getResourceAsStream("multiplePipelines.json")) {
            List<IngestNodePipeline> pipelines = IngestNodePipeline.createFrom(src);
            Assert.assertEquals(8, pipelines.size());
        }

    }
}
