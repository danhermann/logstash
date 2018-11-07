package org.logstash.plugins.ingestnode;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class IngestNodePipeline {
    private String name = null;
    private List<IngestProcessor> processors;

    public IngestNodePipeline(String name, List<IngestProcessor> processors) {
        this.name = name;
        this.processors = processors;
    }

    public static List<IngestNodePipeline> createFrom(InputStream i) throws IOException {
        List<IngestNodePipeline> pipelines = new ArrayList<>();
        ObjectMapper m = new ObjectMapper();
        JsonNode root = m.readTree(i);

        Iterator<Map.Entry<String, JsonNode>> iterator = root.fields();
        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> entry = iterator.next();
            pipelines.add(new IngestNodePipeline(entry.getKey(),
                    IngestProcessor.createFrom(entry.getValue().get("processors"))));
        }

        return pipelines;
    }

    /**
     * Converts this pipeline into the string JSON format that the ES ingest node expects
     * in the form of:
     *
     * { "processors" : [ {"proc1" : {...}}, {"proc2" : {...}} ] }
     *
     */
    public String toIngestNodeFormat() {
        ObjectMapper m = new ObjectMapper();
        ObjectNode root = m.createObjectNode();

        ArrayNode processors = m.createArrayNode();
        for (IngestProcessor p : this.processors) {
            ObjectNode processorNode = m.createObjectNode();
            processorNode.set(p.getName(), p.getParameters());
            processors.add(processorNode);
        }
        root.set("processors", processors);

        String jsonString = null;
        try {
            jsonString = m.writerWithDefaultPrettyPrinter().writeValueAsString(root);
        } catch (JsonProcessingException e) {
            // these should be pre-validated so that the JSON is always valid
            throw new IllegalStateException(e);
        }
        return jsonString;
    }

    public String getName() {
        return name;
    }

    public List<IngestProcessor> getProcessors() {
        return processors;
    }

    static class IngestProcessor {
        private String name = null;
        private JsonNode parameters;

        IngestProcessor(String name, JsonNode paramters) {
            this.name = name;
            this.parameters = paramters;
        }

        static List<IngestProcessor> createFrom(JsonNode n) {
            List<IngestProcessor> processors = new ArrayList<>();

            for (int k = 0; k < n.size(); k++) {
                JsonNode processorNode = n.get(k);
                Iterator<Map.Entry<String, JsonNode>> iterator = processorNode.fields();
                while (iterator.hasNext()) {
                    Map.Entry<String, JsonNode> entry = iterator.next();
                    processors.add(new IngestProcessor(entry.getKey(), entry.getValue()));
                }
            }

            return processors;
        }

        public String getName() {
            return name;
        }

        public JsonNode getParameters() {
            return parameters;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setParameters(JsonNode parameters) {
            this.parameters = parameters;
        }
    }
}

