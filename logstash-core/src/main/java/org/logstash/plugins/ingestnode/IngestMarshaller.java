package org.logstash.plugins.ingestnode;

import org.elasticsearch.ingest.IngestDocument;
import org.logstash.Event;

public class IngestMarshaller {

    public static Event toEvent(IngestDocument document) {
        return null;
    }

    public static IngestDocument toDocument(Event e) {
        return null;
    }
}
