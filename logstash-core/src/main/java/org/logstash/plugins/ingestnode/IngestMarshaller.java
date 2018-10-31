package org.logstash.plugins.ingestnode;

import org.elasticsearch.ingest.IngestDocument;
import org.joda.time.DateTime;
import org.logstash.Event;
import org.logstash.Javafier;
import org.logstash.Timestamp;
import org.logstash.Valuefier;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.Map;

class IngestMarshaller {

    static final String INGEST_TIMESTAMP = "timestamp";

    private IngestMarshaller() {
    }

    static Event toEvent(IngestDocument document) {
        Event e = new Event();

        Map<String, Object> source = document.getSourceAndMetadata();
        Map<String, Object> metadata = document.getIngestMetadata();

        // handle timestamp separately
        ZonedDateTime z = (ZonedDateTime)metadata.get(INGEST_TIMESTAMP);
        if (z != null) {
            e.setTimestamp(new Timestamp(z.toEpochSecond() * 1000 +
                    z.getLong(ChronoField.MILLI_OF_SECOND)));
        }

        // handle version separately
        String version = (String)source.get(Event.VERSION);
        if (version != null) {
            e.getData().putInterned(Event.VERSION, version);
        }

        for (Map.Entry<String, Object> entry : source.entrySet()) {
            if (!entry.getKey().equals(Event.VERSION)) {
                e.setField(entry.getKey(), Valuefier.convert(entry.getValue()));
            }
        }

        for (Map.Entry<String, Object> entry : metadata.entrySet()) {
            if (!entry.getKey().equals(INGEST_TIMESTAMP)) {
                e.setField(entry.getKey(), Valuefier.convert(entry.getValue()));
            }
        }

        return e;
    }

    static IngestDocument toDocument(Event e) {
        Map<String, Object> data = new HashMap<>();
        Map<String, Object> metadata = new HashMap<>();

        // handle timestamp separately
        try {
            DateTime t = e.getTimestamp().getTime();
            ZonedDateTime z = ZonedDateTime.of(t.getYear(), t.getMonthOfYear(), t.getDayOfMonth(),
                    t.getHourOfDay(), t.getMinuteOfHour(), t.getSecondOfMinute(), t.getMillisOfSecond() * 1000000,
                    ZoneOffset.UTC);
            metadata.put(INGEST_TIMESTAMP, z);
        } catch (IOException ex) {
            // should never happen
            throw new RuntimeException(ex);
        }

        for (Map.Entry<String, Object> entry : e.getData().entrySet()) {
            if (!entry.getKey().equals(Event.TIMESTAMP)) {
                data.put(entry.getKey(), Javafier.deep(entry.getValue()));
            }
        }

        for (Map.Entry<String, Object> entry : e.getMetadata().entrySet()) {
            metadata.put(entry.getKey(), Javafier.deep(entry.getValue()));
        }

        return new IngestDocument(data, metadata);
    }
}
