package org.logstash.rocksqueue;

import org.logstash.Event;

class EventSequencePair {
    Event event;
    long seqNum;

    EventSequencePair(Event e, long seqNum) {
        this.event = e;
        this.seqNum = seqNum;
    }
}
