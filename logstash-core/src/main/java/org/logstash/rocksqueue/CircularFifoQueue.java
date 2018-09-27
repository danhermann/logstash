package org.logstash.rocksqueue;

import org.logstash.Event;

public final class CircularFifoQueue {

    private final int max;

    private final Event[] events;

    private int size;

    private int start;

    public CircularFifoQueue(final int max) {
        this.max = max;
        events = new Event[max];
        size = 0;
        start = 0;
    }

    public boolean add(final Event event) {
        final boolean result;
        if (size < max) {
            events[(start + size) % max] = event;
            ++size;
            result = true;
        } else {
            result = false;
        }
        return result;
    }

    public Event get() {
        final Event event;
        if (size > 0) {
            event = events[start];
            --size;
            start = (start + 1) % max;
        } else {
            event = null;
        }
        return event;
    }

    public void rewindOne() {
        ++size;
        start = (start - 1 + max) % max;
    }

    public int size() {
        return size;
    }
}