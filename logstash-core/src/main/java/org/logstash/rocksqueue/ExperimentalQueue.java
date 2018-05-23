package org.logstash.rocksqueue;

import org.logstash.Event;
import org.logstash.ext.JrubyEventExtLibrary;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;

abstract class ExperimentalQueue implements Closeable {

    abstract void closeBatch(RocksBatch batch) throws IOException;

    @Override
    public abstract void close();

    abstract boolean isEmpty();

    abstract RocksBatch readBatch(int batchSize);

    abstract void open();

    abstract void enqueue(Event event);

    abstract void enqueueBatch(Collection<JrubyEventExtLibrary.RubyEvent> events);
}
