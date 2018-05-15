package org.logstash.rocksqueue;

import org.jruby.RubyArray;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.logstash.execution.QueueBatch;
import org.logstash.ext.JrubyEventExtLibrary;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.logstash.RubyUtil.RUBY;

public class RocksBatch implements QueueBatch, Closeable {

    private final RocksQueue queue;

    // a batch must contain all events between minSequenceId and maxSequenceId
    private List<RocksQueue.EventSequencePair> events;

    public RocksBatch(final RocksQueue queue, int batchSize) {
        this.queue = queue;
        this.events = new ArrayList<>(batchSize);
    }

    @Override
    public int filteredSize() {
        return events.size();
    }

    @Override
    public RubyArray to_a() {
        ThreadContext context = RUBY.getCurrentContext();
        final RubyArray result = context.runtime.newArray(events.size());
        for (final RocksQueue.EventSequencePair esp : events) {
            if (!esp.event.isCancelled()) {
                result.add(JrubyEventExtLibrary.RubyEvent.newRubyEvent(context.runtime, esp.event));
            }
        }
        return result;
    }

    @Override
    public void merge(IRubyObject event) {
        throw new UnsupportedOperationException("merge() should never be called");
    }

    @Override
    public void close() throws IOException {
        queue.closeBatch(this);
    }

    void add(RocksQueue.EventSequencePair pair) {
        events.add(pair);
    }

    long minSequenceId() {
        return events.size() > 0 ? events.get(0).seqNum : Long.MAX_VALUE;
    }

    long maxSequenceId() {
        return events.size() > 0 ? events.get(events.size() - 1).seqNum : Long.MIN_VALUE;
    }
}
