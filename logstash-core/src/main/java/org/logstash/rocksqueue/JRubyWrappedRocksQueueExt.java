package org.logstash.rocksqueue;

import org.jruby.Ruby;
import org.jruby.RubyBoolean;
import org.jruby.RubyClass;
import org.jruby.RubyFixnum;
import org.jruby.RubyObject;
import org.jruby.RubyString;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.logstash.RubyUtil;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wraps a RocksQueue instance directly and provides the following Ruby-ified functions:
 * open/close
 * collect_stats -- not yet
 * push/read -- no?
 * read/write clients
 */

@JRubyClass(name = "WrappedRocksQueue")
public class JRubyWrappedRocksQueueExt extends RubyObject {

    private ExperimentalQueue queue;
    private final AtomicBoolean isClosed = new AtomicBoolean();

    @JRubyMethod(name = "initialize", required = 4)
    public IRubyObject rubyInitialize(ThreadContext context, IRubyObject[] args) throws IOException {
        RubyString pipelineId = (RubyString) args[0];
        RubyString dirPath = (RubyString) args[1];
        RubyFixnum batchSize = (RubyFixnum) args[2];
        RubyFixnum workers = (RubyFixnum) args[3];

        //queue = new PartitionedRocksQueue(pipelineId.asJavaString(), dirPath.asJavaString(), batchSize.getIntValue(), workers.getIntValue());
        queue = new SingleTableRocksQueue(pipelineId.asJavaString(), dirPath.asJavaString(), batchSize.getIntValue(), workers.getIntValue());
        queue.open();

        return context.nil;
    }

    public JRubyWrappedRocksQueueExt(final Ruby runtime, final RubyClass metaClass) {
        super(runtime, metaClass);
    }

    public void close() throws IOException {
        queue.close();
        isClosed.set(true);
    }

    @JRubyMethod(name = "close")
    public IRubyObject rubyClose(ThreadContext context) {
        try {
            close();
        } catch (IOException e) {
            throw RubyUtil.newRubyIOError(context.runtime, e);
        }
        return context.nil;
    }

    @JRubyMethod(name = "write_client")
    public IRubyObject rubyWriteClient(final ThreadContext context) {
        return JRubyRocksWriteClientExt.create(queue);
    }

    @JRubyMethod(name = "read_client")
    public IRubyObject rubyReadClient(final ThreadContext context) {

        return JRubyRocksReadClientExt.create(queue);
    }

    @JRubyMethod(name = "is_empty?")
    public IRubyObject rubyIsEmpty(ThreadContext context) {
        return RubyBoolean.newBoolean(context.runtime, queue.isEmpty());
    }

    @JRubyMethod(name = "collect_stats")
    public IRubyObject rubyCollectStats(final ThreadContext context, final IRubyObject pipelineMetric) {
        throw new UnsupportedOperationException("not implemented yet");
    }

    /*
    @JRubyMethod(name = {"push", "<<"})
    public void rubyPush(ThreadContext context, IRubyObject event) {
        queue.enqueue(((JrubyEventExtLibrary.RubyEvent) event).getEvent());
    }

    @JRubyMethod(name = "read_batch")
    public IRubyObject rubyReadBatch(ThreadContext context, IRubyObject size, IRubyObject wait) {
        return queue.ruby_read_batch(context, size, wait);
    }
    */

}
