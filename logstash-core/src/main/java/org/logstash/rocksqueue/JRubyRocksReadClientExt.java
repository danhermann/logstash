package org.logstash.rocksqueue;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.anno.JRubyClass;
import org.logstash.RubyUtil;
import org.logstash.execution.QueueBatch;
import org.logstash.execution.QueueReadClient;
import org.logstash.execution.QueueReadClientBase;

import java.io.IOException;

@JRubyClass(name = "AckedReadClient", parent = "QueueReadClientBase")
public class JRubyRocksReadClientExt extends QueueReadClientBase implements QueueReadClient {

    private final RocksQueue queue;

    public static JRubyRocksReadClientExt create(final RocksQueue queue) {
        return new JRubyRocksReadClientExt(RubyUtil.RUBY, RubyUtil.ROCKS_READ_CLIENT_CLASS, queue);
    }

    public JRubyRocksReadClientExt(final Ruby runtime, final RubyClass metaClass) {
        super(runtime, metaClass);
        this.queue = null;
    }

    private JRubyRocksReadClientExt(final Ruby runtime, final RubyClass metaClass,
                                    final RocksQueue queue) {
        super(runtime, metaClass);
        this.queue = queue;
    }

    @Override
    public void close() throws IOException {
        queue.close();
    }

    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    @Override
    public QueueBatch newBatch() {
        return new RocksBatch(queue, batchSize);
    }

    @Override
    public QueueBatch readBatch() {
        RocksBatch batch = queue.readBatch(batchSize);
        startMetrics(batch);
        return batch;
    }

}
