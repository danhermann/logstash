package org.logstash.rocksqueue;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyObject;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.logstash.RubyUtil;
import org.logstash.ext.JrubyEventExtLibrary;

import java.util.Collection;

@JRubyClass(name = "RocksWriteClient")
public class JRubyRocksWriteClientExt extends RubyObject {
    private RocksQueue queue;

    public JRubyRocksWriteClientExt(final Ruby runtime, final RubyClass metaClass) {
        super(runtime, metaClass);
    }

    private JRubyRocksWriteClientExt(final Ruby runtime, final RubyClass metaClass,
                                     final RocksQueue queue) {
        super(runtime, metaClass);
        this.queue = queue;
    }

    public static JRubyRocksWriteClientExt create(RocksQueue queue) {
        return new JRubyRocksWriteClientExt(
                RubyUtil.RUBY, RubyUtil.ROCKS_WRITE_CLIENT_CLASS, queue);
    }

    @JRubyMethod(name = {"push", "<<"}, required = 1)
    public IRubyObject rubyPush(final ThreadContext context, IRubyObject event) {
        queue.enqueue(((JrubyEventExtLibrary.RubyEvent) event).getEvent());
        return this;
    }

    @JRubyMethod(name = "push_batch", required = 1)
    public IRubyObject rubyPushBatch(final ThreadContext context, IRubyObject batch) {
        queue.enqueueBatch((Collection<JrubyEventExtLibrary.RubyEvent>) batch);
        return this;
    }

}
