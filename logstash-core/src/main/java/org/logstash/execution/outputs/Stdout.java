package org.logstash.execution.outputs;

import org.apache.commons.io.output.CloseShieldOutputStream;
import org.logstash.Event;
import org.logstash.execution.LogstashPlugin;
import org.logstash.execution.LsConfiguration;
import org.logstash.execution.LsContext;
import org.logstash.execution.Output;
import org.logstash.execution.PluginConfigSpec;
import org.logstash.execution.PluginHelper;
import org.logstash.execution.QueueReader;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

@LogstashPlugin(name = "stdout")
public class Stdout implements Output {
    public static final String DEFAULT_CODEC_NAME = "line"; // no codec support, yet

    private OutputStream stdout;
    private volatile boolean stopRequested = false;
    private final CountDownLatch isStopped = new CountDownLatch(1);

    /**
     * Required Constructor Signature only taking a {@link LsConfiguration}.
     *
     * @param configuration Logstash Configuration
     * @param context       Logstash Context
     */
    public Stdout(final LsConfiguration configuration, final LsContext context) {
        this(configuration, context, System.out);
    }

    Stdout(final LsConfiguration configuration, final LsContext context, OutputStream targetStream) {
        stdout = new CloseShieldOutputStream(targetStream);
    }

    @Override
    public void output(QueueReader reader) {
        final Event event = new Event();
        final PrintStream printer = new PrintStream(stdout); // replace this with a codec
        try {
            long sequence = reader.poll(event);
            while (!stopRequested && sequence > -1L) {
                try {
                    printer.println(event.toJson()); // use codec here
                    reader.acknowledge(sequence);
                } catch (final IOException ex) {
                    throw new IllegalStateException(ex);
                }
                sequence = reader.poll(event);
            }
        } finally {
            try {
                stdout.close();
            } catch (IOException e) {
                // do nothing
            }
            isStopped.countDown();
        }
    }

    @Override
    public void stop() {
        stopRequested = true;
    }

    @Override
    public void awaitStop() throws InterruptedException {
        isStopped.await();
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        return PluginHelper.commonOutputOptions();
    }
}
