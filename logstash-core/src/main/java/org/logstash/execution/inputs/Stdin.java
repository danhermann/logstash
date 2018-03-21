package org.logstash.execution.inputs;

import org.apache.commons.io.input.CloseShieldInputStream;
import org.logstash.execution.Codec;
import org.logstash.execution.Input;
import org.logstash.execution.LogstashPlugin;
import org.logstash.execution.LsConfiguration;
import org.logstash.execution.LsContext;
import org.logstash.execution.PluginConfigSpec;
import org.logstash.execution.PluginHelper;
import org.logstash.execution.QueueWriter;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

@LogstashPlugin(name = "stdin")
public class Stdin implements Input{

    public static final String DEFAULT_CODEC_NAME = "line"; // no codec support, yet
    private static final int BUFFER_SIZE = 16_384;

    private String hostname;
    private InputStream stdin;
    private Codec codec;
    private volatile boolean stopRequested = false;
    private final CountDownLatch isStopped = new CountDownLatch(1);

    /**
     * Required Constructor Signature only taking a {@link LsConfiguration}.
     * @param configuration Logstash Configuration
     * @param context Logstash Context
     */
    public Stdin(final LsConfiguration configuration, final LsContext context) {
        this(configuration, context, System.in);
    }

    Stdin(final LsConfiguration configuration, final LsContext context, InputStream sourceStream) {
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostname = "[unknownHost]";
        }
        stdin = new CloseShieldInputStream(sourceStream);
    }

    @Override
    public void start(QueueWriter writer) {
        Scanner input = new Scanner(stdin); // replace scanner with codec
        byte[] buffer = new byte[BUFFER_SIZE];

        try {
            while (!stopRequested && stdin.read(buffer) > -1) {
                final String message = input.next();
                Map<String, Object> event = new HashMap<>();
                event.putIfAbsent("hostname", hostname);
                event.put("message", message);
                writer.push(event);
            }

            codec.flush();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        finally {
            try {
                stdin.close();
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
        return PluginHelper.commonInputOptions();
    }
}
