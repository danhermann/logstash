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
import org.logstash.execution.codecs.CodecFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

@LogstashPlugin(name = "stdin")
public class Stdin implements Input{

    public static final PluginConfigSpec<String> CODEC_CONFIG =
            LsConfiguration.stringSetting("codec", "line");

    private static final int BUFFER_SIZE = 16_384;
    static final int EVENT_BUFFER_LENGTH = 64;

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
        codec = CodecFactory.getInstance().getCodec(configuration.get(CODEC_CONFIG),
                configuration, context);
        stdin = new CloseShieldInputStream(sourceStream);
    }

    @Override
    public void start(QueueWriter writer) {
        byte[] buffer = new byte[BUFFER_SIZE];
        @SuppressWarnings({"unchecked"})
        Map<String, Object>[] eventBuffer =
                (HashMap<String, Object>[]) Array.newInstance(
                        new HashMap<String, Object>().getClass(), EVENT_BUFFER_LENGTH);

        try {
            int bytesRead, events;
            while (!stopRequested && (bytesRead = stdin.read(buffer)) > -1) {
                do {
                    events = codec.decode(buffer, 0, bytesRead, eventBuffer);
                    bytesRead = 0;
                    sendEvents(writer, eventBuffer, events);

                } while (events == eventBuffer.length);
            }

            eventBuffer = codec.flush();
            sendEvents(writer, eventBuffer, eventBuffer.length);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } finally {
            try {
                stdin.close();
            } catch (IOException e) {
                // do nothing
            }
            isStopped.countDown();
        }
    }

    private void sendEvents(QueueWriter writer, Map<String, Object>[] events, int eventCount) {
        for (int k = 0; k < eventCount; k++) {
            Map<String, Object> event = events[k];
            event.putIfAbsent("hostname", hostname);
            writer.push(event);
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
        return PluginHelper.commonInputOptions(Collections.singletonList(CODEC_CONFIG));
    }
}
