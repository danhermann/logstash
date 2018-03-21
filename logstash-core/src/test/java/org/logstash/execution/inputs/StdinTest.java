package org.logstash.execution.inputs;

import org.junit.Test;
import org.logstash.execution.LsConfiguration;
import org.logstash.execution.QueueWriter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class StdinTest {

    private static boolean streamWasClosed = false;

    /**
     * Verifies that the stdin input is reloadable because it does not close the underlying
     * input stream which, outside of test cases, is always {@link java.lang.System#in}.
     */
    @Test
    public void testUnderlyingStreamIsNotClosed() {
        InputStream dummyInputStream = new ByteArrayInputStream(new byte[0]) {
            @Override
            public void close() throws IOException {
                streamWasClosed = true;
                super.close();
            }
        };
        Stdin stdin = new Stdin(new LsConfiguration(Collections.EMPTY_MAP), null, dummyInputStream);
        Thread t = new Thread(() -> stdin.start(new TestQueueWriter()));
        t.start();
        try {
            stdin.awaitStop();
        } catch (InterruptedException e) {
            fail("Stdin.awaitStop failed with exception: " + e);
        }

        assertFalse(streamWasClosed);
    }

    @Test
    public void testEvents() {
        String testInput = "foo" + System.lineSeparator() + "bar" + System.lineSeparator() + "baz" + System.lineSeparator();
        InputStream dummyInputStream = new ByteArrayInputStream(testInput.getBytes());
        Stdin stdin = new Stdin(new LsConfiguration(Collections.EMPTY_MAP), null, dummyInputStream);
        TestQueueWriter queueWriter = new TestQueueWriter();
        Thread t = new Thread(() -> stdin.start(queueWriter));
        t.start();
        try {
            Thread.sleep(50);
            stdin.awaitStop();
        } catch (InterruptedException e) {
            fail("Stdin.awaitStop failed with exception: " + e);
        }

        assertEquals(3, queueWriter.getEvents().size());
    }

    @Test
    public void testMoreEventsPerReadThanBufferSize() {
        int expectedEvents = Stdin.EVENT_BUFFER_LENGTH + 2;
        StringBuilder s = new StringBuilder("");
        for (int k = 0; k < expectedEvents; k++) {
            s.append("z").append(System.lineSeparator());
        }
        InputStream dummyInputStream = new ByteArrayInputStream(s.toString().getBytes());
        Stdin stdin = new Stdin(new LsConfiguration(Collections.EMPTY_MAP), null, dummyInputStream);
        TestQueueWriter queueWriter = new TestQueueWriter();
        Thread t = new Thread(() -> stdin.start(queueWriter));
        t.start();
        try {
            Thread.sleep(50);
            stdin.awaitStop();
        } catch (InterruptedException e) {
            fail("Stdin.awaitStop failed with exception: " + e);
        }

        assertEquals(expectedEvents, queueWriter.getEvents().size());
    }

}

class TestQueueWriter implements QueueWriter {

    private List<Map<String, Object>> events = new ArrayList<>();

    @Override
    public long push(Map<String, Object> event) {
        synchronized (this) {
            events.add(event);
        }
        return 0;
    }

    @Override
    public long watermark() {
        return 0;
    }

    @Override
    public long highWatermark() {
        return 0;
    }

    public List<Map<String, Object>> getEvents() {
        return events;
    }
}
