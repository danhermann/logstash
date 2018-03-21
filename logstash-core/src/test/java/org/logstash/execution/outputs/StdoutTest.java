package org.logstash.execution.outputs;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Test;
import org.logstash.Event;
import org.logstash.execution.QueueReader;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class StdoutTest {
    private static boolean streamWasClosed = false;

    /**
     * Verifies that the stdout output is reloadable because it does not close the underlying
     * output stream which, outside of test cases, is always {@link java.lang.System#out}.
     */
    @Test
    public void testUnderlyingStreamIsNotClosed() {
        OutputStream dummyOutputStream = new ByteArrayOutputStream(0) {
            @Override
            public void close() throws IOException {
                streamWasClosed = true;
                super.close();
            }
        };
        Stdout stdout = new Stdout(null, null, dummyOutputStream);
        Thread t = new Thread(() -> stdout.output(new TestQueueReader(getTestEvents())));
        t.start();
        try {
            stdout.awaitStop();
        } catch (InterruptedException e) {
            fail("Stdin.awaitStop failed with exception: " + e);
        }

        assertFalse(streamWasClosed);
    }

    @Test
    public void testEvents() throws JsonProcessingException {
        StringBuilder expectedOutput = new StringBuilder();
        Event[] testEvents = getTestEvents();
        for (Event e : testEvents) {
            expectedOutput.append(String.format(e.toJson() + "%n"));
        }

        OutputStream dummyOutputStream = new ByteArrayOutputStream(0);
        Stdout stdout = new Stdout(null, null, dummyOutputStream);
        Thread t = new Thread(() -> stdout.output(new TestQueueReader(testEvents)));
        t.start();
        try {
            Thread.sleep(50);
            stdout.awaitStop();
        } catch (InterruptedException e) {
            fail("Stdin.awaitStop failed with exception: " + e);
        }

        assertEquals(expectedOutput.toString(), dummyOutputStream.toString());
    }

    private static Event[] getTestEvents() {
        Event e1 = new Event();
        e1.setField("myField", "event1");
        Event e2 = new Event();
        e2.setField("myField", "event2");
        Event e3 = new Event();
        e3.setField("myField", "event3");
        return new Event[]{e1, e2, e3};
    }

}

// returns the supplied events in order for testing purposes
class TestQueueReader implements QueueReader {

    private Event[] events;
    private int index = 0;

    public TestQueueReader(Event[] events) {
        this.events = events;
    }

    @Override
    public long poll(Event event) {
        if (events.length > index) {
            event.overwrite(events[index]);
            index++;
            return index;
        }
        return -1L;
    }

    @Override
    public long poll(Event event, long millis) {
        return poll(event);
    }

    @Override
    public void acknowledge(long sequenceNum) {
        // do nothing
    }
}
