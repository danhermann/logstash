package org.logstash.rocksqueue;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import org.logstash.Event;

/**
 * <p>Queue of {@link Event} expected to be backed by some form of physical storage.</p>
 * Implementations are assumed to be threadsafe.
 */
public interface HybridQueue extends Closeable {

    /**
     * <p>Enqueues an {@link Event} in a blocking fashion.</p>
     * Implementations should behave analogous to {@link ArrayBlockingQueue#put(Object)}.
     * @param event Event to enqueue
     * @throws InterruptedException On interrupt during enqueue
     */
    void enqueue(Event event) throws InterruptedException;

    /**
     * <p>Dequeues an {@link Event} in a blocking fashion.</p>
     * Implementations should behave analogous to {@link ArrayBlockingQueue#take()}.
     * @return {@link Event}.
     * @throws InterruptedException On interrupt during dequeue
     */
    Event dequeue() throws InterruptedException;

    /**
     * <p>Dequeues an {@link Event} in a blocking fashion, subject to a timeout.</p>
     * Implementations should behave analogous to {@link ArrayBlockingQueue#poll(long, TimeUnit)}.
     * @return {@link Event}.
     * @throws InterruptedException On interrupt during dequeue
     */
    Event poll(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Returns {@code true} iff the queue is empty.
     * @return {@code true} iff the queue is empty
     */
    boolean empty();

    /**
     * <p>Implementation of a local-disk-backed {@link Event} queue.</p>
     * This queue guarantees the number of {@link Event} that are in-flight and not yet physically
     * written to disk is limited by a configurable upper bound.
     */
    final class Local implements HybridQueue {

        /**
         * {@link ExecutorService} managing worker threads.
         */
        private final ExecutorService exec = Executors.newSingleThreadExecutor();

        /**
         * Workers moving data from {@link HybridQueue.Local#writeBuffer} and
         * {@link HybridQueue.Local#readBuffer} while simultaneously persisting it to the file
         * system.
         */
        private final QueueWorker worker;

        /**
         * Watermark Database.
         */
        private final QueueIndex index;

        /**
         * Buffer for incoming {@link Event} that have yet to be written to the file system.
         * Chosen to be half the size of the upper bound of allowed in-flight {@link Event}, with
         * the other half of the allow in-flight event count being used for worker I/O buffering
         * and timing `fsync` calls.
         */
        private final BlockingQueue<Event> writeBuffer;

        /**
         * <p>Buffer for already persisted {@link Event} reader for processing and available to queue
         * consumers via {@link HybridQueue.Local#dequeue()}.</p>
         * Chosen to be of a fixed size of {@code 1024}.
         */
        private final BlockingQueue<Event> readBuffer;

        /**
         * Ctor.
         * @param ack Maximum number of in flight {@link Event}
         * @param directory Directory to store backing data in
         */
        public Local(final int ack, final String directory) {
            this.writeBuffer = writebuf(ack / 2);
            this.readBuffer = new ArrayBlockingQueue<>(1024);
            try {
                this.index = new QueueIndex.IndexFile(1, Paths.get(directory));
                this.worker = new QueueWorker.LogWorker(
                    this.index,
                    Paths.get(directory, String.format("%d.log", 0)), 0, readBuffer,
                    writeBuffer, ack
                );
                this.exec.execute(worker);
            } catch (final IOException ex) {
                throw new IllegalStateException(ex);
            }
        }

        @Override
        public void enqueue(final Event event) throws InterruptedException {
            writeBuffer.put(event);
        }

        @Override
        public Event dequeue() throws InterruptedException {
            return readBuffer.take();
        }

        @Override
        public Event poll(final long timeout, final TimeUnit unit) throws InterruptedException {
            return readBuffer.poll(timeout, unit);
        }

        @Override
        public boolean empty() {
            return readBuffer.isEmpty() && writeBuffer.isEmpty() && this.worker.flushed();
        }

        @Override
        public void close() throws IOException {
            this.worker.shutdown();
            this.worker.close();
            this.index.close();
            exec.shutdown();
        }

        /**
         * Sets up the queue's write buffer.
         * @param ack Ack interval to set up buffer for
         * @return {@link ArrayBlockingQueue} of half the ack interval if ack interval is larger
         * than 1, {@link SynchronousQueue} without capacity if it is 1.
         */
        private static BlockingQueue<Event> writebuf(final int ack) {
            final BlockingQueue<Event> res;
            if (ack > 1) {
                res = new ArrayBlockingQueue<>(ack);
            } else {
                res = new SynchronousQueue<>();
            }
            return res;
        }

    }

}