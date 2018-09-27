package org.logstash.rocksqueue;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.logstash.Event;

public interface QueueWorker extends Runnable, Closeable {

    /**
     * Returns {@code true} iff all data in this worker's backing datafile and associated
     * buffers has been consumed.
     * @return {@code true} iff all data handled by this worker has been flushed to a
     * consumer
     */
    boolean flushed();

    /**
     * Signal this worker to stop processing {@link Event} and flush all internal buffers.
     */
    void shutdown();

    /**
     * <p>Background worker passing events from {@link HybridQueue.Local#writeBuffer} to
     * {@link HybridQueue.Local#readBuffer} while simultaneously persisting them to the
     * file system.</p>
     * <p>This worker tries to actively (in a blocking manner) promote persisted data to
     * deserialized in memory storage as capacity becomes available.</p>
     */
    final class LogWorker implements QueueWorker {

        /**
         * Offset Index Database.
         */
        private final QueueIndex index;

        /**
         * Size (in number of {@link Event}) of the un-contended output queue-buffer available
         * to this QueueWorker.
         */
        private static final int OUT_BUFFER_SIZE = 100;

        /**
         * Size (in byte) of the internal I/O buffers. Gives the upper bound for the size of
         * a serialized {@link Event} representation.
         */
        private static final int BYTE_BUFFER_SIZE = 1 << 16;

        /**
         * Maximum number of in-flight {@link Event} that have yet to be persisted to disk.
         */
        private final int ack;

        /**
         * {@link FileChannel} used in conjunction with
         * {@link QueueWorker.LogWorker#obuf} to physically write to the file system.
         */
        private final FileChannel out;

        /**
         * {@link FileChannel} used to read persisted data from the filesystem.
         */
        private final FileChannel in;

        /**
         * Output buffer used to buffer writes by {@link QueueWorker.LogWorker#out}.
         */
        private final ByteBuffer obuf = ByteBuffer.allocateDirect(BYTE_BUFFER_SIZE);

        /**
         * Input buffer used to buffer reads by {@link QueueWorker.LogWorker#in}.
         */
        private final ByteBuffer ibuf = ByteBuffer.allocateDirect(BYTE_BUFFER_SIZE);

        /**
         * Incoming {@link Event} buffer shared across all workers on a
         * {@link HybridQueue.Local}.
         */
        private final BlockingQueue<Event> writeBuffer;

        /**
         * Outgoing {@link Event} buffer shared across all workers on a
         * {@link HybridQueue.Local}.
         */
        private final BlockingQueue<Event> readBuffer;

        /**
         * {@link CountDownLatch} indicating that this worker has no more in flight events or
         * buffered data and can be safely closed.
         */
        private final CountDownLatch shutdown = new CountDownLatch(1);

        /**
         * Boolean indicating whether or not this worker is active.
         */
        private volatile boolean running = true;

        /**
         * Un-contended queue to buffer {@link Event} deserialized from reads on
         * {@link QueueWorker.LogWorker#in} to.
         */
        private final CircularFifoQueue outBuffer = new CircularFifoQueue(OUT_BUFFER_SIZE);

        /**
         * Deserialization buffer used to pass data to {@link Event#deserialize(byte[])}.
         */
        private final byte[] readByteBuffer = new byte[BYTE_BUFFER_SIZE];

        /**
         * Partition of this QueueWorker.
         */
        private final int partition;

        private long lastReadFileOffset;

        /**
         * <p>Offset on the backing file to read the next {@link Event} from in case
         * {@link QueueWorker.LogWorker#outBuffer} is depleted.</p>
         * Note that {@link QueueWorker.LogWorker#obuf} must be flushed in order to
         * make this number correspond to a physical offset on
         * {@link QueueWorker.LogWorker#in}.
         */
        private long readOffset;

        /**
         * Largest valid offset on the backing data file.
         */
        private long writeOffset;

        /**
         * Number of {@link Event} written to disk.
         */
        private long count;

        /**
         * Number of {@link Event} successfully passed to
         * {@link QueueWorker.LogWorker#readBuffer}.
         */
        private long flushed;

        /**
         * Ctor.
         * @param index {@link QueueIndex.IndexFile} storing offsets.
         * @param file Backing data {@link Path}
         * @param readBuffer Same instance as {@link HybridQueue.Local#readBuffer}
         * @param writeBuffer Same instance as {@link HybridQueue.Local#writeBuffer}
         * @param ack Maximum number of in-flight events, that are either stored serialized
         * in {@link QueueWorker.LogWorker#obuf} or already written to
         * {@link QueueWorker.LogWorker#out}, but not yet `fsync`ed to the file system
         * @throws IOException On failure to open backing data file for either reads or writes
         */
        LogWorker(final QueueIndex index, final Path file, final int partition,
            final BlockingQueue<Event> readBuffer, final BlockingQueue<Event> writeBuffer,
            final int ack) throws IOException {
            this.index = index;
            this.partition = partition;
            this.out =
                FileChannel.open(file, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
            this.in = FileChannel.open(file, StandardOpenOption.READ);
            this.readBuffer = readBuffer;
            this.writeBuffer = writeBuffer;
            count = 0L;
            flushed = 0L;
            writeOffset = this.index.getWriteOffset(partition);
            readOffset = this.index.getReadOffset(partition);
            // Use half the ack interval for serialized buffering, except for the case of
            // ack == 1 which has serialized buffering only
            this.ack = Math.max(1, ack / 2);
        }

        @Override
        public void run() {
            while (running) {
                try {
                    final Event event = this.writeBuffer.poll(10L, TimeUnit.MILLISECONDS);
                    if (event != null) {
                        write(event);
                    }
                    if ((count % (long) ack == 0L || event == null && count == flushed)
                        && obuf.position() > 0) {
                        flush();
                    }
                    int j = 0;
                    while (j < 5 && advanceFile()) {
                        ++j;
                    }
                } catch (final InterruptedException | IOException ex) {
                    throw new IllegalStateException(ex);
                }
            }
            try {
                flush();
            } catch (final IOException ex) {
                throw new IllegalStateException(ex);
            }
            this.shutdown.countDown();
        }

        @Override
        public void shutdown() {
            running = false;
        }

        @Override
        public boolean flushed() {
            return writeOffset == readOffset && obuf.position() == 0 &&
                outBuffer.size() == 0;
        }

        @Override
        public void close() throws IOException {
            try {
                this.shutdown.await();
            } catch (final InterruptedException ex) {
                throw new IllegalStateException(ex);
            }
            this.in.close();
            this.out.close();
            this.index.append(partition, readOffset, writeOffset);
        }

        /**
         * Serialize and write a {@link Event} to the backing file.
         * @param event Event to persist
         * @throws IOException On failure to serialize or write event to underlying storage
         */
        private void write(final Event event) throws IOException {
            final boolean fullyRead =
                writeOffset + (long) obuf.position() == readOffset;
            ++count;
            final byte[] data = event.serialize();
            maybeFlush(data.length + Integer.BYTES);
            obuf.putInt(data.length);
            obuf.put(data);
            if (count == flushed - 1L && readBuffer.offer(event)
                || fullyRead && outBuffer.add(event)) {
                readOffset = writeOffset + (long) obuf.position();
            }
        }

        /**
         * Flush {@link QueueWorker.LogWorker#obuf} to the filesystem if another write
         * of given size could not be buffered to it.
         * @param size Size of the next write
         * @throws IOException On failure to flush buffer to filesystem
         */
        private void maybeFlush(final int size) throws IOException {
            final int pos = obuf.position();
            if (pos > 0 && BYTE_BUFFER_SIZE - pos < size) {
                flush();
            }
        }

        /**
         * <p>Flushes {@link QueueWorker.LogWorker#obuf} to the filesystem.</p>
         * Note that the method triggers `fsync` and therefore guarantees physical persistence
         * within the limits of the backing file system.
         * @throws IOException On failure to flush buffer to filesystem
         */
        private void flush() throws IOException {
            obuf.flip();
            writeOffset += (long) out.write(obuf);
            out.force(true);
            index.append(partition, readOffset, writeOffset);
            obuf.clear();
        }

        /**
         * Tries to advance as many {@link Event}s as possible from
         * {@link QueueWorker.LogWorker#outBuffer} to {@link QueueWorker.LogWorker#readBuffer}.
         */
        private void advanceBuffers() {
            final int size = outBuffer.size();
            for (int i = 0; i < size; ++i) {
                if (readBuffer.offer(outBuffer.get())) {
                    flushed++;
                } else {
                    outBuffer.rewindOne();
                    break;
                }
            }
        }

        /**
         * Promotes {@link Event}s that are only buffered in serialized form in the file system
         * to deserialized buffers.
         * @return {@code true} iff at least one {@link Event} was deserialized and buffered
         * @throws IOException On failure to read from underlying storage
         */
        private boolean advanceFile() throws IOException {
            if (flushed + (long) outBuffer.size() < count &&
                this.readOffset == writeOffset) {
                this.flush();
            }
            this.advanceBuffers();
            int remaining = OUT_BUFFER_SIZE - outBuffer.size();
            final int before = remaining;
            if (remaining > 0 && readOffset < writeOffset) {
                if(lastReadFileOffset != readOffset) {
                    this.in.position(readOffset);
                }
                ibuf.clear();
                lastReadFileOffset += (long) in.read(ibuf);
                ibuf.flip();
                while (ibuf.remaining() >= Integer.BYTES && remaining > 0) {
                    final int len = ibuf.getInt();
                    ibuf.get(readByteBuffer, 0, len);
                    outBuffer.add(Event.deserialize(readByteBuffer));
                    readOffset += (long) (len + Integer.BYTES);
                    --remaining;
                }
            }
            return before > remaining;
        }
    }
}