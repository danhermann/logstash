package org.logstash.rocksqueue;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public interface QueueIndex extends Closeable {

    /**
     * Returns the current read offset for a partition.
     * @param partition Partition Id
     * @return Current read offset for partition
     */
    long getReadOffset(int partition);

    /**
     * Returns the current write offset for a partition.
     * @param partition Partition Id
     * @return Current write offset for partition
     */
    long getWriteOffset(int partition);

    /**
     * Appends a new offset pair for a partition.
     * @param partition Partition id
     * @param highWatermark High getReadOffset
     * @param watermark Watermark
     */
    void append(int partition, long highWatermark,
        long watermark) throws IOException;

    /**
     * File Backed {@link QueueIndex}.
     */
    final class IndexFile implements QueueIndex {

        /**
         * Offsets array.
         */
        private final long[] offsets;

        /**
         * Output {@link FileChannel} that appends to the index file.
         */
        private final FileChannel out;

        /**
         * Write Buffer.
         */
        private final ByteBuffer buffer =
            ByteBuffer.allocateDirect(Integer.BYTES + 2 * Long.BYTES);

        IndexFile(final int partitions, final Path dir) throws IOException {
            final Path path = dir.resolve("queue.index");
            offsets = loadOffsets(partitions, path);
            out = FileChannel.open(
                path, StandardOpenOption.APPEND, StandardOpenOption.CREATE
            );
        }

        @Override
        public synchronized long getReadOffset(final int partition) {
            return this.offsets[2 * partition];
        }

        @Override
        public synchronized long getWriteOffset(final int partition) {
            return this.offsets[2 * partition + 1];
        }

        @Override
        public synchronized void append(final int partition, final long writeOffset,
            final long readOffset) throws IOException {
            this.offsets[2 * partition] = readOffset;
            this.offsets[2 * partition + 1] = writeOffset;
            buffer.position(0);
            buffer.putInt(partition);
            buffer.putLong(readOffset);
            buffer.putLong(writeOffset);
            buffer.position(0);
            out.write(buffer);
            out.force(false);
        }

        @Override
        public void close() throws IOException {
            out.force(true);
            out.close();
        }

        /**
         * Loads the getReadOffset file from given index path.
         * @param partitions Number of partitions expected to be found in the getReadOffset file
         * @param index Index file path
         * @return Watermark array
         */
        private static long[] loadOffsets(final int partitions, final Path index) {
            final long[] offsets = new long[2 * partitions];
            final File file = index.toFile();
            if (file.exists()) {
                try (final DataInputStream stream = new DataInputStream(
                    new BufferedInputStream(new FileInputStream(index.toFile()))
                )
                ) {
                    while (true) {
                        final int part = stream.readInt();
                        offsets[2 * part] = stream.readLong();
                        offsets[2 * part + 1] = stream.readLong();
                    }
                } catch (final EOFException ignored) {
                    //End of Index File
                } catch (final IOException ex) {
                    throw new IllegalStateException(ex);
                }
            }
            return offsets;
        }
    }
}