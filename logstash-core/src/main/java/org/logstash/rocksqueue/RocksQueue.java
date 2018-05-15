package org.logstash.rocksqueue;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.Event;
import org.logstash.LengthPrefixedEventSerializer;
import org.logstash.ext.JrubyEventExtLibrary;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Statistics;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static org.logstash.common.ByteUtils.longFromBytes;
import static org.logstash.common.ByteUtils.longToBytes;
import static org.logstash.common.Util.nanosToMillis;

public class RocksQueue implements Closeable {

    private static final Logger logger = LogManager.getLogger(RocksQueue.class);
    private static final int HIGH_WATERMARK = 0;
    private static final int LOW_WATERMARK = 0;
    private static final int EVENT_CACHE_SIZE_FACTOR = 5;

    private String dirPath;
    private String pipelineId;
    private Options options;
    private RocksDB rocksDb;
    private Statistics statistics;
    private AtomicLong maxSequenceId;
    private AtomicBoolean isClosed;
    //private LengthPrefixedEventSerializer serializer;


    private long enqueueCount;
    private long enqueueTotalTime;
    private long enqueueMinTime = Long.MAX_VALUE;
    private long enqueueMaxTime = Long.MIN_VALUE;

    private long readBatchCount;

    private ArrayDeque<EventSequencePair> eventCache; // all ops are O(1) and we don't need thread safety
    private int eventCacheSize;
    private ReentrantLock eventCacheLock = new ReentrantLock();

    RocksQueue(String pipelineId, String dirPath, int batchSize, int pipelineWorkers) {
        this.dirPath = dirPath;
        this.pipelineId = pipelineId;
        eventCacheSize = batchSize * pipelineWorkers * EVENT_CACHE_SIZE_FACTOR;
        eventCache = new ArrayDeque<>(eventCacheSize);
        //serializer = new LengthPrefixedEventSerializer();
        maxSequenceId = new AtomicLong();
        isClosed = new AtomicBoolean(false);
    }

    public void open() throws IOException {

        RocksDB.loadLibrary();

        statistics = new Statistics();
        options = new Options()
                .setArenaBlockSize(64 * 1024)
                .setUseDirectIoForFlushAndCompaction(true)
                .setCompressionType(CompressionType.LZ4_COMPRESSION)
                .setCompactionStyle(CompactionStyle.LEVEL)
                .setCreateIfMissing(true)
                .setStatistics(statistics);

        RocksIterator iterator = null;
        try {
System.out.println("Initializing RocksDB queue");
            rocksDb = RocksDB.open(options, dirPath);
System.out.println("Opened RocksDB queue");
            iterator = rocksDb.newIterator();
System.out.println("Opened iterator");

            // read up to eventCache.size() records
            iterator.seekToFirst();
System.out.println("Seeked to first record");
            int recordCount = 0;
            while (iterator.isValid() && recordCount < eventCacheSize) {
                eventCache.add(new EventSequencePair(
                        //serializer.deserialize(iterator.value()), longFromBytes(iterator.key()))); // no lock required here
                        Event.deserialize(iterator.value()), longFromBytes(iterator.key())));
                recordCount++;
                iterator.next();
            }

System.out.println("Reads existing records: "+recordCount);

            // find max sequence ID
            if (recordCount > 0) {
                iterator.seekToLast();
                maxSequenceId.set(iterator.isValid() ? longFromBytes(iterator.key(), 0) : 0);
            } else {
                maxSequenceId.set(0);
            }


System.out.println("Found max seq id: "+maxSequenceId.get());

        } catch (RocksDBException e) {
            // do some error handling
            throw new IllegalStateException(e);
        } finally {
            if (iterator != null) {
                iterator.close();
            }
        }
    }

    void enqueue(Event event) {
        long seqId = maxSequenceId.addAndGet(1);

        long startTime, endTime;
        // write to rocks
        try {
            startTime = System.nanoTime();
            rocksDb.put(longToBytes(seqId), event.serialize());
            //rocksDb.put(longToBytes(seqId), serializer.serialize(event));
            endTime = System.nanoTime();
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        } catch (RocksDBException e) {
            throw new IllegalStateException(e);
        }

        // add to eventCache if not full
        try {
            eventCacheLock.lock();
            eventCache.add(new EventSequencePair(event, seqId));
            recordEnqueueStats(startTime, endTime);
        } finally {
            eventCacheLock.unlock();
        }
    }

    void enqueueBatch(Collection<JrubyEventExtLibrary.RubyEvent> events) {

        throw new UnsupportedOperationException("most (all?) inputs enqueue single events so the PoC doesn't need this implementation");
        /*
        long highestBatchSeqId = maxSequenceId.addAndGet(events.size());

        // write batch to rocks
        JrubyEventExtLibrary.RubyEvent event;
        try (
                WriteBatch batch = new WriteBatch();
                WriteOptions options = new WriteOptions()
        ) {
            Iterator<JrubyEventExtLibrary.RubyEvent> iterator = events.iterator();
            int loopCounter = 0;
            while ((event = iterator.next()) != null) {
                batch.put(longToBytes(highestBatchSeqId - events.size() + loopCounter++),
                        serializer.serialize(event.getEvent()));
            }
            rocksDb.write(options, batch);
        } catch (RocksDBException e) {
            // handle the error
            throw new IllegalStateException(e);
        }

        eventCacheLock.lock();
        try {
            Iterator<JrubyEventExtLibrary.RubyEvent> iterator = events.iterator();
            while ((event = iterator.next()) != null) {
            }
        } finally {
            eventCacheLock.unlock();
        }
        */
    }

    boolean isEmpty() {
        eventCacheLock.lock();
        try {
            return eventCache.size() == 0;
        } finally {
            eventCacheLock.unlock();
        }
    }

    RocksBatch readBatch(int batchSize) {
        if (eventCache.size() > 0) {
            eventCacheLock.lock();
            RocksBatch batch = new RocksBatch(this, batchSize);
            try {
                if (eventCache.size() > 0) {
                    int counter = 0;
                    while (eventCache.size() > 0 && counter < batchSize) {
                        batch.add(eventCache.remove());
                        counter++;
                    }
                }
                recordReadBatchStats();
            } finally {
                eventCacheLock.unlock();
            }
            return batch;
        } else {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                // do nothing
            }
            return new RocksBatch(this, 0);
        }
    }

    void closeBatch(RocksBatch batch) {
        if (batch.filteredSize() == 0) {
            return;
        }

        try {
            rocksDb.deleteRange(longToBytes(batch.minSequenceId()), longToBytes(batch.maxSequenceId()));
        } catch (RocksDBException e) {
            // handle error
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            // print stats for PoC
            printStats();

            try {
System.out.println("Starting compaction");
                rocksDb.compactRange();
System.out.println("Finished compaction");
            } catch (RocksDBException e) {
                logger.error(String.format("Error compacting RocksDB queue '%s'", pipelineId), e);

            }

            if (statistics != null) {
                statistics.close();
            }

            if (options != null) {
                options.close();
            }

            if (rocksDb != null) {
                rocksDb.close();
            }
        }
    }

    private void printStats() {
        System.out.println(String.format("Compaction style     : %s", options.compactionStyle()));
        System.out.println(String.format("Compression type     : %s", options.compressionType()));
        System.out.println(String.format("Block size           : %d", options.arenaBlockSize()));
        System.out.println(String.format("Direct IO            : %s", options.useDirectIoForFlushAndCompaction()));

        System.out.println(String.format("Enqueue count        : %d", enqueueCount));
        System.out.println(String.format("Enqueue agg duration : %g", nanosToMillis(enqueueTotalTime)));
        System.out.println(String.format("Enqueue min duration : %g", nanosToMillis(enqueueMinTime)));
        System.out.println(String.format("Enqueue max duration : %g", nanosToMillis(enqueueMaxTime)));
        System.out.println(String.format("Enqueue avg duration : %g", nanosToMillis(enqueueTotalTime / (double)enqueueCount)));

        System.out.println(String.format("ReadBatch count      : %d", readBatchCount));

        System.out.println(String.format("RocksDB statistics   : %s", statistics));
    }

    private void recordEnqueueStats(long startTime, long endTime) {
        long enqueueDuration = endTime - startTime;
        enqueueTotalTime += enqueueDuration;
        enqueueCount++;
        if (enqueueDuration < enqueueMinTime) {
            enqueueMinTime = enqueueDuration;
        }
        if (enqueueDuration > enqueueMaxTime) {
            enqueueMaxTime = enqueueDuration;
        }
    }

    private void recordReadBatchStats() {
        readBatchCount++;
    }

    class EventSequencePair {
        Event event;
        long seqNum;

        EventSequencePair(Event e, long seqNum) {
            this.event = e;
            this.seqNum = seqNum;
        }
    }
}
