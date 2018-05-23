package org.logstash.rocksqueue;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.Event;
import org.logstash.ext.JrubyEventExtLibrary;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static org.logstash.common.ByteUtils.longFromBytes;
import static org.logstash.common.Util.nanosToMillis;

public class SqliteQueue extends ExperimentalQueue implements Closeable {

    private static final Logger logger = LogManager.getLogger(PartitionedRocksQueue.class);
    private static final int HIGH_WATERMARK = 0;
    private static final int LOW_WATERMARK = 0;
    private static final int EVENT_CACHE_SIZE_FACTOR = 5;

    private String dirPath;
    private String pipelineId;
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
    private boolean runStatsThread = true;

    private Connection connection;
    private PreparedStatement enqueueStatement;

    SqliteQueue(String pipelineId, String dirPath, int batchSize, int pipelineWorkers) {
        this.dirPath = dirPath;
        this.pipelineId = pipelineId;
        eventCacheSize = batchSize * pipelineWorkers * EVENT_CACHE_SIZE_FACTOR;
        eventCache = new ArrayDeque<>(eventCacheSize);
        //serializer = new LengthPrefixedEventSerializer();
        maxSequenceId = new AtomicLong();
        isClosed = new AtomicBoolean(false);
        Thread statsThread = new Thread(() -> {
            while (runStatsThread) {
                logStats();
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    // do nothing
                }
            }
        });
        statsThread.start();

    }

    public void open() {
        try {

            connection = DriverManager.getConnection("jdbc:sqlite:/home/dan/dev/rq/data/queue/main.db");
            Statement statement = connection.createStatement();
            statement.executeUpdate("DROP TABLE IF EXISTS queue");
            statement.executeUpdate("CREATE TABLE queue (idkey INTEGER PRIMARY KEY, val BLOB)");

        /*
            // read up to eventCache.size() records
            iterator.seekToFirst();
            logger.info("Seeked to first record");
            int recordCount = 0;
            while (iterator.isValid() && recordCount < eventCacheSize) {
                eventCache.add(new RocksQueue.EventSequencePair(
                        //serializer.deserialize(iterator.value()), longFromBytes(iterator.key()))); // no lock required here
                        Event.deserialize(iterator.value()), longFromBytes(iterator.key())));
                recordCount++;
                iterator.next();
            }
            */

        /*
            // find max sequence ID
            if (recordCount > 0) {
                iterator.seekToLast();
                maxSequenceId.set(iterator.isValid() ? longFromBytes(iterator.key(), 0) : 0);
            } else {
                maxSequenceId.set(0);
            }
            */
            maxSequenceId.set(0);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }

    }

    void enqueue(Event event) {
        long seqId = maxSequenceId.addAndGet(1);

        long startTime, endTime;
        try {
            startTime = System.nanoTime();
            //enqueueStatement.executeUpdate();
            enqueueStatement = connection.prepareStatement("INSERT INTO queue (idkey, val) VALUES (?,?)");
            enqueueStatement.setLong(1, seqId);
            enqueueStatement.setBytes(2, event.serialize());
            enqueueStatement.execute();
            endTime = System.nanoTime();
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        } catch (SQLException e) {
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

        throw new UnsupportedOperationException("not implemented in the PoC");
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

    public void closeBatch(RocksBatch batch) {
        if (batch.filteredSize() == 0) {
            return;
        }

        /*
        try {
            rocksDb.deleteRange(longToBytes(batch.minSequenceId()), longToBytes(batch.maxSequenceId()));
        } catch (RocksDBException e) {
            // handle error
            throw new IllegalStateException(e);
        }
        */

    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            runStatsThread = false;
            logStats();

            try {
                enqueueStatement.close();
                connection.close();
            } catch (SQLException e) {
                logger.error(String.format("Error closing queue '%s'", pipelineId), e);
            }

        }
    }

    private void logStats() {
        /*
        if (options != null) {
            logger.info(String.format("Compaction style     : %s", options.compactionStyle()));
            logger.info(String.format("Compression type     : %s", options.compressionType()));
            logger.info(String.format("Block size           : %d", options.arenaBlockSize()));
            logger.info(String.format("Direct IO            : %s", options.useDirectIoForFlushAndCompaction()));
        }
        */

        logger.info(String.format("Enqueue count        : %d", enqueueCount));
        logger.info(String.format("Enqueue agg duration : %g", nanosToMillis(enqueueTotalTime)));
        logger.info(String.format("Enqueue min duration : %g", nanosToMillis(enqueueMinTime)));
        logger.info(String.format("Enqueue max duration : %g", nanosToMillis(enqueueMaxTime)));
        logger.info(String.format("Enqueue avg duration : %g", nanosToMillis(enqueueTotalTime / (double) enqueueCount)));

        logger.info(String.format("ReadBatch count      : %d", readBatchCount));

        /*
        if (statistics != null) {
            logger.info(String.format("RocksDB statistics   : %s", statistics));
        }
        */
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

 }