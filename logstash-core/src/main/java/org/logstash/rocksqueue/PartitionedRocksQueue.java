package org.logstash.rocksqueue;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.Event;
import org.logstash.ext.JrubyEventExtLibrary;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Statistics;

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static org.logstash.common.ByteUtils.longFromBytes;
import static org.logstash.common.ByteUtils.longToBytes;
import static org.logstash.common.Util.nanosToMillis;

public class PartitionedRocksQueue extends ExperimentalQueue implements Closeable {

    private static final Logger logger = LogManager.getLogger(PartitionedRocksQueue.class);
    private static final int HIGH_WATERMARK = 0;
    private static final int LOW_WATERMARK = 0;
    private static final int EVENT_CACHE_SIZE_FACTOR = 5;
    private static final int EVENTS_PER_PARTITION = 1_000_000;
    private static byte[] MANIFEST;

    private String dirPath;
    private String pipelineId;
    private Options partitionOptions;
    private Options manifestOptions;
    private ColumnFamilyOptions partitionCfOptions;
    private ColumnFamilyOptions manifestCfOptions;
    private RocksDB rocksDb;
    private Statistics statistics;
    private AtomicLong maxSequenceId;
    private AtomicLong partitionId;
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

    static {
        try {
            MANIFEST = "manifest".getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            // will never throw for the above statement
        }
    }

    PartitionedRocksQueue(String pipelineId, String dirPath, int batchSize, int pipelineWorkers) {
        this.dirPath = dirPath;
        this.pipelineId = pipelineId;
        eventCacheSize = batchSize * pipelineWorkers * EVENT_CACHE_SIZE_FACTOR;
        eventCache = new ArrayDeque<>(eventCacheSize);
        //serializer = new LengthPrefixedEventSerializer();
        maxSequenceId = new AtomicLong();
        partitionId = new AtomicLong();
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
        //statsThread.start();
    }

    public void open() {

        RocksDB.loadLibrary();

        statistics = new Statistics();

        partitionCfOptions = new ColumnFamilyOptions()
                .setCompactionStyle(CompactionStyle.LEVEL)
                .setDisableAutoCompactions(true)
                .setCompressionType(CompressionType.LZ4_COMPRESSION);

        manifestCfOptions = new ColumnFamilyOptions()
                .setCompactionStyle(CompactionStyle.LEVEL)
                .setDisableAutoCompactions(false)
                .setCompressionType(CompressionType.LZ4_COMPRESSION);


        partitionOptions = new Options()
                //.setArenaBlockSize(64 * 1024)
                .setUseDirectIoForFlushAndCompaction(true)
                .setCompressionType(partitionCfOptions.compressionType())
                .setCompactionStyle(partitionCfOptions.compactionStyle())
                //.optimizeLevelStyleCompaction(1024*1024*100) // no significant effects
                //.setIncreaseParallelism(4)
                //.setNumLevels(1)
                .setStatistics(statistics)
                .setDisableAutoCompactions(partitionCfOptions.disableAutoCompactions())
                //.setStatsDumpPeriodSec(3)
                .setCreateIfMissing(true);

        // use copy constructor in newer release
        manifestOptions = new Options()
                //.setArenaBlockSize(partitionOptions.arenaBlockSize())
                .setUseDirectIoForFlushAndCompaction(partitionOptions.useDirectIoForFlushAndCompaction())
                .setCompressionType(manifestCfOptions.compressionType())
                .setCompactionStyle(manifestCfOptions.compactionStyle())
                //.optimizeLevelStyleCompaction(1024*1024*100) // no significant effects
                //.setIncreaseParallelism(4)
                //.setNumLevels(partitionOptions.numLevels())
                .setStatistics(statistics)
                .setDisableAutoCompactions(manifestCfOptions.disableAutoCompactions()) // override this one
                //.setStatsDumpPeriodSec(partitionOptions.statsDumpPeriodSec())
                .setCreateIfMissing(partitionOptions.createIfMissing());

        List<ColumnFamilyDescriptor> initialColumnFamilies = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, partitionCfOptions),
                new ColumnFamilyDescriptor(MANIFEST, manifestCfOptions)
        );


        RocksIterator iterator = null;

        try {

logger.info("Initializing RocksDB queue");
            rocksDb = RocksDB.open(partitionOptions, dirPath);
logger.info("Opened RocksDB queue");

            ColumnFamilyDescriptor d;

rocksDb.createColumnFamily(null);
            iterator = rocksDb.newIterator();
logger.info("Opened iterator");

            // read up to eventCache.size() records
            iterator.seekToFirst();
logger.info("Seeked to first record");
            int recordCount = 0;
            while (iterator.isValid() && recordCount < eventCacheSize) {
                eventCache.add(new EventSequencePair(
                        //serializer.deserialize(iterator.value()), longFromBytes(iterator.key()))); // no lock required here
                        Event.deserialize(iterator.value()), longFromBytes(iterator.key())));
                recordCount++;
                iterator.next();
            }

logger.info("Read existing records: "+recordCount);

            // find max sequence ID
            if (recordCount > 0) {
                iterator.seekToLast();
                maxSequenceId.set(iterator.isValid() ? longFromBytes(iterator.key(), 0) : 0);
            } else {
                maxSequenceId.set(0);
            }

            List<byte[]> columnFamilies = RocksDB.listColumnFamilies(partitionOptions, dirPath);
            for (byte[] columnFamily:columnFamilies) {
                System.out.println(new String(columnFamily, "UTF-8"));
            }

//System.exit(1);


            logger.info("Found max seq id: "+maxSequenceId.get());

        } catch (IOException e) {
            // do some error handling
            throw new IllegalStateException(e);
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

        throw new UnsupportedOperationException("not implemented in the PoC");
        /*
        long highestBatchSeqId = maxSequenceId.addAndGet(events.size());

        // write batch to rocks
        JrubyEventExtLibrary.RubyEvent event;
        try (
                WriteBatch batch = new WriteBatch();
                WriteOptions partitionOptions = new WriteOptions()
        ) {
            Iterator<JrubyEventExtLibrary.RubyEvent> iterator = events.iterator();
            int loopCounter = 0;
            while ((event = iterator.next()) != null) {
                batch.put(longToBytes(highestBatchSeqId - events.size() + loopCounter++),
                        serializer.serialize(event.getEvent()));
            }
            rocksDb.write(partitionOptions, batch);
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
            runStatsThread = false;
            logStats();

            try {
logger.info("Starting compaction");
                rocksDb.compactRange();
logger.info("Finished compaction");
            } catch (RocksDBException e) {
                logger.error(String.format("Error compacting RocksDB queue '%s'", pipelineId), e);

            }

            if (statistics != null) {
                statistics.close();
                statistics = null;
            }

            if (partitionOptions != null) {
                partitionOptions.close();
            }

            if (rocksDb != null) {
                rocksDb.close();
            }
        }
    }

    private void logStats() {
        if (partitionOptions != null) {
            logger.info(String.format("Compaction style     : %s", partitionOptions.compactionStyle()));
            logger.info(String.format("Compression type     : %s", partitionOptions.compressionType()));
            logger.info(String.format("Block size           : %d", partitionOptions.arenaBlockSize()));
            logger.info(String.format("Direct IO            : %s", partitionOptions.useDirectIoForFlushAndCompaction()));
        }

        logger.info(String.format("Enqueue count        : %d", enqueueCount));
        logger.info(String.format("Enqueue agg duration : %g", nanosToMillis(enqueueTotalTime)));
        logger.info(String.format("Enqueue min duration : %g", nanosToMillis(enqueueMinTime)));
        logger.info(String.format("Enqueue max duration : %g", nanosToMillis(enqueueMaxTime)));
        logger.info(String.format("Enqueue avg duration : %g", nanosToMillis(enqueueTotalTime / (double)enqueueCount)));

        logger.info(String.format("ReadBatch count      : %d", readBatchCount));

        if (statistics != null) {
            logger.info(String.format("RocksDB statistics   : %s", statistics));
        }
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
