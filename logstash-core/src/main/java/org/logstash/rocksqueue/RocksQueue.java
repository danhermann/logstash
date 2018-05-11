package org.logstash.rocksqueue;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.Closeable;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class RocksQueue implements Closeable {

    public void open() {
        // a static method that loads the RocksDB C++ library.
        RocksDB.loadLibrary();

        // the Options class contains a set of configurable DB options
        // that determines the behaviour of the database.
        try (final Options options = new Options().setCreateIfMissing(true)) {

            // a factory method that returns a RocksDB instance
            try (final RocksDB db = RocksDB.open(options, "/home/dan/dev/testing/stdin/rocksdb")) {

                // do something
                long start = System.nanoTime();

                //write lots of records
                /*
                for (int k = 0; k < 100_000; k++) {
                    db.put(("mykey" + k).getBytes(), "myvalue".getBytes());
                }
                */

                // read lots of records
                long recordCount = 0;
                RocksIterator iterator = db.newIterator();
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                    iterator.value();
                    recordCount++;
                }
                System.out.println("found record count: "+recordCount);

                long end = System.nanoTime();

                //String s = new String(db.get(new ReadOptions().setReadTier(ReadTier.PERSISTED_TIER).setTailing(false), "mykey".getBytes()));
                //System.out.println("Retrieved value from db: "+s);
                System.out.println("Executed in ms: " + NANOSECONDS.toMillis(end - start));
            }
        } catch (RocksDBException e) {
            // do some error handling
        }

    }

    public void push() {

    }

    public void pushBatch() {

    }

    public boolean isEmpty() {
        return true;
    }

    public void readBatch() {

    }

    // ?
    public void newBatch() {

    }

    @Override
    public void close() {

    }
}
