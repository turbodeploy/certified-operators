package com.vmturbo.kvstore;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.ecwid.consul.v1.ConsulClient;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Consul distribute lock integration test.
 */
@Ignore("Please setup local Consul before running.")
public class ConsulDistributeLockIntegrationTest {

    private static final String LOCK_SESSION = "lock-session";
    private static final String LOCK_KEY = "lock-key";
    private final Logger logger = LogManager.getLogger();
    StringBuffer stringBuffer = new StringBuffer();

    /**
     * Verify lock will timeout after 120s.
     * TTL is set to 60s, timeout is 2*60 = 120s.
     */
    @Test
    public void testLockTimeout() {
        long start = System.currentTimeMillis();
        final Lock lock = new ConsulDistributedLock(new ConsulClient(), LOCK_SESSION, LOCK_KEY);
        try {
            if (lock.lock(true)) {
                Thread.sleep(new Random().nextInt(1000));
            }
        } catch (Exception e) {
            fail();
        } finally {
            // intentionally skip unlock
            // lock.unlock();
        }
        final Lock newLock = new ConsulDistributedLock(new ConsulClient(), LOCK_SESSION, LOCK_KEY);
        try {
            // even the lock is not unlocked, it will be forcefully unlocked after 120s.
            assertTrue("Session should be able to acquire lock", newLock.lock(true));
        } catch (Exception e) {
            fail();
        } finally {
            lock.unlock();
        }
        long stop = System.currentTimeMillis();
        logger.info("Time used:{} seconds", TimeUnit.MILLISECONDS.toSeconds(stop - start));
    }

    /**
     * Verify if set the `isBlock` to false, it should return right away with false (failing to
     * acquire lock).
     */
    @Test
    public void testLockWithoutBlocking() {
        long start = System.currentTimeMillis();
        final Lock lock = new ConsulDistributedLock(new ConsulClient(), LOCK_SESSION, LOCK_KEY);
        try {
            if (lock.lock(true)) {
                Thread.sleep(new Random().nextInt(1000));
            }
        } catch (Exception e) {
            fail();
        } finally {
            // intentionally skip unlock
            // lock.unlock();
        }
        final Lock newLock = new ConsulDistributedLock(new ConsulClient(), LOCK_SESSION, LOCK_KEY);
        try {
            // if set the `isBlock` to false, it should return right away with false (failing to acquire lock)
            assertFalse("Session should not be able to acquire lock", newLock.lock(false));
        } catch (Exception e) {
            fail();
        } finally {
            lock.unlock();
        }
        long stop = System.currentTimeMillis();
        logger.info("Time used:{} seconds", TimeUnit.MILLISECONDS.toSeconds(stop - start));
    }

    /**
     * Tests lock is mutual exclusive.
     *
     * @throws Exception if any.
     */
    @Test
    public void testLock() throws Exception {
        startThread(1);
        startThread(2);
        startThread(3);
        startThread(4);
        startThread(5);

        assertTrue("Thread 1 should not be interfered by other threads",
                stringBuffer.indexOf("11") >= 0);
        assertTrue("Thread 2 should not be interfered by other threads",
                stringBuffer.indexOf("22") >= 0);
        assertTrue("Thread 3 should not be interfered by other threads",
                stringBuffer.indexOf("33") >= 0);
        assertTrue("Thread 4 should not be interfered by other threads",
                stringBuffer.indexOf("44") >= 0);
        assertTrue("Thread 5 should not be interfered by other threads",
                stringBuffer.indexOf("55") >= 0);
    }

    private void startThread(int i) throws InterruptedException {
        Thread t = new Thread(new LockRunner(i));
        t.start();
        t.join();
    }

    /**
     * Test lock runner thread.
     */
    class LockRunner implements Runnable {

        private final Logger logger = LogManager.getLogger();
        private final int flag;

        LockRunner(int flag) {
            this.flag = flag;
        }

        @Override
        public void run() {
            final Lock lock = new ConsulDistributedLock(new ConsulClient(), LOCK_SESSION, LOCK_KEY);
            try {
                if (lock.lock(true)) {
                    logger.info("Thread " + flag + " start!");
                    stringBuffer.append(flag);
                    Thread.sleep(new Random().nextInt(1000));
                    logger.info("Thread " + flag + " end!");
                    stringBuffer.append(flag);
                }
            } catch (Exception e) {
                fail();
            } finally {
                lock.unlock();
            }
        }
    }
}
