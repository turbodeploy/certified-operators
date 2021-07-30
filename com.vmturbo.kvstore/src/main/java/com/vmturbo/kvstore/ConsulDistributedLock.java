package com.vmturbo.kvstore;

import java.time.LocalDateTime;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.kv.model.PutParams;
import com.ecwid.consul.v1.session.model.NewSession;
import com.ecwid.consul.v1.session.model.Session;

/**
 * Distributed locks for Consul to support mutual exclusion on same key.
 * It is recommended practice to always immediately follow a call to lock with a try block, most typically in a before/after construction such as:
 *    public void m() {
 *      final ConsulDistributedLock lock = new ConsulDistributedLock(consulClient, lockSession, lockKey);
 *      try {
 *          if (lock.lock(true)) { // block until condition holds
 *              // ... method body
 *          }
 *      } catch (InterruptedException e) {
 *          Thread.currentThread().interrupt();
 *      } catch (RuntimeException e) {
 *      } finally {
 *        lock.unlock()
 *      }
 *    }
 * Reference:
 * https://learn.hashicorp.com/tutorials/consul/distributed-semaphore
 * https://programmer.group/implementation-of-distributed-lock-based-on-constul.html
 * http://blog.didispace.com
 */
public class ConsulDistributedLock implements Lock {

    private static final String prefix = "lock/";

    private final ConsulClient consulClient;
    private final String sessionName;
    private String sessionId = null;
    private final String keyPath;

    /**
     * Create Consul distribute lock.
     *
     * @param consulClient consul client
     * @param sessionName session name for the lock
     * @param lockKey lock key, will append with prefix
     */
    public ConsulDistributedLock(@Nonnull final ConsulClient consulClient,
            final @Nonnull String sessionName,
            final @Nonnull String lockKey) {
        this.consulClient = Objects.requireNonNull(consulClient);
        this.sessionName = Objects.requireNonNull(sessionName);
        this.keyPath = prefix + Objects.requireNonNull(lockKey);
    }

    private String createSession() {
        NewSession newSession = new NewSession();
        newSession.setName(sessionName);
        newSession.setBehavior(Session.Behavior.DELETE);
        // session will be forcibly invalidated after 60 seconds to avoid locking indefinitely.
        newSession.setTtl("60s");
        return consulClient.sessionCreate(newSession, null).getValue();
    }

    private void destroySession() {
        if (sessionId != null) {
            consulClient.sessionDestroy(sessionId, null);
            sessionId = null;
        }
    }

    /**
     * Create session and acquires the distributed lock.
     * Acquires the lock if it is not held by another session and returns immediately.
     * If the lock is held by another session then the current session will lies dormant until
     * the lock has been acquired.
     * Lock can only be held for maximum 60 seconds to avoid indefinite lock.
     * @param isBlock block or not, it should be blocked most of the time.
     * @return Consul distributed lock.
     */
    @Override
    public synchronized boolean lock(final boolean isBlock) throws InterruptedException {
        // 300 * 1000ms = 5 * 60 * 1000ms = 5 * 1min = 5min
        return lock(isBlock, 1000L, 300);
    }

    /**
     * Acquires the lock.
     *
     * @param block block or not, it should be blocked most of the time.
     * @param retryInterval when block is true, retry interval
     * @param maxRetryTimes when block is true, maximum retry times. If it's not set, it
     *         will try to acquire lock forever.
     * @return Consul distributed lock.
     * @throws InterruptedException if the thread is interrupted.
     */
    private boolean lock(final boolean block, @Nonnull final Long retryInterval,
            final Integer maxRetryTimes) throws InterruptedException {
        if (sessionId != null) {
            throw new RuntimeException(sessionId + " - Already locked!");
        }
        sessionId = createSession();
        int count = 1;
        while (true) {
            PutParams putParams = new PutParams();
            putParams.setAcquireSession(sessionId);
            if (consulClient.setKVValue(keyPath, "lock:" + LocalDateTime.now(), putParams)
                    .getValue()) {
                return true;
            } else if (block) {
                if (maxRetryTimes != null && count >= maxRetryTimes) {
                    return false;
                } else {
                    count++;
                    if (retryInterval != null) {
                        Thread.sleep(retryInterval);
                    }
                    continue;
                }
            } else {
                return false;
            }
        }
    }

    /**
     * Release lock.
     *
     * @return if the lock is release successfully.
     */
    @Override
    public synchronized boolean unlock() {
        final PutParams putParams = new PutParams();
        putParams.setReleaseSession(sessionId);
        final boolean result = consulClient.setKVValue(keyPath, "unlock:" + LocalDateTime.now(),
                putParams).getValue();

        destroySession();
        return result;
    }
}
