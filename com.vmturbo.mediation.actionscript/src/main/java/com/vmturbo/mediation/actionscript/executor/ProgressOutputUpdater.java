package com.vmturbo.mediation.actionscript.executor;

import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.sdk.probe.IProgressTracker;

/**
 * Monitor stdout for an executing action script, and periodically post progress updates showing
 * recent output.
 *
 * <p>We register as an observer of an {@link OutputHandler} instance, but we don't perform a
 * progress update unless a minimum period has elapsed since last update, in order to reduce
 * chatter.</p>
 */
class ProgressOutputUpdater implements Observer, AutoCloseable {
    // thread that monitors output and performs progress updates
    private Thread thread = null;
    // true of we have been notified of new output since the last progress update
    private boolean newOutputAvailable = false;
    // last time that keep alive message sent
    private long lastKeepAliveTimestamp = 0L;

    /**
     * Create a new instance.
     *
     * @param progressTracker the {@link IProgressTracker} to update with recent output
     * @param outputHandler   the {@link OutputHandler} that we will observe
     * @param updateIntervalSecs  minimum number of seconds between progress updates
     * @param keepAliveIntervalSecs the number of seconds when keep alive message should be sent.
     */
    ProgressOutputUpdater(final @Nonnull IProgressTracker progressTracker, final @Nonnull OutputHandler outputHandler,
                          final int updateIntervalSecs, final int keepAliveIntervalSecs) {
        outputHandler.addObserver(this);
        this.thread = new Thread(() -> {
            do {
                try {
                    Thread.sleep(updateIntervalSecs * 1000);
                    boolean needUpdate = false;
                    synchronized (ProgressOutputUpdater.this) {
                        if (newOutputAvailable
                            || (System.currentTimeMillis() - lastKeepAliveTimestamp
                                   >= TimeUnit.SECONDS.toMillis(keepAliveIntervalSecs))) {
                            newOutputAvailable = false;
                            needUpdate = true;
                            lastKeepAliveTimestamp = System.currentTimeMillis();
                        }
                    }
                    if (needUpdate) {
                            String output = outputHandler.assembleOutput(false);
                            String message = "Action Script execution is in progress; recent output:\n" + output;
                            progressTracker.updateActionProgress(ActionResponseState.IN_PROGRESS, message, 50);
                        }
                } catch (InterruptedException e) {
                    break;
                }
            } while (true);
        });
        thread.start();
    }

    @Override
    public void update(final @Nonnull Observable o, final Object arg) {
        synchronized (this) {
            this.newOutputAvailable = true;
        }
    }

    @Override
    public void close() {
        if (thread != null) {
            thread.interrupt();
        }
    }
}
