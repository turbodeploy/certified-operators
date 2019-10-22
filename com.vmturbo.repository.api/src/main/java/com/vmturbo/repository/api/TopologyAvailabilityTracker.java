package com.vmturbo.repository.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

/**
 * Utility class that listens to the repository for notifications, and gives users to
 * convenient way to wait for a particular topology to be available in the
 * repository. Use the {@link TopologyAvailabilityTracker#queueTopologyRequest(long, long)}
 * method, which returns a request object users can wait on.
 */
public class TopologyAvailabilityTracker implements RepositoryListener {

    private final long realtimeTopologyContextId;

    private final Object availabilityLock = new Object();

    private static final CompletableFuture<Void> COMPLETE;

    static {
        COMPLETE = new CompletableFuture<>();
        COMPLETE.complete(null);
    }

    /**
     * A map from topology context ID to the latest topology result we got a notification (available or
     * failure) for in that context.
     *
     * <p>We never remove entries from this map, but it shouldn't grow much since there is only
     * one entry per topology context (i.e. one for realtime, and then one per plan).
     */
    @GuardedBy("availabilityLock")
    private final Map<Long, TopologyAvailabilityStatus> latestTopologyByContext = new HashMap<>();

    /**
     * Queued requests for topology availability information.
     *
     * <p>This list increases in size with calls to
     * {@link TopologyAvailabilityTracker#queueTopologyRequest(long, long)}, and
     * shrinks in size as notifications about topologies come in from the repository.
     */
    @GuardedBy("availabilityLock")
    private final List<QueuedTopologyRequest> queuedRequests = new ArrayList<>();

    /**
     * Construct a new tracker. It's the responsibility of the caller to add it as a listener
     * to the caller's instance of {@link Repository}.
     *
     * @param realtimeTopologyContextId The realtime topology context ID used to distinguish
     *                                  realtime and plan topology notifications.
     */
    public TopologyAvailabilityTracker(final long realtimeTopologyContextId) {
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    /**
     * Wait for a topology to become available in the repository. This is a blocking call.
     * It will return once the tracker received a notification from the repository component
     * saying the topology is available, or after the provided time limit goes by.
     *
     * @param topologyContextId The context ID of the topology to wait for.
     * @param topologyId The topology ID to wait for. Note that if the context ID is the realtime
     *                   context, the method will also return if the tracker receives a "newer"
     *                   topology than the desired one.
     * @return A {@link QueuedTopologyRequest} representing this request. Users can call
     * {@link QueuedTopologyRequest#waitForTopology(long, TimeUnit)} to wait for the topology
     * to become available.
     */
    @Nonnull
    public QueuedTopologyRequest queueTopologyRequest(final long topologyContextId,
                                                      final long topologyId) {
        QueuedTopologyRequest topologyRequest = null;
        synchronized (availabilityLock) {
            final TopologyAvailabilityStatus existingResult = latestTopologyByContext.get(topologyContextId);
            if (existingResult == null || !existingResult.matches(isPlan(topologyContextId), topologyId)) {
                // Don't have it yet. Need to wait.
                topologyRequest = new QueuedTopologyRequest(topologyContextId, topologyId);
                queuedRequests.add(topologyRequest);
            } else {
                topologyRequest = new CompleteTopologyRequest(topologyContextId, existingResult);
            }
        }

        return topologyRequest;
    }

    /**
     * {@inheritDoc}
     *
     * @param topologyId {@inheritDoc}}
     * @param topologyContextId {@inheritDoc}
     */
    @Override
    public void onSourceTopologyAvailable(final long topologyId,
                                          final long topologyContextId) {
        processNotification(topologyContextId, topologyId, false, Optional.empty());
    }

    /**
     * {@inheritDoc}
     *
     * @param topologyId {@inheritDoc}}
     * @param topologyContextId {@inheritDoc}
     * @param failureDescription {@inheritDoc}
     */
    @Override
    public void onSourceTopologyFailure(final long topologyId,
                                        final long topologyContextId,
                                        @Nonnull final String failureDescription) {
        processNotification(topologyContextId, topologyId, false, Optional.of(failureDescription));
    }

    /**
     * {@inheritDoc}
     *
     * @param projectedTopologyId {@inheritDoc}
     * @param topologyContextId {@inheritDoc}
     */
    @Override
    public void onProjectedTopologyAvailable(final long projectedTopologyId,
                                             final long topologyContextId) {
        processNotification(topologyContextId, projectedTopologyId, true, Optional.empty());
    }

    /**
     * {@inheritDoc}
     *
     * @param projectedTopologyId {@inheritDoc}
     * @param topologyContextId {@inheritDoc}
     * @param failureDescription {@inheritDoc}
     */
    @Override
    public void onProjectedTopologyFailure(final long projectedTopologyId,
                                           final long topologyContextId,
                                           @Nonnull final String failureDescription) {
        processNotification(topologyContextId, projectedTopologyId, true, Optional.of(failureDescription));
    }

    private void processNotification(final long topologyContextId,
                                     final long topologyId,
                                     final boolean projected,
                                     final Optional<String> failureDescription) {
        synchronized (availabilityLock) {
            final TopologyAvailabilityStatus availabilityStatus =
                latestTopologyByContext.compute(topologyContextId, (k, existingId) -> {
                    if (existingId != null && existingId.topologyId > topologyId) {
                        return existingId;
                    } else {
                        return new TopologyAvailabilityStatus(topologyId, projected, failureDescription);
                    }
                });

            final Iterator<QueuedTopologyRequest> reqIt = queuedRequests.iterator();
            while (reqIt.hasNext()) {
                final QueuedTopologyRequest nextReq = reqIt.next();
                // Check for matches on the same context.
                if (topologyContextId == nextReq.topologyContextId) {
                    if (availabilityStatus.matches(isPlan(topologyContextId), nextReq.topologyId)) {
                        if (failureDescription.isPresent()) {
                            nextReq.completableFuture.completeExceptionally(
                                TopologyUnavailableException.failed(topologyContextId,
                                    topologyId, failureDescription.get()));
                        } else {
                            nextReq.completableFuture.complete(nextReq);
                        }
                        // Remove the queued waiter, since it found the topology it was looking for!
                        reqIt.remove();
                    }
                }
            }
        }
    }

    private boolean isPlan(final long topologyContextId) {
        return topologyContextId != realtimeTopologyContextId;
    }

    /**
     * Wrapper object to hold information about a topology's status.
     */
    private static class TopologyAvailabilityStatus {

        private final long topologyId;

        private final boolean projectedTopology;

        private final Optional<String> failureDescription;

        private TopologyAvailabilityStatus(final long topologyId,
                                           final boolean projectedTopology,
                                           final Optional<String> failureDescription) {
            this.topologyId = topologyId;
            this.projectedTopology = projectedTopology;
            this.failureDescription = failureDescription;
        }

        private boolean matches(final boolean isPlan, final long inputTopologyId) {
            if (isPlan) {
                return this.topologyId == inputTopologyId || projectedTopology;
            } else {
                return this.topologyId >= inputTopologyId;
            }
        }
    }

    /**
     * Wrapper object to hold information about a waiter for a topology to become available.
     */
    public static class QueuedTopologyRequest {
        protected final long topologyContextId;
        protected final long topologyId;
        private final CompletableFuture<QueuedTopologyRequest> completableFuture = new CompletableFuture<>();

        private QueuedTopologyRequest(final long topologyContextId,
                                      final long topologyId) {
            this.topologyContextId = topologyContextId;
            this.topologyId = topologyId;
        }

        /**
         * Wait for this request to be fulfilled (i.e. for the topology to become available in
         * the repository).
         *
         * @param waitTime The amount of time to wait.
         * @param waitTimeUnit The time unit for the wait time.
         * @throws InterruptedException If the thread is interrupted while waiting.
         * @throws TopologyUnavailableException If the topology is not available. This could be due to
         *        timeout, or due to a reported error in processing the topology in the repository.
         */
        public void waitForTopology(final long waitTime, final TimeUnit waitTimeUnit)
                throws TopologyUnavailableException, InterruptedException {
            try {
                completableFuture.get(waitTime, waitTimeUnit);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof TopologyUnavailableException) {
                    throw (TopologyUnavailableException)(e.getCause());
                } else {
                    throw new IllegalStateException("Unexpected execution exception.", e);
                }
            } catch (TimeoutException e) {
                throw TopologyUnavailableException.timeout(topologyContextId, topologyId,
                    waitTimeUnit.toMinutes(waitTime));
            }
        }
    }

    /**
     * An implementation of {@link QueuedTopologyRequest} that returns immediately, because
     * the topology notification was already received from the repository before the request is
     * constructed.
     */
    private static class CompleteTopologyRequest extends QueuedTopologyRequest {
        private final TopologyAvailabilityStatus availabilityStatus;

        private CompleteTopologyRequest(final long topologyContextId,
                                        final TopologyAvailabilityStatus availabilityStatus) {
            super(topologyContextId, availabilityStatus.topologyId);
            this.availabilityStatus = availabilityStatus;
        }

        /**
         * {@inheritDoc}.
         */
        @Override
        public void waitForTopology(final long waitTime, final TimeUnit waitTimeUnit)
                throws TopologyUnavailableException {
            if (availabilityStatus.failureDescription.isPresent()) {
                // If we already processed the notification, but the notification is a failure,
                // throw an exception.
                throw TopologyUnavailableException.failed(topologyContextId,
                    topologyId, availabilityStatus.failureDescription.get());
            }
        }
    }

    /**
     * Exception thrown when a topology is not available, either due to timeout or because the
     * repository component failed to save it.
     */
    public static class TopologyUnavailableException extends Exception {

        private TopologyUnavailableException(final String msg) {
            super(msg);
        }

        static TopologyUnavailableException failed(final long topologyContextId,
                                                   final long topologyId,
                                                   final String failureDescription) {
            return new TopologyUnavailableException("Repository failed to save topology "
                + topologyId + " in context: " + topologyContextId + ". Error: "
                + failureDescription);
        }

        static TopologyUnavailableException timeout(final long topologyContextId,
                                                    final long topologyId,
                                                    final long minWaited) {
            return new TopologyUnavailableException("Topology "
                + topologyId + " in context: " + topologyContextId +
                " still not available after " + minWaited + " minutes.");
        }
    }
}
