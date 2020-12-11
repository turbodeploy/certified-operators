package com.vmturbo.topology.processor.communication.queues;

import java.util.Optional;

import javax.annotation.Nonnull;

/**
 * An interface representing a queue of IDiscoveryQueueElements for a specific probe type and
 * discovery type. Classes implementing this need not be thread safe.
 */
public interface IDiscoveryQueue {

    /**
     * Add an {@link IDiscoveryQueueElement} to the queue unless there's already an element for the
     * same target on the queue. In that case don't add the new element and return the existing
     * element. If the new element is added, return it.
     *
     * @param element the IDiscoveryQueueElement representing a discovery to run.
     * @return the added element if it was added to the queue.  If it was not added return the
     * existing element in the queue that has the same target.
     * @throws DiscoveryQueueException if the element DiscoveryType or Probe ID is not correct for
     * this DiscoveryQueue.
     */
    IDiscoveryQueueElement add(@Nonnull IDiscoveryQueueElement element)
            throws DiscoveryQueueException;

    /**
     * Remove the IOperationQueueElement at the head of the queue and return it wrapped by an Optional.
     * If the queue is empty, return Optional.empty().
     *
     * @return an {@link Optional} wrapping the head of the queue or Optional.empty() if the queue
     * is empty.
     */
    Optional<IDiscoveryQueueElement> remove();

    /**
     * Return the IOperationQueueElement at the head of the queue wrapped by an Optional without
     * altering the queue. If the queue is empty, return Optional.empty().
     *
     * @return an {@link Optional} wrapping the head of the queue or Optional.empty() if the queue
     * is empty.
     */
    Optional<IDiscoveryQueueElement> peek();

    /**
     * Return whether the queue is empty or not.
     *
     * @return True if the queue is empty, false if not.
     */
    boolean isEmpty();

    /**
     * Handle a target being removed from the topology by removing any queue element for that
     * target.
     *
     * @param targetId the id of the target.
     * @return True if there is an element in the queue for the target, false if not.
     */
    boolean handleTargetRemoval(long targetId);

    /**
     * Get the size of the queue.
     *
     * @return the number of elements in the queue.
     */
    int size();

    /**
     * Adds to the queue are usually made in order of queueing time.  There is an exception when
     * a transport is closed and its dedicated queue entries must be re-assigned.  In that case,
     * we call sort afterwards to get the queue elements in the right order.
     */
    void sort();
}
