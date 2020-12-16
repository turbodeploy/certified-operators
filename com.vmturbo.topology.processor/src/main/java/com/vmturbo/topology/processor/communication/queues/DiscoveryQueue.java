package com.vmturbo.topology.processor.communication.queues;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;

/**
 * Queue designed to handle single probe type and DiscoveryType. It ensures that each target only
 * has one related element in the queue. This class is not thread safe.
 */
@NotThreadSafe
public class DiscoveryQueue implements IDiscoveryQueue {

    private final Logger logger = LogManager.getLogger();

    private final long probeId;

    private final DiscoveryType discoveryType;

    private final Deque<IDiscoveryQueueElement> innerQ = Queues.newArrayDeque();

    private final Map<Long, IDiscoveryQueueElement> targetIdMap = Maps.newHashMap();

    /**
     * Create a {@link DiscoveryQueue} to handle a certain probe ID's discoveries of a certain type.
     *
     * @param probeId the ID of the probe type that this queue supports.
     * @param discoveryType {@link DiscoveryType} supported by this queue.
     */
    public DiscoveryQueue(long probeId, DiscoveryType discoveryType) {
        this.probeId = probeId;
        this.discoveryType = discoveryType;
    }

    @Override
    public IDiscoveryQueueElement add(@Nonnull IDiscoveryQueueElement element)
            throws DiscoveryQueueException {
        if (element.getDiscoveryType() != discoveryType
                || element.getTarget().getProbeId() != probeId) {
            throw new DiscoveryQueueException("Expected probe ID " + probeId
                    + " and discovery type " + discoveryType + ", but "
                    + "actual probe ID was " + element.getTarget().getProbeId()
                    + " and discovery type was " + element.getDiscoveryType());
        }
        final long targetId = element.getTarget().getId();
        if (targetIdMap.containsKey(targetId)) {
            IDiscoveryQueueElement existingElement = targetIdMap.get(targetId);
            if (element.runImmediately()) {
                logger.debug("Adding an element to run immediately {}. "
                        + "Found existing element {}.", element, existingElement);
                innerQ.remove(existingElement);
                existingElement.setRunImmediately(true);
                // move to front and then sort in case there are other runImmediately elements in
                // the queue
                innerQ.addFirst(existingElement);
                sort();
               return existingElement;
            } else {
                return existingElement;
            }
        } else {
            targetIdMap.put(targetId, element);
            if (element.runImmediately()) {
                innerQ.addFirst(element);
                sort();
            } else {
                innerQ.add(element);
            }
            return element;
        }
    }

    @Override
    public Optional<IDiscoveryQueueElement> remove() {
        if (innerQ.isEmpty()) {
            return Optional.empty();
        } else {
            IDiscoveryQueueElement retVal = innerQ.remove();
            targetIdMap.remove(retVal.getTarget().getId());
            return Optional.of(retVal);
        }
    }

    @Override
    public Optional<IDiscoveryQueueElement> peek() {
        return Optional.ofNullable(innerQ.peek());
    }

    @Override
    public boolean isEmpty() {
        return innerQ.isEmpty();
    }

    @Override
    public boolean handleTargetRemoval(long targetId) {
        IDiscoveryQueueElement toRemove = targetIdMap.remove(targetId);
        if (toRemove != null) {
            innerQ.remove(toRemove);
            return true;
        }
        return false;
    }

    @Override
    public int size() {
        return innerQ.size();
    }

    @Override
    public void sort() {
        IDiscoveryQueueElement[] queueContents =
                innerQ.toArray(new IDiscoveryQueueElement[innerQ.size()]);
        Arrays.sort(queueContents, Comparator.naturalOrder());
        innerQ.clear();
        innerQ.addAll(Arrays.asList(queueContents));
    }
}
