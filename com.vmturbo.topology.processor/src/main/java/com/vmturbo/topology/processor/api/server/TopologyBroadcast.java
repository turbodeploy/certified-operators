package com.vmturbo.topology.processor.api.server;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.chunking.OversizedElementException;

/**
 * Interface for sending topology broadcasts. It's up to implementation to pack separate entities
 * to chunks in order to increase performance and fault-tolerance. From time to time
 * implementation may decide to sent the next chunk or data and block code execution until the
 * chunk is sent. This guarantees the memory efficiency.
 */
public interface TopologyBroadcast {

    /**
     * Appends the next topology entity to the notification. This call may block until the next
     * chunk of entities is sent.
     *
     * @param entity to add to broadcast.
     * @throws InterruptedException if thread has been interrupted
     * @throws NullPointerException if {@code entity} is {@code null}
     * @throws IllegalStateException if {@link #finish()} has been already called
     * @throws CommunicationException persistent communication exception
     * @throws OversizedElementException If the entity is too large to be sent.
     */
    void append(@Nonnull TopologyEntityDTO entity)
        throws CommunicationException, InterruptedException, OversizedElementException;

    /**
     * Appends the next topology extension entity to the notification.
     * This call may block until the next chunk is sent.
     *
     * @param extension to add to broadcast.
     * @throws InterruptedException   if thread has been interrupted
     * @throws NullPointerException   if {@code entity} is {@code null}
     * @throws IllegalStateException  if {@link #finish()} has been already called
     * @throws CommunicationException persistent communication exception
     * @throws OversizedElementException If the extension is too large to be sent.
     */
    void appendExtension(@Nonnull TopologyDTO.TopologyExtension extension)
        throws CommunicationException, InterruptedException, OversizedElementException;

    /**
     * Marks the topology broadcast as complete and sends the last data (if required). This call
     * will block until the rest of broadcast is sent. Subsequent calls to
     * {@link #append(TopologyEntityDTO)} will result in throwing {@link IllegalStateException}.
     *
     * @returns the number of topology entities totally processed (passed to
     *      {@link #append(TopologyEntityDTO)}
     * @throws InterruptedException if thread has been interrupted
     * @throws CommunicationException persistent communication exception
     */
    long finish() throws CommunicationException, InterruptedException;

    /**
     * Returns the id of the topology, that will be broadcasted. The id will be unique across
     * the topology broadcasts.
     *
     * @return id of the topology.
     */
    long getTopologyId();

    /**
     * Returns the id of the topology context for the topology that will be broadcast.
     * Used to differentiate, for example, between real and plan contexts.
     * There is one real-time topology context, but may be several plans.
     *
     * @return id of the topology.
     */
    long getTopologyContextId();

    /**
     * Returns the type of topology being broadcast: realtime or plan.
     *
     *@return the type of topology: realtime or plan
     */
    TopologyType getTopologyType();

    /**
     * Returns the time when topology created.
     *
     * @return timestamp of the topology
     */
    long getCreationTime();
}
