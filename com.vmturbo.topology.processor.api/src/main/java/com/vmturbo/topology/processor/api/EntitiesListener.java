package com.vmturbo.topology.processor.api;

import javax.annotation.Nonnull;

import io.opentracing.SpanContext;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.RemoteIterator;

/**
 * Listener to receive topology broadcasts.
 */
public interface EntitiesListener {

    /**
     * A notification sent by the Topology Processor to report the current topology.
     * This notification may be triggered periodically according to a schedule
     * or sent when a topology processor client requests it.
     *
     * <p>A topology consists of the fully stitched set of entities in all
     * discovered environments. Internally, topology comes in chunks, so use
     * {@link RemoteIterator#nextChunk()} to retrieve the next chunk of data.
     *
     * <p>Consumer (implementation of this interface) is responseble for consuming ALL the
     * elements from this remote iterator. Otherwise, sender side will still think, that message
     * is not yet processed and may cause inter-component blocks.
     *
     * @param topologyInfo contains basic fields of topology message:
     *                     topologyContextId: An ID used by the market runner to identify plans
     *                     for the purpose of accepting/rejecting/preempting market run requests. The
     *                     market runner
     *                     will only execute one plan with a given topologyContextId at a time.
     *                     topologyId: The ID of the topology that the set of entities represents.
     *                     As consumers of the topology operate upon it, they should include this ID
     *                     in their log messages so that transactions can be audited in the logs
     *                     across
     *                     different components in the system. The topology processor will
     *                     generate a new
     *                     ID each time it broadcasts the topology.
     *                     creationTime: The time of topology message created.
     * @param topologyDTOs Remote iterator to retrieve the complete set of stitched entities that
     *                     compose the the full topology snapshot. This object is valid only
     *                     during the method
     *                     call. it will throw {@link IllegalStateException} if called after the
     *                     method exists.
     * @param tracingContext Distributed tracing context.
     */
    void onTopologyNotification(@Nonnull TopologyInfo topologyInfo,
                                @Nonnull RemoteIterator<TopologyDTO.Topology.DataSegment> topologyDTOs,
                                @Nonnull SpanContext tracingContext);
}
