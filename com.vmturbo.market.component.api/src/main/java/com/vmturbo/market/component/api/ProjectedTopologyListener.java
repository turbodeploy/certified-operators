package com.vmturbo.market.component.api;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.opentracing.SpanContext;

import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.RemoteIterator;

/**
 * An object that listens to the market's notifications about
 * traders after the optimal state has been attained
 */
public interface ProjectedTopologyListener {

    /**
     * Callback receiving the trader after the market has completed running.
     *
     * @param  metadata Additional data about the projected topology.
     * @param topology contains the traders after the plan has completed.
     * @param tracingContext Distributed tracing context.
     */
    void onProjectedTopologyReceived(@Nonnull final ProjectedTopology.Metadata metadata,
                                 @Nonnull final RemoteIterator<ProjectedTopologyEntity> topology,
                                 @Nullable SpanContext tracingContext);
}
