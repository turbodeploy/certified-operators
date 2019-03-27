package com.vmturbo.stitching;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * A wrapper around {@link EntityDTO}. New {@link StitchingEntity} will be created and added to the
 * graph. It also contains info of how to set up relationship with existing entities in the graph.
 */
public class EntityToAdd {

    /**
     * The new entity to be added to the graph.
     */
    private EntityDTO entityDTO;

    /**
     * The consumer which will be connected to the new entity
     */
    private final StitchingEntity consumer;

    /**
     * The provider that the new entity will be connected to
     */
    private final StitchingEntity provider;

    /**
     * The type of the connection
     */
    private final ConnectionType connectionType;

    public EntityToAdd(@Nonnull final EntityDTO entityDTO,
                       @Nonnull final StitchingEntity consumer,
                       @Nonnull final StitchingEntity provider,
                       @Nonnull final ConnectionType connectionType) {
        this.entityDTO = Objects.requireNonNull(entityDTO);
        this.consumer = Objects.requireNonNull(consumer);
        this.provider = Objects.requireNonNull(provider);
        this.connectionType = Objects.requireNonNull(connectionType);
    }

    public EntityDTO getEntityDTO() {
        return entityDTO;
    }

    public StitchingEntity getConsumer() {
        return consumer;
    }

    public StitchingEntity getProvider() {
        return provider;
    }

    public ConnectionType getConnectionType() {
        return connectionType;
    }
}