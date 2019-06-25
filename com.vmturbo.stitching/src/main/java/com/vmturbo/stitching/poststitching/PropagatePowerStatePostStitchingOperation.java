package com.vmturbo.stitching.poststitching;

import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;

/**
 * This post-stitching operation propagates the power state from VirtualMachine to its consumers,
 * except for GuestLoad application. For example: Application discovered by ACM probe and then
 * stitched to the VM discovered by VC target. If VM's state is POWERED_OFF or SUSPENDED, we should
 * set its consumers' state to UNKNOWN, so they will not participate in market analysis.
 *
 * Note: GuestLoad application is discovered from the same target as the VM, so it holds the same
 * state as VM and should not be touched.
 */
public class PropagatePowerStatePostStitchingOperation implements PostStitchingOperation {

    private static final Logger logger = LogManager.getLogger();

    public static final String APPLICATION_TYPE_PATH = "common_dto.EntityDTO.ApplicationData.type";

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(
            @Nonnull StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.entityTypeScope(EntityType.VIRTUAL_MACHINE);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity> performOperation(
            @Nonnull Stream<TopologyEntity> entities,
            @Nonnull EntitySettingsCollection settingsCollection,
            @Nonnull EntityChangesBuilder<TopologyEntity> resultBuilder) {
        entities.filter(this::shouldPropagate)
            .forEach(entity -> resultBuilder.queueUpdateEntityAlone(entity, this::propagate));
        return resultBuilder.build();
    }

    /**
     * Whether or not the entity's power state should be propagated to its consumers. Currently,
     * we only propagate if its state is POWERED_OFF or SUSPENDED.
     *
     * @param topologyEntity the entity to propagate state from
     * @return true if the state of the entity should be propagated, otherwise false
     */
    private boolean shouldPropagate(@Nonnull TopologyEntity topologyEntity) {
        final EntityState entityState = topologyEntity.getEntityState();
        return entityState == EntityState.POWERED_OFF || entityState == EntityState.SUSPENDED;
    }

    /**
     * Propagate the power state of the given entity to its consumers. Currently, we set VM's
     * consumers' state to UNKNOWN except GuestLoad applications.
     *
     * @param topologyEntity the entity to propagate state from
     */
    private void propagate(@Nonnull TopologyEntity topologyEntity) {
        topologyEntity.getConsumers().stream()
            .map(TopologyEntity::getTopologyEntityDtoBuilder)
            .filter(consumer -> !isGuestLoadApplication(consumer))
            .forEach(consumer -> {
                logger.debug("Changing state of entity {} from {} to UNKNOWN since state of its " +
                    "provider {} is {}", consumer.getOid(), consumer.getEntityState(),
                    topologyEntity, topologyEntity.getEntityState());
                consumer.setEntityState(EntityState.UNKNOWN);
            });
    }

    /**
     * Check if the given entity is a GuestLoad application.
     *
     * @param entityBuilder the entity to check
     * @return true if the entity is a GuestLoad application, otherwise false
     */
    private boolean isGuestLoadApplication(@Nonnull TopologyEntityDTO.Builder entityBuilder) {
        return entityBuilder.getEntityType() == EntityType.APPLICATION_VALUE &&
            SupplyChainConstants.GUEST_LOAD.equals(
                entityBuilder.getEntityPropertyMapMap().get(APPLICATION_TYPE_PATH));
    }
}
