package com.vmturbo.stitching.poststitching;

import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;

/**
 * This post-stitching operation propagates the power state from VirtualMachine to its consumers,
 * except for Services, BusinessTransactions and BusinessApplications.
 * <p/>
 * For example: Application discovered by ACM probe and then
 * stitched to the VM discovered by VC target. If VM's state is POWERED_OFF or SUSPENDED, we should
 * set its consumers' state to UNKNOWN, so they will not participate in market analysis.
 * <p/>
 * We propagate the UNKNOWN state recursively, so for example if a VM hosting containers is
 * UNKNOWN we will mark the ContainerPod on the VM, the Container on the ContainerPod, and
 * the ApplicationComponent on the Container as UNKNOWN as well.
 */
public class PropagatePowerStatePostStitchingOperation implements PostStitchingOperation {

    private static final Logger logger = LogManager.getLogger();

    /**
     * A set of entity types which should be excluded from power state propagation.
     * The entities of these types can be consumers of VMs but they have nothing with a power state.
     */
    private static final Set<Integer> IMMUTABLE_STATE_TYPES = Sets.newHashSet(
            EntityType.BUSINESS_APPLICATION_VALUE, EntityType.BUSINESS_TRANSACTION_VALUE,
            EntityType.SERVICE_VALUE);

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
     * we only propagate if its state is POWERED_OFF, SUSPENDED, or UNKNOWN.
     *
     * @param topologyEntity the entity to propagate state from
     * @return true if the state of the entity should be propagated, otherwise false
     */
    private boolean shouldPropagate(@Nonnull TopologyEntity topologyEntity) {
        final EntityState entityState = topologyEntity.getEntityState();
        return entityState == EntityState.POWERED_OFF
            || entityState == EntityState.SUSPENDED
            || entityState == EntityState.UNKNOWN;
    }

    /**
     * Propagate the power state of the given entity to its consumers. Currently, we set VM's
     * consumers' state to UNKNOWN except Services, BusinessTransactions and BusinessApplications.
     *
     * @param topologyEntity the entity to propagate state from
     */
    private void propagate(@Nonnull TopologyEntity topologyEntity) {
        topologyEntity.getConsumers().stream()
            .filter(consumer -> !IMMUTABLE_STATE_TYPES.contains(consumer.getEntityType()))
            .forEach(consumer -> {
                logger.debug("Changing state of entity {} from {} to UNKNOWN since state of its " +
                    "provider {} is {}", consumer.getOid(), consumer.getEntityState(),
                    topologyEntity, topologyEntity.getEntityState());
                consumer.getTopologyEntityDtoBuilder().setEntityState(EntityState.UNKNOWN);
                propagate(consumer);
            });
    }
}
