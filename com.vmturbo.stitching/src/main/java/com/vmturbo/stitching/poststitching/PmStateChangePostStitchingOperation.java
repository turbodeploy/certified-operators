package com.vmturbo.stitching.poststitching;

import java.util.Collection;
import java.util.HashSet;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

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
 * A post-stitching operation that changes the state for failover hosts to active if at least one
 * other host in the cluster is not active.
 */
public class PmStateChangePostStitchingOperation implements PostStitchingOperation {

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(
            @Nonnull StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.entityTypeScope(EntityType.VIRTUAL_DATACENTER);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity> performOperation(
            @Nonnull Stream<TopologyEntity> entities,
            @Nonnull EntitySettingsCollection settingsCollection,
            @Nonnull EntityChangesBuilder<TopologyEntity> resultBuilder) {
        entities.forEach(e -> {
            boolean changeState = false;
            final Collection<TopologyEntity> failoverHosts = new HashSet<>();
            for (TopologyEntity p : e.getProviders()) {
                if (p.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE) {
                    // TODO this may not be enough
                    // strictly speaking 'host HA state' is a separate from power/connectivity state
                    // and may have to be separately discovered and passed
                    if (p.getEntityState() == EntityState.UNKNOWN) {
                        changeState = true;
                    } else if (p.getEntityState() == EntityState.FAILOVER) {
                        failoverHosts.add(p);
                    }
                }
            }
            if (changeState && !failoverHosts.isEmpty()) {
                // some host went down, flip the failover ones into activity
                for (TopologyEntity failoverHost : failoverHosts) {
                    resultBuilder.queueUpdateEntityAlone(failoverHost,
                            f -> f.getTopologyEntityImpl()
                                    .setEntityState(EntityState.POWERED_ON));
                }
            }
        });
        return resultBuilder.build();
    }
}
