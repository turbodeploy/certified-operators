/*
 * (C) Turbonomic ${YEAR}.
 */

package com.vmturbo.topology.processor.stitching.prestitching;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableSet;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.PreStitchingOperation;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.TopologicalChange;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.prestitching.ConnectedNetworkPreStitchingOperation;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingEntityData;
import com.vmturbo.topology.processor.stitching.StitchingResultBuilder;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournal;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * {@link ConnectedNetworkPreStitchingOperationTest} checking that {@link
 * ConnectedNetworkPreStitchingOperation} is working as expected.
 */
public class ConnectedNetworkPreStitchingOperationTest {
    private static final String STORAGE_ID = "storageId";

    /**
     * Checks that connected relationships created from layered over relationships between VM and
     * network are clearing correctly. Layered over collection cleared correctly too.
     */
    @Test
    public void checkVmToNetworkRelationships() {
        // Prepare context
        final PreStitchingOperation operation = new ConnectedNetworkPreStitchingOperation();
        final TargetStore targetStore = Mockito.mock(TargetStore.class);
        Mockito.when(targetStore.getAll()).thenReturn(Collections.emptyList());
        final StitchingContext.Builder stitchingContextBuilder =
                        StitchingContext.newBuilder(0, targetStore);
        final IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);
        stitchingContextBuilder.setIdentityProvider(identityProvider);
        // Prepare entities
        final EntityDTO.Builder vmBuilder =
                        EntityDTO.newBuilder().setEntityType(EntityType.VIRTUAL_MACHINE)
                                        .setId("vmId");
        final EntityDTO.Builder networkBuilder =
                        EntityDTO.newBuilder().setEntityType(EntityType.NETWORK).setId("networkId");
        vmBuilder.addLayeredOver(networkBuilder.getId());
        vmBuilder.addLayeredOver(STORAGE_ID);
        final TopologyStitchingEntity vmStitchingEntity = new TopologyStitchingEntity(
                        StitchingEntityData.newBuilder(vmBuilder).build());
        final TopologyStitchingEntity networkStitchingEntity = new TopologyStitchingEntity(
                        StitchingEntityData.newBuilder(networkBuilder).build());
        networkStitchingEntity.addConnectedFrom(ConnectionType.AGGREGATED_BY_CONNECTION,
                        vmStitchingEntity);
        vmStitchingEntity.addConnectedTo(ConnectionType.AGGREGATED_BY_CONNECTION,
                        networkStitchingEntity);
        final Collection<StitchingEntity> stitchingEntities =
                        ImmutableSet.of(vmStitchingEntity, networkStitchingEntity);
        final StitchingResultBuilder resultBuilder =
                        new StitchingResultBuilder(stitchingContextBuilder.build());
        // Execute operation
        operation.performOperation(stitchingEntities.stream(), resultBuilder);
        final TopologicalChangelog<StitchingEntity> changelog = resultBuilder.build();
        final List<TopologicalChange<StitchingEntity>> changes = changelog.getChanges();
        final IStitchingJournal<StitchingEntity> stitchingJournal = new StitchingJournal<>();
        changes.forEach(change -> change.applyChange(stitchingJournal));
        // Check results
        Assert.assertThat(vmStitchingEntity.getEntityBuilder().getLayeredOverList(),
                        CoreMatchers.is(Collections.singletonList(STORAGE_ID)));
        Assert.assertThat(vmStitchingEntity.getConnectedToByType()
                                        .getOrDefault(ConnectionType.AGGREGATED_BY_CONNECTION,
                                                        Collections.emptySet()),
                        CoreMatchers.is(Collections.emptySet()));
        Assert.assertThat(networkStitchingEntity.getConnectedFromByType()
                                        .getOrDefault(ConnectionType.AGGREGATED_BY_CONNECTION,
                                                        Collections.emptySet()),
                        CoreMatchers.is(Collections.emptySet()));
    }

}
