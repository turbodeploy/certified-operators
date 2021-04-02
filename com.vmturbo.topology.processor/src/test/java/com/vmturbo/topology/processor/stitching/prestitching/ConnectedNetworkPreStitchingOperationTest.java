/*
 * (C) Turbonomic ${YEAR}.
 */

package com.vmturbo.topology.processor.stitching.prestitching;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
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
    private static final String NETWORK1_ID = "network1Id";
    private static final String NETWORK2_ID = "network2Id";
    private static final String NETWORK1_DISPLAY_NAME = "network1DisplayName";
    private static final String VM1_ID = "vm1Id";
    private static final String NETWORK2_DISPLAY_NAME = "network2DisplayName";
    private static final String NETWORK3_DISPLAY_NAME = "network3DisplayName";
    private static final int TARGET1_ID = 1;
    private static final int TARGET2_ID = 2;
    private static final int VM1_OID = 1;
    private static final int VM2_OID = 2;
    private static final int VM3_OID = 5;
    private static final int VM4_OID = 6;
    private static final int NETWORK1_OID = 3;
    private static final int NETWORK2_OID = 4;
    private static final int NETWORK3_OID = 7;
    private static final String VM2_ID = "vm2Id";

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
        final EntityDTO.Builder vm1Builder = createBuilder(EntityType.VIRTUAL_MACHINE, VM1_ID);
        final EntityDTO.Builder vm2Builder = createBuilder(EntityType.VIRTUAL_MACHINE, VM2_ID);
        final EntityDTO.Builder vm3Builder = createBuilder(EntityType.VIRTUAL_MACHINE, VM1_ID);
        final EntityDTO.Builder vm4Builder = createBuilder(EntityType.VIRTUAL_MACHINE, VM2_ID);
        final EntityDTO.Builder network1Builder = createBuilder(EntityType.NETWORK, NETWORK1_ID)
                        .setDisplayName(NETWORK1_DISPLAY_NAME);
        final EntityDTO.Builder network2Builder = createBuilder(EntityType.NETWORK, NETWORK2_ID)
                        .setDisplayName(NETWORK2_DISPLAY_NAME);
        final EntityDTO.Builder network3Builder = createBuilder(EntityType.NETWORK, NETWORK1_ID)
                        .setDisplayName(NETWORK3_DISPLAY_NAME);
        vm1Builder.addLayeredOver(network1Builder.getId());
        vm1Builder.addLayeredOver(network2Builder.getId());
        vm1Builder.addLayeredOver(STORAGE_ID);
        vm2Builder.addLayeredOver(network1Builder.getId());
        vm3Builder.addLayeredOver(network3Builder.getId());
        vm4Builder.addLayeredOver(network3Builder.getId());
        final TopologyStitchingEntity vm1Se =
                        createStitchingEntity(vm1Builder, VM1_OID, TARGET1_ID);
        final TopologyStitchingEntity vm2Se =
                        createStitchingEntity(vm2Builder, VM2_OID, TARGET1_ID);
        final TopologyStitchingEntity vm3Se =
                        createStitchingEntity(vm3Builder, VM3_OID, TARGET2_ID);
        final TopologyStitchingEntity vm4Se =
                        createStitchingEntity(vm4Builder, VM4_OID, TARGET2_ID);
        final TopologyStitchingEntity network1Se =
                        createStitchingEntity(network1Builder, NETWORK1_OID, TARGET1_ID);
        final TopologyStitchingEntity network2Se =
                        createStitchingEntity(network2Builder, NETWORK2_OID, TARGET1_ID);
        final TopologyStitchingEntity network3Se =
                        createStitchingEntity(network3Builder, NETWORK3_OID, TARGET2_ID);
        final Collection<StitchingEntity> stitchingEntities =
                        ImmutableSet.of(vm1Se, vm2Se, vm3Se, vm4Se, network1Se, network2Se,
                                        network3Se);
        final StitchingResultBuilder resultBuilder =
                        new StitchingResultBuilder(stitchingContextBuilder.build());
        network1Se.addConnectedFrom(ConnectionType.AGGREGATED_BY_CONNECTION, vm1Se);
        network1Se.addConnectedFrom(ConnectionType.AGGREGATED_BY_CONNECTION, vm2Se);
        vm1Se.addConnectedTo(ConnectionType.AGGREGATED_BY_CONNECTION, network1Se);
        vm1Se.addConnectedTo(ConnectionType.AGGREGATED_BY_CONNECTION, network2Se);
        // Execute operation
        operation.performOperation(stitchingEntities.stream(), resultBuilder);
        final TopologicalChangelog<StitchingEntity> changelog = resultBuilder.build();
        final List<TopologicalChange<StitchingEntity>> changes = changelog.getChanges();
        final IStitchingJournal<StitchingEntity> stitchingJournal = new StitchingJournal<>();
        changes.forEach(change -> change.applyChange(stitchingJournal));
        // Check results
        Assert.assertThat(vm1Se.getEntityBuilder().getLayeredOverList(),
                        CoreMatchers.is(Collections.singletonList(STORAGE_ID)));
        Assert.assertThat(
                        vm1Se.getEntityBuilder().getVirtualMachineData().getConnectedNetworkList(),
                        Matchers.containsInAnyOrder(NETWORK1_DISPLAY_NAME, NETWORK2_DISPLAY_NAME));
        // ConnectionType.AGGREGATED_BY_CONNECTION relationships left untouched
        Assert.assertThat(vm1Se.getConnectedToByType()
                                        .getOrDefault(ConnectionType.AGGREGATED_BY_CONNECTION,
                                                        Collections.emptySet()),
                        Matchers.containsInAnyOrder(network1Se, network2Se));
        Assert.assertThat(network1Se.getConnectedFromByType()
                                        .getOrDefault(ConnectionType.AGGREGATED_BY_CONNECTION,
                                                        Collections.emptySet()),
                        Matchers.containsInAnyOrder(vm2Se, vm1Se));
        // No intersections between different targets
        Assert.assertThat(
                        vm3Se.getEntityBuilder().getVirtualMachineData().getConnectedNetworkList(),
                        Matchers.containsInAnyOrder(NETWORK3_DISPLAY_NAME));
    }

    @Nonnull
    private static TopologyStitchingEntity createStitchingEntity(EntityDTO.Builder builder, int oid,
                    int targetId) {
        return new TopologyStitchingEntity(
                        StitchingEntityData.newBuilder(builder).oid(oid).targetId(targetId)
                                        .build());
    }

    @Nonnull
    private static EntityDTO.Builder createBuilder(EntityType type, String id) {
        return EntityDTO.newBuilder().setEntityType(type).setId(id);
    }

}
