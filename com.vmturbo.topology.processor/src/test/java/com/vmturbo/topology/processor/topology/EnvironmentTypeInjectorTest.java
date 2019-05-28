package com.vmturbo.topology.processor.topology;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.ListUtils;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.PlanScenarioOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ReservationOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.EnvironmentTypeInjector.InjectionSummary;

/**
 * Unit tests for {@link EnvironmentTypeInjector}.
 */
public class EnvironmentTypeInjectorTest {

    private static final long AWS_TARGET_ID = 1L;
    private static final long VC_TARGET_ID = 2L;

    private static final long ENTITY_OID = 7L;

    private TargetStore targetStore = mock(TargetStore.class);

    private EnvironmentTypeInjector environmentTypeInjector =
            new EnvironmentTypeInjector(targetStore);

    @Before
    public void setup() {
        addFakeTarget(AWS_TARGET_ID, SDKProbeType.AWS);
        addFakeTarget(VC_TARGET_ID, SDKProbeType.VCENTER);
    }

    @Test
    public void testDiscoveredCloudEntity() {
        final TopologyGraph<TopologyEntity> graph = oneEntityGraph(builder -> builder.setOrigin(Origin.newBuilder()
                .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                        .addDiscoveringTargetIds(AWS_TARGET_ID))));

        final InjectionSummary injectionSummary = environmentTypeInjector.injectEnvironmentType(graph);

        assertTrue(graph.getEntity(ENTITY_OID).isPresent());
        assertThat(graph.getEntity(ENTITY_OID).get().getEnvironmentType(), is(EnvironmentType.CLOUD));

        assertThat(injectionSummary.getConflictingTypeCount(), is(0));
        assertThat(injectionSummary.getUnknownCount(), is(0));
        assertThat(injectionSummary.getEnvTypeCounts(), is(ImmutableMap.of(EnvironmentType.CLOUD, 1)));
    }

    @Test
    public void testDiscoveredOnPremEntity() {
        final TopologyGraph<TopologyEntity> graph = oneEntityGraph(builder -> builder.setOrigin(Origin.newBuilder()
                .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                        .addDiscoveringTargetIds(VC_TARGET_ID))));

        final InjectionSummary injectionSummary = environmentTypeInjector.injectEnvironmentType(graph);

        assertTrue(graph.getEntity(ENTITY_OID).isPresent());
        assertThat(graph.getEntity(ENTITY_OID).get().getEnvironmentType(), is(EnvironmentType.ON_PREM));

        assertThat(injectionSummary.getConflictingTypeCount(), is(0));
        assertThat(injectionSummary.getUnknownCount(), is(0));
        assertThat(injectionSummary.getEnvTypeCounts(), is(ImmutableMap.of(EnvironmentType.ON_PREM, 1)));
    }

    @Test
    public void testPlanEntity() {
        final TopologyGraph<TopologyEntity> graph = oneEntityGraph(builder -> builder.setOrigin(Origin.newBuilder()
            .setPlanScenarioOrigin(PlanScenarioOrigin.newBuilder()
                .setPlanId(1111))));

        final InjectionSummary injectionSummary = environmentTypeInjector.injectEnvironmentType(graph);

        assertTrue(graph.getEntity(ENTITY_OID).isPresent());
        assertThat(graph.getEntity(ENTITY_OID).get().getEnvironmentType(), is(EnvironmentType.ON_PREM));

        assertThat(injectionSummary.getConflictingTypeCount(), is(0));
        assertThat(injectionSummary.getUnknownCount(), is(0));
        assertThat(injectionSummary.getEnvTypeCounts(), is(ImmutableMap.of(EnvironmentType.ON_PREM, 1)));
    }

    @Test
    public void testReservationEntity() {
        final TopologyGraph<TopologyEntity> graph = oneEntityGraph(builder -> builder.setOrigin(Origin.newBuilder()
            .setReservationOrigin(ReservationOrigin.newBuilder()
                .setReservationId(112))));

        final InjectionSummary injectionSummary = environmentTypeInjector.injectEnvironmentType(graph);

        assertTrue(graph.getEntity(ENTITY_OID).isPresent());
        assertThat(graph.getEntity(ENTITY_OID).get().getEnvironmentType(), is(EnvironmentType.ON_PREM));

        assertThat(injectionSummary.getConflictingTypeCount(), is(0));
        assertThat(injectionSummary.getUnknownCount(), is(0));
        assertThat(injectionSummary.getEnvTypeCounts(), is(ImmutableMap.of(EnvironmentType.ON_PREM, 1)));
    }

    @Test
    public void testUnsetOriginEntity() {
        final TopologyGraph<TopologyEntity> graph = oneEntityGraph(builder -> {});

        final InjectionSummary injectionSummary = environmentTypeInjector.injectEnvironmentType(graph);

        assertTrue(graph.getEntity(ENTITY_OID).isPresent());
        assertThat(graph.getEntity(ENTITY_OID).get().getEnvironmentType(), is(EnvironmentType.UNKNOWN_ENV));

        assertThat(injectionSummary.getConflictingTypeCount(), is(0));
        // One unknown count.
        assertThat(injectionSummary.getUnknownCount(), is(1));
        assertThat(injectionSummary.getEnvTypeCounts(), is(Collections.emptyMap()));
    }

    @Test
    public void testOverrideSetUnknownEnvType() {
        final TopologyGraph<TopologyEntity> graph = oneEntityGraph(builder -> {
            builder.setEnvironmentType(EnvironmentType.UNKNOWN_ENV);
            builder.setOrigin(Origin.newBuilder()
                .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                        .addDiscoveringTargetIds(AWS_TARGET_ID)));
        });

        final InjectionSummary injectionSummary = environmentTypeInjector.injectEnvironmentType(graph);

        assertTrue(graph.getEntity(ENTITY_OID).isPresent());
        assertThat(graph.getEntity(ENTITY_OID).get().getEnvironmentType(), is(EnvironmentType.CLOUD));

        assertThat(injectionSummary.getConflictingTypeCount(), is(0));
        // One unknown count.
        assertThat(injectionSummary.getUnknownCount(), is(0));
        assertThat(injectionSummary.getEnvTypeCounts(), is(ImmutableMap.of(EnvironmentType.CLOUD, 1)));
    }

    @Test
    public void testNoOverrideSetEnvType() {
        final long targetId = 1;
        final TopologyGraph<TopologyEntity> graph = oneEntityGraph(builder -> {
            builder.setEnvironmentType(EnvironmentType.ON_PREM);
            builder.setOrigin(Origin.newBuilder()
                    .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                            .addDiscoveringTargetIds(targetId)));
        });

        final InjectionSummary injectionSummary = environmentTypeInjector.injectEnvironmentType(graph);

        assertTrue(graph.getEntity(ENTITY_OID).isPresent());
        // The original environment type.
        assertThat(graph.getEntity(ENTITY_OID).get().getEnvironmentType(), is(EnvironmentType.ON_PREM));

        assertThat(injectionSummary.getConflictingTypeCount(), is(1));
        // One unknown count.
        assertThat(injectionSummary.getUnknownCount(), is(0));
        assertThat(injectionSummary.getEnvTypeCounts(), is(Collections.emptyMap()));
    }

    @Nonnull
    private TopologyGraph<TopologyEntity> oneEntityGraph(final Consumer<TopologyEntityDTO.Builder> entityCustomizer) {
        final TopologyEntityDTO.Builder entityBuilder = TopologyEntityDTO.newBuilder()
                .setOid(ENTITY_OID)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE);
        entityCustomizer.accept(entityBuilder);
        final TopologyEntity.Builder entity = TopologyEntity.newBuilder(entityBuilder);
        return TopologyEntityTopologyGraphCreator.newGraph(ImmutableMap.of(ENTITY_OID, entity));
    }

    private void addFakeTarget(final long targetId, final SDKProbeType probeType) {
        List<Target> curFakeTargets = ListUtils.emptyIfNull(targetStore.getAll());
        final Target newFakeTarget = mock(Target.class);
        when(newFakeTarget.getId()).thenReturn(targetId);
        when(targetStore.getProbeTypeForTarget(targetId)).thenReturn(Optional.of(probeType));
        when(targetStore.getAll()).thenReturn(
                ListUtils.union(Collections.singletonList(newFakeTarget), curFakeTargets));
    }
}
