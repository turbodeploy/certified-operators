package com.vmturbo.topology.processor.topology;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.topology.EphemeralEntityEditor.EditSummary;

/**
 * Tests {@link EphemeralEntityEditor}.
 */
public class EphemeralEntityEditorTest {
    @SuppressWarnings("unchecked")
    private final TopologyGraph<TopologyEntity> graph = mock(TopologyGraph.class);

    private final TopologyEntity containerSpec = mock(TopologyEntity.class);

    private final TopologyEntity container = mock(TopologyEntity.class);

    private final TopologyEntity container2 = mock(TopologyEntity.class);

    private final EphemeralEntityEditor editor = new EphemeralEntityEditor();

    private final TopologyEntityDTO.Builder ephemeralBuilder = TopologyEntityDTO.newBuilder()
        .setEntityType(EntityType.CONTAINER.getNumber());

    private final TopologyEntityDTO.Builder ephemeralBuilder2 = TopologyEntityDTO.newBuilder()
        .setEntityType(EntityType.CONTAINER.getNumber());

    private final HistoricalValues peakValues = HistoricalValues.newBuilder()
        .setHistUtilization(1.0)
        .setMaxQuantity(2.0)
        .setPercentile(3.0)
        .build();

    private final HistoricalValues usedValues = HistoricalValues.newBuilder()
        .setHistUtilization(1.5)
        .setMaxQuantity(2.5)
        .setPercentile(3.5)
        .build();

    private static final int COMM_1_TYPE = CommodityDTO.CommodityType.VCPU_VALUE;
    private static final int COMM_2_TYPE = CommodityDTO.CommodityType.VCPU_REQUEST_VALUE;

    private final CommoditySoldDTO sold1 = CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder().setType(COMM_1_TYPE))
            .setIsResizeable(true)
            .setHistoricalPeak(peakValues)
            .build();

    private final CommoditySoldDTO sold2NoKey = CommoditySoldDTO.newBuilder()
        .setCommodityType(CommodityType.newBuilder().setType(COMM_2_TYPE))
        .setHistoricalUsed(usedValues)
        .setHistoricalPeak(peakValues)
        .build();

    private final CommoditySoldDTO sold2WithKey = CommoditySoldDTO.newBuilder()
        .setCommodityType(CommodityType.newBuilder().setType(COMM_2_TYPE).setKey("foo"))
        .setIsResizeable(false)
        .setHistoricalUsed(usedValues)
        .build();

    private final Map<Integer, List<CommoditySoldDTO>> persistentCommsSold = ImmutableMap.of(
        COMM_1_TYPE, Collections.singletonList(sold1),
        COMM_2_TYPE, Arrays.asList(sold2NoKey, sold2WithKey));

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        when(containerSpec.getEntityType()).thenReturn(EntityType.CONTAINER_SPEC.getNumber());
        when(containerSpec.soldCommoditiesByType()).thenReturn(persistentCommsSold);

        when(container.getEntityType()).thenReturn(EntityType.CONTAINER.getNumber());
        when(container.getTopologyEntityDtoBuilder()).thenReturn(ephemeralBuilder);

        when(container2.getEntityType()).thenReturn(EntityType.CONTAINER.getNumber());
        when(container2.getTopologyEntityDtoBuilder()).thenReturn(ephemeralBuilder2);
    }

    /**
     * Test running {@link EphemeralEntityEditor} on an empty graph.
     */
    @Test
    public void testEmptyGraph() {
        when(graph.entitiesOfType(EntityType.CONTAINER_SPEC.getNumber())).thenReturn(Stream.empty());
        editor.applyEdits(graph, true);
    }

    /**
     * Test running {@link EphemeralEntityEditor} on a graph with a container spec
     * having no relationship to any containers.
     */
    @Test
    public void testEntityWithNoRelationships() {
        when(graph.entitiesOfType(EntityType.CONTAINER_SPEC.getNumber())).thenReturn(Stream.of(containerSpec));
        when(containerSpec.getAggregatedEntities()).thenReturn(Collections.emptyList());

        editor.applyEdits(graph, true);
    }

    /**
     * Test running {@link EphemeralEntityEditor} on a graph with a container spec with
     * relationships but no commodities to transfer.
     */
    @Test
    public void testEntityWithNoCommoditiesOnPersistent() {
        when(graph.entitiesOfType(EntityType.CONTAINER_SPEC.getNumber())).thenReturn(Stream.of(containerSpec));
        when(containerSpec.getAggregatedEntities()).thenReturn(Collections.singletonList(container));
        when(containerSpec.soldCommoditiesByType()).thenReturn(Collections.emptyMap());

        editor.applyEdits(graph, true);
    }

    /**
     * Test running {@link EphemeralEntityEditor} on a graph with a container spec with
     * relationships with commodities but none that match.
     */
    @Test
    public void testEntityWithNoMatchingCommodities() {
        when(graph.entitiesOfType(EntityType.CONTAINER_SPEC.getNumber())).thenReturn(Stream.of(containerSpec));
        when(containerSpec.getAggregatedEntities()).thenReturn(Collections.singletonList(container));
        when(containerSpec.soldCommoditiesByType()).thenReturn(persistentCommsSold);

        final CommoditySoldDTO.Builder ephemeralCommSold = CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder().setType(3));
        final CommoditySoldDTO beforeEdits = ephemeralCommSold.build();
        ephemeralBuilder.addCommoditySoldList(ephemeralCommSold);

        editor.applyEdits(graph, true);
        assertThat(ephemeralBuilder.getCommoditySoldList(0), matchesHistory(beforeEdits));
    }

    /**
     * Test running {@link EphemeralEntityEditor} on a graph with a container spec with
     * relationships commodities to transfer that don't have any keys.
     */
    @Test
    public void testEntityWithNoCommodityKey() {
        when(graph.entitiesOfType(EntityType.CONTAINER_SPEC.getNumber())).thenReturn(Stream.of(containerSpec));
        when(containerSpec.getAggregatedEntities()).thenReturn(Collections.singletonList(container));
        when(containerSpec.soldCommoditiesByType()).thenReturn(persistentCommsSold);

        final CommoditySoldDTO.Builder ephemeralCommSold = CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder().setType(COMM_1_TYPE));
        ephemeralBuilder.addCommoditySoldList(ephemeralCommSold);

        editor.applyEdits(graph, true);
        assertThat(ephemeralBuilder.getCommoditySoldList(0), matchesHistory(sold1));
    }

    /**
     * Test running {@link EphemeralEntityEditor} on a graph with a container spec with
     * relationships but non-matching commodity keys.
     */
    @Test
    public void testEntityWithCommodityKeyNotMatching() {
        when(graph.entitiesOfType(EntityType.CONTAINER_SPEC.getNumber())).thenReturn(Stream.of(containerSpec));
        when(containerSpec.getAggregatedEntities()).thenReturn(Collections.singletonList(container));
        when(containerSpec.soldCommoditiesByType()).thenReturn(persistentCommsSold);

        final CommoditySoldDTO.Builder ephemeralCommSold = CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder().setType(COMM_2_TYPE).setKey("bar"));
        final CommoditySoldDTO beforeEdits = ephemeralCommSold.build();
        ephemeralBuilder.addCommoditySoldList(ephemeralCommSold);

        editor.applyEdits(graph, true);
        assertThat(ephemeralBuilder.getCommoditySoldList(0), matchesHistory(beforeEdits));
    }

    /**
     * Test running {@link EphemeralEntityEditor} on a graph with a container spec with
     * relationships with matching commodity keys that should be transferred.
     */
    @Test
    public void testEntityWithCommodityKeyMatching() {
        when(graph.entitiesOfType(EntityType.CONTAINER_SPEC.getNumber())).thenReturn(Stream.of(containerSpec));
        when(containerSpec.getAggregatedEntities()).thenReturn(Collections.singletonList(container));
        when(containerSpec.soldCommoditiesByType()).thenReturn(persistentCommsSold);

        final CommoditySoldDTO.Builder ephemeralCommSold = CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder().setType(COMM_2_TYPE).setKey("foo"));
        ephemeralBuilder.addCommoditySoldList(ephemeralCommSold);

        editor.applyEdits(graph, true);
        assertThat(ephemeralBuilder.getCommoditySoldList(0), matchesHistory(sold2WithKey));
    }

    /**
     * Test running {@link EphemeralEntityEditor} where the container has resizable=false
     * and containerSpec has resizable=true. In this case we should not override the container
     * setting with the container spec setting.
     */
    @Test
    public void testDoNotOverrideEphemeralNotResizable() {
        when(graph.entitiesOfType(EntityType.CONTAINER_SPEC.getNumber())).thenReturn(Stream.of(containerSpec));
        when(containerSpec.getAggregatedEntities()).thenReturn(Collections.singletonList(container));
        when(containerSpec.soldCommoditiesByType()).thenReturn(persistentCommsSold);

        final CommoditySoldDTO.Builder ephemeralCommSold = CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder().setType(COMM_1_TYPE))
            .setIsResizeable(false);
        ephemeralBuilder.addCommoditySoldList(ephemeralCommSold);

        editor.applyEdits(graph, true);
        assertThat(ephemeralBuilder.getCommoditySoldList(0).getIsResizeable(), is(false));
    }

    /**
     * Test running {@link EphemeralEntityEditor} where the container has resizable=true
     * and containerSpec has resizable=false SHOULD override the container's setting.
     */
    @Test
    public void testOverridesEphemeralResizableTrue() {
        when(graph.entitiesOfType(EntityType.CONTAINER_SPEC.getNumber())).thenReturn(Stream.of(containerSpec));
        when(containerSpec.getAggregatedEntities()).thenReturn(Collections.singletonList(container));
        when(containerSpec.soldCommoditiesByType()).thenReturn(persistentCommsSold);

        final CommoditySoldDTO.Builder ephemeralCommSold = CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder().setType(COMM_2_TYPE).setKey("foo"))
            .setIsResizeable(true);
        ephemeralBuilder.addCommoditySoldList(ephemeralCommSold);

        editor.applyEdits(graph, true);
        assertThat(ephemeralBuilder.getCommoditySoldList(0).getIsResizeable(), is(false));
    }

    /**
     * Test running {@link EphemeralEntityEditor} on a graph with a container spec with
     * relationships with multiple matching commodities that should be transferred.
     */
    @Test
    public void testEntityWithMultipleMatchingCommodities() {
        when(graph.entitiesOfType(EntityType.CONTAINER_SPEC.getNumber())).thenReturn(Stream.of(containerSpec));
        when(containerSpec.getAggregatedEntities()).thenReturn(Collections.singletonList(container));
        when(containerSpec.soldCommoditiesByType()).thenReturn(persistentCommsSold);

        final CommoditySoldDTO.Builder first = CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder().setType(COMM_1_TYPE));
        final CommoditySoldDTO.Builder second = CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder().setType(COMM_2_TYPE));
        ephemeralBuilder.addCommoditySoldList(first);
        ephemeralBuilder.addCommoditySoldList(second);

        editor.applyEdits(graph, true);
        assertThat(ephemeralBuilder.getCommoditySoldList(0), matchesHistory(sold1));
        assertThat(ephemeralBuilder.getCommoditySoldList(1), matchesHistory(sold2NoKey));
    }

    /**
     * Test running {@link EphemeralEntityEditor} on a graph with multiple entities
     * with commodities that should be transferred.
     */
    @Test
    public void testMultipleEntities() {
        when(graph.entitiesOfType(EntityType.CONTAINER_SPEC.getNumber())).thenReturn(Stream.of(containerSpec));
        when(containerSpec.getAggregatedEntities()).thenReturn(Arrays.asList(container, container2));
        when(containerSpec.soldCommoditiesByType()).thenReturn(persistentCommsSold);

        final CommoditySoldDTO.Builder first = CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder().setType(COMM_1_TYPE));
        final CommoditySoldDTO.Builder second = CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder().setType(COMM_2_TYPE));
        ephemeralBuilder.addCommoditySoldList(first);
        ephemeralBuilder.addCommoditySoldList(second);
        ephemeralBuilder2.addCommoditySoldList(CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder().setType(COMM_2_TYPE).setKey("foo")));

        editor.applyEdits(graph, true);
        assertThat(ephemeralBuilder.getCommoditySoldList(0), matchesHistory(sold1));
        assertThat(ephemeralBuilder.getCommoditySoldList(1), matchesHistory(sold2NoKey));
        assertThat(ephemeralBuilder2.getCommoditySoldList(0), matchesHistory(sold2WithKey));
    }

    /**
     * Test that VCPU & VCPU_REQUEST resize is disabled when scaling group members have
     * inconsistent capacities.
     */
    @Test
    public void testDisableInconsistentCapacities() {
        when(graph.entitiesOfType(EntityType.CONTAINER_SPEC.getNumber())).thenReturn(Stream.of(containerSpec));
        when(containerSpec.getAggregatedEntities()).thenReturn(Arrays.asList(container, container2));
        when(containerSpec.soldCommoditiesByType()).thenReturn(persistentCommsSold);

        final CommoditySoldDTO.Builder vcpuSold = CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE))
            .setCapacity(10.0)
            .setIsResizeable(true);
        final CommoditySoldDTO.Builder vcpuRequestSold = CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE))
            .setCapacity(10.0)
            .setIsResizeable(true);
        ephemeralBuilder.addCommoditySoldList(vcpuSold);
        ephemeralBuilder.addCommoditySoldList(vcpuRequestSold);
        ephemeralBuilder2.addCommoditySoldList(vcpuSold.setCapacity(11.0));
        ephemeralBuilder2.addCommoditySoldList(vcpuRequestSold.setCapacity(9.0));

        editor.applyEdits(graph, true);
        assertCommoditiesNotResizable(ephemeralBuilder);
        assertCommoditiesNotResizable(ephemeralBuilder2);
    }

    /**
     * Test that VCPU and VCPU_REQUEST resize is disabled when scaling group members have
     * inconsistent capacities and the inconsistency is due to different scaling factors.
     */
    @Test
    public void testDisableInconsistentCapacitiesBecauseOfScalingFactor() {
        when(graph.entitiesOfType(EntityType.CONTAINER_SPEC.getNumber())).thenReturn(Stream.of(containerSpec));
        when(containerSpec.getAggregatedEntities()).thenReturn(Arrays.asList(container, container2));
        when(containerSpec.soldCommoditiesByType()).thenReturn(persistentCommsSold);

        ephemeralBuilder.addCommoditySoldList(vcpuSold());
        ephemeralBuilder.addCommoditySoldList(vcpuRequestSold());
        ephemeralBuilder2.addCommoditySoldList(vcpuSold().setScalingFactor(11.0));
        ephemeralBuilder2.addCommoditySoldList(vcpuRequestSold().setCapacity(9.0));

        editor.applyEdits(graph, true);
        assertCommoditiesNotResizable(ephemeralBuilder);
        assertCommoditiesNotResizable(ephemeralBuilder2);
    }

    /**
     * Test that containers running on nodes with different speeds are still resizable
     * when they have capacities that are consistent in millicores after applying
     * the consistent scaling factor.
     */
    @Test
    public void testSetConsistentScalingFactor() {
        final TopologyEntity container2 = mock(TopologyEntity.class);
        final TopologyEntityDTO.Builder ephemeralBuilder2 = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.CONTAINER.getNumber());

        when(container2.getEntityType()).thenReturn(EntityType.CONTAINER.getNumber());
        when(container2.getTopologyEntityDtoBuilder()).thenReturn(ephemeralBuilder2);
        setupVmProvider(container, 111L, 1.0, 6, 1.0f);
        setupVmProvider(container2, 222L, 2.0, 3, 0.5f);

        when(graph.entitiesOfType(EntityType.CONTAINER_SPEC.getNumber())).thenReturn(Stream.of(containerSpec));
        when(containerSpec.getAggregatedEntities()).thenReturn(Arrays.asList(container, container2));
        when(containerSpec.soldCommoditiesByType()).thenReturn(persistentCommsSold);

        ephemeralBuilder.addCommoditySoldList(vcpuSold().setScalingFactor(1.0));
        ephemeralBuilder.addCommoditySoldList(vcpuRequestSold().setScalingFactor(1.0));
        ephemeralBuilder2.addCommoditySoldList(vcpuSold().setScalingFactor(2.0));
        ephemeralBuilder2.addCommoditySoldList(vcpuRequestSold().setScalingFactor(2.0));

        assertFalse(ephemeralBuilder.getAnalysisSettings().hasConsistentScalingFactor());
        assertFalse(ephemeralBuilder2.getAnalysisSettings().hasConsistentScalingFactor());

        final EditSummary editSummary = editor.applyEdits(graph, true);
        assertCommoditiesResizable(ephemeralBuilder);
        assertCommoditiesResizable(ephemeralBuilder2);
        assertEquals(2, editSummary.getContainerConsistentScalingFactorSet());
        assertTrue(ephemeralBuilder.getAnalysisSettings().hasConsistentScalingFactor());
        assertTrue(ephemeralBuilder2.getAnalysisSettings().hasConsistentScalingFactor());
        assertEquals(0.5f, ephemeralBuilder2.getAnalysisSettings().getConsistentScalingFactor(), 0);
    }

    /**
     * Check that with the consistent scaling feature flag disabled we always get a value of
     * 1.0 for the CSF.
     */
    @Test
    public void testSetConsistentScalingFeatureFlagDisabled() {
        final TopologyEntity container2 = mock(TopologyEntity.class);
        final TopologyEntityDTO.Builder ephemeralBuilder2 = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.CONTAINER.getNumber());

        when(container2.getEntityType()).thenReturn(EntityType.CONTAINER.getNumber());
        when(container2.getTopologyEntityDtoBuilder()).thenReturn(ephemeralBuilder2);
        setupVmProvider(container, 111L, 1.0, 6, 1.0f);
        setupVmProvider(container2, 222L, 2.0, 3, 0.5f);

        when(graph.entitiesOfType(EntityType.CONTAINER_SPEC.getNumber())).thenReturn(Stream.of(containerSpec));
        when(containerSpec.getAggregatedEntities()).thenReturn(Arrays.asList(container, container2));
        when(containerSpec.soldCommoditiesByType()).thenReturn(persistentCommsSold);

        ephemeralBuilder.addCommoditySoldList(vcpuSold().setScalingFactor(1.0));
        ephemeralBuilder.addCommoditySoldList(vcpuRequestSold().setScalingFactor(1.0));
        ephemeralBuilder2.addCommoditySoldList(vcpuSold().setScalingFactor(2.0));
        ephemeralBuilder2.addCommoditySoldList(vcpuRequestSold().setScalingFactor(2.0));

        editor.applyEdits(graph, false);
        assertCommoditiesNotResizable(ephemeralBuilder);
        assertCommoditiesNotResizable(ephemeralBuilder2);
        assertEquals(1.0f, ephemeralBuilder.getAnalysisSettings().getConsistentScalingFactor(), 0);
        assertEquals(1.0f, ephemeralBuilder2.getAnalysisSettings().getConsistentScalingFactor(), 0);
    }

    /**
     * Test that containers running on nodes with different speeds are not resizable
     * when they have capacities that are inconsistent in millicores after applying
     * the consistent scaling factor.
     */
    @Test
    public void testConsistentScalingFactorStillDifferent() {
        final TopologyEntity container2 = mock(TopologyEntity.class);
        final TopologyEntityDTO.Builder ephemeralBuilder2 = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.CONTAINER.getNumber());

        when(container2.getEntityType()).thenReturn(EntityType.CONTAINER.getNumber());
        when(container2.getTopologyEntityDtoBuilder()).thenReturn(ephemeralBuilder2);
        setupVmProvider(container, 111L, 1.0, 6, 1.0f);
        setupVmProvider(container2, 222L, 3.0, 3, 1.0f);

        when(graph.entitiesOfType(EntityType.CONTAINER_SPEC.getNumber())).thenReturn(Stream.of(containerSpec));
        when(containerSpec.getAggregatedEntities()).thenReturn(Arrays.asList(container, container2));
        when(containerSpec.soldCommoditiesByType()).thenReturn(persistentCommsSold);

        ephemeralBuilder.addCommoditySoldList(vcpuSold().setScalingFactor(1.0));
        ephemeralBuilder.addCommoditySoldList(vcpuRequestSold().setScalingFactor(1.0));
        ephemeralBuilder2.addCommoditySoldList(vcpuSold().setScalingFactor(3.0));
        ephemeralBuilder2.addCommoditySoldList(vcpuRequestSold().setScalingFactor(3.0));

        editor.applyEdits(graph, true);
        assertCommoditiesNotResizable(ephemeralBuilder);
        assertCommoditiesNotResizable(ephemeralBuilder2);
    }

    private void assertCommoditiesNotResizable(TopologyEntityDTO.Builder ephemeralBuilder) {
        ephemeralBuilder.getCommoditySoldListList().forEach(commSold -> {
            assertFalse(commSold.getIsResizeable());
        });
    }

    private void assertCommoditiesResizable(TopologyEntityDTO.Builder ephemeralBuilder) {
        ephemeralBuilder.getCommoditySoldListList().forEach(commSold -> {
            assertTrue(commSold.getIsResizeable());
        });
    }

    private CommoditySoldDTO.Builder vcpuSold() {
        return CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE))
            .setCapacity(10.0)
            .setIsResizeable(true);
    }

    private CommoditySoldDTO.Builder vcpuRequestSold() {
        return CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE))
            .setCapacity(10.0)
            .setIsResizeable(true);
    }

    private void setupVmProvider(@Nonnull final TopologyEntity container,
                                 final long vmOid,
                                 final double scalingFactor,
                                 final int numCpus,
                                 final float consistentScalingFactor) {
        final TopologyEntity pod = mock(TopologyEntity.class);
        when(pod.getEntityType()).thenReturn(EntityType.CONTAINER_POD_VALUE);

        final TopologyEntityDTO.Builder vmBuilder = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setAnalysisSettings(AnalysisSettings.newBuilder()
                .setConsistentScalingFactor(consistentScalingFactor))
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setVirtualMachine(
                VirtualMachineInfo.newBuilder().setNumCpus(numCpus)));
        final Builder vcpuSold = vcpuSold();
        vcpuSold.setCapacity(vcpuSold.getCapacity() * numCpus);
        vcpuSold().setScalingFactor(scalingFactor);
        vmBuilder.addCommoditySoldList(vcpuSold);

        final TopologyEntity vm = mock(TopologyEntity.class);
        when(vm.getOid()).thenReturn(vmOid);
        when(vm.getEntityType()).thenReturn(EntityType.VIRTUAL_MACHINE_VALUE);
        when(vm.getTopologyEntityDtoBuilder()).thenReturn(vmBuilder);

        when(pod.getProviders()).thenReturn(Collections.singletonList(vm));
        when(container.getProviders()).thenReturn(Collections.singletonList(pod));
    }

    /**
     * Matcher that checks matching of commodity sold history with an expected value.
     */
    public static class CommoditySoldHistoryMatcher extends TypeSafeMatcher<CommoditySoldDTO> {

        private String reasonMatchFails;

        private final CommoditySoldDTO expectedValue;

        /**
         * Create a new {@link CommoditySoldHistoryMatcher}.
         *
         * @param expectedValue The expected value to match.
         */
        public CommoditySoldHistoryMatcher(@Nonnull final CommoditySoldDTO expectedValue) {
            this.expectedValue = Objects.requireNonNull(expectedValue);
        }

        @Override
        protected boolean matchesSafely(CommoditySoldDTO commoditySoldDTO) {
            if (expectedValue.hasIsResizeable() != commoditySoldDTO.hasIsResizeable()) {
                reasonMatchFails = String.format("expected hasIsResizeable=%s but was %s",
                    expectedValue.hasIsResizeable(), commoditySoldDTO.hasIsResizeable());
                return false;
            }
            if (expectedValue.hasIsResizeable()) {
                if (expectedValue.getIsResizeable() != commoditySoldDTO.getIsResizeable()) {
                    reasonMatchFails = String.format("expected getIsResizeable=%s but was %s",
                        expectedValue.getIsResizeable(), commoditySoldDTO.getIsResizeable());
                    return false;
                }
            }

            if (expectedValue.hasHistoricalPeak() != commoditySoldDTO.hasHistoricalPeak()) {
                reasonMatchFails = String.format("expected hasHistoricalPeak=%s but was %s",
                    expectedValue.hasHistoricalPeak(), commoditySoldDTO.hasHistoricalPeak());
                return false;
            }
            if (expectedValue.hasHistoricalPeak()) {
                if (expectedValue.getHistoricalPeak().equals(commoditySoldDTO.getHistoricalPeak())) {
                    reasonMatchFails = String.format("expected historicalPeak=%s but was %s",
                        expectedValue.getHistoricalPeak(), commoditySoldDTO.getHistoricalPeak());
                }
            }

            if (expectedValue.hasHistoricalUsed() != commoditySoldDTO.hasHistoricalUsed()) {
                reasonMatchFails = String.format("expected hasHistoricalUsed=%s but was %s",
                    expectedValue.hasHistoricalUsed(), commoditySoldDTO.hasHistoricalUsed());
                return false;
            }
            if (expectedValue.hasHistoricalUsed()) {
                if (expectedValue.getHistoricalUsed().equals(commoditySoldDTO.getHistoricalUsed())) {
                    reasonMatchFails = String.format("expected historicalUsed=%s but was %s",
                        expectedValue.getHistoricalUsed(), commoditySoldDTO.getHistoricalUsed());
                }
            }

            return true;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText(reasonMatchFails);
        }
    }

    /**
     * Create a new CommoditySoldHistoryMatcher.
     *
     * @param expected The {@link CommoditySoldDTO} expected to match.
     * @return A new CommoditySoldHistoryMatcher.
     */
    public static CommoditySoldHistoryMatcher matchesHistory(@Nonnull final CommoditySoldDTO expected) {
        return new CommoditySoldHistoryMatcher(expected);
    }
}