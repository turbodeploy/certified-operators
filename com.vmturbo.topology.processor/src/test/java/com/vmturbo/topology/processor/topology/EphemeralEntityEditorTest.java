package com.vmturbo.topology.processor.topology;

import static org.junit.Assert.assertThat;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Tests {@link EphemeralEntityEditor}.
 */
public class EphemeralEntityEditorTest {
    @SuppressWarnings("unchecked")
    private final TopologyGraph<TopologyEntity> graph = mock(TopologyGraph.class);

    private final TopologyEntity containerSpec = mock(TopologyEntity.class);

    private final TopologyEntity container = mock(TopologyEntity.class);

    private final EphemeralEntityEditor editor = new EphemeralEntityEditor();

    private final TopologyEntityDTO.Builder ephemeralBuilder = TopologyEntityDTO.newBuilder()
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

    private static final int COMM_1_TYPE = 1;
    private static final int COMM_2_TYPE = 2;

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
    }

    /**
     * Test running {@link EphemeralEntityEditor} on an empty graph.
     */
    @Test
    public void testEmptyGraph() {
        when(graph.entitiesOfType(EntityType.CONTAINER_SPEC.getNumber())).thenReturn(Stream.empty());
        editor.applyEdits(graph);
    }

    /**
     * Test running {@link EphemeralEntityEditor} on a graph with a container spec
     * having no relationship to any containers.
     */
    @Test
    public void testEntityWithNoRelationships() {
        when(graph.entitiesOfType(EntityType.CONTAINER_SPEC.getNumber())).thenReturn(Stream.of(containerSpec));
        when(containerSpec.getAggregatedEntities()).thenReturn(Collections.emptyList());

        editor.applyEdits(graph);
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

        editor.applyEdits(graph);
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

        editor.applyEdits(graph);
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

        editor.applyEdits(graph);
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
            .setCommodityType(CommodityType.newBuilder().setType(2).setKey("bar"));
        final CommoditySoldDTO beforeEdits = ephemeralCommSold.build();
        ephemeralBuilder.addCommoditySoldList(ephemeralCommSold);

        editor.applyEdits(graph);
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
            .setCommodityType(CommodityType.newBuilder().setType(2).setKey("foo"));
        ephemeralBuilder.addCommoditySoldList(ephemeralCommSold);

        editor.applyEdits(graph);
        assertThat(ephemeralBuilder.getCommoditySoldList(0), matchesHistory(sold2WithKey));
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

        editor.applyEdits(graph);
        assertThat(ephemeralBuilder.getCommoditySoldList(0), matchesHistory(sold1));
        assertThat(ephemeralBuilder.getCommoditySoldList(1), matchesHistory(sold2NoKey));
    }

    /**
     * Test running {@link EphemeralEntityEditor} on a graph with multiple entities
     * with commodities that should be transferred.
     */
    @Test
    public void testMultipleEntities() {
        final TopologyEntity container2 = mock(TopologyEntity.class);
        final TopologyEntityDTO.Builder ephemeralBuilder2 = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.CONTAINER.getNumber());

        when(container2.getEntityType()).thenReturn(EntityType.CONTAINER.getNumber());
        when(container2.getTopologyEntityDtoBuilder()).thenReturn(ephemeralBuilder2);

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

        editor.applyEdits(graph);
        assertThat(ephemeralBuilder.getCommoditySoldList(0), matchesHistory(sold1));
        assertThat(ephemeralBuilder.getCommoditySoldList(1), matchesHistory(sold2NoKey));
        assertThat(ephemeralBuilder2.getCommoditySoldList(0), matchesHistory(sold2WithKey));
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