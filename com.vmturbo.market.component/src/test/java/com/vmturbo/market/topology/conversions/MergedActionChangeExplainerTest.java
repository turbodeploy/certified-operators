package com.vmturbo.market.topology.conversions;

import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.VMEM_VALUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Congestion;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Efficiency;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.WastedCostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.commons.Pair;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.market.topology.conversions.CommoditiesResizeTracker.CommodityLookupType;
import com.vmturbo.market.topology.conversions.cloud.CloudActionSavingsCalculator.CalculatedSavings;
import com.vmturbo.market.topology.conversions.cloud.CloudActionSavingsCalculator.TraxSavingsDetails;
import com.vmturbo.market.topology.conversions.cloud.CloudActionSavingsCalculator.TraxSavingsDetails.TraxTierCostDetails;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.trax.Trax;

/**
 * Unit tests for merged actions explanations. An RDS Database server will have its compute and storage actions combined.
 * So this test makes sure we explain both parts of the action.
 */
@RunWith(Parameterized.class)
public class MergedActionChangeExplainerTest {

    private static final long DBS1_OID = 1L;
    private static final long DBS1_SL = 1000L;
    private static final long DBS_TIER1_OID = 10L;
    private static final long DBS_TIER2_OID = 11L;
    private static final TopologyDTO.CommodityType VMEM = TopologyDTO.CommodityType.newBuilder().setType(
            VMEM_VALUE).build();
    private static final TopologyDTO.CommodityType IOPS = TopologyDTO.CommodityType.newBuilder().setType(
            STORAGE_ACCESS_VALUE).build();

    private final CommoditiesResizeTracker commoditiesResizeTracker = new CommoditiesResizeTracker();
    private final Map<Long, ShoppingListInfo> shoppingListInfoMap = Maps.newHashMap();
    private final Map<Long, TopologyDTO.ProjectedTopologyEntity> projectedTopology = Maps.newHashMap();
    private final CommodityIndex commodityIndex = CommodityIndex.newFactory().newIndex();
    private final CloudTopologyConverter cloudTc = mock(CloudTopologyConverter.class);
    private final ActionDTOs.MoveTO moveTO = ActionDTOs.MoveTO.newBuilder()
            .setShoppingListToMove(DBS1_SL)
            .setSource(DBS_TIER1_OID)
            .setDestination(DBS_TIER2_OID)
            .setMoveExplanation(ActionDTOs.MoveExplanation.getDefaultInstance())
            .build();

    private final double sourceComputeCost;
    private final double sourceStorageCost;
    private final double projectedComputeCost;
    private final double projectedStorageCost;
    private final boolean isVmemCongested;
    private final boolean isIopsCongested;
    private final double sourceVmemSold;
    private final double sourceIopsSold;
    private final double projVmemSold;
    private final double projIopsSold;
    private final Optional<Congestion> expectedCongestion;
    private final Optional<Efficiency> expectedEfficiency;

    /**
     * Constructor for the parameterized test.
     * @param testCaseName test case name
     * @param sourceComputeCost source compute cost
     * @param sourceStorageCost source storage cost
     * @param projectedComputeCost projected compute cost
     * @param projectedStorageCost projected storage cost
     * @param isVmemCongested is vmem congested?
     * @param isIopsCongested is iops congested?
     * @param sourceVmemSold source Vmem sold capacity
     * @param sourceIopsSold source Iops sold capacity
     * @param projVmemSold projected Vmem sold capacity
     * @param projIopsSold projected Iops sold capacity
     * @param expectedCongestion optional expected congestion message
     * @param expectedEfficiency optional expected efficiency message
     */
    public MergedActionChangeExplainerTest(String testCaseName, double sourceComputeCost, double sourceStorageCost,
                                           double projectedComputeCost, double projectedStorageCost,
                                           boolean isVmemCongested, boolean isIopsCongested,
                                           double sourceVmemSold, double sourceIopsSold,
                                           double projVmemSold, double projIopsSold,
                                           Optional<Congestion> expectedCongestion,
                                           Optional<Efficiency> expectedEfficiency) {
        this.sourceComputeCost = sourceComputeCost;
        this.sourceStorageCost = sourceStorageCost;
        this.projectedComputeCost = projectedComputeCost;
        this.projectedStorageCost = projectedStorageCost;
        this.isVmemCongested = isVmemCongested;
        this.isIopsCongested = isIopsCongested;
        this.sourceVmemSold = sourceVmemSold;
        this.sourceIopsSold = sourceIopsSold;
        this.projVmemSold = projVmemSold;
        this.projIopsSold = projIopsSold;
        this.expectedCongestion = expectedCongestion;
        this.expectedEfficiency = expectedEfficiency;
    }

    /**
     * Setup the test.
     */
    @Before
    public void setup() {
        shoppingListInfoMap.put(DBS1_SL, new ShoppingListInfo(DBS1_SL, DBS1_OID, DBS_TIER1_OID,
                Collections.emptySet(), null, EntityType.DATABASE_SERVER_TIER_VALUE, Collections.emptyList()));
    }

    /**
     * The parameters for testMergedActionChangeExplanations.
     * @return the parameters for testMergedActionChangeExplanations.
     */
    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> parametersForTestMergedActionChangeExplanations() {
        return Arrays.asList(new Object[][] {
                {"testCongestedCompute", 5, 5, 10, 5, true, false, 500, 5000, 700, 5000,
                        Optional.of(Congestion.newBuilder().addAllCongestedCommodities(
                                ChangeExplainer.commTypes2ReasonCommodities(Collections.singleton(VMEM))).build()),
                        Optional.empty()},
                {"testCongestedComputeWithUnderUtilizedStorage", 5, 5, 10, 3, true, false, 500, 5000, 1000, 1000,
                        Optional.of(Congestion.newBuilder()
                                .addAllCongestedCommodities(ChangeExplainer.commTypes2ReasonCommodities(Collections.singleton(VMEM)))
                                .addAllUnderUtilizedCommodities(ChangeExplainer.commTypes2ReasonCommodities(Collections.singleton(IOPS))).build()),
                        Optional.empty()},
                {"testCongestedComputeWithReducedCostStorage", 5, 5, 10, 3, true, false, 500, 5000, 1000, 5000,
                        Optional.of(Congestion.newBuilder()
                                .addAllCongestedCommodities(ChangeExplainer.commTypes2ReasonCommodities(Collections.singleton(VMEM)))
                                .setIsWastedCost(true)
                                .setWastedCostCategory(WastedCostCategory.STORAGE).build()),
                        Optional.empty()},
                {"testCongestedStorage", 5, 5, 5, 10, false, true, 500, 5000, 500, 7000,
                        Optional.of(Congestion.newBuilder()
                                .addAllCongestedCommodities(ChangeExplainer.commTypes2ReasonCommodities(Collections.singleton(IOPS))).build()),
                        Optional.empty()},
                {"testCongestedStorageWithUnderUtilizedCompute", 5, 5, 3, 10, false, true, 500, 5000, 300, 10000,
                        Optional.of(Congestion.newBuilder()
                                .addAllCongestedCommodities(ChangeExplainer.commTypes2ReasonCommodities(Collections.singleton(IOPS)))
                                .addAllUnderUtilizedCommodities(ChangeExplainer.commTypes2ReasonCommodities(Collections.singleton(VMEM))).build()),
                        Optional.empty()},
                {"testCongestedStorageWithReducedCostCompute", 5, 5, 3, 10, false, true, 500, 5000, 500, 10000,
                        Optional.of(Congestion.newBuilder()
                                .addAllCongestedCommodities(ChangeExplainer.commTypes2ReasonCommodities(Collections.singleton(IOPS)))
                                .setIsWastedCost(true).setWastedCostCategory(WastedCostCategory.COMPUTE).build()),
                        Optional.empty()},
                {"testUnderutilizedComputeAndStorage", 5, 5, 3, 3, false, false, 500, 5000, 300, 1000,
                        Optional.empty(),
                        Optional.of(Efficiency.newBuilder().addAllUnderUtilizedCommodities(
                                ChangeExplainer.commTypes2ReasonCommodities(ImmutableList.of(IOPS, VMEM))).build())},
                {"testUnderutilizedComputeAndReducedCostStorage", 5, 5, 3, 3, false, false, 500, 5000, 300, 5000,
                        Optional.empty(),
                        Optional.of(Efficiency.newBuilder()
                                .addAllUnderUtilizedCommodities(ChangeExplainer.commTypes2ReasonCommodities(ImmutableList.of(VMEM)))
                                .setIsWastedCost(true).setWastedCostCategory(WastedCostCategory.STORAGE)
                                .build())},
                {"testUnderutilizedStorageAndReducedCostCompute", 5, 5, 3, 3, false, false, 500, 5000, 500, 1000,
                        Optional.empty(),
                        Optional.of(Efficiency.newBuilder()
                                .addAllUnderUtilizedCommodities(ChangeExplainer.commTypes2ReasonCommodities(ImmutableList.of(IOPS)))
                                .setIsWastedCost(true).setWastedCostCategory(WastedCostCategory.COMPUTE)
                                .build())},
                {"testReducedCostComputeAndStorage", 5, 5, 3, 3, false, false, 500, 5000, 500, 5000,
                        Optional.empty(),
                        Optional.of(Efficiency.newBuilder().setIsWastedCost(true).build())},
                {"testNoFreeScaleUp", 5, 5, 3, 3, false, false, 500, 5000, 700, 7000,
                        Optional.empty(),
                        Optional.of(Efficiency.newBuilder().setIsWastedCost(true).build())},
        });
    }

    /**
     * The base merged action change explanations test for the parameters defined above.
     */
    @Test
    public void testMergedActionChangeExplanations() {
        final CalculatedSavings savings = setupCosts(sourceComputeCost, sourceStorageCost, projectedComputeCost, projectedStorageCost);
        commoditiesResizeTracker.save(DBS1_OID, DBS_TIER1_OID, VMEM, isVmemCongested, CommodityLookupType.CONSUMER);
        commoditiesResizeTracker.save(DBS1_OID, DBS_TIER1_OID, IOPS, isIopsCongested, CommodityLookupType.CONSUMER);

        TopologyEntityDTO sourceDBS = createDBS(DBS1_OID, new Pair<>(VMEM, sourceVmemSold), new Pair<>(IOPS, sourceIopsSold));
        commodityIndex.addEntity(sourceDBS);
        TopologyEntityDTO projectedDBS = createDBS(DBS1_OID, new Pair<>(VMEM, projVmemSold), new Pair<>(IOPS, projIopsSold));
        projectedTopology.put(DBS1_OID, TopologyDTO.ProjectedTopologyEntity.newBuilder().setEntity(projectedDBS).build());

        ChangeExplainer changeExplainer = new MergedActionChangeExplainer(commoditiesResizeTracker, cloudTc, shoppingListInfoMap, commodityIndex);
        Optional<ChangeProviderExplanation.Builder> changeProviderExplanation = changeExplainer.changeExplanationFromTracker(moveTO, savings, projectedTopology);

        assertTrue(changeProviderExplanation.isPresent());
        assertEquals(expectedCongestion.isPresent(), changeProviderExplanation.get().hasCongestion());
        assertEquals(expectedEfficiency.isPresent(), changeProviderExplanation.get().hasEfficiency());

        if (expectedCongestion.isPresent()) {
            Congestion actualCongestion = changeProviderExplanation.get().getCongestion();
            assertTrue(CollectionUtils.isEqualCollection(expectedCongestion.get().getCongestedCommoditiesList(), actualCongestion.getCongestedCommoditiesList()));
            assertTrue(CollectionUtils.isEqualCollection(expectedCongestion.get().getUnderUtilizedCommoditiesList(), actualCongestion.getUnderUtilizedCommoditiesList()));
            assertEquals(expectedCongestion.get().getIsWastedCost(), actualCongestion.getIsWastedCost());
            assertEquals(expectedCongestion.get().getWastedCostCategory(), actualCongestion.getWastedCostCategory());
        } else if (expectedEfficiency.isPresent()) {
            Efficiency actualEfficiency = changeProviderExplanation.get().getEfficiency();
            assertTrue(CollectionUtils.isEqualCollection(expectedEfficiency.get().getUnderUtilizedCommoditiesList(),
                    actualEfficiency.getUnderUtilizedCommoditiesList()));
            assertEquals(expectedEfficiency.get().getIsWastedCost(), actualEfficiency.getIsWastedCost());
            assertEquals(expectedEfficiency.get().getWastedCostCategory(), actualEfficiency.getWastedCostCategory());
            assertEquals(0, expectedEfficiency.get().getScaleUpCommodityCount());
        }
    }

    private CalculatedSavings setupCosts(double sourceComputeCost, double sourceStorageCost, double projectedComputeCost, double projectedStorageCost) {
        CostJournal<TopologyEntityDTO> sourceCostJournal = mock(CostJournal.class);
        when(sourceCostJournal.getHourlyCostForCategory(CostCategory.ON_DEMAND_COMPUTE)).thenReturn(Trax.trax(sourceComputeCost));
        when(sourceCostJournal.getHourlyCostForCategory(CostCategory.STORAGE)).thenReturn(Trax.trax(sourceStorageCost));

        CostJournal<TopologyEntityDTO> projectedCostJournal = mock(CostJournal.class);
        when(projectedCostJournal.getHourlyCostForCategory(CostCategory.ON_DEMAND_COMPUTE)).thenReturn(Trax.trax(projectedComputeCost));
        when(projectedCostJournal.getHourlyCostForCategory(CostCategory.STORAGE)).thenReturn(Trax.trax(projectedStorageCost));

        final TraxTierCostDetails sourceTierCostDetails = TraxTierCostDetails.builder().costJournal(sourceCostJournal).build();
        final TraxTierCostDetails projectedTierCostDetails = TraxTierCostDetails.builder().costJournal(projectedCostJournal).build();

        return CalculatedSavings.builder().cloudSavingsDetails(TraxSavingsDetails.builder()
                .sourceTierCostDetails(sourceTierCostDetails)
                .projectedTierCostDetails(projectedTierCostDetails).build()).build();
    }

    private TopologyEntityDTO createDBS(long oid, Pair<TopologyDTO.CommodityType, Double>... soldCommsWithCapacities) {
        List<TopologyDTO.CommoditySoldDTO> commsSold = new ArrayList();
        for (Pair<TopologyDTO.CommodityType, Double> soldCommTypeWithCapacity : soldCommsWithCapacities) {
            TopologyDTO.CommoditySoldDTO commSold = TopologyDTO.CommoditySoldDTO.newBuilder()
                    .setCommodityType(soldCommTypeWithCapacity.first)
                    .setCapacity(soldCommTypeWithCapacity.second).build();
            commsSold.add(commSold);
        }
        return TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setEntityType(EntityType.DATABASE_SERVER_VALUE)
                .addAllCommoditySoldList(commsSold)
                .build();
    }
}