package com.vmturbo.market.topology.conversions;

import static com.vmturbo.trax.Trax.trax;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Efficiency;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.market.topology.MarketTier;
import com.vmturbo.market.topology.OnDemandMarketTier;
import com.vmturbo.market.topology.conversions.ActionInterpreter.CalculatedSavings;
import com.vmturbo.market.topology.conversions.CloudEntityResizeTracker.CommodityUsageType;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.Congestion;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveExplanation;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Tests Explanations for cloud actions.
 */
public class InterpretCloudExplanationTest {

    private static final long SL_TO_MOVE = 1000L;
    private static final long VM1_OID = 500L;
    private static final long TIER1_OID = 101L;
    private static final long TIER2_OID = 102L;
    private static final long MARKET_TIER1_OID = 1L;
    private static final long MARKET_TIER2_OID = 2L;

    private final CommodityConverter commodityConverter = mock(CommodityConverter.class);
    // Map of shopping list id to shopping list info
    private final Map<Long, ShoppingListInfo> shoppingListInfoMap = Maps.newHashMap();
    private final CloudTopologyConverter cloudTc = mock(CloudTopologyConverter.class);
    // The incoming topology into market-componnent
    private final Map<Long, TopologyEntityDTO> originalTopology = Maps.newHashMap();
    // The oid to traderTO map
    private final Map<Long, EconomyDTOs.TraderTO> oidToTraderTOMap = Maps.newHashMap();
    // The cloud entity resize tracker
    private final CloudEntityResizeTracker cert = mock(CloudEntityResizeTracker.class);
    // Projected RI coverage
    private final Map<Long, EntityReservedInstanceCoverage> projectedRiCoverage = Maps.newHashMap();
    private static final CommodityType VCPU = CommodityType.newBuilder().setType(
        CommodityDTO.CommodityType.VCPU_VALUE).build();
    private static final CommodityType VMEM = CommodityType.newBuilder().setType(
        CommodityDTO.CommodityType.VMEM_VALUE).build();
    // Tier excluder
    private final TierExcluder tierExcluder = mock(TierExcluder.class);
    private final Map<Long, ProjectedTopologyEntity> projectedTopology = Maps.newHashMap();
    private final CloudTopology<TopologyEntityDTO> originalCloudTopology = mock(CloudTopology.class);
    private final Map<Long, CostJournal<TopologyEntityDTO>> projectedCosts = Maps.newHashMap();
    private final TopologyCostCalculator topologyCostCalculator = mock(TopologyCostCalculator.class);
    // We mock the interpreting of move action. We are only interested in the explanation.
    private final Move interpretedMoveAction = ActionDTO.Move.newBuilder().setTarget(ActionEntity.newBuilder()
        .setId(500L).setType(EntityType.VIRTUAL_MACHINE_VALUE)).build();
    private ProjectedRICoverageCalculator riCoverageCalculator;
    private final ActionTO move = ActionTO.newBuilder().setMove(
        MoveTO.newBuilder()
            .setShoppingListToMove(SL_TO_MOVE).setSource(MARKET_TIER1_OID)
            .setDestination(MARKET_TIER2_OID)
            .setMoveExplanation(
                MoveExplanation.newBuilder().setCongestion(Congestion.getDefaultInstance()).build()))
        .setImportance(0)
        .setIsNotExecutable(false)
        .build();
    private ActionInterpreter ai;
    private Optional<EntityReservedInstanceCoverage> initialCoverage;
    private EntityReservedInstanceCoverage projectedCoverage;

    /**
     * Setup all the inputs to ActionInterpreter.
     */
    @Before
    public void setup() {
        shoppingListInfoMap.put(SL_TO_MOVE, new ShoppingListInfo(SL_TO_MOVE, VM1_OID, TIER1_OID,
            null, null, EntityType.COMPUTE_TIER_VALUE, Collections.emptyList()));

        MarketTier computeTier = new OnDemandMarketTier(TopologyEntityDTO.newBuilder()
            .setOid(TIER2_OID)
            .setEntityType(EntityType.COMPUTE_TIER_VALUE).build());
        when(cloudTc.getMarketTier(MARKET_TIER2_OID)).thenReturn(computeTier);

        riCoverageCalculator = mock(ProjectedRICoverageCalculator.class);
        when(riCoverageCalculator.getProjectedReservedInstanceCoverage()).thenReturn(projectedRiCoverage);

        // We create action interpreter as a spy because we want to mock the interpretation of the
        // move action, but we want to test the explanation.
        ai = spy(new ActionInterpreter(commodityConverter, shoppingListInfoMap,
            cloudTc, originalTopology, oidToTraderTOMap, cert, riCoverageCalculator, tierExcluder,
            CommodityIndex.newFactory()::newIndex));

        initialCoverage = Optional.of(EntityReservedInstanceCoverage.newBuilder().setEntityId(VM1_OID)
            .putCouponsCoveredByRi(1L, 4)
            .putCouponsCoveredByRi(2L, 4).build());
        projectedCoverage = EntityReservedInstanceCoverage.newBuilder().setEntityId(VM1_OID)
            .putCouponsCoveredByRi(1L, 8)
            .putCouponsCoveredByRi(2L, 8).build();
        doReturn(Optional.of(interpretedMoveAction)).when(ai).interpretMoveAction(move.getMove(), projectedTopology, originalCloudTopology);
        when(cloudTc.isMarketTier(any())).thenReturn(true);

        IdentityGenerator.initPrefix(5L);
    }

    /**
     * A VM has congested commodities, under-utilized commodities, increased RI coverage and savings.
     * Congestion has the highest priority in this case. We test that in this test case.
     */
    @Test
    public void testInterpretCongestion() {
        // Congested / Under-utilized commodities
        Set<CommodityType> congestedCommodities = ImmutableSet.of(VCPU);
        Set<CommodityType> underUtilizedCommodities = ImmutableSet.of(VMEM);
        when(cert.getCommoditiesResizedByUsageType(VM1_OID)).thenReturn(
            ImmutableMap.of(CommodityUsageType.CONGESTED, congestedCommodities,
                            CommodityUsageType.UNDER_UTILIZED, underUtilizedCommodities));
        // Savings
        doReturn(new CalculatedSavings(trax(10))).when(ai).calculateActionSavings(move, originalCloudTopology,
            projectedCosts, topologyCostCalculator);

        // RI Coverage increase
        when(cloudTc.getRiCoverageForEntity(VM1_OID)).thenReturn(initialCoverage);
        projectedRiCoverage.put(VM1_OID, projectedCoverage);

        List<Action> actions = ai.interpretAction(move, projectedTopology, originalCloudTopology, projectedCosts, topologyCostCalculator);

        assertTrue(!actions.isEmpty());
        assertTrue(!actions.get(0).getExplanation().getMove().getChangeProviderExplanation(0)
            .getCongestion().getCongestedCommoditiesList().isEmpty());
    }

    /**
     * An VM has under-utilized commodities, increased RI coverage and savings.
     * It does not have any commodity congested.
     * Increased RI coverage will take precedence.
     */
    @Test
    public void testInterpretEfficiencyWithRiCoverageIncrease() {
        // Congested / Under-utilized commodities
        Set<CommodityType> underUtilizedCommodities = ImmutableSet.of(VMEM);
        when(cert.getCommoditiesResizedByUsageType(VM1_OID)).thenReturn(
            ImmutableMap.of(CommodityUsageType.CONGESTED, Collections.emptySet(),
                CommodityUsageType.UNDER_UTILIZED, underUtilizedCommodities));

        // Savings
        doReturn(new CalculatedSavings(trax(10))).when(ai).calculateActionSavings(move, originalCloudTopology,
            projectedCosts, topologyCostCalculator);

        // RI Coverage increases
        when(cloudTc.getRiCoverageForEntity(VM1_OID)).thenReturn(initialCoverage);
        when(riCoverageCalculator.getProjectedRICoverageForEntity(VM1_OID)).thenReturn(projectedCoverage);

        List<Action> actions = ai.interpretAction(move, projectedTopology, originalCloudTopology, projectedCosts, topologyCostCalculator);

        assertTrue(!actions.isEmpty());
        Efficiency efficiency = actions.get(0).getExplanation().getMove().getChangeProviderExplanation(0).getEfficiency();
        assertTrue(efficiency.getIsRiCoverageIncreased());
        assertTrue(efficiency.getUnderUtilizedCommoditiesList().isEmpty());
    }

    /**
     * An VM has under-utilized commodities, and savings.
     * It does not have congested commodity nor RI coverage increase.
     * Under-utilized commodities will take precedence.
     */
    @Test
    public void testInterpretEfficiencyWithUnderUtilizedCommodity() {
        // Congested / Under-utilized commodities
        Set<CommodityType> underUtilizedCommodities = ImmutableSet.of(VMEM);
        when(cert.getCommoditiesResizedByUsageType(VM1_OID)).thenReturn(
            ImmutableMap.of(CommodityUsageType.CONGESTED, Collections.emptySet(),
                CommodityUsageType.UNDER_UTILIZED, underUtilizedCommodities));
        // Savings
        doReturn(new CalculatedSavings(trax(10))).when(ai).calculateActionSavings(move, originalCloudTopology,
            projectedCosts, topologyCostCalculator);

        // No RI Coverage
        when(cloudTc.getRiCoverageForEntity(VM1_OID)).thenReturn(Optional.empty());
        projectedRiCoverage.put(VM1_OID, null);

        List<Action> actions = ai.interpretAction(move, projectedTopology, originalCloudTopology, projectedCosts, topologyCostCalculator);

        assertTrue(!actions.isEmpty());
        Efficiency efficiency = actions.get(0).getExplanation().getMove().getChangeProviderExplanation(0).getEfficiency();
        assertTrue(!efficiency.getUnderUtilizedCommoditiesList().isEmpty());
        assertFalse(efficiency.getIsRiCoverageIncreased());
        assertFalse(efficiency.getIsWastedCost());
    }

    /**
     * A VM has savings.
     * It does not have congested / under-utilized commodities. It also does not have RI
     * coverage increase.
     */
    @Test
    public void testInterpretEfficiencyWithWastedCost() {
        // Congested / Under-utilized commodities
        when(cert.getCommoditiesResizedByUsageType(VM1_OID)).thenReturn(
            ImmutableMap.of(CommodityUsageType.CONGESTED, Collections.emptySet(),
                CommodityUsageType.UNDER_UTILIZED, Collections.emptySet()));

        // Savings
        doReturn(new CalculatedSavings(trax(10))).when(ai).calculateActionSavings(move, originalCloudTopology,
            projectedCosts, topologyCostCalculator);

        // RI Coverage remains same
        when(cloudTc.getRiCoverageForEntity(VM1_OID)).thenReturn(initialCoverage);
        projectedRiCoverage.put(VM1_OID, initialCoverage.get());

        List<Action> actions = ai.interpretAction(move, projectedTopology, originalCloudTopology, projectedCosts, topologyCostCalculator);

        assertTrue(!actions.isEmpty());
        Efficiency efficiency = actions.get(0).getExplanation().getMove().getChangeProviderExplanation(0).getEfficiency();
        assertTrue(efficiency.getUnderUtilizedCommoditiesList().isEmpty());
        assertFalse(efficiency.getIsRiCoverageIncreased());
        assertTrue(efficiency.getIsWastedCost());
    }

    /**
     * A VM does not have any congested/under-utilized commodities. It has an investment.
     * We should not have an investment if there was no congested commodity.
     * The RI coverage remained the same.
     * We make the category efficiency, without setting any of the fields of the efficiency message
     * and print an error that we could not explain this action.
     */
    @Test
    public void testInterpretUnexplainableAction() {
        // Congested / Under-utilized commodities
        when(cert.getCommoditiesResizedByUsageType(VM1_OID)).thenReturn(
            ImmutableMap.of(CommodityUsageType.CONGESTED, Collections.emptySet(),
                CommodityUsageType.UNDER_UTILIZED, Collections.emptySet()));

        // Savings
        doReturn(new CalculatedSavings(trax(-10))).when(ai).calculateActionSavings(move, originalCloudTopology,
            projectedCosts, topologyCostCalculator);

        // RI Coverage remains same
        when(cloudTc.getRiCoverageForEntity(VM1_OID)).thenReturn(initialCoverage);
        projectedRiCoverage.put(VM1_OID, initialCoverage.get());

        List<Action> actions = ai.interpretAction(move, projectedTopology, originalCloudTopology, projectedCosts, topologyCostCalculator);

        assertTrue(!actions.isEmpty());
        Efficiency efficiency = actions.get(0).getExplanation().getMove().getChangeProviderExplanation(0).getEfficiency();
        assertTrue(efficiency.getUnderUtilizedCommoditiesList().isEmpty());
        assertFalse(efficiency.getIsRiCoverageIncreased());
        assertFalse(efficiency.getIsWastedCost());
    }
}
