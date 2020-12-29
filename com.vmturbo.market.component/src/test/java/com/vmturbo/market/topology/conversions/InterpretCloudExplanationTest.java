package com.vmturbo.market.topology.conversions;

import static com.vmturbo.market.topology.conversions.CommoditiesResizeTracker.CommodityLookupType.CONSUMER;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.VMEM_VALUE;
import static com.vmturbo.trax.Trax.trax;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Efficiency;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.market.topology.MarketTier;
import com.vmturbo.market.topology.OnDemandMarketTier;
import com.vmturbo.market.topology.conversions.ActionInterpreter.CalculatedSavings;
import com.vmturbo.market.topology.conversions.CommoditiesResizeTracker.CommodityTypeWithLookup;
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
    private final CommoditiesResizeTracker commoditiesResizeTracker = mock(CommoditiesResizeTracker.class);
    // Projected RI coverage
    private final Map<Long, EntityReservedInstanceCoverage> projectedRiCoverage = Maps.newHashMap();
    private static final CommodityType VCPU = CommodityType.newBuilder().setType(
        CommodityDTO.CommodityType.VCPU_VALUE).build();
    private static final CommodityType VMEM = CommodityType.newBuilder().setType(
        VMEM_VALUE).build();
    private static final CommodityTypeWithLookup VCPUWithLookup = ImmutableCommodityTypeWithLookup.builder()
        .commodityType(VCPU).lookupType(CONSUMER).build();
    private static final CommodityTypeWithLookup VMEMWithLookup = ImmutableCommodityTypeWithLookup.builder()
        .commodityType(VMEM).lookupType(CONSUMER).build();
    private final CommodityIndex commodityIndex = mock(CommodityIndex.class);
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
        when(commodityIndex.getCommSold(VM1_OID, VMEM)).thenReturn(
            Optional.of(CommoditySoldDTO.newBuilder().setCommodityType(VMEM).setCapacity(100).build()));
        ai = spy(new ActionInterpreter(commodityConverter, shoppingListInfoMap,
            cloudTc, originalTopology, oidToTraderTOMap, commoditiesResizeTracker, riCoverageCalculator, tierExcluder,
            Suppliers.memoize(() -> commodityIndex), null));

        initialCoverage = Optional.of(EntityReservedInstanceCoverage.newBuilder().setEntityId(VM1_OID)
            .putCouponsCoveredByRi(1L, 4)
            .putCouponsCoveredByRi(2L, 4)
            .setEntityCouponCapacity(16).build());
        projectedCoverage = EntityReservedInstanceCoverage.newBuilder().setEntityId(VM1_OID)
            .putCouponsCoveredByRi(1L, 8)
            .putCouponsCoveredByRi(2L, 8)
            .setEntityCouponCapacity(16).build();
        doReturn(Optional.of(interpretedMoveAction)).when(ai).interpretMoveAction(move.getMove(), projectedTopology, originalCloudTopology);
        when(cloudTc.isMarketTier(any())).thenReturn(true);
        projectedTopology.put(VM1_OID, ProjectedTopologyEntity.newBuilder().setEntity(
            TopologyEntityDTO.newBuilder().setOid(VM1_OID).setEntityType(10).addCommoditySoldList(
                CommoditySoldDTO.newBuilder().setCommodityType(VMEM).setCapacity(4000))).build());

        IdentityGenerator.initPrefix(5L);
    }

    /**
     * A VM has congested commodities, under-utilized commodities, increased RI coverage and savings.
     * Congestion has the highest priority in this case. We test that in this test case.
     */
    @Test
    public void testInterpretCongestion() {
        // Congested / Under-utilized commodities
        Set<CommodityTypeWithLookup> congestedCommodities = ImmutableSet.of(VCPUWithLookup);
        Set<CommodityTypeWithLookup> underUtilizedCommodities = ImmutableSet.of(VMEMWithLookup);
        when(commoditiesResizeTracker.getCongestedCommodityTypes(VM1_OID, TIER1_OID)).thenReturn(congestedCommodities);
        when(commoditiesResizeTracker.getUnderutilizedCommodityTypes(VM1_OID, TIER1_OID)).thenReturn(underUtilizedCommodities);

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
        Set<CommodityTypeWithLookup> underUtilizedCommodities = ImmutableSet.of(VMEMWithLookup);
        when(commoditiesResizeTracker.getCongestedCommodityTypes(VM1_OID, TIER1_OID)).thenReturn(Collections.emptySet());
        when(commoditiesResizeTracker.getUnderutilizedCommodityTypes(VM1_OID, TIER1_OID)).thenReturn(underUtilizedCommodities);

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
     * An VM has under-utilized commodities, increased RI coverage and savings.
     * It does not have any commodity congested.
     * RI coverage will not take precedence since the coverage increase is less than 1%.
     */
    @Test
    public void testInterpretEfficiencyWithInsufficientRiCoverageIncrease() {
        // Congested / Under-utilized commodities
        Set<CommodityTypeWithLookup> underUtilizedCommodities = ImmutableSet.of(VMEMWithLookup);
        when(commoditiesResizeTracker.getCongestedCommodityTypes(VM1_OID, TIER1_OID)).thenReturn(Collections.emptySet());
        when(commoditiesResizeTracker.getUnderutilizedCommodityTypes(VM1_OID, TIER1_OID)).thenReturn(underUtilizedCommodities);

        // Savings
        doReturn(new CalculatedSavings(trax(10))).when(ai).calculateActionSavings(move, originalCloudTopology,
            projectedCosts, topologyCostCalculator);

        // RI Coverage increases
        when(cloudTc.getRiCoverageForEntity(VM1_OID)).thenReturn(Optional.empty());
        when(riCoverageCalculator.getProjectedRICoverageForEntity(VM1_OID)).thenReturn(Optional.of(EntityReservedInstanceCoverage.newBuilder()
                .setEntityId(VM1_OID)
                .putCouponsCoveredByRi(1L, 0.00001)
                .setEntityCouponCapacity(16).build()).get());

        List<Action> actions = ai.interpretAction(move, projectedTopology, originalCloudTopology, projectedCosts, topologyCostCalculator);

        assertTrue(!actions.isEmpty());
        Efficiency efficiency = actions.get(0).getExplanation().getMove().getChangeProviderExplanation(0).getEfficiency();
        assertFalse(efficiency.getIsRiCoverageIncreased());
    }

    /**
     * An VM has under-utilized commodities, and savings.
     * It does not have congested commodity nor RI coverage increase.
     * Under-utilized commodities will take precedence.
     */
    @Test
    public void testInterpretEfficiencyWithUnderUtilizedCommodity() {
        // Congested / Under-utilized commodities
        Set<CommodityTypeWithLookup> underUtilizedCommodities = ImmutableSet.of(VMEMWithLookup);
        when(commoditiesResizeTracker.getCongestedCommodityTypes(VM1_OID, TIER1_OID)).thenReturn(Collections.emptySet());
        when(commoditiesResizeTracker.getUnderutilizedCommodityTypes(VM1_OID, TIER1_OID)).thenReturn(underUtilizedCommodities);
        // Savings
        doReturn(new CalculatedSavings(trax(10))).when(ai).calculateActionSavings(move, originalCloudTopology,
            projectedCosts, topologyCostCalculator);

        // No RI Coverage
        when(cloudTc.getRiCoverageForEntity(VM1_OID)).thenReturn(Optional.empty());
        projectedRiCoverage.put(VM1_OID, null);

        Builder vmemSoldCommodity = CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder().setType(VMEM_VALUE));
        Map<Long, ProjectedTopologyEntity> projectedTopologyMap = new HashMap<>();
        Map<Long, TopologyEntityDTO> originalTopologyMap = new HashMap<>();
        TopologyEntityDTO topologyEntityDTO = TopologyEntityDTO.newBuilder()
                .setOid(VM1_OID)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addCommoditySoldList(vmemSoldCommodity.setCapacity(100L)
                        .build())
                .build();
        originalTopologyMap.put(VM1_OID, topologyEntityDTO);
        topologyEntityDTO = TopologyEntityDTO.newBuilder()
                .setOid(VM1_OID)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addCommoditySoldList(vmemSoldCommodity.setCapacity(10L)
                        .build())
                .build();
        ProjectedTopologyEntity projectedTopology = ProjectedTopologyEntity.newBuilder()
                .setEntity(topologyEntityDTO)
                .build();
        projectedTopologyMap.put(VM1_OID, projectedTopology);
        ai = spy(new ActionInterpreter(commodityConverter, shoppingListInfoMap,
            cloudTc, originalTopologyMap, oidToTraderTOMap, commoditiesResizeTracker,
            riCoverageCalculator, tierExcluder, Suppliers.memoize(() -> commodityIndex), null));
        doReturn(Optional.of(interpretedMoveAction)).when(ai).interpretMoveAction(move.getMove(), projectedTopologyMap, originalCloudTopology);
        List<Action> actions = ai.interpretAction(move, projectedTopologyMap, originalCloudTopology, projectedCosts, topologyCostCalculator);

        assertFalse(actions.isEmpty());
        Efficiency efficiency = actions.get(0).getExplanation().getMove().getChangeProviderExplanation(0).getEfficiency();
        assertFalse(efficiency.getUnderUtilizedCommoditiesList().isEmpty());
        assertFalse(efficiency.getIsRiCoverageIncreased());
        assertFalse(efficiency.getIsWastedCost());
    }

    /**
     * This test checks if the number of commodities in an action explanation is based on the fact that
     * actual capacity value is smaller (or underutilized) in projectedTopology compared to originalTopology.
     */
    @Test
    public void testInterpretEfficiencyWithOnlyFewerUnderUtilizedCommodity() {
        Set<CommodityTypeWithLookup> underUtilizedCommodities = ImmutableSet.of(VMEMWithLookup, VCPUWithLookup);
        when(commoditiesResizeTracker.getCongestedCommodityTypes(VM1_OID, TIER1_OID)).thenReturn(Collections.emptySet());
        when(commoditiesResizeTracker.getUnderutilizedCommodityTypes(VM1_OID, TIER1_OID)).thenReturn(underUtilizedCommodities);
        when(cloudTc.getRiCoverageForEntity(VM1_OID)).thenReturn(Optional.empty());
        CommoditySoldDTO cpuSoldCommodity = CommoditySoldDTO.newBuilder()
                .setCapacity(10L)
                .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE)
                        .build()).build();
        when(commodityIndex.getCommSold(VM1_OID, VCPU)).thenReturn(Optional.of(cpuSoldCommodity));
        Builder vmemSoldCommodity = CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder().setType(VMEM_VALUE));
        Map<Long, ProjectedTopologyEntity> projectedTopologyMap = new HashMap<>();

        TopologyEntityDTO topologyEntityDTO = TopologyEntityDTO.newBuilder()
                .setOid(VM1_OID)
                .setEntityType(EntityType.DATABASE_VALUE)
                .addCommoditySoldList(vmemSoldCommodity.setCapacity(10L) // smaller VMEM capacity.
                        .build())
                .addCommoditySoldList(cpuSoldCommodity) // identical VCPU capacity.
                .build();
        ProjectedTopologyEntity projectedTopology = ProjectedTopologyEntity.newBuilder()
                .setEntity(topologyEntityDTO)
                .build();

        projectedTopologyMap.put(VM1_OID, projectedTopology);
        ai = spy(new ActionInterpreter(commodityConverter, shoppingListInfoMap,
                cloudTc, Maps.newHashMap(), oidToTraderTOMap, commoditiesResizeTracker, riCoverageCalculator, tierExcluder,
            Suppliers.memoize(() -> commodityIndex), null));

        doReturn(Optional.of(interpretedMoveAction)).when(ai).interpretMoveAction(move.getMove(), projectedTopologyMap, originalCloudTopology);
        List<Action> actions = ai.interpretAction(move, projectedTopologyMap, originalCloudTopology, projectedCosts, topologyCostCalculator);
        assertThat(actions.get(0).getExplanation().getMove().getChangeProviderExplanation(0).getEfficiency()
                .getUnderUtilizedCommoditiesList().size(), is(1));
        assertThat(actions.get(0).getExplanation().getMove().getChangeProviderExplanation(0).getEfficiency()
                .getUnderUtilizedCommoditiesList().iterator().next().getCommodityType().getType(), is(VMEM_VALUE));
    }

    /**
     * This test checks if the number of commodities in an action explanation is based on the fact that
     * actual capacity value is smaller (or underutilized) in projectedTopology compared to originalTopology.
     */
    @Test
    public void testInterpretCSgCompliance() {
        Set<CommodityTypeWithLookup> underUtilizedCommodities = ImmutableSet.of(VMEMWithLookup, VCPUWithLookup);
        when(commoditiesResizeTracker.getCongestedCommodityTypes(VM1_OID, TIER1_OID)).thenReturn(Collections.emptySet());
        when(commoditiesResizeTracker.getUnderutilizedCommodityTypes(VM1_OID, TIER1_OID)).thenReturn(underUtilizedCommodities);
        when(cloudTc.getRiCoverageForEntity(VM1_OID)).thenReturn(Optional.empty());
        CommoditySoldDTO cpuSoldCommodity = CommoditySoldDTO.newBuilder()
            .setCapacity(10L)
            .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE)
                .build()).build();
        when(commodityIndex.getCommSold(VM1_OID, VCPU)).thenReturn(Optional.of(cpuSoldCommodity));
        Builder vmemSoldCommodity = CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder().setType(VMEM_VALUE));
        Map<Long, ProjectedTopologyEntity> projectedTopologyMap = new HashMap<>();

        TopologyEntityDTO topologyEntityDTO = TopologyEntityDTO.newBuilder()
            .setOid(VM1_OID)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditySoldList(vmemSoldCommodity.setCapacity(100L) // same capacity
                .build())
            .addCommoditySoldList(cpuSoldCommodity) // identical VCPU capacity.
            .build();
        ProjectedTopologyEntity projectedTopology = ProjectedTopologyEntity.newBuilder()
            .setEntity(topologyEntityDTO)
            .build();

        projectedTopologyMap.put(VM1_OID, projectedTopology);
        ai = spy(new ActionInterpreter(commodityConverter, shoppingListInfoMap,
            cloudTc, Maps.newHashMap(), oidToTraderTOMap, commoditiesResizeTracker, riCoverageCalculator, tierExcluder,
            Suppliers.memoize(() -> commodityIndex), null));

        MoveTO csgMoveTO = move.getMove().toBuilder().setScalingGroupId("testScalingGroup").build();

        doReturn(Optional.of(interpretedMoveAction)).when(ai).interpretMoveAction(csgMoveTO, projectedTopologyMap, originalCloudTopology);
        List<Action> actions = ai.interpretAction(move.toBuilder().setMove(csgMoveTO).build(), projectedTopologyMap, originalCloudTopology, projectedCosts, topologyCostCalculator);
        assertTrue(actions.get(0).getExplanation().getMove().getChangeProviderExplanation(0).getCompliance().getIsCsgCompliance());
    }

    /**
     * A VM has savings.
     * It does not have congested / under-utilized commodities. It also does not have RI
     * coverage increase.
     */
    @Test
    public void testInterpretEfficiencyWithWastedCost() {
        // Congested / Under-utilized commodities
        when(commoditiesResizeTracker.getCongestedCommodityTypes(VM1_OID, TIER1_OID)).thenReturn(Collections.emptySet());
        when(commoditiesResizeTracker.getUnderutilizedCommodityTypes(VM1_OID, TIER1_OID)).thenReturn(Collections.emptySet());

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
     * A VM does not have any congested/under-utilized commodities. Action results in zero
     * savings/investment. In this case the action should be explained as "efficiency". This test
     * covers explanation of AWS volume move from IO1 to IO2.
     */
    @Test
    public void testInterpretZeroCostWithNoCommodityChangeAction() {
        // Empty congested / under-utilized commodities
        when(commoditiesResizeTracker.getCongestedCommodityTypes(VM1_OID, TIER1_OID))
                .thenReturn(Collections.emptySet());
        when(commoditiesResizeTracker.getUnderutilizedCommodityTypes(VM1_OID, TIER1_OID))
                .thenReturn(Collections.emptySet());

        // Zero savings
        doReturn(new CalculatedSavings(trax(0))).when(ai)
                .calculateActionSavings(move, originalCloudTopology, projectedCosts,
                        topologyCostCalculator);

        // RI Coverage remains same
        when(cloudTc.getRiCoverageForEntity(VM1_OID)).thenReturn(initialCoverage);
        projectedRiCoverage.put(VM1_OID, initialCoverage.get());

        final List<Action> actions = ai.interpretAction(move, projectedTopology,
                originalCloudTopology, projectedCosts, topologyCostCalculator);

        assertEquals(1, actions.size());
        final ChangeProviderExplanation explanation = actions.get(0).getExplanation().getMove()
                .getChangeProviderExplanation(0);
        // We expect efficiency explanation with no additional fields populated
        assertTrue(explanation.hasEfficiency());
        final Efficiency efficiency = explanation.getEfficiency();
        assertTrue(efficiency.getUnderUtilizedCommoditiesList().isEmpty());
        assertFalse(efficiency.getIsRiCoverageIncreased());
        assertFalse(efficiency.getIsWastedCost());
        assertTrue(efficiency.getScaleUpCommodityList().isEmpty());
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
        when(commoditiesResizeTracker.getCongestedCommodityTypes(VM1_OID, TIER1_OID)).thenReturn(Collections.emptySet());
        when(commoditiesResizeTracker.getUnderutilizedCommodityTypes(VM1_OID, TIER1_OID)).thenReturn(Collections.emptySet());

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
        assertEquals(0, efficiency.getScaleUpCommodityCount());
    }
}
