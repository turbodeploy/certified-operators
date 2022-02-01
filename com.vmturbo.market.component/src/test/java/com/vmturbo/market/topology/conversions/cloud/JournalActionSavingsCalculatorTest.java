package com.vmturbo.market.topology.conversions.cloud;

import static com.vmturbo.trax.Trax.trax;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Allocate;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverage;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.cost.calculation.journal.CostJournal.CostSourceFilter;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.market.topology.TopologyEntitiesHandlerTest;
import com.vmturbo.market.topology.conversions.cloud.CloudActionSavingsCalculator.CalculatedSavings;
import com.vmturbo.market.topology.conversions.cloud.CloudActionSavingsCalculator.TraxSavingsDetails;

/**
 * A unit test for {@link JournalActionSavingsCalculator}.
 */
@RunWith(MockitoJUnitRunner.class)
public class JournalActionSavingsCalculatorTest {

    private static final double ERROR = 1E-3;


    private final JournalActionSavingsCalculatorFactory calculatorFactory =
            new JournalActionSavingsCalculatorFactory();

    @Mock
    private TopologyCostCalculator costCalculator;

    @Mock
    private CloudTopology<TopologyEntityDTO> sourceCloudTopology;

    @Mock
    private CloudCostData sourceCloudCostData;

    /**
     * Setup.
     */
    @Before
    public void setup() {
        when(costCalculator.getCloudCostData()).thenReturn(sourceCloudCostData);
    }

    /**
     * Test a scale action with an investment.
     * @throws FileNotFoundException Thrown from {@link TopologyEntitiesHandlerTest#readCloudTopologyFromJsonFile()}.
     * @throws InvalidProtocolBufferException Thrown from {@link TopologyEntitiesHandlerTest#readCloudTopologyFromJsonFile()}.
     */
    @Test
    public void testScaleActionInvestment() throws FileNotFoundException, InvalidProtocolBufferException {

        /*
        Setup entities
         */
        Set<Builder> topologyEntityDTOBuilders
                = TopologyEntitiesHandlerTest.readCloudTopologyFromJsonFile();
        final TopologyEntityDTO vm1 = topologyEntityDTOBuilders.stream()
                .filter(builder -> builder.getDisplayName().equalsIgnoreCase("TestVM1"))
                .findFirst().get().build();

        final TopologyEntityDTO sourceTier = topologyEntityDTOBuilders.stream()
                .filter(builder -> builder.getDisplayName().equalsIgnoreCase("m1.large"))
                .findFirst().get().build();
        final TopologyEntityDTO destTier = topologyEntityDTOBuilders.stream()
                .filter(builder -> builder.getDisplayName().equalsIgnoreCase("m1.medium"))
                .findFirst().get().build();

        // setup Action
        final Action.Builder action = Action.newBuilder()
                .setInfo(ActionInfo.newBuilder()
                        .setScale(Scale.newBuilder()
                                .setTarget(ActionEntity.newBuilder()
                                        .setId(vm1.getOid())
                                        .setType(vm1.getEntityType()))
                                .addChanges(ChangeProvider.newBuilder()
                                        .setSource(ActionEntity.newBuilder()
                                                .setId(sourceTier.getOid())
                                                .setType(sourceTier.getEntityType()))
                                        .setDestination(ActionEntity.newBuilder()
                                                .setId(destTier.getOid())
                                                .setType(destTier.getEntityType())))));

        /*
        Setup source costs
         */
        final CostJournal<TopologyEntityDTO> sourceCostJournal = mock(CostJournal.class);
        // Source compute cost = 4 + 3 + 4 = 11 (compute + license + reserved license)
        when(sourceCostJournal.getHourlyCostFilterEntries(eq(CostCategory.ON_DEMAND_COMPUTE), eq(CostSourceFilter.EXCLUDE_BUY_RI_DISCOUNT_FILTER)))
                .thenReturn(trax(4));
        when(sourceCostJournal.getHourlyCostFilterEntries(eq(CostCategory.ON_DEMAND_COMPUTE), eq(CostSourceFilter.ON_DEMAND_RATE)))
                .thenReturn(trax(6));
        when(sourceCostJournal.getHourlyCostFilterEntries(eq(CostCategory.ON_DEMAND_LICENSE), any())).thenReturn(trax(3d));
        when(sourceCostJournal.getHourlyCostFilterEntries(eq(CostCategory.RESERVED_LICENSE), any())).thenReturn(trax(4d));
        // Total Source cost = 20
        when(sourceCostJournal.getTotalHourlyCost()).thenReturn(trax(20d));
        when(costCalculator.calculateCostForEntity(any(), eq(vm1))).thenReturn(Optional.of(sourceCostJournal));

        /*
        Setup projected costs
         */
        final CostJournal<TopologyEntityDTO> projectedCostJournal = mock(CostJournal.class);
        // Destination compute cost = 16 + 2 + 0 = 18
        when(projectedCostJournal.getHourlyCostFilterEntries(eq(CostCategory.ON_DEMAND_COMPUTE), any())).thenReturn(trax(16d));
        when(projectedCostJournal.getHourlyCostFilterEntries(eq(CostCategory.ON_DEMAND_LICENSE), any())).thenReturn(trax(2d));
        when(projectedCostJournal.getHourlyCostFilterEntries(eq(CostCategory.RESERVED_LICENSE), any())).thenReturn(trax(0d));
        final Map<Long, CostJournal<TopologyEntityDTO>> projectedCosts = ImmutableMap.of(
                vm1.getOid(), projectedCostJournal);

        /*
        Setup source & projected topologies
         */
        final Map<Long, TopologyEntityDTO> sourceTopologyMap = ImmutableMap.of(
                vm1.getOid(), vm1,
                sourceTier.getOid(), sourceTier);
        final Map<Long, ProjectedTopologyEntity> projectedTopologyMap = ImmutableMap.of(
                destTier.getOid(), ProjectedTopologyEntity.newBuilder()
                        .setEntity(destTier)
                        .build());

        /*
        Setup RI coverage
         */
        final EntityReservedInstanceCoverage sourceCoverage = EntityReservedInstanceCoverage.newBuilder()
                .setEntityId(vm1.getOid())
                .setEntityCouponCapacity(3.0)
                .putCouponsCoveredByRi(123L, 1.0)
                .putCouponsCoveredByRi(456L, 1.0)
                .build();
        when(sourceCloudCostData.getFilteredRiCoverage(eq(vm1.getOid()))).thenReturn(Optional.of(sourceCoverage));
        // no projected RI coverage
        final Map<Long, EntityReservedInstanceCoverage> projectedCoverageMap = ImmutableMap.of();


        /*
        Create savings calculator
         */
        final JournalActionSavingsCalculator savingsCalculator = calculatorFactory.newCalculator(
                sourceTopologyMap,
                sourceCloudTopology,
                costCalculator,
                projectedTopologyMap,
                projectedCosts,
                projectedCoverageMap);

        final CalculatedSavings actualSavings = savingsCalculator.calculateSavings(action);

        // ASSERT savings per hour
        assertTrue(actualSavings.savingsPerHour().isPresent());
        assertThat(actualSavings.savingsPerHour().get().getValue(), closeTo(-7.0, ERROR));

        // ASSERT savings details
        assertTrue(actualSavings.cloudSavingsDetails().isPresent());

        final TraxSavingsDetails actualDetails = actualSavings.cloudSavingsDetails().get();
        // on-demand rate should be 8 + 3 = 11
        assertThat(actualDetails.sourceTierCostDetails().onDemandRate().getValue(), closeTo(9.0, ERROR));
        assertTrue(actualDetails.sourceTierCostDetails().cloudCommitmentCoverage().isPresent());
        // on-demand cost should be 4 + 3 + 4 (compute + on-demand license + reserved license)
        assertThat(actualDetails.sourceTierCostDetails().onDemandCost().getValue(), closeTo(11.0, ERROR));

        assertThat(actualDetails.projectedTierCostDetails().onDemandRate().getValue(), closeTo(18.0, ERROR));
        assertThat(actualDetails.projectedTierCostDetails().onDemandCost().getValue(), closeTo(18.0, ERROR));
    }

    /**
     * Test an allocation action savings for RI inventory.
     * @throws FileNotFoundException Thrown from {@link TopologyEntitiesHandlerTest#readCloudTopologyFromJsonFile()}.
     * @throws InvalidProtocolBufferException Thrown from {@link TopologyEntitiesHandlerTest#readCloudTopologyFromJsonFile()}.
     */
    @Test
    public void testAllocateNoRIBuyAction() throws FileNotFoundException, InvalidProtocolBufferException {

        /*
        Setup entities
         */
        final Set<Builder> topologyEntityDTOBuilders
                = TopologyEntitiesHandlerTest.readCloudTopologyFromJsonFile();
        final TopologyEntityDTO vm1 = topologyEntityDTOBuilders.stream()
                .filter(builder -> builder.getDisplayName().equalsIgnoreCase("TestVM1"))
                .findFirst().get().build();

        final TopologyEntityDTO computeTier = topologyEntityDTOBuilders.stream()
                .filter(builder -> builder.getDisplayName().equalsIgnoreCase("m1.large"))
                .findFirst().get().build();

        // setup Action
        final Action.Builder action = Action.newBuilder()
                .setInfo(ActionInfo.newBuilder()
                        .setAllocate(Allocate.newBuilder()
                                .setTarget(ActionEntity.newBuilder()
                                        .setId(vm1.getOid())
                                        .setType(vm1.getEntityType()))
                                .setWorkloadTier(ActionEntity.newBuilder()
                                        .setId(computeTier.getOid())
                                        .setType(computeTier.getEntityType()))));

        /*
        Setup source costs
         */
        final CostJournal<TopologyEntityDTO> sourceCostJournal = mock(CostJournal.class);
        when(sourceCostJournal.getHourlyCostFilterEntries(eq(CostCategory.ON_DEMAND_COMPUTE), eq(CostSourceFilter.EXCLUDE_BUY_RI_DISCOUNT_FILTER)))
                .thenReturn(trax(4));
        when(sourceCostJournal.getHourlyCostFilterEntries(eq(CostCategory.ON_DEMAND_COMPUTE), eq(CostSourceFilter.ON_DEMAND_RATE)))
                .thenReturn(trax(6));
        when(sourceCostJournal.getHourlyCostFilterEntries(eq(CostCategory.ON_DEMAND_LICENSE), any())).thenReturn(trax(0d));
        when(sourceCostJournal.getHourlyCostFilterEntries(eq(CostCategory.RESERVED_LICENSE), any())).thenReturn(trax(0d));
        // Total Source cost = 20
        when(sourceCostJournal.getTotalHourlyCost()).thenReturn(trax(20d));
        when(costCalculator.calculateCostForEntity(any(), eq(vm1))).thenReturn(Optional.of(sourceCostJournal));

        /*
        Setup projected costs
         */
        final CostJournal<TopologyEntityDTO> projectedCostJournal = mock(CostJournal.class);
        when(projectedCostJournal.getHourlyCostFilterEntries(eq(CostCategory.ON_DEMAND_COMPUTE), eq(CostSourceFilter.EXCLUDE_BUY_RI_DISCOUNT_FILTER)))
                .thenReturn(trax(1));
        when(projectedCostJournal.getHourlyCostFilterEntries(eq(CostCategory.ON_DEMAND_COMPUTE), eq(CostSourceFilter.ON_DEMAND_RATE)))
                .thenReturn(trax(6));
        when(projectedCostJournal.getHourlyCostFilterEntries(eq(CostCategory.ON_DEMAND_LICENSE), any())).thenReturn(trax(0));
        when(projectedCostJournal.getHourlyCostFilterEntries(eq(CostCategory.RESERVED_LICENSE), any())).thenReturn(trax(0d));
        final Map<Long, CostJournal<TopologyEntityDTO>> projectedCosts = ImmutableMap.of(
                vm1.getOid(), projectedCostJournal);

        /*
        Setup source & projected topologies
         */
        final Map<Long, TopologyEntityDTO> sourceTopologyMap = ImmutableMap.of(
                vm1.getOid(), vm1,
                computeTier.getOid(), computeTier);
        final Map<Long, ProjectedTopologyEntity> projectedTopologyMap = ImmutableMap.of(
                computeTier.getOid(), ProjectedTopologyEntity.newBuilder()
                        .setEntity(computeTier)
                        .build());

        /*
        Setup RI coverage
         */
        final EntityReservedInstanceCoverage sourceCoverage = EntityReservedInstanceCoverage.newBuilder()
                .setEntityId(vm1.getOid())
                .setEntityCouponCapacity(3.0)
                .putCouponsCoveredByRi(123L, 1.0)
                .build();
        when(sourceCloudCostData.getFilteredRiCoverage(eq(vm1.getOid()))).thenReturn(Optional.of(sourceCoverage));
        // no projected RI coverage
        final Map<Long, EntityReservedInstanceCoverage> projectedCoverageMap = ImmutableMap.of(
                vm1.getOid(), EntityReservedInstanceCoverage.newBuilder()
                        .setEntityId(vm1.getOid())
                        .setEntityCouponCapacity(3.0)
                        .putCouponsCoveredByRi(123L, 2.0)
                        .putCouponsCoveredByBuyRi(456L, 1.0)
                        .build());


        /*
        Create savings calculator
         */
        final JournalActionSavingsCalculator savingsCalculator = calculatorFactory.newCalculator(
                sourceTopologyMap,
                sourceCloudTopology,
                costCalculator,
                projectedTopologyMap,
                projectedCosts,
                projectedCoverageMap);

        final CalculatedSavings actualSavings = savingsCalculator.calculateSavings(action);


        /*
        ASSERTS
         */

        // ASSERT savings per hour
        assertTrue(actualSavings.savingsPerHour().isPresent());
        assertThat(actualSavings.savingsPerHour().get().getValue(), closeTo(3.0, ERROR));

        // ASSERT savings details
        assertTrue(actualSavings.cloudSavingsDetails().isPresent());

        final TraxSavingsDetails actualDetails = actualSavings.cloudSavingsDetails().get();
        // on-demand rate should be 8 + 3 = 11
        assertThat(actualDetails.sourceTierCostDetails().onDemandRate().getValue(), closeTo(6.0, ERROR));
        assertTrue(actualDetails.sourceTierCostDetails().cloudCommitmentCoverage().isPresent());
        // on-demand cost should be 4 + 3 + 4 (compute + on-demand license + reserved license)
        assertThat(actualDetails.sourceTierCostDetails().onDemandCost().getValue(), closeTo(4.0, ERROR));

        assertThat(actualDetails.projectedTierCostDetails().onDemandRate().getValue(), closeTo(6.0, ERROR));
        assertThat(actualDetails.projectedTierCostDetails().onDemandCost().getValue(), closeTo(1.0, ERROR));

        // check projected RI coverage
        final Optional<CloudCommitmentCoverage> projectedCoverage =
                actualDetails.projectedTierCostDetails().cloudCommitmentCoverage();
        assertTrue(projectedCoverage.isPresent());
        assertThat(projectedCoverage.get().getUsed().getCoupons(), closeTo(2.0, ERROR));
    }

    /**
     * Test an allocation action savings for RI buy recommendation.
     * @throws FileNotFoundException Thrown from {@link TopologyEntitiesHandlerTest#readCloudTopologyFromJsonFile()}.
     * @throws InvalidProtocolBufferException Thrown from {@link TopologyEntitiesHandlerTest#readCloudTopologyFromJsonFile()}.
     */
    @Test
    public void testRIBuyAllocateAction() throws FileNotFoundException, InvalidProtocolBufferException {

        /*
        Setup entities
         */
        final Set<Builder> topologyEntityDTOBuilders
                = TopologyEntitiesHandlerTest.readCloudTopologyFromJsonFile();
        final TopologyEntityDTO vm1 = topologyEntityDTOBuilders.stream()
                .filter(builder -> builder.getDisplayName().equalsIgnoreCase("TestVM1"))
                .findFirst().get().build();

        final TopologyEntityDTO computeTier = topologyEntityDTOBuilders.stream()
                .filter(builder -> builder.getDisplayName().equalsIgnoreCase("m1.large"))
                .findFirst().get().build();

        // setup Action
        final Action.Builder action = Action.newBuilder()
                .setInfo(ActionInfo.newBuilder()
                        .setAllocate(Allocate.newBuilder()
                                .setTarget(ActionEntity.newBuilder()
                                        .setId(vm1.getOid())
                                        .setType(vm1.getEntityType()))
                                .setWorkloadTier(ActionEntity.newBuilder()
                                        .setId(computeTier.getOid())
                                        .setType(computeTier.getEntityType()))
                                .setIsBuyRecommendationCoverage(true)));

        /*
        Setup source costs
         */
        final CostJournal<TopologyEntityDTO> sourceCostJournal = mock(CostJournal.class);
        when(sourceCostJournal.getHourlyCostFilterEntries(eq(CostCategory.ON_DEMAND_COMPUTE), eq(CostSourceFilter.INCLUDE_ALL)))
                .thenReturn(trax(4));
        when(sourceCostJournal.getHourlyCostFilterEntries(eq(CostCategory.ON_DEMAND_COMPUTE), eq(CostSourceFilter.ON_DEMAND_RATE)))
                .thenReturn(trax(6));
        when(sourceCostJournal.getHourlyCostFilterEntries(eq(CostCategory.ON_DEMAND_LICENSE), any())).thenReturn(trax(0d));
        when(sourceCostJournal.getHourlyCostFilterEntries(eq(CostCategory.RESERVED_LICENSE), any())).thenReturn(trax(0d));
        // Total Source cost = 20
        when(sourceCostJournal.getTotalHourlyCost()).thenReturn(trax(20d));
        when(costCalculator.calculateCostForEntity(any(), eq(vm1))).thenReturn(Optional.of(sourceCostJournal));

        /*
        Setup projected costs
         */
        final CostJournal<TopologyEntityDTO> projectedCostJournal = mock(CostJournal.class);
        when(projectedCostJournal.getHourlyCostFilterEntries(eq(CostCategory.ON_DEMAND_COMPUTE), eq(CostSourceFilter.INCLUDE_ALL)))
                .thenReturn(trax(0));
        when(projectedCostJournal.getHourlyCostFilterEntries(eq(CostCategory.ON_DEMAND_COMPUTE), eq(CostSourceFilter.ON_DEMAND_RATE)))
                .thenReturn(trax(6));
        when(projectedCostJournal.getHourlyCostFilterEntries(eq(CostCategory.ON_DEMAND_LICENSE), any())).thenReturn(trax(0));
        when(projectedCostJournal.getHourlyCostFilterEntries(eq(CostCategory.RESERVED_LICENSE), any())).thenReturn(trax(0d));
        final Map<Long, CostJournal<TopologyEntityDTO>> projectedCosts = ImmutableMap.of(
                vm1.getOid(), projectedCostJournal);

        /*
        Setup source & projected topologies
         */
        final Map<Long, TopologyEntityDTO> sourceTopologyMap = ImmutableMap.of(
                vm1.getOid(), vm1,
                computeTier.getOid(), computeTier);
        final Map<Long, ProjectedTopologyEntity> projectedTopologyMap = ImmutableMap.of(
                computeTier.getOid(), ProjectedTopologyEntity.newBuilder()
                        .setEntity(computeTier)
                        .build());

        /*
        Setup RI coverage
         */
        final EntityReservedInstanceCoverage sourceCoverage = EntityReservedInstanceCoverage.newBuilder()
                .setEntityId(vm1.getOid())
                .setEntityCouponCapacity(3.0)
                .putCouponsCoveredByRi(123L, 1.0)
                .build();
        when(sourceCloudCostData.getFilteredRiCoverage(eq(vm1.getOid()))).thenReturn(Optional.of(sourceCoverage));
        // no projected RI coverage
        final Map<Long, EntityReservedInstanceCoverage> projectedCoverageMap = ImmutableMap.of(
                vm1.getOid(), EntityReservedInstanceCoverage.newBuilder()
                        .setEntityId(vm1.getOid())
                        .setEntityCouponCapacity(3.0)
                        .putCouponsCoveredByRi(123L, 2.0)
                        .putCouponsCoveredByBuyRi(456L, 1.0)
                        .build());


        /*
        Create savings calculator
         */
        final JournalActionSavingsCalculator savingsCalculator = calculatorFactory.newCalculator(
                sourceTopologyMap,
                sourceCloudTopology,
                costCalculator,
                projectedTopologyMap,
                projectedCosts,
                projectedCoverageMap);

        final CalculatedSavings actualSavings = savingsCalculator.calculateSavings(action);


        /*
        ASSERTS
         */

        // ASSERT savings per hour
        assertTrue(actualSavings.savingsPerHour().isPresent());
        assertThat(actualSavings.savingsPerHour().get().getValue(), closeTo(4.0, ERROR));

        // ASSERT savings details
        assertTrue(actualSavings.cloudSavingsDetails().isPresent());

        final TraxSavingsDetails actualDetails = actualSavings.cloudSavingsDetails().get();
        // on-demand rate should be 8 + 3 = 11
        assertThat(actualDetails.sourceTierCostDetails().onDemandRate().getValue(), closeTo(6.0, ERROR));
        assertTrue(actualDetails.sourceTierCostDetails().cloudCommitmentCoverage().isPresent());
        // on-demand cost should be 4 + 3 + 4 (compute + on-demand license + reserved license)
        assertThat(actualDetails.sourceTierCostDetails().onDemandCost().getValue(), closeTo(4.0, ERROR));

        assertThat(actualDetails.projectedTierCostDetails().onDemandRate().getValue(), closeTo(6.0, ERROR));
        assertThat(actualDetails.projectedTierCostDetails().onDemandCost().getValue(), closeTo(0.0, ERROR));

        // check projected RI coverage
        final Optional<CloudCommitmentCoverage> projectedCoverage =
                actualDetails.projectedTierCostDetails().cloudCommitmentCoverage();
        assertTrue(projectedCoverage.isPresent());
        assertThat(projectedCoverage.get().getUsed().getCoupons(), closeTo(3.0, ERROR));
    }

    /**
     * Test a deactivate action savings.
     * @throws FileNotFoundException Thrown from {@link TopologyEntitiesHandlerTest#readCloudTopologyFromJsonFile()}.
     * @throws InvalidProtocolBufferException Thrown from {@link TopologyEntitiesHandlerTest#readCloudTopologyFromJsonFile()}.
     */
    @Test
    public void testDeactivateAction() throws FileNotFoundException, InvalidProtocolBufferException {
        /*
        Setup entities
         */
        final Set<Builder> topologyEntityDTOBuilders
                = TopologyEntitiesHandlerTest.readCloudTopologyFromJsonFile();
        final TopologyEntityDTO vm1 = topologyEntityDTOBuilders.stream()
                .filter(builder -> builder.getDisplayName().equalsIgnoreCase("TestVM1"))
                .findFirst().get().build();

        final TopologyEntityDTO computeTier = topologyEntityDTOBuilders.stream()
                .filter(builder -> builder.getDisplayName().equalsIgnoreCase("m1.large"))
                .findFirst().get().build();

        // setup Action
        final Action.Builder action = Action.newBuilder()
                .setInfo(ActionInfo.newBuilder()
                        .setDeactivate(Deactivate.newBuilder()
                                .setTarget(ActionEntity.newBuilder()
                                        .setId(vm1.getOid())
                                        .setType(vm1.getEntityType()))));

        /*
        Setup source costs
         */
        final CostJournal<TopologyEntityDTO> sourceCostJournal = mock(CostJournal.class);
        when(sourceCostJournal.getHourlyCostFilterEntries(eq(CostCategory.ON_DEMAND_COMPUTE), any())).thenReturn(trax(6));
        when(sourceCostJournal.getHourlyCostFilterEntries(eq(CostCategory.ON_DEMAND_LICENSE), any())).thenReturn(trax(2d));
        when(sourceCostJournal.getHourlyCostFilterEntries(eq(CostCategory.RESERVED_LICENSE), any())).thenReturn(trax(0d));
        // Total Source cost = 20
        when(sourceCostJournal.getTotalHourlyCost()).thenReturn(trax(20d));
        when(costCalculator.calculateCostForEntity(any(), eq(vm1))).thenReturn(Optional.of(sourceCostJournal));

        /*
        Setup projected costs
         */
        final CostJournal<TopologyEntityDTO> projectedCostJournal = mock(CostJournal.class);
        when(projectedCostJournal.getHourlyCostFilterEntries(eq(CostCategory.ON_DEMAND_COMPUTE), any())).thenReturn(trax(0));
        when(projectedCostJournal.getHourlyCostFilterEntries(eq(CostCategory.ON_DEMAND_LICENSE), any())).thenReturn(trax(0));
        when(projectedCostJournal.getHourlyCostFilterEntries(eq(CostCategory.RESERVED_LICENSE), any())).thenReturn(trax(0d));
        final Map<Long, CostJournal<TopologyEntityDTO>> projectedCosts = ImmutableMap.of(
                vm1.getOid(), projectedCostJournal);

        /*
        Setup source & projected topologies
         */
        final Map<Long, TopologyEntityDTO> sourceTopologyMap = ImmutableMap.of(
                vm1.getOid(), vm1,
                computeTier.getOid(), computeTier);
        when(sourceCloudTopology.getTierProviders(eq(vm1.getOid()))).thenReturn(Collections.singleton(computeTier));
        final Map<Long, ProjectedTopologyEntity> projectedTopologyMap = ImmutableMap.of(
                vm1.getOid(), ProjectedTopologyEntity.newBuilder()
                        .setEntity(vm1)
                        .build(),
                computeTier.getOid(), ProjectedTopologyEntity.newBuilder()
                        .setEntity(computeTier)
                        .build());

        /*
        Setup RI coverage
         */
        when(sourceCloudCostData.getFilteredRiCoverage(eq(vm1.getOid()))).thenReturn(Optional.empty());
        // no projected RI coverage
        final Map<Long, EntityReservedInstanceCoverage> projectedCoverageMap = ImmutableMap.of();


        /*
        Create savings calculator
         */
        final JournalActionSavingsCalculator savingsCalculator = calculatorFactory.newCalculator(
                sourceTopologyMap,
                sourceCloudTopology,
                costCalculator,
                projectedTopologyMap,
                projectedCosts,
                projectedCoverageMap);

        final CalculatedSavings actualSavings = savingsCalculator.calculateSavings(action);


        /*
        ASSERTS
         */

        // ASSERT savings per hour
        assertTrue(actualSavings.savingsPerHour().isPresent());
        assertThat(actualSavings.savingsPerHour().get().getValue(), closeTo(8.0, ERROR));
    }

}
