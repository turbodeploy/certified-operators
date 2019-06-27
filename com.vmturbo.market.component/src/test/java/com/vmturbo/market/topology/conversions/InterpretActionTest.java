package com.vmturbo.market.topology.conversions;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySet;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.analysis.AnalysisUtil;
import com.vmturbo.commons.analysis.InvalidTopologyException;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.calculation.CostJournal;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.market.runner.cost.MarketPriceTable;
import com.vmturbo.market.topology.MarketTier;
import com.vmturbo.market.topology.OnDemandMarketTier;
import com.vmturbo.market.topology.TopologyEntitiesHandlerTest;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.CompoundMoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.DeactivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.InitialPlacement;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveExplanation;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionBySupplyTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ReconfigureTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test various actions.
 */
public class InterpretActionTest {

    private static final TopologyInfo REALTIME_TOPOLOGY_INFO =  TopologyInfo.newBuilder()
            .setTopologyType(TopologyType.REALTIME)
            .build();

    // No AZ in this json file. Entities are connected to Region.
    private static String SIMPLE_CLOUD_TOPOLOGY_NO_AZ_JSON_FILE =
        "protobuf/messages/simple-cloudTopology-no-AZ.json";

    private CommodityDTOs.CommoditySpecificationTO economyCommodity1;
    private CommodityType topologyCommodity1;
    private CommodityDTOs.CommoditySpecificationTO economyCommodity2;
    private CommodityType topologyCommodity2;

    private MarketPriceTable marketPriceTable = mock(MarketPriceTable.class);
    private CloudCostData ccd = mock(CloudCostData.class);

    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
        // The commodity types in topologyCommodity
        // map to the base type in economy commodity.
        topologyCommodity1 = CommodityType.newBuilder()
                        .setType(1)
                        .setKey("blah")
                        .build();
        topologyCommodity2 = CommodityType.newBuilder()
                        .setType(2)
                        .setKey("blahblah")
                        .build();
        economyCommodity1 = CommodityDTOs.CommoditySpecificationTO.newBuilder()
                        .setType(0)
                        .setBaseType(1)
                        .build();
        economyCommodity2 = CommodityDTOs.CommoditySpecificationTO.newBuilder()
                        .setType(1)
                        .setBaseType(2)
                        .build();
        when(ccd.getExistingRiBought()).thenReturn(new ArrayList());
    }

    @Test
    public void testCommodityIdsAreInvertible() {
        final TopologyConverter converter =
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, true,
                AnalysisUtil.QUOTE_FACTOR,
                AnalysisUtil.LIVE_MARKET_MOVE_COST_FACTOR,
                marketPriceTable, ccd, CommodityIndex.newFactory());

        final CommodityType segmentationFoo = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.SEGMENTATION_VALUE)
            .setKey("foo")
            .build();
        final CommodityType segmentationBar = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.SEGMENTATION_VALUE)
            .setKey("bar")
            .build();
        final CommodityType cpu = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.SEGMENTATION_VALUE)
            .build();
        final CommodityType cpuRed = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.SEGMENTATION_VALUE)
            .setKey("red")
            .build();

        int segmentationFooId = converter.getCommodityConverter()
                .toMarketCommodityId(segmentationFoo);
        int segmentationBarId = converter.getCommodityConverter()
                .toMarketCommodityId(segmentationBar);
        int cpuId = converter.getCommodityConverter().toMarketCommodityId(cpu);
        int cpuRedId = converter.getCommodityConverter().toMarketCommodityId(cpuRed);

        assertEquals(segmentationFoo, converter.getCommodityConverter().commodityIdToCommodityType(segmentationFooId));
        assertEquals(segmentationBar, converter.getCommodityConverter().commodityIdToCommodityType(segmentationBarId));
        assertEquals(cpu, converter.getCommodityConverter().commodityIdToCommodityType(cpuId));
        assertEquals(cpuRed, converter.getCommodityConverter().commodityIdToCommodityType(cpuRedId));
    }

    @Test
    public void testExecutableFlag() {
        long modelSeller = 1234;
        int modelType = 1;
        final Map<Long, ProjectedTopologyEntity> projectedTopology =
            ImmutableMap.of(modelSeller, entity(modelSeller, modelType, EnvironmentType.ON_PREM));
        final TopologyConverter converter =
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, true,
                AnalysisUtil.QUOTE_FACTOR, AnalysisUtil.LIVE_MARKET_MOVE_COST_FACTOR,
                marketPriceTable, ccd, CommodityIndex.newFactory());

        final ActionTO executableActionTO = ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                // Using ProvisionBySupply because it requires the least setup, and all
                // we really care about is the executable flag.
                .setProvisionBySupply(ProvisionBySupplyTO.newBuilder()
                        .setProvisionedSeller(-1)
                        .setModelSeller(modelSeller)
                        .build())
                .build();

        final ActionTO notExecutableActionTO = ActionTO.newBuilder(executableActionTO)
                .setIsNotExecutable(true)
                .build();

        assertTrue(converter.interpretAction(executableActionTO, projectedTopology, null, null, null).get().getExecutable());
        assertFalse(converter.interpretAction(notExecutableActionTO, projectedTopology, null, null, null).get().getExecutable());
    }

    private ProjectedTopologyEntity entity(final long id, final int type, final EnvironmentType envType) {
        return entity(id, type, envType, EntityState.POWERED_ON);
    }

    private ProjectedTopologyEntity entity(final long id, final int type,
                                           final EnvironmentType envType, final EntityState entityState) {
        return ProjectedTopologyEntity.newBuilder()
            .setEntity(TopologyEntityDTO.newBuilder()
                .setOid(id)
                .setEntityType(type)
                .setEnvironmentType(envType)
                .setEntityState(entityState))
            .build();
    }

    private ProjectedTopologyEntity entity(TopologyEntityDTO topologyEntity) {
        return ProjectedTopologyEntity.newBuilder()
            .setEntity(topologyEntity)
            .build();
    }

    @Test
    public void testInterpretMoveAction() throws IOException {
        TopologyDTO.TopologyEntityDTO entityDto =
                TopologyConverterFromMarketTest.messageFromJsonFile("protobuf/messages/vm-1.dto.json");
        long resourceId = entityDto.getCommoditiesBoughtFromProviders(0).getVolumeId();

        final TopologyConverter converter =
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, true,
                AnalysisUtil.QUOTE_FACTOR, AnalysisUtil.LIVE_MARKET_MOVE_COST_FACTOR,
                marketPriceTable, ccd, CommodityIndex.newFactory());

        final Set<TraderTO> traderTOs =
            converter.convertToMarket(ImmutableMap.of(entityDto.getOid(), entityDto));
        final TraderTO vmTraderTO = TopologyConverterToMarketTest.getVmTrader(traderTOs);
        ShoppingListTO shoppingList = vmTraderTO.getShoppingListsList().get(0);
        ShoppingListTO shoppingList1 = vmTraderTO.getShoppingListsList().get(1);

        long srcId = 1234;
        long srcId1 = shoppingList1.getSupplier();
        long destId = 5678;
        int srcType = 0;
        int destType = 1;
        int resourceType = 2;

        Map<Long, ProjectedTopologyEntity> projectedTopology = ImmutableMap.of(
            srcId, entity(srcId, srcType, EnvironmentType.ON_PREM),
            srcId1, entity(srcId1, srcType, EnvironmentType.ON_PREM, EntityState.MAINTENANCE),
            destId, entity(destId, destType, EnvironmentType.ON_PREM),
            entityDto.getOid(), entity(entityDto.getOid(), entityDto.getEntityType(), EnvironmentType.ON_PREM),
            resourceId, entity(resourceId, resourceType, EnvironmentType.ON_PREM));

        ActionInfo actionInfo = converter.interpretAction(
                ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                .setCompoundMove(CompoundMoveTO.newBuilder()
                    .addMoves(MoveTO.newBuilder()
                        .setShoppingListToMove(shoppingList.getOid())
                        .setSource(srcId)
                        .setDestination(destId)
                        .setMoveExplanation(MoveExplanation.getDefaultInstance())))
                .build(), projectedTopology, null, null, null).get().getInfo();
        ActionInfo actionInfoWithOutSource = converter.interpretAction(
                ActionTO.newBuilder()
                        .setImportance(0.1)
                        .setIsNotExecutable(false)
                        .setCompoundMove(CompoundMoveTO.newBuilder()
                                .addMoves(MoveTO.newBuilder()
                                        .setShoppingListToMove(shoppingList.getOid())
                                        .setDestination(destId)
                                        .setMoveExplanation(MoveExplanation.newBuilder()
                                                .setInitialPlacement(InitialPlacement
                                                        .getDefaultInstance()))))
                        .build(), projectedTopology, null, null, null)
                .get().getInfo();
        // Created a MoveTO whose source is in Maintenance state and has InitialPlacement explanation.
        // This ActionTO will be interpreted to an Action with Evacuation explanation and \
        // a not available source.
        Action actionWithMaintenanceSource = converter.interpretAction(
            ActionTO.newBuilder()
                .setImportance(0.1)
                .setIsNotExecutable(false)
                .setMove(MoveTO.newBuilder()
                    .setShoppingListToMove(shoppingList1.getOid())
                    .setDestination(destId)
                    .setMoveExplanation(MoveExplanation.newBuilder().setInitialPlacement(
                        InitialPlacement.getDefaultInstance())).build()).build(),
            projectedTopology, null, null, null).get();
        List<ChangeProviderExplanation> explanations =
            actionWithMaintenanceSource.getExplanation().getMove().getChangeProviderExplanationList();
        ActionInfo actionInfoWithMaintenanceSource = actionWithMaintenanceSource.getInfo();

        assertEquals(ActionTypeCase.MOVE, actionInfo.getActionTypeCase());
        assertEquals(1, actionInfo.getMove().getChangesList().size());
        assertEquals(srcId, actionInfo.getMove().getChanges(0).getSource().getId());
        assertEquals(srcType, actionInfo.getMove().getChanges(0).getSource().getType());
        assertEquals(destId, actionInfo.getMove().getChanges(0).getDestination().getId());
        assertEquals(destType, actionInfo.getMove().getChanges(0).getDestination().getType());
        assertEquals(resourceId, actionInfo.getMove().getChanges(0).getResource().getId());
        assertEquals(resourceType, actionInfo.getMove().getChanges(0).getResource().getType());

        assertEquals(ActionTypeCase.MOVE, actionInfoWithOutSource.getActionTypeCase());
        assertEquals(1, actionInfoWithOutSource.getMove().getChangesList().size());
        assertFalse(actionInfoWithOutSource.getMove().getChanges(0).hasSource());
        assertEquals(destId, actionInfo.getMove().getChanges(0).getDestination().getId());
        assertEquals(destType, actionInfo.getMove().getChanges(0).getDestination().getType());

        assertEquals(1, explanations.size());
        assertTrue(explanations.get(0).hasEvacuation());
        assertEquals(srcId1, explanations.get(0).getEvacuation().getSuspendedEntity());
        assertFalse(explanations.get(0).getEvacuation().getIsAvailable());
        assertEquals(ActionTypeCase.MOVE, actionInfoWithMaintenanceSource.getActionTypeCase());
        assertEquals(1, actionInfoWithMaintenanceSource.getMove().getChangesList().size());
        ChangeProvider changeProvider = actionWithMaintenanceSource.getInfo().getMove().getChanges(0);
        assertEquals(srcId1, changeProvider.getSource().getId());
        assertEquals(destId, changeProvider.getDestination().getId());
    }

    @Test
    public void testInterpretReconfigureAction() throws IOException, InvalidTopologyException {
        long reconfigureSourceId = 1234;
        int reconfigureSourceType = 1;

        TopologyDTO.TopologyEntityDTO entityDto =
                        TopologyConverterFromMarketTest.messageFromJsonFile("protobuf/messages/vm-1.dto.json");
        final Map<Long, ProjectedTopologyEntity> projectedTopology =
            ImmutableMap.of(
                reconfigureSourceId, entity(reconfigureSourceId, reconfigureSourceType, EnvironmentType.ON_PREM),
                entityDto.getOid(), entity(entityDto.getOid(), entityDto.getEntityType(), EnvironmentType.CLOUD));
        final TopologyConverter converter =
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, true,
                AnalysisUtil.QUOTE_FACTOR, AnalysisUtil.LIVE_MARKET_MOVE_COST_FACTOR,
                marketPriceTable, ccd, CommodityIndex.newFactory());

        final Set<TraderTO> traderTOs =
                converter.convertToMarket(ImmutableMap.of(entityDto.getOid(), entityDto));
        final TraderTO vmTraderTO = TopologyConverterToMarketTest.getVmTrader(traderTOs);
        ShoppingListTO shoppingList = vmTraderTO.getShoppingListsList().get(0);

        ActionInfo actionInfo = converter.interpretAction(
            ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                .setReconfigure(ReconfigureTO.newBuilder()
                    .setShoppingListToReconfigure(shoppingList.getOid())
                    .setSource(reconfigureSourceId))
                .build(), projectedTopology, null, null, null).get().getInfo();

        assertThat(actionInfo.getActionTypeCase(), is(ActionTypeCase.RECONFIGURE));
        assertThat(actionInfo.getReconfigure().getSource().getId(), is(reconfigureSourceId));
        assertThat(actionInfo.getReconfigure().getSource().getType(), is(reconfigureSourceType));
        assertThat(actionInfo.getReconfigure().getSource().getEnvironmentType(), is(EnvironmentType.ON_PREM));
        assertThat(actionInfo.getReconfigure().getTarget().getId(), is(entityDto.getOid()));
        assertThat(actionInfo.getReconfigure().getTarget().getType(), is(entityDto.getEntityType()));
        assertThat(actionInfo.getReconfigure().getTarget().getEnvironmentType(), is(EnvironmentType.CLOUD));
    }

    @Test
    public void testInterpretReconfigureActionWithoutSource() throws IOException, InvalidTopologyException {
        TopologyDTO.TopologyEntityDTO entityDto =
                        TopologyConverterFromMarketTest.messageFromJsonFile("protobuf/messages/vm-1.dto.json");
        final Map<Long, ProjectedTopologyEntity> projectedTopology =
            ImmutableMap.of(
                entityDto.getOid(), entity(entityDto.getOid(), entityDto.getEntityType(), EnvironmentType.CLOUD));
        final TopologyConverter converter =
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, true,
                AnalysisUtil.QUOTE_FACTOR, AnalysisUtil.LIVE_MARKET_MOVE_COST_FACTOR,
                marketPriceTable, ccd, CommodityIndex.newFactory());

        final Set<TraderTO> traderTOs =
                converter.convertToMarket(ImmutableMap.of(entityDto.getOid(), entityDto));
        final TraderTO vmTraderTO = TopologyConverterToMarketTest.getVmTrader(traderTOs);
        ShoppingListTO shoppingList = vmTraderTO.getShoppingListsList().get(0);

        ActionInfo actionInfo = converter.interpretAction(
            ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                .setReconfigure(ReconfigureTO.newBuilder()
                    .setShoppingListToReconfigure(shoppingList.getOid()))
                .build(), projectedTopology, null, null, null).get().getInfo();

        assertThat(actionInfo.getActionTypeCase(), is(ActionTypeCase.RECONFIGURE));
        assertThat(actionInfo.getReconfigure().getTarget().getId(), is(entityDto.getOid()));
        assertThat(actionInfo.getReconfigure().getTarget().getType(), is(entityDto.getEntityType()));
        assertThat(actionInfo.getReconfigure().getTarget().getEnvironmentType(), is(EnvironmentType.CLOUD));
        assertFalse(actionInfo.getReconfigure().hasSource());
    }

    @Test
    public void testInterpretProvisionBySupplyAction() throws Exception {
        long modelSeller = 1234;
        int modelType = 1;
        final Map<Long, ProjectedTopologyEntity> projectedTopology =
            ImmutableMap.of(modelSeller, entity(modelSeller, modelType, EnvironmentType.CLOUD));
        final TopologyConverter converter =
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, true,
                AnalysisUtil.QUOTE_FACTOR, AnalysisUtil.LIVE_MARKET_MOVE_COST_FACTOR,
                marketPriceTable, ccd, CommodityIndex.newFactory());

        ActionInfo actionInfo = converter.interpretAction(
                ActionTO.newBuilder()
                    .setImportance(0.)
                    .setIsNotExecutable(false)
                    .setProvisionBySupply(ProvisionBySupplyTO.newBuilder()
                        .setProvisionedSeller(-1)
                        .setModelSeller(modelSeller))
                    .build(), projectedTopology, null, null, null).get().getInfo();

        assertThat(actionInfo.getActionTypeCase(), is(ActionTypeCase.PROVISION));
        assertThat(actionInfo.getProvision().getProvisionedSeller(), is(-1L));
        assertThat(actionInfo.getProvision().getEntityToClone().getId(), is(modelSeller));
        assertThat(actionInfo.getProvision().getEntityToClone().getType(), is(modelType));
        assertThat(actionInfo.getProvision().getEntityToClone().getEnvironmentType(), is(EnvironmentType.CLOUD));
    }

    @Test
    public void testInterpretResizeAction() throws Exception {
        long entityToResize = 1;
        int  entityType = 1;
        final Map<Long, ProjectedTopologyEntity> projectedTopology =
            ImmutableMap.of(entityToResize, entity(entityToResize, entityType, EnvironmentType.ON_PREM));
        final CommodityConverter commConverter = mock(CommodityConverter.class);
        final TopologyConverter converter = Mockito.spy(
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, true,
                AnalysisUtil.QUOTE_FACTOR, AnalysisUtil.LIVE_MARKET_MOVE_COST_FACTOR,
                marketPriceTable, commConverter, CommodityIndex.newFactory()));
        Mockito.doReturn(Optional.of(topologyCommodity1))
               .when(commConverter).economyToTopologyCommodity(eq(economyCommodity1));

        final float oldCapacity = 10;
        final float newCapacity = 9;
        final ActionTO resizeAction = ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                .setResize(ResizeTO.newBuilder()
                    .setSellingTrader(entityToResize)
                    .setNewCapacity(newCapacity)
                    .setOldCapacity(oldCapacity)
                    .setSpecification(economyCommodity1))
                .build();
        final ActionInfo actionInfo =
                converter.interpretAction(resizeAction, projectedTopology, null, null, null).get().getInfo();

        assertThat(actionInfo.getActionTypeCase(), is(ActionTypeCase.RESIZE));
        assertThat(actionInfo.getResize().getTarget().getId(), is(entityToResize));
        assertThat(actionInfo.getResize().getTarget().getType(), is(entityType));
        assertThat(actionInfo.getResize().getTarget().getEnvironmentType(), is(EnvironmentType.ON_PREM));
        assertThat(actionInfo.getResize().getCommodityType(), is(topologyCommodity1));
        assertThat(actionInfo.getResize().getOldCapacity(), is(oldCapacity));
        assertThat(actionInfo.getResize().getNewCapacity(), is(newCapacity));
    }

    @Test
    public void testInterpretActivateAction() throws Exception {
        long entityToActivate = 1;
        int  entityType = 1;
        final Map<Long, ProjectedTopologyEntity> projectedTopology =
            ImmutableMap.of(entityToActivate, entity(entityToActivate, entityType, EnvironmentType.ON_PREM));
        final CommodityConverter commConverter = mock(CommodityConverter.class);
        final TopologyConverter converter = Mockito.spy(
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, true,
                AnalysisUtil.QUOTE_FACTOR, AnalysisUtil.LIVE_MARKET_MOVE_COST_FACTOR,
                marketPriceTable, commConverter, CommodityIndex.newFactory()));
        // Insert the commodity type into the converter's mapping
        final CommodityType expectedCommodityType = CommodityType.newBuilder()
            .setType(12)
            .setKey("Foo")
            .build();

        final int marketCommodityId = converter.getCommodityConverter().
                toMarketCommodityId(expectedCommodityType);
        final  CommodityDTOs.CommoditySpecificationTO economyCommodity =
            CommodityDTOs.CommoditySpecificationTO.newBuilder()
                .setType(marketCommodityId)
                .setBaseType(expectedCommodityType.getType())
                .build();

        Mockito.doReturn(Optional.of(topologyCommodity1))
               .when(commConverter).economyToTopologyCommodity(eq(economyCommodity));
        Mockito.doReturn(Optional.of(topologyCommodity2))
                .when(commConverter).economyToTopologyCommodity(eq(economyCommodity2));

        final ActionTO activateAction = ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                .setActivate(ActivateTO.newBuilder()
                    .setTraderToActivate(entityToActivate)
                    .setModelSeller(2)
                    .setMostExpensiveCommodity(economyCommodity.getBaseType())
                    .addTriggeringBasket(economyCommodity)
                    .addTriggeringBasket(economyCommodity2))
                .build();
        final Action action = converter.interpretAction(activateAction, projectedTopology, null, null, null).get();
        final ActionInfo actionInfo = action.getInfo();

        assertThat(actionInfo.getActionTypeCase(), is(ActionTypeCase.ACTIVATE));
        assertThat(actionInfo.getActivate().getTarget().getId(), is(entityToActivate));
        assertThat(actionInfo.getActivate().getTarget().getType(), is(entityType));
        assertThat(actionInfo.getActivate().getTarget().getEnvironmentType(), is(EnvironmentType.ON_PREM));
        assertThat(actionInfo.getActivate().getTriggeringCommoditiesList(),
                IsIterableContainingInAnyOrder.containsInAnyOrder(topologyCommodity1,
                        topologyCommodity2));
        assertThat(action.getExplanation().getActivate().getMostExpensiveCommodity(),
            is(expectedCommodityType.getType()));
    }

    @Test
    public void testInterpretDeactivateAction() throws Exception {
        long entityToDeactivate = 1;
        int  entityType = 1;
        final Map<Long, ProjectedTopologyEntity> projectedTopology =
            ImmutableMap.of(entityToDeactivate, entity(entityToDeactivate, entityType, EnvironmentType.ON_PREM));
        final CommodityConverter commConverter = mock(CommodityConverter.class);
        final TopologyConverter converter = Mockito.spy(
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, true,
                AnalysisUtil.QUOTE_FACTOR, AnalysisUtil.LIVE_MARKET_MOVE_COST_FACTOR,
                marketPriceTable, commConverter, CommodityIndex.newFactory()));
        Mockito.doReturn(Optional.of(topologyCommodity1))
                .when(commConverter).economyToTopologyCommodity(eq(economyCommodity1));
        Mockito.doReturn(Optional.of(topologyCommodity2))
                .when(commConverter).economyToTopologyCommodity(eq(economyCommodity2));

        final ActionTO deactivateAction = ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                .setDeactivate(DeactivateTO.newBuilder()
                        .setTraderToDeactivate(entityToDeactivate)
                        .addTriggeringBasket(economyCommodity1)
                        .addTriggeringBasket(economyCommodity2)
                        .build())
                .build();
        final ActionInfo actionInfo =
                converter.interpretAction(deactivateAction, projectedTopology, null, null, null).get().getInfo();

        assertThat(actionInfo.getActionTypeCase(), is(ActionTypeCase.DEACTIVATE));
        assertThat(actionInfo.getDeactivate().getTarget().getId(), is(entityToDeactivate));
        assertThat(actionInfo.getDeactivate().getTarget().getType(), is(entityType));
        assertThat(actionInfo.getDeactivate().getTarget().getEnvironmentType(), is(EnvironmentType.ON_PREM));
        assertThat(actionInfo.getDeactivate().getTriggeringCommoditiesList(),
                IsIterableContainingInAnyOrder.containsInAnyOrder(topologyCommodity1,
                        topologyCommodity2));
    }

    @Test
    public void testInterpretMoveAction_Cloud() throws IOException {
        interpretMoveToCheaperTemplateActionForCloud(false, true);
    }

    @Test
    public void testInterpretCompoundMoveAction_Cloud() throws IOException {
        interpretMoveToCheaperTemplateActionForCloud(true, true);
    }

    @Test
    public void testInterpretMoveAction_Cloud_No_AZ() throws IOException {
        interpretMoveToCheaperTemplateActionForCloud(false, false);
    }

    @Test
    public void testInterpretCompoundMoveAction_Cloud_No_AZ() throws IOException {
        interpretMoveToCheaperTemplateActionForCloud(true, false);
    }

    private void interpretMoveToCheaperTemplateActionForCloud(boolean isVmShopTogether, boolean hasAZ)
            throws IOException {
        Set<TopologyEntityDTO.Builder> topologyEntityDTOBuilders = hasAZ ?
            TopologyEntitiesHandlerTest.readCloudTopologyFromJsonFile() :
            TopologyEntitiesHandlerTest.readCloudTopologyFromJsonFile(
                SIMPLE_CLOUD_TOPOLOGY_NO_AZ_JSON_FILE);
        TopologyEntityDTO.Builder vmBuilder = topologyEntityDTOBuilders.stream().filter(
                builder -> builder.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                .collect(Collectors.toList()).get(0);
        // Set the shopTogether flag
        vmBuilder.getAnalysisSettingsBuilder().setShopTogether(isVmShopTogether);
        Set<TopologyEntityDTO> topologyEntityDTOs = topologyEntityDTOBuilders.stream()
                .map(builder -> builder.build()).collect(Collectors.toSet());
        Map<Long, TopologyEntityDTO> originalTopology = topologyEntityDTOs.stream()
                .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));
        CloudTopologyConverter mockCloudTc = mock(CloudTopologyConverter.class);
        // Get handle to the templates, region and BA TopologyEntityDTO
        TopologyEntityDTO m1Medium = null;
        TopologyEntityDTO m1Large = null;
        TopologyEntityDTO region = null;
        TopologyEntityDTO vm = null;
        for (TopologyEntityDTO topologyEntityDTO : topologyEntityDTOs) {
            if (topologyEntityDTO.getDisplayName().contains("m1.large")
                    && topologyEntityDTO.getEntityType() == EntityType.COMPUTE_TIER_VALUE) {
                m1Large = topologyEntityDTO;
            } else if (topologyEntityDTO.getDisplayName().contains("m1.medium")
                    && topologyEntityDTO.getEntityType() == EntityType.COMPUTE_TIER_VALUE) {
                m1Medium = topologyEntityDTO;
            } else if (topologyEntityDTO.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                vm = topologyEntityDTO;
            } else if (topologyEntityDTO.getEntityType() == EntityType.REGION_VALUE) {
                region = topologyEntityDTO;
            }
        }

        MarketTier sourceMarketTier = new OnDemandMarketTier(m1Large, region);
        MarketTier destMarketTier = new OnDemandMarketTier(m1Medium, region);
        when(mockCloudTc.getMarketTier(1)).thenReturn(sourceMarketTier);
        when(mockCloudTc.getMarketTier(2)).thenReturn(destMarketTier);
        when(mockCloudTc.isMarketTier(1l)).thenReturn(true);
        when(mockCloudTc.isMarketTier(2l)).thenReturn(true);

        ShoppingListInfo slInfo = new ShoppingListInfo(5, vm.getOid(), null, null, null, Arrays.asList());
        Map<Long, ShoppingListInfo> slInfoMap = ImmutableMap.of(5l, slInfo);
        TopologyCostCalculator mockTopologyCostCalculator = mock(TopologyCostCalculator.class);
        CostJournal<TopologyEntityDTO> sourceCostJournal = mock(CostJournal.class);
        // Source compute cost = 10 + 2 +3 = 15 (compute + ip + license)
        when(sourceCostJournal.getHourlyCostForCategory(CostCategory.ON_DEMAND_COMPUTE)).thenReturn(10d);
        when(sourceCostJournal.getHourlyCostForCategory(CostCategory.IP)).thenReturn(2d);
        when(sourceCostJournal.getHourlyCostForCategory(CostCategory.LICENSE)).thenReturn(3d);
        // Total Source cost = 20
        when(sourceCostJournal.getTotalHourlyCostExcluding(anySet())).thenReturn(20d);
        when(mockTopologyCostCalculator.calculateCostForEntity(any(), eq(vm))).thenReturn(Optional.of(sourceCostJournal));

        Map<Long, CostJournal<TopologyEntityDTO>> projectedCosts = new HashMap<>();
        CostJournal<TopologyEntityDTO> projectedCostJournal = mock(CostJournal.class);
        // Destination compute cost = 9 + 1 + 2 = 12
        when(projectedCostJournal.getHourlyCostForCategory(CostCategory.ON_DEMAND_COMPUTE)).thenReturn(9d);
        when(projectedCostJournal.getHourlyCostForCategory(CostCategory.IP)).thenReturn(1d);
        when(projectedCostJournal.getHourlyCostForCategory(CostCategory.LICENSE)).thenReturn(2d);
        // Total destination cost = 15
        when(projectedCostJournal.getTotalHourlyCostExcluding(anySet())).thenReturn(15d);
        projectedCosts.put(vm.getOid(), projectedCostJournal);
        ActionInterpreter interpreter = new ActionInterpreter(mock(CommodityConverter.class),
                slInfoMap, mockCloudTc, originalTopology, ImmutableMap.of(),
                new CloudEntityResizeTracker(), Maps.newHashMap());
        ActionTO actionTO = null;
        // Assuming that 1 is the oid of trader created for m1.large x region and 2 is the oid
        // created for m1.medium x region
        if (isVmShopTogether) {
            actionTO = ActionTO.newBuilder().setImportance(0).setIsNotExecutable(false)
                    .setCompoundMove(
                            CompoundMoveTO.newBuilder().addMoves(
                                MoveTO.newBuilder()
                            .setShoppingListToMove(5)
                            .setSource(1)
                            .setDestination(2)
                            .setMoveExplanation(MoveExplanation.getDefaultInstance()).build())
                                    .build()).build();
        } else {
            actionTO = ActionTO.newBuilder().setImportance(0).setIsNotExecutable(false)
                    .setMove(MoveTO.newBuilder()
                            .setShoppingListToMove(5)
                            .setSource(1)
                            .setDestination(2)
                            .setMoveExplanation(MoveExplanation.getDefaultInstance())).build();
        }
        // We don't create projected topology entries for the "fake" traders (1 and 2), since
        // those are completely internal to the market.
        Map<Long, ProjectedTopologyEntity> projectedTopology = ImmutableMap.of(
                vm.getOid(), entity(vm),
                m1Large.getOid(), entity(m1Large),
                m1Medium.getOid(), entity(m1Medium));
        Optional<Action> action = interpreter.interpretAction(actionTO, projectedTopology,
                mock(TopologyEntityCloudTopology.class), projectedCosts, mockTopologyCostCalculator);
        if (isVmShopTogether) {
            assertEquals(5, action.get().getSavingsPerHour().getAmount(), 0.0001);
        } else {
            assertEquals(3, action.get().getSavingsPerHour().getAmount(), 0.0001);
        }
        assertThat(action.get().getInfo().getMove().getChanges(0).getSource().getId(), is(m1Large.getOid()));
        assertThat(action.get().getInfo().getMove().getChanges(0).getSource().getType(), is(m1Large.getEntityType()));
        assertThat(action.get().getInfo().getMove().getChanges(0).getSource().getEnvironmentType(), is(m1Large.getEnvironmentType()));

        assertThat(action.get().getInfo().getMove().getChanges(0).getDestination().getId(), is(m1Medium.getOid()));
        assertThat(action.get().getInfo().getMove().getChanges(0).getDestination().getType(), is(m1Medium.getEntityType()));
        assertThat(action.get().getInfo().getMove().getChanges(0).getDestination().getEnvironmentType(), is(m1Medium.getEnvironmentType()));

        assertThat(action.get().getInfo().getMove().getTarget().getId(), is(vm.getOid()));
        assertThat(action.get().getInfo().getMove().getTarget().getType(), is(vm.getEntityType()));
        assertThat(action.get().getInfo().getMove().getTarget().getEnvironmentType(), is(vm.getEnvironmentType()));
    }
}
