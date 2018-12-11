package com.vmturbo.market.topology.conversions;

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
import java.util.Collections;
import java.util.HashMap;
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

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.analysis.InvalidTopologyException;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.calculation.CostJournal;
import com.vmturbo.cost.calculation.DiscountApplicator;
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

    private static final Map<Long, Integer> entityIdToEntityTypeMap =
        Collections.emptyMap();

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
        when(ccd.getAllRiBought()).thenReturn(new ArrayList());
    }

    @Test
    public void testCommodityIdsAreInvertible() {
        final TopologyConverter converter =
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, true, 0.75f, marketPriceTable, ccd);

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
        final Map<Long, Integer> entityIdToTypeMap =
            ImmutableMap.of(modelSeller, modelType);
        final TopologyConverter converter =
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, true, 0.75f, marketPriceTable, ccd);

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

        assertTrue(converter.interpretAction(executableActionTO, entityIdToTypeMap, null, null, null).get().getExecutable());
        assertFalse(converter.interpretAction(notExecutableActionTO, entityIdToTypeMap, null, null, null).get().getExecutable());
    }

    @Test
    public void testInterpretMoveAction() throws IOException {
        long srcId = 1234;
        long destId = 5678;
        int srcType = 0;
        int destType = 1;
        int resourceType = 2;
        TopologyDTO.TopologyEntityDTO entityDto =
                TopologyConverterFromMarketTest.messageFromJsonFile("protobuf/messages/vm-1.dto.json");
        long resourceId = entityDto.getCommoditiesBoughtFromProviders(0).getVolumeId();

        Map<Long, Integer> entityIdTypeMap =
            ImmutableMap.of(srcId, srcType,
                            destId, destType,
                            entityDto.getOid(), entityDto.getEntityType(),
                            resourceId, resourceType);

        final TopologyConverter converter =
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, true, 0.75f, marketPriceTable, ccd);

        final Set<TraderTO> traderTOs =
            converter.convertToMarket(ImmutableMap.of(entityDto.getOid(), entityDto));
        final TraderTO vmTraderTO = TopologyConverterToMarketTest.getVmTrader(traderTOs);
        ShoppingListTO shoppingList = vmTraderTO.getShoppingListsList().get(0);

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
                .build(), entityIdTypeMap, null, null, null).get().getInfo();
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
                        .build(), entityIdTypeMap, null, null, null)
                .get().getInfo();

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
    }

    @Test
    public void testInterpretReconfigureAction() throws IOException, InvalidTopologyException {
        long reconfigureSourceId = 1234;
        int reconfigureSourceType = 1;

        TopologyDTO.TopologyEntityDTO entityDto =
                        TopologyConverterFromMarketTest.messageFromJsonFile("protobuf/messages/vm-1.dto.json");
        final Map<Long, Integer> entityIdToTypeMap =
            ImmutableMap.of(
                reconfigureSourceId, reconfigureSourceType,
                entityDto.getOid(), entityDto.getEntityType());
        final TopologyConverter converter =
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, true, 0.75f, marketPriceTable, ccd);

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
                .build(), entityIdToTypeMap, null, null, null).get().getInfo();

        assertEquals(ActionTypeCase.RECONFIGURE, actionInfo.getActionTypeCase());
        assertEquals(reconfigureSourceId, actionInfo.getReconfigure().getSource().getId());
    }

    @Test
    public void testInterpretReconfigureActionWithoutSource() throws IOException, InvalidTopologyException {
        TopologyDTO.TopologyEntityDTO entityDto =
                        TopologyConverterFromMarketTest.messageFromJsonFile("protobuf/messages/vm-1.dto.json");
        final Map<Long, Integer> entityIdToTypeMap =
            ImmutableMap.of(
                entityDto.getOid(), entityDto.getEntityType());
        final TopologyConverter converter =
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, true, 0.75f, marketPriceTable, ccd);

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
                .build(), entityIdToTypeMap, null, null, null).get().getInfo();

        assertEquals(ActionTypeCase.RECONFIGURE, actionInfo.getActionTypeCase());
        assertFalse(actionInfo.getReconfigure().hasSource());
    }

    @Test
    public void testInterpretProvisionBySupplyAction() throws Exception {
        long modelSeller = 1234;
        int modelType = 1;
        final Map<Long, Integer> entityIdToTypeMap =
            ImmutableMap.of(modelSeller, modelType);
        final TopologyConverter converter =
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, true, 0.75f, marketPriceTable, ccd);

        ActionInfo actionInfo = converter.interpretAction(
                ActionTO.newBuilder()
                    .setImportance(0.)
                    .setIsNotExecutable(false)
                    .setProvisionBySupply(ProvisionBySupplyTO.newBuilder()
                        .setProvisionedSeller(-1)
                        .setModelSeller(modelSeller))
                    .build(), entityIdToTypeMap, null, null, null).get().getInfo();

        assertEquals(ActionTypeCase.PROVISION, actionInfo.getActionTypeCase());
        assertEquals(-1, actionInfo.getProvision().getProvisionedSeller());
        assertEquals(modelSeller, actionInfo.getProvision().getEntityToClone().getId());
    }

    @Test
    public void testInterpretResizeAction() throws Exception {
        long entityToResize = 1;
        int  entityType = 1;
        final Map<Long, Integer> entityIdToTypeMap =
            ImmutableMap.of(entityToResize, entityType);
        final CommodityConverter commConverter = mock(CommodityConverter.class);
        final TopologyConverter converter = Mockito.spy(
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, true, 0.75f, marketPriceTable, commConverter));
        Mockito.doReturn(Optional.of(topologyCommodity1))
               .when(commConverter).economyToTopologyCommodity(eq(economyCommodity1));

        final long oldCapacity = 10;
        final long newCapacity = 9;
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
                converter.interpretAction(resizeAction, entityIdToTypeMap, null, null, null).get().getInfo();

        assertEquals(ActionTypeCase.RESIZE, actionInfo.getActionTypeCase());
        assertEquals(entityToResize, actionInfo.getResize().getTarget().getId());
        assertEquals(topologyCommodity1, actionInfo.getResize().getCommodityType());
        assertEquals(oldCapacity, actionInfo.getResize().getOldCapacity(), 0);
        assertEquals(newCapacity, actionInfo.getResize().getNewCapacity(), 0);
    }

    @Test
    public void testInterpretActivateAction() throws Exception {
        long entityToActivate = 1;
        int  entityType = 1;
        final Map<Long, Integer> entityIdToTypeMap =
            ImmutableMap.of(entityToActivate, entityType);
        final CommodityConverter commConverter = mock(CommodityConverter.class);
        final TopologyConverter converter = Mockito.spy(
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, true, 0.75f, marketPriceTable, commConverter));
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
        final Action action = converter.interpretAction(activateAction, entityIdToTypeMap, null, null, null).get();
        final ActionInfo actionInfo = action.getInfo();

        assertEquals(ActionTypeCase.ACTIVATE, actionInfo.getActionTypeCase());
        assertEquals(entityToActivate, actionInfo.getActivate().getTarget().getId());
        assertThat(actionInfo.getActivate().getTriggeringCommoditiesList(),
                IsIterableContainingInAnyOrder.containsInAnyOrder(topologyCommodity1,
                        topologyCommodity2));
        assertEquals(expectedCommodityType.getType(),
            action.getExplanation().getActivate().getMostExpensiveCommodity());
    }

    @Test
    public void testInterpretDeactivateAction() throws Exception {
        long entityToDeactivate = 1;
        int  entityType = 1;
        final Map<Long, Integer> entityIdToTypeMap =
            ImmutableMap.of(entityToDeactivate, entityType);
        final CommodityConverter commConverter = mock(CommodityConverter.class);
        final TopologyConverter converter = Mockito.spy(
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, true, 0.75f, marketPriceTable, commConverter));
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
                converter.interpretAction(deactivateAction, entityIdToTypeMap, null, null, null).get().getInfo();

        assertEquals(ActionTypeCase.DEACTIVATE, actionInfo.getActionTypeCase());
        assertEquals(entityToDeactivate, actionInfo.getDeactivate().getTarget().getId());
        assertThat(actionInfo.getDeactivate().getTriggeringCommoditiesList(),
                IsIterableContainingInAnyOrder.containsInAnyOrder(topologyCommodity1,
                        topologyCommodity2));
    }

    @Test
    public void testInterpretMoveAction_Cloud() throws IOException {
        interpretMoveToCheaperTemplateActionForCloud(false);
    }

    @Test
    public void testInterpretCompoundMoveAction_Cloud() throws IOException {
        interpretMoveToCheaperTemplateActionForCloud(true);
    }


    private void interpretMoveToCheaperTemplateActionForCloud(boolean isVmShopTogether) throws IOException {
        Set<TopologyEntityDTO.Builder> topologyEntityDTOBuilders = TopologyEntitiesHandlerTest
                .readCloudTopologyFromJsonFile();
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
                slInfoMap, mockCloudTc, originalTopology, ImmutableMap.of());
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
        Map<Long, Integer> entityIdToType = ImmutableMap.of(
                vm.getOid(), EntityType.VIRTUAL_MACHINE_VALUE,
                1l, EntityType.COMPUTE_TIER_VALUE,
                2l, EntityType.COMPUTE_TIER_VALUE);
        Optional<Action> action = interpreter.interpretAction(actionTO, entityIdToType,
                mock(TopologyEntityCloudTopology.class), projectedCosts, mockTopologyCostCalculator);
        if (isVmShopTogether) {
            assertEquals(5, action.get().getSavingsPerHour().getAmount(), 0.0001);
        } else {
            assertEquals(3, action.get().getSavingsPerHour().getAmount(), 0.0001);
        }
        assertEquals(m1Large.getOid(), action.get().getInfo().getMove().getChanges(0).getSource().getId());
        assertEquals(m1Medium.getOid(), action.get().getInfo().getMove().getChanges(0).getDestination().getId());
        assertEquals(vm.getOid(), action.get().getInfo().getMove().getTarget().getId());
    }
}
