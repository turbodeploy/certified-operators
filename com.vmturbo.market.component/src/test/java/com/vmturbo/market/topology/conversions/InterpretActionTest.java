package com.vmturbo.market.topology.conversions;

import static com.vmturbo.trax.Trax.trax;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;

import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity.Suffix;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ScaleExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.components.common.utils.CommodityTypeAllocatorConstants;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopology;
import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.market.runner.FakeEntityCreator;
import com.vmturbo.market.runner.MarketMode;
import com.vmturbo.market.topology.MarketTier;
import com.vmturbo.market.topology.OnDemandMarketTier;
import com.vmturbo.market.topology.TopologyConversionConstants;
import com.vmturbo.market.topology.TopologyConverterUtil;
import com.vmturbo.market.topology.TopologyEntitiesHandlerTest;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper.ConsistentScalingHelperFactory;
import com.vmturbo.market.topology.conversions.TierExcluder.TierExcluderFactory;
import com.vmturbo.market.topology.conversions.cloud.CloudActionSavingsCalculator;
import com.vmturbo.market.topology.conversions.cloud.CloudActionSavingsCalculator.CalculatedSavings;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.CompoundMoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.Congestion;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.DeactivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.InitialPlacement;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveExplanation;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO.CommodityContext;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.Performance;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionBySupplyTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ReconfigureConsumerTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ReconfigureTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTriggerTraderTO;
import com.vmturbo.platform.analysis.protobuf.BalanceAccountDTOs.BalanceAccountDTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.Context;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderStateTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.test.utils.FeatureFlagTestRule;
import com.vmturbo.trax.Trax;

/**
 * Test various actions.
 */
@RunWith(MockitoJUnitRunner.class)
public class InterpretActionTest {

    private static final TopologyInfo REALTIME_TOPOLOGY_INFO = TopologyInfo.newBuilder()
            .setTopologyType(TopologyType.REALTIME)
            .build();

    // No AZ in this json file. Entities are connected to Region.
    private static final String SIMPLE_CLOUD_TOPOLOGY_NO_AZ_JSON_FILE =
        "protobuf/messages/simple-cloudTopology-no-AZ.json";

    private CommodityDTOs.CommoditySpecificationTO economyCommodity1;
    private CommodityType topologyCommodity1;
    private CommodityDTOs.CommoditySpecificationTO economyCommodity2;
    private CommodityType topologyCommodity2;

    private CloudRateExtractor marketCloudRateExtractor = mock(CloudRateExtractor.class);
    private CloudCostData ccd = mock(CloudCostData.class);
    private TierExcluderFactory tierExcluderFactory = mock(TierExcluderFactory.class);
    private ConsistentScalingHelperFactory consistentScalingHelperFactory =
            mock(ConsistentScalingHelperFactory.class);
    private ReversibilitySettingFetcher reversibilitySettingFetcher =
            mock(ReversibilitySettingFetcher.class);
    private CloudActionSavingsCalculator actionSavingsCalculator =
            mock(CloudActionSavingsCalculator.class);
    private FakeEntityCreator fakeEntityCreator = mock(FakeEntityCreator.class);

    /**
     * Rule to manage feature flag enablement.
     */
    @Rule
    public FeatureFlagTestRule mergedPeakFeatureFlag =
            new FeatureFlagTestRule(FeatureFlags.ENABLE_MERGED_PEAK_UPDATE_FUNCTION);

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
        TierExcluder tierExcluder = mock(TierExcluder.class);
        when(tierExcluder.getReasonSettings(any())).thenReturn(Optional.of(Collections.EMPTY_SET));
        when(tierExcluderFactory.newExcluder(any(), any(), any())).thenReturn(tierExcluder);
        ConsistentScalingHelper consistentScalingHelper = mock(ConsistentScalingHelper.class);
        when(consistentScalingHelper.getScalingGroupId(any())).thenReturn(Optional.empty());
        when(consistentScalingHelper.getScalingGroupUsage(any())).thenReturn(Optional.empty());
        when(consistentScalingHelperFactory.newConsistentScalingHelper(any(), any()))
            .thenReturn(consistentScalingHelper);

        when(actionSavingsCalculator.calculateSavings(any())).thenReturn(CalculatedSavings.NO_SAVINGS_USD);
        when(fakeEntityCreator.isFakeComputeClusterOid(anyLong())).thenReturn(false);
    }

    @Test
    public void testCommodityIdsAreInvertible() {
        final TopologyConverter converter =
                new TopologyConverterUtil.Builder()
                        .topologyInfo(REALTIME_TOPOLOGY_INFO)
                        .includeGuaranteedBuyer(true)
                        .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                        .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                        .marketCloudRateExtractor(marketCloudRateExtractor)
                        .cloudCostData(ccd)
                        .commodityIndexFactory(CommodityIndex.newFactory())
                        .tierExcluderFactory(tierExcluderFactory)
                        .consistentScalingHelperFactory(consistentScalingHelperFactory)
                        .reversibilitySettingFetcher(reversibilitySettingFetcher)
                        .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                        .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                        .fakeEntityCreator(Mockito.mock(FakeEntityCreator.class))
                        .useVMReservationAsUsed(true)
                        .singleVMonHost(false)
                        .customUtilizationThreshold(0.5f)
                        .build();
        converter.setConvertToMarketComplete();

        final CommodityType segmentationFoo = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.SEGMENTATION_VALUE)
            .setKey("foo")
            .build();
        final CommodityType segmentationBar = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.SEGMENTATION_VALUE)
            .setKey("bar")
            .build();

        int segmentationFooId = converter.getCommodityConverter()
                .commoditySpecification(segmentationFoo).getType();
        int segmentationBarId = converter.getCommodityConverter()
                .commoditySpecification(segmentationBar).getType();
        final CommodityType cpu = CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.SEGMENTATION_VALUE)
                .build();
        final CommodityType cpuRed = CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.SEGMENTATION_VALUE)
                .setKey("red")
                .build();
        final int cpuId = converter.getCommodityConverter().commoditySpecification(cpu).getType();
        final int cpuRedId = converter.getCommodityConverter().commoditySpecification(cpuRed).getType();
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
                new TopologyConverterUtil.Builder()
                        .topologyInfo(REALTIME_TOPOLOGY_INFO)
                        .includeGuaranteedBuyer(true)
                        .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                        .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                        .marketCloudRateExtractor(marketCloudRateExtractor)
                        .cloudCostData(ccd)
                        .commodityIndexFactory(CommodityIndex.newFactory())
                        .tierExcluderFactory(tierExcluderFactory)
                        .consistentScalingHelperFactory(consistentScalingHelperFactory)
                        .reversibilitySettingFetcher(reversibilitySettingFetcher)
                        .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                        .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                        .fakeEntityCreator(Mockito.mock(FakeEntityCreator.class))
                        .useVMReservationAsUsed(true)
                        .singleVMonHost(false)
                        .customUtilizationThreshold(0.5f)
                        .build();
        converter.setConvertToMarketComplete();
        CommodityDTOs.CommoditySpecificationTO cs = converter.getCommodityConverter().commoditySpecification(CommodityType.newBuilder()
                .setKey("Seg")
                .setType(11 + CommodityTypeAllocatorConstants.ACCESS_COMM_TYPE_START_COUNT).build());
        final ActionTO executableActionTO = ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                // Using ProvisionBySupply because it requires the least setup, and all
                // we really care about is the executable flag.
                .setProvisionBySupply(ProvisionBySupplyTO.newBuilder()
                        .setProvisionedSeller(-1)
                        .setModelSeller(modelSeller)
                        .setMostExpensiveCommodity(CommodityDTOs.CommoditySpecificationTO.newBuilder()
                                .setType(0 + CommodityTypeAllocatorConstants.ACCESS_COMM_TYPE_START_COUNT).setBaseType(cs.getBaseType()).build())
                        .build())
                .build();

        final ActionTO notExecutableActionTO = ActionTO.newBuilder(executableActionTO)
                .setIsNotExecutable(true)
                .build();

        assertTrue(converter.interpretAction(executableActionTO, projectedTopology, null, actionSavingsCalculator).get(0).getExecutable());
        assertFalse(converter.interpretAction(notExecutableActionTO, projectedTopology, null, actionSavingsCalculator).get(0).getExecutable());
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
        long resourceId = entityDto.getConnectedEntityList(0).getConnectedEntityId();

        final TopologyConverter converter =
                new TopologyConverterUtil.Builder()
                        .topologyInfo(REALTIME_TOPOLOGY_INFO)
                        .includeGuaranteedBuyer(true)
                        .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                        .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                        .marketCloudRateExtractor(marketCloudRateExtractor)
                        .cloudCostData(ccd)
                        .commodityIndexFactory(CommodityIndex.newFactory())
                        .tierExcluderFactory(tierExcluderFactory)
                        .consistentScalingHelperFactory(consistentScalingHelperFactory)
                        .reversibilitySettingFetcher(reversibilitySettingFetcher)
                        .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                        .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                        .fakeEntityCreator(Mockito.mock(FakeEntityCreator.class))
                        .useVMReservationAsUsed(true)
                        .singleVMonHost(false)
                        .customUtilizationThreshold(0.5f)
                        .build();
        converter.setConvertToMarketComplete();
        converter.setUseVMReservationAsUsed(true);

        final Collection<TraderTO> traderTOs =
            converter.convertToMarket(ImmutableMap.of(entityDto.getOid(), entityDto));
        final TraderTO vmTraderTO = TopologyConverterToMarketTest.getVmTrader(traderTOs);
        // We sort the shopping list based on provider entity type, and then based on volume id.
        // So the index of volume shopping list will be 2 last since its provider id type is the greatest.
        ShoppingListTO shoppingList = vmTraderTO.getShoppingListsList().get(2);
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
                .build(), projectedTopology, null, actionSavingsCalculator).get(0).getInfo();
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
                        .build(), projectedTopology, null, actionSavingsCalculator)
                .get(0).getInfo();
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
            projectedTopology, null, actionSavingsCalculator).get(0);
        List<ChangeProviderExplanation> explanations =
            actionWithMaintenanceSource.getExplanation().getMove().getChangeProviderExplanationList();
        ActionInfo actionInfoWithMaintenanceSource = actionWithMaintenanceSource.getInfo();

        assertEquals(ActionTypeCase.MOVE, actionInfo.getActionTypeCase());
        assertEquals(1, actionInfo.getMove().getChangesList().size());
        assertEquals(srcId, actionInfo.getMove().getChanges(0).getSource().getId());
        assertEquals(srcType, actionInfo.getMove().getChanges(0).getSource().getType());
        assertEquals(destId, actionInfo.getMove().getChanges(0).getDestination().getId());
        assertEquals(destType, actionInfo.getMove().getChanges(0).getDestination().getType());

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

        Action configVolumeAction = converter.interpretAction(
            ActionTO.newBuilder()
                .setImportance(0.1)
                .setIsNotExecutable(false)
                .setMove(MoveTO.newBuilder()
                    .setShoppingListToMove(shoppingList1.getOid())
                    .setDestination(destId)
                    .setMoveExplanation(MoveExplanation.getDefaultInstance())).build(),
            projectedTopology, null, actionSavingsCalculator).get(0);

    }

    /**
     * Config volume move won't be interpreted.
     *
     * @throws IOException In case of file loading error.
     */
    @Test
    public void testInterpretConfigVolumeMoveAction() throws IOException {
        TopologyDTO.TopologyEntityDTO vm =
            TopologyConverterFromMarketTest.messageFromJsonFile("protobuf/messages/vm-1.dto.json");
        TopologyDTO.TopologyEntityDTO source =
            TopologyConverterFromMarketTest.messageFromJsonFile("protobuf/messages/ds-1.dto.json");
        TopologyDTO.TopologyEntityDTO destination =
            TopologyConverterFromMarketTest.messageFromJsonFile("protobuf/messages/ds-2.dto.json");
        TopologyDTO.TopologyEntityDTO configVolume =
            TopologyConverterFromMarketTest.messageFromJsonFile("protobuf/messages/volume-2.dto.json");
        TopologyDTO.TopologyEntityDTO regularVolume =
            TopologyConverterFromMarketTest.messageFromJsonFile("protobuf/messages/volume-3.dto.json");

        final long shoppingListId1 = 5L;
        final ShoppingListInfo slInfo1 = new ShoppingListInfo(shoppingListId1, vm.getOid(), source.getOid(),
            Collections.emptySet(), configVolume.getOid(), source.getEntityType(), Arrays.asList());
        final long shoppingListId2 = 6L;
        final ShoppingListInfo slInfo2 = new ShoppingListInfo(shoppingListId2, vm.getOid(), source.getOid(),
            Collections.emptySet(), regularVolume.getOid(), source.getEntityType(), Arrays.asList());
        final Map<Long, ShoppingListInfo> slInfoMap = ImmutableMap.of(
            shoppingListId1, slInfo1, shoppingListId2, slInfo2);

        final Map<Long, TopologyEntityDTO> originalTopology = ImmutableMap.of(
            vm.getOid(), vm, source.getOid(), source, destination.getOid(), destination,
            configVolume.getOid(), configVolume, regularVolume.getOid(), regularVolume);

        final Map<Long, ProjectedTopologyEntity> projectedTopology = ImmutableMap.of(
            vm.getOid(), entity(vm),
            source.getOid(), entity(source),
            destination.getOid(), entity(destination),
            configVolume.getOid(), entity(configVolume),
            regularVolume.getOid(), entity(regularVolume));

        final CommodityConverter mockCommodityConverter = mock(CommodityConverter.class);
        CloudTopologyConverter mockCloudTc = mock(CloudTopologyConverter.class);

        final ActionInterpreter interpreter = new ActionInterpreter(mockCommodityConverter,
            slInfoMap, mockCloudTc, originalTopology, ImmutableMap.of(),
            new CommoditiesResizeTracker(), mock(ProjectedRICoverageCalculator.class), mock(TierExcluder.class),
            CommodityIndex.newFactory()::newIndex, null, new HashMap<>(), Mockito.mock(
                FakeEntityCreator.class));

        final TopologyEntityCloudTopology originalCloudTopology =
            new TopologyEntityCloudTopologyFactory
                .DefaultTopologyEntityCloudTopologyFactory(mock(GroupMemberRetriever.class))
                .newCloudTopology(originalTopology.values().stream());

        final MoveTO configVolumeMove = MoveTO.newBuilder()
            .setShoppingListToMove(shoppingListId1)
            .setSource(source.getOid())
            .setDestination(destination.getOid())
            .setMoveExplanation(MoveExplanation.getDefaultInstance())
            .build();

        final Optional<Move> moveOptional = interpreter.interpretMoveAction(
            configVolumeMove, projectedTopology, originalCloudTopology);
        assertFalse(moveOptional.isPresent());

        // When Analysis produces a compound move for a config volume, we should not be
        // creating a move action for it.
        final CompoundMoveTO compoundMoveForConfigVolume = CompoundMoveTO.newBuilder()
                .addMoves(configVolumeMove).build();
        final Optional<Move> compMoveOptional = interpreter.interpretCompoundMoveAction(
                compoundMoveForConfigVolume, projectedTopology, originalCloudTopology);
        assertFalse(compMoveOptional.isPresent());

        final MoveTO regularVolumeMove = MoveTO.newBuilder()
            .setShoppingListToMove(shoppingListId2)
            .setSource(source.getOid())
            .setDestination(destination.getOid())
            .setMoveExplanation(MoveExplanation.getDefaultInstance())
            .build();

        final Optional<Move> moveOptional1 = interpreter.interpretMoveAction(
            regularVolumeMove, projectedTopology, originalCloudTopology);
        assertTrue(moveOptional1.isPresent());
        final Move move = moveOptional1.get();
        assertThat(move.getTarget().getId(), is(vm.getOid()));
        assertThat(move.getChangesCount(), is(1));
        assertThat(move.getChanges(0).getSource().getId(), is(source.getOid()));
        assertThat(move.getChanges(0).getDestination().getId(), is(destination.getOid()));
        assertThat(move.getChanges(0).getResourceCount(), is(1));
        assertThat(move.getChanges(0).getResource(0).getId(), is(regularVolume.getOid()));
    }

    /**
     * Test congestion with time slots.
     *
     * @throws Exception Any unexpected exception
     */
    @Test
    public void testInterpretMoveActionWithTimeSlotsAndSuffix() throws Exception {
        final TopologyDTO.TopologyEntityDTO businessUser =
            TopologyConverterFromMarketTest.messageFromJsonFile("protobuf/messages/topology-with-timeslots.json");

        final long shoppingListId = 5L;
        final ShoppingListInfo slInfo = new ShoppingListInfo(shoppingListId, businessUser.getOid(), 666L,
            Collections.emptySet(), null, null, Arrays.asList());
        final Map<Long, ShoppingListInfo> slInfoMap = ImmutableMap.of(shoppingListId, slInfo);
        final Map<Long, TopologyEntityDTO> originalTopology = ImmutableMap.of(businessUser.getOid(),
            businessUser);
        final int congestedCommodityMarketId = 50;
        final int slot = 1;
        // this comes from the imported topology file
        final int totalSlotNumber = 3;
        final CommodityType congestedCommodityType = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.POOL_CPU_VALUE)
            .build();
        final CommodityConverter mockCommodityConverter = mock(CommodityConverter.class);
        when(mockCommodityConverter.commodityIdToCommodityTypeAndSlot(eq(congestedCommodityMarketId)))
            .thenReturn((new Pair(congestedCommodityType, Optional.of(slot))));
        CloudTopologyConverter mockCloudTc = mock(CloudTopologyConverter.class);

        //This map is used to generate reservation suffix inside reasonCommodity
        CommodityBoughtDTO poolCpuCommBought = businessUser.getCommoditiesBoughtFromProviders(0)
                .getCommodityBoughtList().stream().filter(comm -> comm.getCommodityType().getType() == CommodityDTO.CommodityType.POOL_CPU_VALUE).findFirst().get();
        Map<Long, Map<CommodityBoughtDTO, Long>> reservationGreaterThanUsedMap = Collections.singletonMap(businessUser.getOid(),
                Collections.singletonMap(poolCpuCommBought, 73364362512107L));

        final ActionInterpreter interpreter = new ActionInterpreter(mockCommodityConverter,
            slInfoMap, mockCloudTc, originalTopology, ImmutableMap.of(),
            new CommoditiesResizeTracker(), mock(ProjectedRICoverageCalculator.class), mock(TierExcluder.class),
            CommodityIndex.newFactory()::newIndex, null, reservationGreaterThanUsedMap, Mockito.mock(
                FakeEntityCreator.class));

        final long moveSrcId = businessUser.getCommoditiesBoughtFromProvidersList().get(0)
            .getProviderId();

        final Map<Long, ProjectedTopologyEntity> projectedTopology = ImmutableMap.of(
            businessUser.getOid(), entity(businessUser),
            moveSrcId, entity(businessUser));

        final TopologyEntityCloudTopology originalCloudTopology =
            new TopologyEntityCloudTopologyFactory
                .DefaultTopologyEntityCloudTopologyFactory(mock(GroupMemberRetriever.class))
                .newCloudTopology(originalTopology.values().stream());

        final ActionTO actionTO = ActionTO.newBuilder().setImportance(0).setIsNotExecutable(false)
            .setMove(MoveTO.newBuilder()
                .setShoppingListToMove(shoppingListId)
                .setSource(moveSrcId)
                .setDestination(businessUser.getOid())
                .setMoveExplanation(MoveExplanation.newBuilder()
                    .setCongestion(Congestion.newBuilder()
                        .addCongestedCommodities(congestedCommodityMarketId))
                    .build())
                .build())
            .build();

        final List<Action> actions = interpreter.interpretAction(actionTO, projectedTopology,
            originalCloudTopology, actionSavingsCalculator);

        assertTrue(!actions.isEmpty());
        final Action action = actions.get(0);
        assertTrue(action.hasExplanation());
        assertTrue(action.getExplanation().hasMove());
        assertEquals(1, action.getExplanation().getMove().getChangeProviderExplanationCount());
        final ChangeProviderExplanation changeProviderExplanation =
            action.getExplanation().getMove().getChangeProviderExplanationList().get(0);
        assertTrue(changeProviderExplanation.hasCongestion());
        assertEquals(1, changeProviderExplanation.getCongestion().getCongestedCommoditiesCount());
        ReasonCommodity reasonCommodity = changeProviderExplanation.getCongestion()
            .getCongestedCommoditiesList().get(0);
        assertEquals(congestedCommodityType, reasonCommodity.getCommodityType());
        assertTrue(reasonCommodity.hasTimeSlot());
        assertEquals(slot, reasonCommodity.getTimeSlot().getSlot());
        assertEquals(totalSlotNumber, reasonCommodity.getTimeSlot().getTotalSlotNumber());
        assertTrue(reasonCommodity.hasSuffix());
        assertEquals(Suffix.RESERVATION, reasonCommodity.getSuffix());
    }

    /**
     * Test interpreting scaling cloud volume action.
     *
     * @throws IOException when passing json file.
     */
    @Test
    public void testInterpretScaleAction() throws IOException {
        final Map<Long, TopologyEntityDTO> topologyDTOs = Stream.of(
                TopologyConverterFromMarketTest.messageFromJsonFile("protobuf/messages/cloud-volume.json"),
                TopologyConverterFromMarketTest.messageFromJsonFile("protobuf/messages/cloud-vm.json"),
                TopologyConverterFromMarketTest.messageFromJsonFile("protobuf/messages/cloud-storageTier.json"),
                TopologyConverterFromMarketTest.messageFromJsonFile("protobuf/messages/cloud-storageTier-dest.json"))
                .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));
        TopologyEntityDTO region = TopologyEntityDTO.newBuilder().setOid(73442089143124L).setEntityType(EntityType.REGION_VALUE).build();
        topologyDTOs.put(region.getOid(), region);
        TopologyEntityDTO businessAccount = TopologyEntityDTO.newBuilder().setOid(15678904L).setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE).build();
        topologyDTOs.put(businessAccount.getOid(), businessAccount);
        AccountPricingData accountPricingData = mock(AccountPricingData.class);
        TopologyEntityDTO serviceProvider = TopologyEntityDTO.newBuilder().setOid(5678974832L).setEntityType(EntityType.SERVICE_PROVIDER_VALUE).build();
        when(ccd.getAccountPricingData(businessAccount.getOid())).thenReturn(Optional.of(accountPricingData));
        CloudTopology cloudTopology = mock(CloudTopology.class);
        when(cloudTopology.getServiceProvider(businessAccount.getOid())).thenReturn(Optional.of(serviceProvider));
        when(cloudTopology.getRegionsFromServiceProvider(serviceProvider.getOid())).thenReturn(new HashSet(Collections.singleton(region)));
        when(cloudTopology.getAggregated(region.getOid(), TopologyConversionConstants.cloudTierTypes)).thenReturn(topologyDTOs.values().stream()
                .filter(s -> s.getEntityType() == EntityType.STORAGE_TIER_VALUE).collect(Collectors.toSet()));

        final TopologyConverter topologyConverter = new TopologyConverterUtil.Builder()
                .topologyInfo(REALTIME_TOPOLOGY_INFO)
                .includeGuaranteedBuyer(false)
                .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                .marketMode(MarketMode.M2Only)
                .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                .cloudCostData(ccd)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(reversibilitySettingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .cloudTopology(cloudTopology)
                .enableOP(false)
                .useVMReservationAsUsed(false)
                .singleVMonHost(false)
                .customUtilizationThreshold(0.5f)
                .build();
        topologyConverter.setConvertToMarketComplete();
        Collection<TraderTO> traderTOs = topologyConverter.convertToMarket(topologyDTOs);
        final TraderTO vmTraderTO = TopologyConverterToMarketTest.getVmTrader(traderTOs);
        assertNotNull(vmTraderTO);
        // Get the shoppingList within the VM trader which represents cloud volume.
        ShoppingListTO volumeSL = vmTraderTO.getShoppingListsCount() > 0 ? vmTraderTO.getShoppingLists(0) : null;
        assertNotNull(volumeSL);
        TraderTO sourceStorageTierTraderTO = traderTOs.stream()
                .filter(tto -> tto.getType() == EntityType.STORAGE_TIER_VALUE
                        && tto.getDebugInfoNeverUseInCode().contains("GP2"))
                .findFirst()
                .orElse(null);
        assertNotNull(sourceStorageTierTraderTO);
        TraderTO destStorageTierTraderTO = traderTOs.stream()
                .filter(tto -> tto.getType() == EntityType.STORAGE_TIER_VALUE
                        && tto.getDebugInfoNeverUseInCode().contains("STANDARD"))
                .findFirst()
                .orElse(null);
        assertNotNull(destStorageTierTraderTO);
        CloudTopology<TopologyEntityDTO> originalCloudTopology = Mockito.mock(CloudTopology.class);
        Mockito.when(originalCloudTopology.getConnectedRegion(org.mockito.Matchers.anyLong())).thenReturn(Optional.of(region));
        Map<Long, ProjectedTopologyEntity> projectedTopology = ImmutableMap.of(
                73442089143120L, entity(73442089143120L, EntityType.VIRTUAL_MACHINE_VALUE, EnvironmentType.CLOUD),
                73442089143125L, entity(73442089143125L, EntityType.VIRTUAL_VOLUME_VALUE, EnvironmentType.CLOUD),
                73363299852962L, entity(73363299852962L, EntityType.STORAGE_TIER_VALUE, EnvironmentType.CLOUD),
                73363299852963L, entity(73363299852963L, EntityType.STORAGE_TIER_VALUE, EnvironmentType.CLOUD));
        ActionTO actionTO = ActionTO.newBuilder()
                .setImportance(0.1)
                .setIsNotExecutable(false)
                .setMove(MoveTO.newBuilder()
                        .setShoppingListToMove(volumeSL.getOid())
                        .setMoveContext(Context.newBuilder().setRegionId(region.getOid()).build())
                        .addCommodityContext(CommodityContext.newBuilder()
                                .setOldCapacity(500)
                                .setNewCapacity(600).build())
                        .setSource(sourceStorageTierTraderTO.getOid())
                        .setDestination(destStorageTierTraderTO.getOid())
                        .setMoveExplanation(MoveExplanation.newBuilder()
                                .setPerformance(Performance.getDefaultInstance())))
                .build();
        final List<Action> actionList = topologyConverter.interpretAction(actionTO, projectedTopology,
                originalCloudTopology, actionSavingsCalculator);
        assertEquals(1, actionList.size());
        Action action = actionList.get(0);
        assertNotNull(action);
        assertTrue(action.getExplanation().getScale() instanceof ScaleExplanation);
        Scale scale = action.getInfo().getScale();
        assertNotNull(scale);
        assertEquals("Scale action target entity should be volume", 73442089143125L, scale.getTarget().getId());
        assertTrue(scale.getChangesCount() == 1);
        ChangeProvider changeProvider = scale.getChanges(0);
        assertEquals("Scale action source is source StorageTier", 73363299852962L, changeProvider.getSource().getId());
        assertEquals("Scale action destination is destination StorageTier",
                73363299852963L, changeProvider.getDestination().getId());
        assertTrue(scale.getCommodityResizesCount() == 1);
        ResizeInfo resizeInfo = scale.getCommodityResizes(0);
        assertEquals("Scale action ResizeInfo old capacity is from MoveTO CommodityContext",
                500, resizeInfo.getOldCapacity(), 0f);
        assertEquals("Scale action ResizeInfo new capacity is from MoveTO CommodityContext",
                600, resizeInfo.getNewCapacity(), 0f);
    }

    /**
     * Test compliance action generation due to segment congestion.
     *
     * @throws Exception Any unexpected exception
     */
    @Test
    public void testInterpretMoveActionDueToSegmentCongestion() throws Exception {
        final TopologyDTO.TopologyEntityDTO entityDto =
                TopologyConverterFromMarketTest.messageFromJsonFile("protobuf/messages/vm-1.dto.json");

        final long shoppingListId = 5L;
        final ShoppingListInfo slInfo = new ShoppingListInfo(shoppingListId, entityDto.getOid(), null,
                        Collections.emptySet(), null, null, Arrays.asList());
        final Map<Long, ShoppingListInfo> slInfoMap = ImmutableMap.of(shoppingListId, slInfo);
        final Map<Long, TopologyEntityDTO> originalTopology = ImmutableMap.of(entityDto.getOid(),
                entityDto);
        final int congestedCommodityMarketId = 50;
        final int slot = 1;

        final CommodityType congestedCommodityType = CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.SEGMENTATION_VALUE)
                .setKey("abc")
                .build();
        final CommodityConverter mockCommodityConverter = mock(CommodityConverter.class);
        when(mockCommodityConverter.commodityIdToCommodityTypeAndSlot(eq(congestedCommodityMarketId)))
                .thenReturn((new Pair(congestedCommodityType, Optional.of(slot))));
        CloudTopologyConverter mockCloudTc = mock(CloudTopologyConverter.class);

        final ActionInterpreter interpreter = new ActionInterpreter(mockCommodityConverter,
                slInfoMap, mockCloudTc, originalTopology, ImmutableMap.of(),
                new CommoditiesResizeTracker(), mock(ProjectedRICoverageCalculator.class), mock(TierExcluder.class),
                CommodityIndex.newFactory()::newIndex, null, new HashMap<>(), Mockito.mock(
                FakeEntityCreator.class));

        final long moveSrcId = entityDto.getCommoditiesBoughtFromProvidersList().get(0)
                .getProviderId();

        final Map<Long, ProjectedTopologyEntity> projectedTopology = ImmutableMap.of(
                entityDto.getOid(), entity(entityDto),
                moveSrcId, entity(entityDto));

        final ActionTO actionTO = ActionTO.newBuilder().setImportance(0).setIsNotExecutable(false)
                .setMove(MoveTO.newBuilder()
                        .setShoppingListToMove(shoppingListId)
                        .setSource(moveSrcId)
                        .setDestination(entityDto.getOid())
                        .setMoveExplanation(MoveExplanation.newBuilder()
                                .setCongestion(Congestion.newBuilder()
                                        .addCongestedCommodities(congestedCommodityMarketId))
                                .build())
                        .build())
                .build();

        final List<Action> actionList = interpreter.interpretAction(actionTO, projectedTopology,
                null, actionSavingsCalculator);

        assertTrue(actionList != null);
        assertEquals(actionList.size(), 1);
        final Action action = actionList.get(0);
        assertTrue(action.hasExplanation());
        assertTrue(action.getExplanation().hasMove());
        assertEquals(1, action.getExplanation().getMove().getChangeProviderExplanationCount());
        final ChangeProviderExplanation changeProviderExplanation =
                action.getExplanation().getMove().getChangeProviderExplanationList().get(0);
        assertTrue(changeProviderExplanation.hasCompliance());
    }

    @Test
    public void testInterpretReconfigureAction() throws IOException {
        long reconfigureSourceId = 1234;
        int reconfigureSourceType = 1;

        TopologyDTO.TopologyEntityDTO entityDto =
                        TopologyConverterFromMarketTest.messageFromJsonFile("protobuf/messages/vm-1.dto.json");
        final Map<Long, ProjectedTopologyEntity> projectedTopology =
            ImmutableMap.of(
                reconfigureSourceId, entity(reconfigureSourceId, reconfigureSourceType, EnvironmentType.ON_PREM),
                entityDto.getOid(), entity(entityDto.getOid(), entityDto.getEntityType(), EnvironmentType.CLOUD));
        final TopologyConverter converter =
                new TopologyConverterUtil.Builder()
                        .topologyInfo(REALTIME_TOPOLOGY_INFO)
                        .includeGuaranteedBuyer(true)
                        .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                        .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                        .marketCloudRateExtractor(marketCloudRateExtractor)
                        .cloudCostData(ccd)
                        .commodityIndexFactory(CommodityIndex.newFactory())
                        .tierExcluderFactory(tierExcluderFactory)
                        .consistentScalingHelperFactory(consistentScalingHelperFactory)
                        .reversibilitySettingFetcher(reversibilitySettingFetcher)
                        .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                        .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                        .fakeEntityCreator(Mockito.mock(FakeEntityCreator.class))
                        .useVMReservationAsUsed(true)
                        .singleVMonHost(false)
                        .customUtilizationThreshold(0.5f)
                        .build();
        converter.setConvertToMarketComplete();

        final Collection<TraderTO> traderTOs =
                converter.convertToMarket(ImmutableMap.of(entityDto.getOid(), entityDto));
        final TraderTO vmTraderTO = TopologyConverterToMarketTest.getVmTrader(traderTOs);
        ShoppingListTO shoppingList = vmTraderTO.getShoppingListsList().get(0);

        ActionInfo actionInfo = converter.interpretAction(
            ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                .setReconfigure(ReconfigureTO.newBuilder().setConsumer(ReconfigureConsumerTO.newBuilder()
                    .setShoppingListToReconfigure(shoppingList.getOid())
                    .setSource(reconfigureSourceId).build()))
                .build(), projectedTopology, null, actionSavingsCalculator).get(0).getInfo();

        assertThat(actionInfo.getActionTypeCase(), is(ActionTypeCase.RECONFIGURE));
        assertThat(actionInfo.getReconfigure().getSource().getId(), is(reconfigureSourceId));
        assertThat(actionInfo.getReconfigure().getSource().getType(), is(reconfigureSourceType));
        assertThat(actionInfo.getReconfigure().getSource().getEnvironmentType(), is(EnvironmentType.ON_PREM));
        assertThat(actionInfo.getReconfigure().getTarget().getId(), is(entityDto.getOid()));
        assertThat(actionInfo.getReconfigure().getTarget().getType(), is(entityDto.getEntityType()));
        assertThat(actionInfo.getReconfigure().getTarget().getEnvironmentType(), is(EnvironmentType.CLOUD));
    }

    @Test
    public void testInterpretReconfigureActionWithoutSource() throws IOException {
        TopologyDTO.TopologyEntityDTO entityDto =
                        TopologyConverterFromMarketTest.messageFromJsonFile("protobuf/messages/vm-1.dto.json");
        final Map<Long, ProjectedTopologyEntity> projectedTopology =
            ImmutableMap.of(
                entityDto.getOid(), entity(entityDto.getOid(), entityDto.getEntityType(), EnvironmentType.CLOUD));
        final TopologyConverter converter =
                new TopologyConverterUtil.Builder()
                        .topologyInfo(REALTIME_TOPOLOGY_INFO)
                        .includeGuaranteedBuyer(true)
                        .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                        .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                        .marketCloudRateExtractor(marketCloudRateExtractor)
                        .cloudCostData(ccd)
                        .commodityIndexFactory(CommodityIndex.newFactory())
                        .tierExcluderFactory(tierExcluderFactory)
                        .consistentScalingHelperFactory(consistentScalingHelperFactory)
                        .reversibilitySettingFetcher(reversibilitySettingFetcher)
                        .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                        .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                        .fakeEntityCreator(Mockito.mock(FakeEntityCreator.class))
                        .useVMReservationAsUsed(true)
                        .singleVMonHost(false)
                        .customUtilizationThreshold(0.5f)
                        .build();
        converter.setConvertToMarketComplete();

        final Collection<TraderTO> traderTOs =
                converter.convertToMarket(ImmutableMap.of(entityDto.getOid(), entityDto));
        final TraderTO vmTraderTO = TopologyConverterToMarketTest.getVmTrader(traderTOs);
        ShoppingListTO shoppingList = vmTraderTO.getShoppingListsList().get(0);

        ActionInfo actionInfo = converter.interpretAction(
            ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                .setReconfigure(ReconfigureTO.newBuilder().setConsumer(ReconfigureConsumerTO.newBuilder()
                    .setShoppingListToReconfigure(shoppingList.getOid())).build())
                .build(), projectedTopology, null, actionSavingsCalculator).get(0).getInfo();

        assertThat(actionInfo.getActionTypeCase(), is(ActionTypeCase.RECONFIGURE));
        assertThat(actionInfo.getReconfigure().getTarget().getId(), is(entityDto.getOid()));
        assertThat(actionInfo.getReconfigure().getTarget().getType(), is(entityDto.getEntityType()));
        assertThat(actionInfo.getReconfigure().getTarget().getEnvironmentType(), is(EnvironmentType.CLOUD));
        assertFalse(actionInfo.getReconfigure().hasSource());
    }

    @Test
    public void testInterpretProvisionBySupplyAction() {
        long modelSeller = 1234;
        int modelType = 1;
        final Map<Long, ProjectedTopologyEntity> projectedTopology =
            ImmutableMap.of(modelSeller, entity(modelSeller, modelType, EnvironmentType.CLOUD));
        final TopologyConverter converter =
                new TopologyConverterUtil.Builder()
                        .topologyInfo(REALTIME_TOPOLOGY_INFO)
                        .includeGuaranteedBuyer(true)
                        .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                        .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                        .marketCloudRateExtractor(marketCloudRateExtractor)
                        .cloudCostData(ccd)
                        .commodityIndexFactory(CommodityIndex.newFactory())
                        .tierExcluderFactory(tierExcluderFactory)
                        .consistentScalingHelperFactory(consistentScalingHelperFactory)
                        .reversibilitySettingFetcher(reversibilitySettingFetcher)
                        .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                        .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                        .fakeEntityCreator(Mockito.mock(FakeEntityCreator.class))
                        .useVMReservationAsUsed(true)
                        .singleVMonHost(false)
                        .customUtilizationThreshold(0.5f)
                        .build();
        converter.setConvertToMarketComplete();
        CommodityDTOs.CommoditySpecificationTO cs = converter.getCommodityConverter()
                .commoditySpecification(CommodityType.newBuilder()
                    .setKey("Seg")
                    .setType(11 + CommodityTypeAllocatorConstants.ACCESS_COMM_TYPE_START_COUNT).build());
        ActionInfo actionInfo = converter.interpretAction(
                ActionTO.newBuilder()
                    .setImportance(0.)
                    .setIsNotExecutable(false)
                    .setProvisionBySupply(ProvisionBySupplyTO.newBuilder()
                        .setProvisionedSeller(-1)
                        .setModelSeller(modelSeller)
                        .setMostExpensiveCommodity(CommodityDTOs.CommoditySpecificationTO.newBuilder()
                                .setType(0 + CommodityTypeAllocatorConstants.ACCESS_COMM_TYPE_START_COUNT).setBaseType(cs.getBaseType()).build()))
                    .build(), projectedTopology, null, actionSavingsCalculator).get(0).getInfo();

        assertThat(actionInfo.getActionTypeCase(), is(ActionTypeCase.PROVISION));
        assertThat(actionInfo.getProvision().getProvisionedSeller(), is(-1L));
        assertThat(actionInfo.getProvision().getEntityToClone().getId(), is(modelSeller));
        assertThat(actionInfo.getProvision().getEntityToClone().getType(), is(modelType));
        assertThat(actionInfo.getProvision().getEntityToClone().getEnvironmentType(), is(EnvironmentType.CLOUD));
    }

    @Test
    public void testInterpretResizeAction() {
        long entityToResize = 1;
        int  entityType = 1;
        final Map<Long, ProjectedTopologyEntity> projectedTopology =
            ImmutableMap.of(entityToResize, entity(entityToResize, entityType, EnvironmentType.ON_PREM));
        final CommodityConverter commConverter = mock(CommodityConverter.class);
        final TopologyConverter topologyConverter = new TopologyConverterUtil.Builder()
                .topologyInfo(REALTIME_TOPOLOGY_INFO)
                .includeGuaranteedBuyer(true)
                .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                .marketMode(MarketMode.M2Only)
                .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                .commodityConverter(commConverter)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(reversibilitySettingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .enableOP(false)
                .useVMReservationAsUsed(false)
                .singleVMonHost(false)
                .customUtilizationThreshold(0.5f)
                .fakeEntityCreator(fakeEntityCreator)
                .build();

        topologyConverter.setConvertToMarketComplete();
        final TopologyConverter converter = spy(topologyConverter);
        Mockito.doReturn(Optional.of(topologyCommodity1))
               .when(commConverter).marketToTopologyCommodity(eq(economyCommodity1));

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
                converter.interpretAction(resizeAction, projectedTopology, null, actionSavingsCalculator).get(0).getInfo();

        assertThat(actionInfo.getActionTypeCase(), is(ActionTypeCase.RESIZE));
        assertThat(actionInfo.getResize().getTarget().getId(), is(entityToResize));
        assertThat(actionInfo.getResize().getTarget().getType(), is(entityType));
        assertThat(actionInfo.getResize().getTarget().getEnvironmentType(), is(EnvironmentType.ON_PREM));
        assertThat(actionInfo.getResize().getCommodityType(), is(topologyCommodity1));
        assertThat(actionInfo.getResize().getOldCapacity(), is(oldCapacity));
        assertThat(actionInfo.getResize().getNewCapacity(), is(newCapacity));
    }

    @Test
    public void testInterpretResizeActionRelatedCommMatching() {
        long entityToResize = 1;
        int  entityType = 1;
        long resizeTriggerTrader = 2;
        int  entityType2 = 2;
        final Map<Long, ProjectedTopologyEntity> projectedTopology =
            ImmutableMap.of(entityToResize, entity(entityToResize, entityType, EnvironmentType.ON_PREM),
                resizeTriggerTrader, entity(resizeTriggerTrader, entityType2, EnvironmentType.ON_PREM));
        final CommodityConverter commConverter = mock(CommodityConverter.class);
        final TopologyConverter topologyConverter = new TopologyConverterUtil.Builder()
                .topologyInfo(REALTIME_TOPOLOGY_INFO)
                .includeGuaranteedBuyer(true)
                .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                .marketMode(MarketMode.M2Only)
                .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                .commodityConverter(commConverter)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(reversibilitySettingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .enableOP(false)
                .useVMReservationAsUsed(false)
                .singleVMonHost(false)
                .customUtilizationThreshold(0.5f)
                .fakeEntityCreator(fakeEntityCreator)
                .build();
        topologyConverter.setConvertToMarketComplete();
        final TopologyConverter converter = spy(topologyConverter);
        Mockito.doReturn(Optional.of(topologyCommodity1))
               .when(commConverter).marketToTopologyCommodity(eq(economyCommodity1));

        final float oldCapacity = 9;
        final float newCapacity = 10;
        final ActionTO resizeAction = ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                .setResize(ResizeTO.newBuilder()
                    .setSellingTrader(entityToResize)
                    .setNewCapacity(newCapacity)
                    .setOldCapacity(oldCapacity)
                    .setSpecification(economyCommodity1)
                    .addResizeTriggerTrader(ResizeTriggerTraderTO.newBuilder()
                            .setTrader(resizeTriggerTrader)
                            .addRelatedCommodities(economyCommodity1.getBaseType())))
                .build();
        final ActionInfo actionInfo =
                converter.interpretAction(resizeAction, projectedTopology, null, actionSavingsCalculator).get(0).getInfo();

        assertThat(actionInfo.getActionTypeCase(), is(ActionTypeCase.RESIZE));
        assertThat(actionInfo.getResize().getTarget().getId(), is(entityToResize));
        assertThat(actionInfo.getResize().getTarget().getType(), is(entityType));
        assertThat(actionInfo.getResize().getTarget().getEnvironmentType(), is(EnvironmentType.ON_PREM));
        assertThat(actionInfo.getResize().getCommodityType(), is(topologyCommodity1));
        assertThat(actionInfo.getResize().getOldCapacity(), is(oldCapacity));
        assertThat(actionInfo.getResize().getNewCapacity(), is(newCapacity));
    }

    @Test
    public void testInterpretResizeActionRelatedCommNotMatching() {
        long entityToResize = 1;
        int  entityType = 1;
        long resizeTriggerTrader = 2;
        int  entityType2 = 2;
        final Map<Long, ProjectedTopologyEntity> projectedTopology =
            ImmutableMap.of(entityToResize, entity(entityToResize, entityType, EnvironmentType.ON_PREM),
                resizeTriggerTrader, entity(resizeTriggerTrader, entityType2, EnvironmentType.ON_PREM));
        final CommodityConverter commConverter = mock(CommodityConverter.class);
        final TopologyConverter topologyConverter = new TopologyConverterUtil.Builder()
                .topologyInfo(REALTIME_TOPOLOGY_INFO)
                .includeGuaranteedBuyer(true)
                .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                .marketMode(MarketMode.M2Only)
                .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                .commodityConverter(commConverter)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(reversibilitySettingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .enableOP(false)
                .useVMReservationAsUsed(false)
                .singleVMonHost(false)
                .customUtilizationThreshold(0.5f)
                .build();
        topologyConverter.setConvertToMarketComplete();
        final TopologyConverter converter = spy(topologyConverter);
        Mockito.doReturn(Optional.of(topologyCommodity1))
               .when(commConverter).marketToTopologyCommodity(eq(economyCommodity1));

        final float oldCapacity = 9;
        final float newCapacity = 10;
        final ActionTO resizeAction = ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                .setResize(ResizeTO.newBuilder()
                    .setSellingTrader(entityToResize)
                    .setNewCapacity(newCapacity)
                    .setOldCapacity(oldCapacity)
                    .setSpecification(economyCommodity1)
                    .addResizeTriggerTrader(ResizeTriggerTraderTO.newBuilder()
                            .setTrader(resizeTriggerTrader)
                            .addRelatedCommodities(economyCommodity2.getBaseType())))
                .build();
        final List<Action> actions =
                converter.interpretAction(resizeAction, projectedTopology, null, actionSavingsCalculator);

        assertTrue(actions.isEmpty());
    }

    @Test
    public void testInterpretActivateAction() {
        long entityToActivate = 1;
        int  entityType = 1;
        final Map<Long, ProjectedTopologyEntity> projectedTopology =
            ImmutableMap.of(entityToActivate, entity(entityToActivate, entityType, EnvironmentType.ON_PREM));
        final CommodityConverter commConverter = mock(CommodityConverter.class);

        final TopologyConverter topologyConverter = new TopologyConverterUtil.Builder()
                .topologyInfo(REALTIME_TOPOLOGY_INFO)
                .includeGuaranteedBuyer(true)
                .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                .marketMode(MarketMode.M2Only)
                .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                .commodityConverter(commConverter)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(reversibilitySettingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .enableOP(false)
                .useVMReservationAsUsed(false)
                .singleVMonHost(false)
                .customUtilizationThreshold(0.5f)
                .build();
        topologyConverter.setConvertToMarketComplete();
        final TopologyConverter converter = spy(topologyConverter);
        // Insert the commodity type into the converter's mapping
        final CommodityType expectedCommodityType = CommodityType.newBuilder()
            .setType(12)
            .setKey("Foo")
            .build();

        final CommoditySpecificationTO spec = CommoditySpecificationTO.newBuilder()
                .setBaseType(12)
                .setType(10)
                .build();
        Mockito.doReturn(spec)
                .when(commConverter).commoditySpecification(eq(expectedCommodityType));


        final int marketCommodityId = converter.getCommodityConverter().
                commoditySpecification(expectedCommodityType).getType();
        final  CommodityDTOs.CommoditySpecificationTO economyCommodity =
            CommodityDTOs.CommoditySpecificationTO.newBuilder()
                .setType(marketCommodityId)
                .setBaseType(expectedCommodityType.getType())
                .build();

        Mockito.doReturn(Optional.of(topologyCommodity1))
               .when(commConverter).marketToTopologyCommodity(eq(economyCommodity));
        Mockito.doReturn(Optional.of(topologyCommodity2))
                .when(commConverter).marketToTopologyCommodity(eq(economyCommodity2));


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
        final Action action = converter.interpretAction(activateAction, projectedTopology, null, actionSavingsCalculator).get(0);
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
    public void testInterpretDeactivateAction()  throws IOException {
        long entityToDeactivate = 1;
        int  entityType = 1;
        final Map<Long, ProjectedTopologyEntity> projectedTopology =
            ImmutableMap.of(entityToDeactivate, entity(entityToDeactivate, entityType, EnvironmentType.ON_PREM));
        final CommodityConverter commConverter = mock(CommodityConverter.class);
        final TopologyConverter topologyConverter = new TopologyConverterUtil.Builder()
                .topologyInfo(REALTIME_TOPOLOGY_INFO)
                .includeGuaranteedBuyer(true)
                .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                .marketMode(MarketMode.M2Only)
                .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                .commodityConverter(commConverter)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(reversibilitySettingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .enableOP(false)
                .useVMReservationAsUsed(false)
                .singleVMonHost(false)
                .customUtilizationThreshold(0.5f)
                .fakeEntityCreator(fakeEntityCreator)
                .build();
        topologyConverter.setConvertToMarketComplete();
        final TopologyConverter converter = spy(topologyConverter);
        Mockito.doReturn(Optional.of(topologyCommodity1))
                .when(commConverter).marketToTopologyCommodity(eq(economyCommodity1));
        Mockito.doReturn(Optional.of(topologyCommodity2))
                .when(commConverter).marketToTopologyCommodity(eq(economyCommodity2));

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
                converter.interpretAction(deactivateAction, projectedTopology, null, actionSavingsCalculator).get(0).getInfo();

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
        interpretMoveToCheaperTemplateActionForCloud(true);
    }

    @Test
    public void testInterpretMoveAction_Cloud_No_AZ() throws IOException {
        interpretMoveToCheaperTemplateActionForCloud(false);
    }

    private void interpretMoveToCheaperTemplateActionForCloud(boolean hasAZ)
            throws IOException {
        Set<TopologyEntityDTO.Builder> topologyEntityDTOBuilders = hasAZ ?
            TopologyEntitiesHandlerTest.readCloudTopologyFromJsonFile() :
            TopologyEntitiesHandlerTest.readCloudTopologyFromJsonFile(
                SIMPLE_CLOUD_TOPOLOGY_NO_AZ_JSON_FILE);
        TopologyEntityDTO.Builder vmBuilder = topologyEntityDTOBuilders.stream().filter(
                builder -> builder.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                .collect(Collectors.toList()).get(0);
        // Set the shopTogether flag
        vmBuilder.getAnalysisSettingsBuilder().setShopTogether(false);
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

        EntityReservedInstanceCoverage ricovergae = EntityReservedInstanceCoverage
                .newBuilder().putCouponsCoveredByRi(123L, 4).setEntityCouponCapacity(6d).build();
        MarketTier sourceMarketTier = new OnDemandMarketTier(m1Large);
        MarketTier destMarketTier = new OnDemandMarketTier(m1Medium);
        when(mockCloudTc.getMarketTier(1)).thenReturn(sourceMarketTier);
        when(mockCloudTc.getMarketTier(2)).thenReturn(destMarketTier);
        when(mockCloudTc.isMarketTier(1l)).thenReturn(true);
        when(mockCloudTc.isMarketTier(2l)).thenReturn(true);
        when(mockCloudTc.getSourceOrDestinationTierFromMoveTo(any(), eq(vm.getOid()), eq(true))).thenReturn(Optional.of(m1Large.getOid()));
        when(mockCloudTc.getSourceOrDestinationTierFromMoveTo(any(), eq(vm.getOid()), eq(false))).thenReturn(Optional.of(m1Medium.getOid()));
        when(mockCloudTc.getRiCoverageForEntity(anyLong())).thenReturn(Optional.of(ricovergae));

        ShoppingListInfo slInfo = new ShoppingListInfo(5, vm.getOid(), null, Collections.emptySet(),
                        null, null, Arrays.asList());
        Map<Long, ShoppingListInfo> slInfoMap = ImmutableMap.of(5l, slInfo);
        CommodityConverter mockedCommodityConverter = mock(CommodityConverter.class);
        CommodityType mockedCommType = CommodityType.newBuilder().setType(10).build();
        when(mockedCommodityConverter.commodityIdToCommodityType(15)).thenReturn(mockedCommType);
        ActionInterpreter interpreter = new ActionInterpreter(mockedCommodityConverter,
                slInfoMap, mockCloudTc, originalTopology, ImmutableMap.of(),
                new CommoditiesResizeTracker(), mock(ProjectedRICoverageCalculator.class), mock(TierExcluder.class),
                CommodityIndex.newFactory()::newIndex, null, new HashMap<>(), fakeEntityCreator);
        // Assuming that 1 is the oid of trader created for m1.large x region and 2 is the oid
        // created for m1.medium x region
        ActionTO actionTO = ActionTO.newBuilder().setImportance(0).setIsNotExecutable(false)
                .setMove(MoveTO.newBuilder()
                        .setShoppingListToMove(5)
                        .setSource(1)
                        .setMoveContext(Context.newBuilder().setRegionId(region.getOid())
                                .setBalanceAccount(BalanceAccountDTO.newBuilder().setId(17438L).build()).build())
                        .setDestination(2)
                        .setMoveExplanation(MoveExplanation.getDefaultInstance())).build();

        // We don't create projected topology entries for the "fake" traders (1 and 2), since
        // those are completely internal to the market.
        Map<Long, ProjectedTopologyEntity> projectedTopology = ImmutableMap.of(
                vm.getOid(), entity(vm),
                m1Large.getOid(), entity(m1Large),
                m1Medium.getOid(), entity(m1Medium));
        final TopologyEntityCloudTopology originalCloudTopology =
                new TopologyEntityCloudTopologyFactory
                        .DefaultTopologyEntityCloudTopologyFactory(mock(GroupMemberRetriever.class))
                        .newCloudTopology(originalTopology.values().stream());

        // setup savings calculator
        when(actionSavingsCalculator.calculateSavings(any())).thenReturn(CalculatedSavings.builder()
                .savingsPerHour(Trax.trax(1.0))
                .build());

        List<Action> actions = interpreter.interpretAction(actionTO, projectedTopology,
                                                              originalCloudTopology, actionSavingsCalculator);

        assertTrue(!actions.isEmpty());
        Action action = actions.get(0);
        // Savings = 17 - 16 = 1
        assertEquals(1, action.getSavingsPerHour().getAmount(), 0.0001);
        assertThat(action.getInfo().getScale().getChanges(0).getSource().getId(), is(m1Large.getOid()));
        assertThat(action.getInfo().getScale().getChanges(0).getSource().getType(), is(m1Large.getEntityType()));
        assertThat(action.getInfo().getScale().getChanges(0).getSource().getEnvironmentType(), is(m1Large.getEnvironmentType()));

        assertThat(action.getInfo().getScale().getChanges(0).getDestination().getId(), is(m1Medium.getOid()));
        assertThat(action.getInfo().getScale().getChanges(0).getDestination().getType(), is(m1Medium.getEntityType()));
        assertThat(action.getInfo().getScale().getChanges(0).getDestination().getEnvironmentType(), is(m1Medium.getEnvironmentType()));

        assertThat(action.getInfo().getScale().getTarget().getId(), is(vm.getOid()));
        assertThat(action.getInfo().getScale().getTarget().getType(), is(vm.getEntityType()));
        assertThat(action.getInfo().getScale().getTarget().getEnvironmentType(), is(vm.getEnvironmentType()));

    }

    /**
     * Check that Deactivate/Suspend action shows savings if the cost journal for the
     * provisioned entity is available.
     * @throws IOException
     */
    @Test
    public void interpretDeactivateWithCost()
            throws IOException {
        Set<TopologyEntityDTO.Builder> topologyEntityDTOBuilders
                = TopologyEntitiesHandlerTest.readCloudTopologyFromJsonFile();
        TopologyEntityDTO.Builder vmBuilder = topologyEntityDTOBuilders.stream().filter(
                builder -> builder.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                .collect(Collectors.toList()).get(0);

        Set<TopologyEntityDTO> topologyEntityDTOs = topologyEntityDTOBuilders.stream()
                .map(builder -> builder.build()).collect(Collectors.toSet());
        Map<Long, TopologyEntityDTO> originalTopology = topologyEntityDTOs.stream()
                .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));

        CloudTopologyConverter mockCloudTc = mock(CloudTopologyConverter.class);

        // Get handle to the templates, region and BA TopologyEntityDTO
        TopologyEntityDTO m1Large = null;
        TopologyEntityDTO vm = null;
        for (TopologyEntityDTO topologyEntityDTO : topologyEntityDTOs) {
            if (topologyEntityDTO.getDisplayName().contains("m1.large")
                    && topologyEntityDTO.getEntityType() == EntityType.COMPUTE_TIER_VALUE) {
                m1Large = topologyEntityDTO;
            } else if (topologyEntityDTO.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE
                        && vmBuilder.getOid() == topologyEntityDTO.getOid()) {
                vm = topologyEntityDTO; //get DTO for the vmBuilder above
            }
        }

        MarketTier sourceMarketTier = new OnDemandMarketTier(m1Large);
        when(mockCloudTc.getMarketTier(1)).thenReturn(sourceMarketTier);
        when(mockCloudTc.isMarketTier(1l)).thenReturn(true);

        ShoppingListInfo slInfo = new ShoppingListInfo(5, vm.getOid(), null, Collections.emptySet(),
                null, null, Arrays.asList());
        Map<Long, ShoppingListInfo> slInfoMap = ImmutableMap.of(5l, slInfo);

        TopologyCostCalculator mockTopologyCostCalculator = mock(TopologyCostCalculator.class);

        CommodityConverter mockedCommodityConverter = mock(CommodityConverter.class);
        CommodityType mockedCommType = CommodityType.newBuilder().setType(10).build();
        when(mockedCommodityConverter.commodityIdToCommodityType(15)).thenReturn(mockedCommType);
        Mockito.doReturn(Optional.of(topologyCommodity1))
                .when(mockedCommodityConverter).marketToTopologyCommodity(eq(economyCommodity1));
        Mockito.doReturn(Optional.of(topologyCommodity2))
                .when(mockedCommodityConverter).marketToTopologyCommodity(eq(economyCommodity2));

        // setup savings calculator
        when(actionSavingsCalculator.calculateSavings(any())).thenReturn(CalculatedSavings.builder()
                .savingsPerHour(Trax.trax(17.0))
                .build());

        // We don't create projected topology entries for the "fake" traders (1 and 2), since
        // those are completely internal to the market.
        Map<Long, ProjectedTopologyEntity> projectedTopology = ImmutableMap.of(
                vm.getOid(), entity(vm),
                m1Large.getOid(), entity(m1Large));

        EconomyDTOs.TraderTO traderTO = EconomyDTOs.TraderTO.newBuilder()
                .setOid(vmBuilder.getOid())
                .setState(TraderStateTO.ACTIVE)
                .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addShoppingLists(ShoppingListTO.newBuilder()
                                    .setOid(-1L)
                                    .setSupplier(1l)
                                .build())
                .build();

        Map<Long, EconomyDTOs.TraderTO>  oidToTraderTOMap
                    = ImmutableMap.of(vmBuilder.getOid(), traderTO);

        ActionInterpreter interpreter = new ActionInterpreter(mockedCommodityConverter,
                slInfoMap,
                mockCloudTc,
                originalTopology,
                oidToTraderTOMap,
                new CommoditiesResizeTracker(),
                mock(ProjectedRICoverageCalculator.class),
                mock(TierExcluder.class),
                CommodityIndex.newFactory()::newIndex,
                null, new HashMap<>(),
                Mockito.mock(FakeEntityCreator.class));

        final ActionTO deactivateActionTO = ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                .setDeactivate(DeactivateTO.newBuilder()
                        .setTraderToDeactivate(vmBuilder.getOid())
                        .addTriggeringBasket(economyCommodity1)
                        .addTriggeringBasket(economyCommodity2)
                        .build())
                .build();

        final TopologyEntityCloudTopology originalCloudTopology =
                new TopologyEntityCloudTopologyFactory
                        .DefaultTopologyEntityCloudTopologyFactory(mock(GroupMemberRetriever.class))
                        .newCloudTopology(originalTopology.values().stream());

        List<Action> actions = interpreter.interpretAction(deactivateActionTO, projectedTopology,
                originalCloudTopology, actionSavingsCalculator);

        assertTrue(!actions.isEmpty());

        Action action = actions.get(0);
        assertEquals(action.getSavingsPerHour(),
                CurrencyAmount.newBuilder().setAmount(17.0).build());
    }

    /**
     * Check that Provision action shows savings if the cost journal for the
     * provisioned entity is available.
     * @throws IOException
     */
    @Test
    public void interpretProvisionWithCost()
            throws IOException {
        Set<TopologyEntityDTO.Builder> topologyEntityDTOBuilders
                = TopologyEntitiesHandlerTest.readCloudTopologyFromJsonFile();
        TopologyEntityDTO.Builder vmBuilder = topologyEntityDTOBuilders.stream().filter(
                builder -> builder.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                .collect(Collectors.toList()).get(0);

        Set<TopologyEntityDTO> topologyEntityDTOs = topologyEntityDTOBuilders.stream()
                .map(builder -> builder.build()).collect(Collectors.toSet());
        Map<Long, TopologyEntityDTO> originalTopology = topologyEntityDTOs.stream()
                .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));

        CloudTopologyConverter mockCloudTc = mock(CloudTopologyConverter.class);

        // Get handle to the templates, region and BA TopologyEntityDTO
        TopologyEntityDTO m1Large = null;
        TopologyEntityDTO vm = null;
        for (TopologyEntityDTO topologyEntityDTO : topologyEntityDTOs) {
            if (topologyEntityDTO.getDisplayName().contains("m1.large")
                    && topologyEntityDTO.getEntityType() == EntityType.COMPUTE_TIER_VALUE) {
                m1Large = topologyEntityDTO;
            } else if (topologyEntityDTO.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE
                    && vmBuilder.getOid() == topologyEntityDTO.getOid()) {
                vm = topologyEntityDTO; //get DTO for the vmBuilder above
            }
        }

        MarketTier sourceMarketTier = new OnDemandMarketTier(m1Large);
        when(mockCloudTc.getMarketTier(1)).thenReturn(sourceMarketTier);
        when(mockCloudTc.isMarketTier(1l)).thenReturn(true);

        ShoppingListInfo slInfo = new ShoppingListInfo(5, vm.getOid(), null, Collections.emptySet(),
                null, null, Arrays.asList());
        Map<Long, ShoppingListInfo> slInfoMap = ImmutableMap.of(5l, slInfo);

        CommodityConverter mockedCommodityConverter = mock(CommodityConverter.class);
        CommodityType mockedCommType = CommodityType.newBuilder().setType(10).build();
        when(mockedCommodityConverter.commodityIdToCommodityType(15)).thenReturn(mockedCommType);
        Mockito.doReturn(Optional.of(topologyCommodity1))
                .when(mockedCommodityConverter).marketToTopologyCommodity(eq(economyCommodity1));
        Mockito.doReturn(Optional.of(topologyCommodity2))
                .when(mockedCommodityConverter).marketToTopologyCommodity(eq(economyCommodity2));

        // We don't create projected topology entries for the "fake" traders (1 and 2), since
        // those are completely internal to the market.
        Map<Long, ProjectedTopologyEntity> projectedTopology = ImmutableMap.of(
                vm.getOid(), entity(vm),
                m1Large.getOid(), entity(m1Large));

        EconomyDTOs.TraderTO traderTO = EconomyDTOs.TraderTO.newBuilder()
                .setOid(vmBuilder.getOid())
                .setState(TraderStateTO.ACTIVE)
                .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addShoppingLists(ShoppingListTO.newBuilder()
                        .setOid(-1L)
                        .setSupplier(1l)
                        .build())
                .build();

        Map<Long, EconomyDTOs.TraderTO>  oidToTraderTOMap
                = ImmutableMap.of(vmBuilder.getOid(), traderTO);

        ActionInterpreter interpreter = new ActionInterpreter(mockedCommodityConverter,
                slInfoMap,
                mockCloudTc,
                originalTopology,
                oidToTraderTOMap,
                new CommoditiesResizeTracker(),
                mock(ProjectedRICoverageCalculator.class),
                mock(TierExcluder.class),
                CommodityIndex.newFactory()::newIndex,
                null, new HashMap<>(),
                Mockito.mock(FakeEntityCreator.class));

        CommodityDTOs.CommoditySpecificationTO cs = mockedCommodityConverter
                .commoditySpecification(CommodityType.newBuilder()
                        .setKey("Seg")
                        .setType(11 + CommodityTypeAllocatorConstants.ACCESS_COMM_TYPE_START_COUNT).build());

        final ActionTO provisionBySupplyActionTO = ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                .setProvisionBySupply(ProvisionBySupplyTO.newBuilder()
                        .setModelSeller(vmBuilder.getOid())
                        .setProvisionedSeller(22l)
                        .setMostExpensiveCommodity(economyCommodity1)
                        .build())
                .build();

        final TopologyEntityCloudTopology originalCloudTopology =
                new TopologyEntityCloudTopologyFactory
                        .DefaultTopologyEntityCloudTopologyFactory(mock(GroupMemberRetriever.class))
                        .newCloudTopology(originalTopology.values().stream());

        // setup savings calculator
        when(actionSavingsCalculator.calculateSavings(any())).thenReturn(CalculatedSavings.builder()
                .savingsPerHour(Trax.trax(-17.0))
                .build());

        List<Action> actions = interpreter.interpretAction(provisionBySupplyActionTO, projectedTopology,
                originalCloudTopology, actionSavingsCalculator);

        assertTrue(!actions.isEmpty());

        Action action = actions.get(0);
        assertEquals(action.getSavingsPerHour(),
                        CurrencyAmount.newBuilder().setAmount(-17.0).build());
    }

    @Test
    public void interpretProvisionWithCostUsingRealTimeTopology()
            throws IOException {
        Set<TopologyEntityDTO.Builder> topologyEntityDTOBuilders
                = TopologyEntitiesHandlerTest.readCloudTopologyFromJsonFile();
        TopologyEntityDTO.Builder vmBuilder = topologyEntityDTOBuilders.stream().filter(
                builder -> builder.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                .collect(Collectors.toList()).get(0);

        Set<TopologyEntityDTO> topologyEntityDTOs = topologyEntityDTOBuilders.stream()
                .map(builder -> builder.build()).collect(Collectors.toSet());
        Map<Long, TopologyEntityDTO> originalTopology = topologyEntityDTOs.stream()
                .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));

        CloudTopologyConverter mockCloudTc = mock(CloudTopologyConverter.class);

        // Get handle to the templates, region and BA TopologyEntityDTO
        TopologyEntityDTO m1Large = null;
        TopologyEntityDTO vm = null;
        for (TopologyEntityDTO topologyEntityDTO : topologyEntityDTOs) {
            if (topologyEntityDTO.getDisplayName().contains("m1.large")
                    && topologyEntityDTO.getEntityType() == EntityType.COMPUTE_TIER_VALUE) {
                m1Large = topologyEntityDTO;
            } else if (topologyEntityDTO.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE
                    && vmBuilder.getOid() == topologyEntityDTO.getOid()) {
                vm = topologyEntityDTO; //get DTO for the vmBuilder above
            }
        }

        MarketTier sourceMarketTier = new OnDemandMarketTier(m1Large);
        when(mockCloudTc.getMarketTier(1)).thenReturn(sourceMarketTier);
        when(mockCloudTc.isMarketTier(1l)).thenReturn(true);

        ShoppingListInfo slInfo = new ShoppingListInfo(5, vm.getOid(), null, Collections.emptySet(),
                null, null, Arrays.asList());
        Map<Long, ShoppingListInfo> slInfoMap = ImmutableMap.of(5l, slInfo);

        TopologyCostCalculator mockTopologyCostCalculator = mock(TopologyCostCalculator.class);
        // Cost journal of the VM in the projected topology contains no costs,
        // cost from the real time topology below will be used
        Map<Long, CostJournal<TopologyEntityDTO>> projectedCosts = new HashMap<>();
        CostJournal<TopologyEntityDTO> projectedCostJournal = mock(CostJournal.class);
        // Destination compute cost = 9 + 1 + 2 + 5 = 19
        when(projectedCostJournal.getHourlyCostFilterEntries(eq(CostCategory.ON_DEMAND_COMPUTE), any())).thenReturn(trax(0d));
        when(projectedCostJournal.getHourlyCostFilterEntries(eq(CostCategory.ON_DEMAND_LICENSE), any())).thenReturn(trax(0d));
        when(projectedCostJournal.getHourlyCostFilterEntries(eq(CostCategory.RESERVED_LICENSE), any())).thenReturn(trax(0d));

        projectedCosts.put(vm.getOid(), projectedCostJournal);

        CommodityConverter mockedCommodityConverter = mock(CommodityConverter.class);
        CommodityType mockedCommType = CommodityType.newBuilder().setType(10).build();
        when(mockedCommodityConverter.commodityIdToCommodityType(15)).thenReturn(mockedCommType);
        Mockito.doReturn(Optional.of(topologyCommodity1))
                .when(mockedCommodityConverter).marketToTopologyCommodity(eq(economyCommodity1));
        Mockito.doReturn(Optional.of(topologyCommodity2))
                .when(mockedCommodityConverter).marketToTopologyCommodity(eq(economyCommodity2));

        // We don't create projected topology entries for the "fake" traders (1 and 2), since
        // those are completely internal to the market.
        Map<Long, ProjectedTopologyEntity> projectedTopology = ImmutableMap.of(
                vm.getOid(), entity(vm),
                m1Large.getOid(), entity(m1Large));

        EconomyDTOs.TraderTO traderTO = EconomyDTOs.TraderTO.newBuilder()
                .setOid(vmBuilder.getOid())
                .setState(TraderStateTO.ACTIVE)
                .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addShoppingLists(ShoppingListTO.newBuilder()
                        .setOid(-1L)
                        .setSupplier(1l)
                        .build())
                .build();

        Map<Long, EconomyDTOs.TraderTO>  oidToTraderTOMap
                = ImmutableMap.of(vmBuilder.getOid(), traderTO);

        ActionInterpreter interpreter = new ActionInterpreter(mockedCommodityConverter,
                slInfoMap,
                mockCloudTc,
                originalTopology,
                oidToTraderTOMap,
                new CommoditiesResizeTracker(),
                mock(ProjectedRICoverageCalculator.class),
                mock(TierExcluder.class),
                CommodityIndex.newFactory()::newIndex,
                null, new HashMap<>(),
                Mockito.mock(FakeEntityCreator.class));

        CommodityDTOs.CommoditySpecificationTO cs = mockedCommodityConverter
                .commoditySpecification(CommodityType.newBuilder()
                        .setKey("Seg")
                        .setType(11 + CommodityTypeAllocatorConstants.ACCESS_COMM_TYPE_START_COUNT).build());

        final ActionTO provisionBySupplyActionTO = ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                .setProvisionBySupply(ProvisionBySupplyTO.newBuilder()
                        .setModelSeller(vmBuilder.getOid())
                        .setProvisionedSeller(22l)
                        .setMostExpensiveCommodity(economyCommodity1)
                        .build())
                .build();

        final TopologyEntityCloudTopology originalCloudTopology =
                new TopologyEntityCloudTopologyFactory
                        .DefaultTopologyEntityCloudTopologyFactory(mock(GroupMemberRetriever.class))
                        .newCloudTopology(originalTopology.values().stream());

        // setup savings calculator
        when(actionSavingsCalculator.calculateSavings(any())).thenReturn(CalculatedSavings.builder()
                .savingsPerHour(Trax.trax(-17.0))
                .build());

        List<Action> actions = interpreter.interpretAction(provisionBySupplyActionTO, projectedTopology,
                originalCloudTopology, actionSavingsCalculator);

        assertTrue(!actions.isEmpty());

        Action action = actions.get(0);
        assertEquals(action.getSavingsPerHour(),
                CurrencyAmount.newBuilder().setAmount(-17.0).build());
    }
}
