package com.vmturbo.market.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.analysis.InvalidTopologyException;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.market.runner.Analysis;
import com.vmturbo.market.runner.AnalysisFactory.AnalysisConfig;
import com.vmturbo.market.runner.cost.MarketPriceTable;
import com.vmturbo.market.runner.cost.MarketPriceTable.ComputePriceBundle;
import com.vmturbo.market.runner.cost.MarketPriceTable.ComputePriceBundle.Builder;
import com.vmturbo.market.runner.cost.MarketPriceTable.DatabasePriceBundle;
import com.vmturbo.market.topology.conversions.CommodityIndex;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper.ConsistentScalingHelperFactory;
import com.vmturbo.market.topology.conversions.MarketAnalysisUtils;
import com.vmturbo.market.topology.conversions.TierExcluder;
import com.vmturbo.market.topology.conversions.TierExcluder.TierExcluderFactory;
import com.vmturbo.market.topology.conversions.TopologyConverter;
import com.vmturbo.platform.analysis.actions.ActionType;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.ede.ReplayActions;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.DeactivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderStateTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.topology.Topology;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.topology.processor.conversions.SdkToTopologyEntityConverter;

/**
 * Unit tests for {@link TopologyEntitiesHandler}.
 */
public class TopologyEntitiesHandlerTest {

    static {
        // allow running this test standalone (not part of a full build)
        IdentityGenerator.initPrefix(0);
    }

    private static final String DSPM_OR_DATASTORE_PATTERN = "DSPM.*|DATASTORE.*";
    private static final String BC_PATTERN = "BICLIQUE.*";

    private static final TopologyInfo REALTIME_TOPOLOGY_INFO =
                    TopologyInfo.newBuilder().setTopologyType(TopologyType.REALTIME).build();

    private final Optional<Integer> maxPlacementIterations = Optional.empty();

    private final boolean useQuoteCacheDuringSNM = false;

    private static final float rightsizeLowerWatermark = 0.1f;

    private static final float rightsizeUpperWatermark = 0.7f;

    private static final float desiredUtilizationTarget = 0.7f;

    private static final float desiredUtilizationRange = 0.1f;

    private static final String SIMPLE_CLOUD_TOPOLOGY_JSON_FILE =
                    "protobuf/messages/simple-cloudTopology.json";
    private static final Gson GSON = new Gson();

    private MarketPriceTable marketPriceTable = mock(MarketPriceTable.class);

    private CloudCostData ccd = mock(CloudCostData.class);

    private static final Logger logger = LogManager.getLogger();

    private TierExcluderFactory tierExcluderFactory = mock(TierExcluderFactory.class);
    private ConsistentScalingHelperFactory consistentScalingHelperFactory =
            mock(ConsistentScalingHelperFactory.class);

    @Before
    public void setup() {
        when(ccd.getExistingRiBought()).thenReturn(new ArrayList<ReservedInstanceData>());
        when(tierExcluderFactory.newExcluder(any(), any(), any())).thenReturn(mock(TierExcluder.class));
        ConsistentScalingHelper consistentScalingHelper = mock(ConsistentScalingHelper.class);
        when(consistentScalingHelper.getScalingGroupId(any())).thenReturn(Optional.empty());
        when(consistentScalingHelper.getScalingGroupUsage(any())).thenReturn(Optional.empty());
        when(consistentScalingHelper.getGroupFactor(any())).thenReturn(1);
        when(consistentScalingHelperFactory.newConsistentScalingHelper(any(), any()))
            .thenReturn(consistentScalingHelper);
    }

    /**
     * Test loading a file that was generated using the hyper-v probe.
     * Move the DTOs through the whole pipe and verify we get actions.
     * TODO: more specific tests for the generated actions
     * @throws IOException when the loaded file is missing
     * @throws InvalidTopologyException not supposed to happen here
     */
    @Test
    public void end2endTest() throws IOException, InvalidTopologyException {
        AnalysisResults result = generateEnd2EndActions(mock(Analysis.class));
        List<ActionTO> actionTOs = result.getActionsList();
        assertFalse(actionTOs.isEmpty());
        // All commodities gotten from protobuf/messages/discoveredEntities.json
        // in generateEnd2End actions have resizable=false, so, there should not be resize actions
        assertFalse(actionTOs.stream().anyMatch(ActionTO::hasResize));
        assertTrue(actionTOs.stream().anyMatch(ActionTO::hasMove));
        assertTrue(actionTOs.stream().anyMatch(ActionTO::hasProvisionBySupply));
        assertTrue(actionTOs.stream().anyMatch(ActionTO::hasActivate));

        // Assert the topologyDTOs returned to check the state of Activated entity
        // We expect as per the json file topology, host#6 being activated
        Optional<TraderTO> traderOptional = result.getProjectedTopoEntityTOList().stream()
                        .filter(to -> to.getDebugInfoNeverUseInCode().contains("Host #6"))
                        .findFirst();
        if (traderOptional.isPresent()) {
            assertEquals(traderOptional.get().getState(), TraderStateTO.ACTIVE);
        } else {
            logger.info("Trader host-6 not found in topology");
        }
    }

    /**
     * Test loading a file that was generated using the hyper-v probe.
     * Move the DTOs through the whole pipe and verify actions are properly collapsed.
     * @throws IOException when the loaded file is missing
     * @throws InvalidTopologyException not supposed to happen here
     */
    @Test
    public void end2endTestCollapsing() throws IOException, InvalidTopologyException {
        AnalysisResults result = generateEnd2EndActions(mock(Analysis.class));
        List<ActionTO> actionTOs = result.getActionsList();

        // Verify that the actions are collapsed
        // Count Move actions - with collapsed actions there should be no more than one per shopping list
        Map<Long, Long> shoppingListMoveCount =
                        actionTOs.stream().filter(ActionTO::hasMove).map(ActionTO::getMove)
                                        .collect(Collectors.groupingBy(
                                                        MoveTO::getShoppingListToMove,
                                                        Collectors.counting()));
        long max = shoppingListMoveCount.values().stream().max(Long::compare).orElse(0L);
        assertTrue("Shopping lists are moved more than once", max <= 1);
        // In collapsed actions there should be no resize actions for traders that are then deactivated
        Set<Long> deactivated = actionTOs.stream().filter(ActionTO::hasDeactivate)
                        .map(ActionTO::getDeactivate).map(DeactivateTO::getTraderToDeactivate)
                        .collect(Collectors.toSet());
        Set<Long> resized = actionTOs.stream().filter(ActionTO::hasResize).map(ActionTO::getResize)
                        .map(ResizeTO::getSellingTrader).collect(Collectors.toSet());
        resized.retainAll(deactivated);
        assertTrue("Traders are resized but later deactivated", resized.isEmpty());
    }

    /**
     * Test replay actions for real time topology. Few deactivate actions are created by analysis for
     * given topology and those action should be added to replay actions.
     * @throws IOException when the loaded file is missing
     * @throws InvalidTopologyException not supposed to happen here
     */
    @Test
    public void replayActionsTestForRealTime() throws IOException, InvalidTopologyException {
        List<CommonDTO.EntityDTO> probeDTOs = messagesFromJsonFile(
                        "protobuf/messages/discoveredEntities.json", EntityDTO::newBuilder);

        Map<Long, CommonDTO.EntityDTO> map = Maps.newHashMap();
        IntStream.range(0, probeDTOs.size()).forEach(i -> map.put((long)i, probeDTOs.get(i)));
        Map<Long, TopologyEntityDTO> topoDTOs = SdkToTopologyEntityConverter
                        .convertToTopologyEntityDTOs(map).stream()
                        .map(TopologyEntityDTO.Builder::build)
                        .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));
        TopologyConverter converter = new TopologyConverter(REALTIME_TOPOLOGY_INFO, true,
                        MarketAnalysisUtils.QUOTE_FACTOR, MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR,
                        marketPriceTable, ccd, CommodityIndex.newFactory(), tierExcluderFactory,
            consistentScalingHelperFactory);
        Set<TraderTO> economyDTOs = converter.convertToMarket(topoDTOs);
        final TopologyInfo topologyInfo = TopologyInfo.newBuilder().setTopologyContextId(7L)
                        .setTopologyType(TopologyType.REALTIME).setTopologyId(1L).build();
        ReplayActions replayActions = new ReplayActions();
        Analysis analysis = mock(Analysis.class);
        mockCommsToAdjustForOverhead(analysis, converter);
        when(analysis.getReplayActions()).thenReturn(replayActions);
        final AnalysisConfig analysisConfig = AnalysisConfig
                        .newBuilder(MarketAnalysisUtils.QUOTE_FACTOR,
                                        MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR,
                                        SuspensionsThrottlingConfig.DEFAULT, Collections.emptyMap())
                        .setRightsizeLowerWatermark(rightsizeLowerWatermark)
                        .setRightsizeUpperWatermark(rightsizeUpperWatermark)
                        .setMaxPlacementsOverride(maxPlacementIterations)
                        .setUseQuoteCacheDuringSNM(useQuoteCacheDuringSNM).build();
        final Topology topology = TopologyEntitiesHandler.createTopology(economyDTOs, topologyInfo,
                analysisConfig, analysis);
        AnalysisResults results = TopologyEntitiesHandler.performAnalysis(economyDTOs, topologyInfo,
                        analysisConfig, analysis, topology);

        // All deactivate actions should be set in analysis' replay actions.
        List<Long> deactivatedActionsTarget = results.getActionsList().stream()
                        .filter(ActionTO::hasDeactivate)
                        .map(actionTo -> actionTo.getDeactivate().getTraderToDeactivate())
                        .collect(Collectors.toList());
        List<Long> replayOids = replayActions.getActions().stream()
                        .map(a -> replayActions.getTraderOids().get(a.getActionTarget()))
                        .collect(Collectors.toList());

        // Check that populated replay actions contain all Deactivate actions.
        assertEquals(deactivatedActionsTarget.size(), replayOids.size());
        assertTrue(replayActions.getActions().stream()
                        .allMatch(a -> a.getType().equals(ActionType.DEACTIVATE)));
        assertTrue(deactivatedActionsTarget.containsAll(replayOids));
    }

    /**
     * Test replay actions for plan topology. This should not affect current
     * state of replay actions.
     * @throws IOException when the loaded file is missing
     * @throws InvalidTopologyException not supposed to happen here
     */
    @Test
    public void replayActionsTestForPlan() throws IOException, InvalidTopologyException {
        ReplayActions replayActions = new ReplayActions();
        Deactivate deactivateAction = mock(Deactivate.class);
        replayActions.getActions().add(deactivateAction);
        Analysis analysis = mock(Analysis.class);

        when(analysis.getReplayActions()).thenReturn(replayActions);

        generateEnd2EndActions(mock(Analysis.class));

        // Unchanged replay actions for plan.
        assertEquals(replayActions, analysis.getReplayActions());
        assertEquals(1, replayActions.getActions().size());
        assertEquals(deactivateAction, replayActions.getActions().get(0));
    }

    /**
     * Load a small topology with 2 PMs and 3 DSs that are fully meshed. We expect to get one biclique.
     * We assert that only the biclique keys BC-T1-0 and BC-T2-0 are used.
     * @throws IOException if the test file cannot be loaded
     * @throws InvalidTopologyException not supposed to happen here
     */
    @Test
    public void bicliquesTest() throws IOException, InvalidTopologyException {
        List<CommonDTO.EntityDTO> probeDTOs =
                        messagesFromJsonFile("protobuf/messages/small.json", EntityDTO::newBuilder);

        Map<Long, CommonDTO.EntityDTO> map = Maps.newHashMap();
        IntStream.range(0, probeDTOs.size()).forEach(i -> map.put((long)i, probeDTOs.get(i)));

        List<TopologyEntityDTO.Builder> topoDTOs =
                        SdkToTopologyEntityConverter.convertToTopologyEntityDTOs(map);
        TopologyConverter topoConverter = new TopologyConverter(REALTIME_TOPOLOGY_INFO, true,
                        MarketAnalysisUtils.QUOTE_FACTOR, MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR,
                        marketPriceTable, ccd, CommodityIndex.newFactory(), tierExcluderFactory,
            consistentScalingHelperFactory);

        Set<TraderTO> traderDTOs = topoConverter.convertToMarket(topoDTOs.stream()
                        .map(TopologyEntityDTO.Builder::build)
                        .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity())));
        Set<String> debugInfos = traderDTOs.stream().map(TraderTO::getCommoditiesSoldList)
                        .flatMap(List::stream).map(CommoditySoldTO::getSpecification)
                        .map(CommoditySpecificationTO::getDebugInfoNeverUseInCode)
                        .filter(key -> key.startsWith("BICLIQUE")).collect(Collectors.toSet());
        assertEquals(Sets.newHashSet("BICLIQUE BC-T1-1", "BICLIQUE BC-T2-1"), debugInfos);

        // Verify that buyers buy commodities from sellers that indeed sell them (all, not only biclique)
        Map<Long, TraderTO> tradersMap = traderDTOs.stream()
                        .collect(Collectors.toMap(TraderTO::getOid, Function.identity()));
        for (TraderTO trader : traderDTOs) {
            for (ShoppingListTO shoppingList : trader.getShoppingListsList()) {
                TraderTO supplier = tradersMap.get(shoppingList.getSupplier());
                for (CommodityBoughtTO commBought : shoppingList.getCommoditiesBoughtList()) {
                    assertTrue(supplier.getCommoditiesSoldList().stream()
                                    .anyMatch(commSold -> commSold.getSpecification()
                                                    .equals(commBought.getSpecification())));
                }
            }
        }
    }

    /**
     * Load a bigger topology. Verify that each storage participates in exactly one biclique,
     * and that each VM buys a unique set of BC keys in each one of its shopping lists.
     * @throws IOException if the test file cannot be loaded
     * @throws InvalidTopologyException not supposed to happen here
     */
    @Test
    public void bcKeysTests() throws IOException, InvalidTopologyException {
        List<CommonDTO.EntityDTO> probeDTOs = messagesFromJsonFile(
                        "protobuf/messages/entities.json", EntityDTO::newBuilder);

        Map<Long, CommonDTO.EntityDTO> map = Maps.newHashMap();
        IntStream.range(0, probeDTOs.size()).forEach(i -> map.put((long)i, probeDTOs.get(i)));

        List<TopologyEntityDTO.Builder> topoDTOs =
                        SdkToTopologyEntityConverter.convertToTopologyEntityDTOs(map);

        Set<TraderTO> traderDTOs = new TopologyConverter(REALTIME_TOPOLOGY_INFO, marketPriceTable,
                        ccd, CommodityIndex.newFactory(), tierExcluderFactory,
            consistentScalingHelperFactory).convertToMarket(
                                        topoDTOs.stream().map(TopologyEntityDTO.Builder::build)
                                                        .collect(Collectors.toMap(
                                                                        TopologyEntityDTO::getOid,
                                                                        Function.identity())));

        for (TraderTO traderTO : traderDTOs) {
            if (traderTO.getDebugInfoNeverUseInCode().startsWith("STORAGE")) {
                long numBcKeys = traderTO.getCommoditiesSoldList().stream()
                                .map(CommoditySoldTO::getSpecification)
                                .map(CommoditySpecificationTO::getDebugInfoNeverUseInCode)
                                .filter(key -> key.startsWith("BICLIQUE")).count();
                assertEquals(1, numBcKeys);
            }
        }

        // BC keys sold are unique
        for (TraderTO traderTO : traderDTOs) {
            if (traderTO.getDebugInfoNeverUseInCode().startsWith("VIRTUAL_MACHINE")) {
                for (ShoppingListTO shoppingList : traderTO.getShoppingListsList()) {
                    List<String> bcKeys = shoppingList.getCommoditiesBoughtList().stream()
                                    .map(c -> c.getSpecification().getDebugInfoNeverUseInCode())
                                    .filter(s -> s.startsWith("BICLIQUE"))
                                    .collect(Collectors.toList());
                    assertEquals(Sets.newHashSet(bcKeys).size(), bcKeys.size());
                }
            }
        }
    }

    /**
     * Test that when shop-together is enabled, we don't create biclique commodities,
     * and instead we populate the 'cliques' property of the trader DTO.
     * Also test that regardless of shop-together, we remove all the DSPMAccess and
     * Datastore commodities.
     * @throws IOException if the test file cannot be loaded
     * @throws InvalidTopologyException not supposed to happen here
     */
    @Test
    public void shopTogetherTest() throws IOException, InvalidTopologyException {
        List<CommonDTO.EntityDTO> nonShopTogetherProbeDTOs = messagesFromJsonFile(
                        "protobuf/messages/nonShopTogetherEntities.json", EntityDTO::newBuilder);
        Map<Long, CommonDTO.EntityDTO> map = Maps.newHashMap();
        IntStream.range(0, nonShopTogetherProbeDTOs.size())
                        .forEach(i -> map.put((long)i, nonShopTogetherProbeDTOs.get(i)));

        Map<Long, TopologyEntityDTO> nonShopTogetherTopoDTOs = SdkToTopologyEntityConverter
                        .convertToTopologyEntityDTOs(map).stream()
                        .map(TopologyEntityDTO.Builder::build)
                        .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));

        TopologyConverter togetherConverter = new TopologyConverter(REALTIME_TOPOLOGY_INFO,
                        marketPriceTable, ccd, CommodityIndex.newFactory(), tierExcluderFactory,
            consistentScalingHelperFactory);
        final Set<TraderTO> traderDTOs = togetherConverter.convertToMarket(nonShopTogetherTopoDTOs);

        // No DSPMAccess and Datastore commodities sold
        final long nspDdSoldShopTogether =
                        countSoldCommodities(traderDTOs, DSPM_OR_DATASTORE_PATTERN);
        assertEquals(0, nspDdSoldShopTogether);

        // No DSPMAccess and Datastore commodities bought
        long nspDdBoughtShopTogether =
                        countBoughtCommodities(traderDTOs, DSPM_OR_DATASTORE_PATTERN);
        assertEquals(0, nspDdBoughtShopTogether);

        // Some BiClique commodities sold
        long bcSoldNonShopTogether = countSoldCommodities(traderDTOs, BC_PATTERN);
        assertTrue(bcSoldNonShopTogether > 0);

        // Some BiClique commodities bought
        long bcBoughtNonShopTogether = countBoughtCommodities(traderDTOs, BC_PATTERN);
        assertTrue(bcBoughtNonShopTogether > 0);

        // check that bicliques are correct in the generated dtos
        checkBicliques(traderDTOs, false);

        // ====== shop together ======
        List<CommonDTO.EntityDTO> shopTogetherProbeDTOs = messagesFromJsonFile(
                        "protobuf/messages/shopTogetherEntities.json", EntityDTO::newBuilder);
        Map<Long, CommonDTO.EntityDTO> shopTogetherMap = Maps.newHashMap();
        IntStream.range(0, shopTogetherProbeDTOs.size())
                        .forEach(i -> shopTogetherMap.put((long)i, shopTogetherProbeDTOs.get(i)));

        Map<Long, TopologyEntityDTO> shopTogetherTopoDTOs = SdkToTopologyEntityConverter
                        .convertToTopologyEntityDTOs(shopTogetherMap).stream()
                        .map(TopologyEntityDTO.Builder::build)
                        .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));

        TopologyConverter shopTogetherConverter = new TopologyConverter(REALTIME_TOPOLOGY_INFO,
                        marketPriceTable, ccd, CommodityIndex.newFactory(), tierExcluderFactory,
            consistentScalingHelperFactory);
        final Set<TraderTO> shopTogetherTraderDTOs =
                        shopTogetherConverter.convertToMarket(shopTogetherTopoDTOs);

        // No DSPMAccess and Datastore commodities sold
        long ddSoldShopTogether =
                        countSoldCommodities(shopTogetherTraderDTOs, DSPM_OR_DATASTORE_PATTERN);
        assertEquals(0, ddSoldShopTogether);

        // No DSPMAccess and Datastore commodities bought
        long ddBoughtNonShopTogether =
                        countBoughtCommodities(shopTogetherTraderDTOs, DSPM_OR_DATASTORE_PATTERN);
        assertEquals(0, ddBoughtNonShopTogether);

        //BiClique commodities will be sold always. There is no harm.
        final long bcSoldShopTogether = countSoldCommodities(shopTogetherTraderDTOs, BC_PATTERN);
        assertEquals(8, bcSoldShopTogether);

        // No BiClique commodities bought
        final long bcBoughtShopTogether =
                        countBoughtCommodities(shopTogetherTraderDTOs, BC_PATTERN);
        assertEquals(0, bcBoughtShopTogether);

        // check that bicliques are correct in the generated dtos
        checkBicliques(shopTogetherTraderDTOs, true);
    }

    /**
     * Test that when used > capacity we don't throw any exception.
     * In the loaded file there are 3 such commodities.
     * We should be able to handle them.
     * @throws IOException if the test file cannot be loaded
     */
    @Test
    public void testInvalidTopology() throws IOException {
        List<CommonDTO.EntityDTO> probeDTOs = messagesFromJsonFile(
                        "protobuf/messages/invalid-topology.json", EntityDTO::newBuilder);

        Map<Long, CommonDTO.EntityDTO> map = Maps.newHashMap();
        IntStream.range(0, probeDTOs.size()).forEach(i -> map.put((long)i, probeDTOs.get(i)));

        List<TopologyEntityDTO.Builder> topoDTOs =
                        SdkToTopologyEntityConverter.convertToTopologyEntityDTOs(map);
        new TopologyConverter(REALTIME_TOPOLOGY_INFO, marketPriceTable, ccd,
                        CommodityIndex.newFactory(), tierExcluderFactory,
            consistentScalingHelperFactory).convertToMarket(
                                        topoDTOs.stream().map(TopologyEntityDTO.Builder::build)
                                                        .collect(Collectors.toMap(
                                                                        TopologyEntityDTO::getOid,
                                                                        Function.identity())));
    }

    @Test
    public void testMoveToCheaperComputeTier_ShopTogether()
                    throws FileNotFoundException, InvalidProtocolBufferException {
        testMoveToCheaperComputeTier(true);
    }

    @Test
    public void testMoveToCheaperComputeTier_ShopAlone()
                    throws FileNotFoundException, InvalidProtocolBufferException {
        testMoveToCheaperComputeTier(false);
    }

    public void testMoveToCheaperComputeTier(boolean isVMShopTogether)
                    throws FileNotFoundException, InvalidProtocolBufferException {
        // Read file
        Set<TopologyEntityDTO.Builder> topologyEntityDTOBuilders = readCloudTopologyFromJsonFile();
        TopologyEntityDTO.Builder vm = topologyEntityDTOBuilders.stream().filter(
                        builder -> builder.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                        .collect(Collectors.toList()).get(0);
        final Set<Integer> entityTypesToSkip = new HashSet<>();
        entityTypesToSkip.add(EntityType.DATABASE_SERVER_VALUE);
        entityTypesToSkip.add(EntityType.DATABASE_TIER_VALUE);
        entityTypesToSkip.add(EntityType.DATABASE_VALUE);
        // Set the shopTogether flag
        vm.getAnalysisSettingsBuilder().setShopTogether(isVMShopTogether);
        Set<TopologyEntityDTO> topologyEntityDTOs = topologyEntityDTOBuilders.stream()
                        .map(TopologyEntityDTO.Builder::build).collect(Collectors.toSet());

        Set<TopologyEntityDTO> dtosToProcess = topologyEntityDTOs.stream()
                        .filter(dto -> (!entityTypesToSkip.contains(dto.getEntityType())))
                        .collect(Collectors.toSet());

        // Get handle to the templates, region and BA TopologyEntityDTO
        TopologyEntityDTO m1Medium = null;
        TopologyEntityDTO m1Large = null;
        TopologyEntityDTO region = null;
        TopologyEntityDTO ba = null;
        for (TopologyEntityDTO topologyEntityDTO : dtosToProcess) {
            if (topologyEntityDTO.getDisplayName().contains("m1.large")
                            && topologyEntityDTO.getEntityType() == EntityType.COMPUTE_TIER_VALUE) {
                m1Large = topologyEntityDTO;
            } else if (topologyEntityDTO.getDisplayName().contains("m1.medium")
                            && topologyEntityDTO.getEntityType() == EntityType.COMPUTE_TIER_VALUE) {
                m1Medium = topologyEntityDTO;
            } else if (topologyEntityDTO.getEntityType() == EntityType.REGION_VALUE) {
                region = topologyEntityDTO;
            } else if (topologyEntityDTO.getEntityType() == EntityType.BUSINESS_ACCOUNT_VALUE) {
                ba = topologyEntityDTO;
            }
        }

        Map<OSType, Double> m1LargePrices = new HashMap<>();
        m1LargePrices.put(OSType.LINUX, 5d);
        m1LargePrices.put(OSType.RHEL, 3d);
        Map<OSType, Double> m1MediumPrices = new HashMap<>();
        m1MediumPrices.put(OSType.LINUX, 4d);
        m1MediumPrices.put(OSType.RHEL, 2d);
        AccountPricingData accountPricingData = mock(AccountPricingData.class);
        when(ccd.getAccountPricingData(ba.getOid())).thenReturn(Optional.ofNullable(accountPricingData));
        when(accountPricingData.getAccountPricingDataOid()).thenReturn(ba.getOid());
        when(ccd.getAccountPricingData(ba.getOid())).thenReturn(Optional.ofNullable(accountPricingData));
        when(marketPriceTable.getComputePriceBundle(m1Large, region.getOid(), accountPricingData))
                        .thenReturn(mockComputePriceBundle(ba.getOid(), m1LargePrices));
        when(marketPriceTable.getComputePriceBundle(m1Medium, region.getOid(), accountPricingData))
                        .thenReturn(mockComputePriceBundle(ba.getOid(), m1MediumPrices));
        when(ccd.getRiCoverageForEntity(anyLong())).thenReturn(Optional.empty());
        final TopologyConverter converter = new TopologyConverter(REALTIME_TOPOLOGY_INFO,
                        marketPriceTable, ccd, CommodityIndex.newFactory(), tierExcluderFactory,
            consistentScalingHelperFactory);
        final Set<EconomyDTOs.TraderTO> traderTOs = converter.convertToMarket(dtosToProcess.stream()
                        .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity())));
        traderTOs.forEach(t -> System.out.println(t));
        // Get handle to the traders which will be used in asserting
        TraderTO m1MediumTrader = null;
        TraderTO m1LargeTrader = null;
        TraderTO testVMTrader = null;
        ShoppingListTO slToMove = null;
        for (TraderTO traderTO : traderTOs) {
            if (traderTO.getType() == EntityType.COMPUTE_TIER_VALUE
                            && traderTO.getDebugInfoNeverUseInCode().contains("m1.medium")) {
                m1MediumTrader = traderTO;
            } else if (traderTO.getType() == EntityType.COMPUTE_TIER_VALUE
                            && traderTO.getDebugInfoNeverUseInCode().contains("m1.large")) {
                m1LargeTrader = traderTO;
            } else if (traderTO.getType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                testVMTrader = traderTO;
            }
        }
        long m1LargeOid = m1LargeTrader.getOid();
        slToMove = testVMTrader.getShoppingListsList().stream()
                        .filter(sl -> sl.getSupplier() == m1LargeOid).collect(Collectors.toList())
                        .get(0);
        // mark VM as movable
        Analysis analysis = mock(Analysis.class);
        mockCommsToAdjustForOverhead(analysis, converter);
        when(analysis.getReplayActions()).thenReturn(new ReplayActions());

        final AnalysisConfig analysisConfig = AnalysisConfig
                        .newBuilder(MarketAnalysisUtils.QUOTE_FACTOR,
                                        MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR,
                                        SuspensionsThrottlingConfig.DEFAULT, Collections.emptyMap())
                        .setRightsizeLowerWatermark(rightsizeLowerWatermark)
                        .setRightsizeUpperWatermark(rightsizeUpperWatermark)
                        .setMaxPlacementsOverride(maxPlacementIterations)
                        .setUseQuoteCacheDuringSNM(useQuoteCacheDuringSNM).build();
        // Call analysis
        final Topology topology = TopologyEntitiesHandler.createTopology(traderTOs,
                REALTIME_TOPOLOGY_INFO, analysisConfig, analysis);
        AnalysisResults results = TopologyEntitiesHandler.performAnalysis(traderTOs,
                        REALTIME_TOPOLOGY_INFO, analysisConfig, analysis, topology);
        logger.info(results.getActionsList());

        // Asserts
        assertEquals(1, results.getActionsCount());
        List<ActionTO> actions = results.getActionsList();
        MoveTO move = actions.get(0).getMove();
        assertEquals(slToMove.getOid(), move.getShoppingListToMove());
        assertEquals(m1LargeTrader.getOid(), move.getSource());
        assertEquals(m1MediumTrader.getOid(), move.getDestination());
    }

    public void testMoveToCheaperDatabaseTier() {
        try {
            // Read file
            Set<TopologyEntityDTO.Builder> topologyEntityDTOBuilders =
                            readCloudTopologyFromJsonFile();
            TopologyEntityDTO.Builder db = topologyEntityDTOBuilders.stream()
                            .filter(builder -> builder.getEntityType() == EntityType.DATABASE_VALUE)
                            .collect(Collectors.toList()).get(0);
            final Set<Integer> entityTypesToSkip = new HashSet<>();
            entityTypesToSkip.add(EntityType.COMPUTE_TIER_VALUE);
            entityTypesToSkip.add(EntityType.STORAGE_TIER_VALUE);
            entityTypesToSkip.add(EntityType.VIRTUAL_MACHINE_VALUE);
            // Set the shopTogether flag
            db.getAnalysisSettingsBuilder().setShopTogether(false);
            Set<TopologyEntityDTO> topologyEntityDTOs = topologyEntityDTOBuilders.stream()
                            .map(TopologyEntityDTO.Builder::build).collect(Collectors.toSet());
            Set<TopologyEntityDTO> dtosToProcess = topologyEntityDTOs.stream()
                            .filter(dto -> (!entityTypesToSkip.contains(dto.getEntityType())))
                            .collect(Collectors.toSet());
            // Get handle to the templates, region and BA TopologyEntityDTO
            TopologyEntityDTO dbm3Large = null;
            TopologyEntityDTO dbm3Medium = null;
            TopologyEntityDTO region = null;
            TopologyEntityDTO ba = null;
            for (TopologyEntityDTO topologyEntityDTO : dtosToProcess) {
                if (topologyEntityDTO.getEntityType() == EntityType.REGION_VALUE) {
                    region = topologyEntityDTO;
                } else if (topologyEntityDTO.getEntityType() == EntityType.BUSINESS_ACCOUNT_VALUE) {
                    ba = topologyEntityDTO;
                } else if (topologyEntityDTO.getDisplayName().contains("db.m3.medium")
                                && topologyEntityDTO
                                                .getEntityType() == EntityType.DATABASE_TIER_VALUE) {
                    dbm3Medium = topologyEntityDTO;
                } else if (topologyEntityDTO.getDisplayName().contains("db.m3.large")
                                && topologyEntityDTO
                                                .getEntityType() == EntityType.DATABASE_TIER_VALUE) {
                    dbm3Large = topologyEntityDTO;
                }
            }
            // Mock the costs for the tiers
            // the prices are DatabaseEdition based, have made other attributes to NULL
            Map<DatabaseEngine, Double> dbm3LargePrices = new HashMap<>();
            dbm3LargePrices.put(DatabaseEngine.POSTGRESQL, 5d);
            dbm3LargePrices.put(DatabaseEngine.MARIADB, 3d);
            Map<DatabaseEngine, Double> dbm3MediumPrices = new HashMap<>();
            dbm3MediumPrices.put(DatabaseEngine.POSTGRESQL, 4d);
            dbm3MediumPrices.put(DatabaseEngine.MARIADB, 2d);

            AccountPricingData accountPricingData = Mockito.mock(AccountPricingData.class);
            // calculateCost
            when(marketPriceTable.getDatabasePriceBundle(dbm3Medium.getOid(), region.getOid(), accountPricingData))
                            .thenReturn(mockDatabasePriceBundle(ba.getOid(), dbm3MediumPrices));
            when(marketPriceTable.getDatabasePriceBundle(dbm3Large.getOid(), region.getOid(), accountPricingData))
                            .thenReturn(mockDatabasePriceBundle(ba.getOid(), dbm3LargePrices));
            when(ccd.getRiCoverageForEntity(anyLong())).thenReturn(Optional.empty());
            final TopologyConverter converter = new TopologyConverter(REALTIME_TOPOLOGY_INFO,
                            marketPriceTable, ccd, CommodityIndex.newFactory(), tierExcluderFactory,
                consistentScalingHelperFactory);
            final Set<EconomyDTOs.TraderTO> traderTOs = converter
                            .convertToMarket(dtosToProcess.stream().collect(Collectors.toMap(
                                            TopologyEntityDTO::getOid, Function.identity())));
            traderTOs.forEach(t -> System.out.println(t));
            // Get handle to the traders which will be used in asserting
            TraderTO m3MediumTrader = null;
            TraderTO m3LargeTrader = null;
            TraderTO testDBTrader = null;
            ShoppingListTO slToMove = null;
            for (TraderTO traderTO : traderTOs) {
                if (traderTO.getType() == EntityType.DATABASE_TIER_VALUE
                                && traderTO.getDebugInfoNeverUseInCode().contains("db.m3.medium")) {
                    m3MediumTrader = traderTO;
                } else if (traderTO.getType() == EntityType.DATABASE_TIER_VALUE
                                && traderTO.getDebugInfoNeverUseInCode().contains("db.m3.large")) {
                    m3LargeTrader = traderTO;
                } else if (traderTO.getType() == EntityType.DATABASE_VALUE) {
                    testDBTrader = traderTO;
                }
            }
            long m3LargeOid = m3LargeTrader.getOid();
            slToMove = testDBTrader.getShoppingListsList().stream()
                            .filter(sl -> sl.getSupplier() == m3LargeOid)
                            .collect(Collectors.toList()).get(0);
            Analysis analysis = mock(Analysis.class);
            mockCommsToAdjustForOverhead(analysis, converter);
            when(analysis.getReplayActions()).thenReturn(new ReplayActions());

            final AnalysisConfig analysisConfig = AnalysisConfig
                            .newBuilder(MarketAnalysisUtils.QUOTE_FACTOR,
                                            MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR,
                                            SuspensionsThrottlingConfig.DEFAULT,
                                            Collections.emptyMap())
                            .setRightsizeLowerWatermark(rightsizeLowerWatermark)
                            .setRightsizeUpperWatermark(rightsizeUpperWatermark)
                            .setMaxPlacementsOverride(maxPlacementIterations)
                            .setUseQuoteCacheDuringSNM(useQuoteCacheDuringSNM).build();
            // Call analysis
            final Topology topology = TopologyEntitiesHandler.createTopology(traderTOs,
                    REALTIME_TOPOLOGY_INFO, analysisConfig, analysis);
            AnalysisResults results = TopologyEntitiesHandler.performAnalysis(traderTOs,
                            REALTIME_TOPOLOGY_INFO, analysisConfig, analysis, topology);
            logger.info(results.getActionsList());

            // Asserts
            assertEquals(1, results.getActionsCount());
            List<ActionTO> actions = results.getActionsList();
            MoveTO move = actions.get(0).getMove();
            assertEquals(slToMove.getOid(), move.getShoppingListToMove());
            assertEquals(m3LargeOid, move.getSource());
            assertEquals(m3MediumTrader.getOid(), move.getDestination());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    private void mockCommsToAdjustForOverhead(Analysis analysis, TopologyConverter converter) {
        Iterator<Integer> iter = MarketAnalysisUtils.COMM_TYPES_TO_ALLOW_OVERHEAD.iterator();
        while (iter.hasNext()) {
            int type = iter.next();
            TopologyDTO.CommodityType commType = TopologyDTO.CommodityType.newBuilder().setType(type).build();
            when(analysis.getCommSpecForCommodity(commType))
                    .thenReturn(converter.getCommSpecForCommodity(commType));
        }
    }

    private ComputePriceBundle mockComputePriceBundle(Long businessAccountId,
                    Map<OSType, Double> osPriceMapping) {
        Builder builder = ComputePriceBundle.newBuilder();
        for (Map.Entry<OSType, Double> e : osPriceMapping.entrySet()) {
            builder.addPrice(businessAccountId, e.getKey(), e.getValue(), false);
        }
        return builder.build();
    }

    private DatabasePriceBundle mockDatabasePriceBundle(Long businessAccountId,
                    Map<DatabaseEngine, Double> engineBasedPriceMapping) {
        DatabasePriceBundle.Builder builder = DatabasePriceBundle.newBuilder();
        for (Map.Entry<DatabaseEngine, Double> e : engineBasedPriceMapping.entrySet()) {
            // price entry for Valid Engine and NULL Edition, DeploymentType and LicenseModel
            builder.addPrice(businessAccountId, e.getKey(), null, null, null, e.getValue());
        }
        return builder.build();
    }

    public static Set<TopologyEntityDTO.Builder> readCloudTopologyFromJsonFile()
                    throws FileNotFoundException, InvalidProtocolBufferException {
        return readCloudTopologyFromJsonFile(SIMPLE_CLOUD_TOPOLOGY_JSON_FILE);
    }

    public static Set<TopologyEntityDTO.Builder> readCloudTopologyFromJsonFile(String fileName)
                    throws FileNotFoundException, InvalidProtocolBufferException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        final URL topologyFileResource = classLoader.getResource(fileName);
        if (topologyFileResource == null) {
            throw new FileNotFoundException("Error reading " + fileName);
        }
        File file = new File(topologyFileResource.getFile());
        final InputStream dtoInputStream = new FileInputStream(file);
        InputStreamReader inputStreamReader = new InputStreamReader(dtoInputStream);
        JsonReader topologyReader = new JsonReader(inputStreamReader);
        List<Object> dtos = GSON.fromJson(topologyReader, List.class);

        Set<TopologyEntityDTO.Builder> topologyDTOBuilders = Sets.newHashSet();
        for (Object dto : dtos) {
            String dtoString = GSON.toJson(dto);
            TopologyEntityDTO.Builder entityDtoBuilder = TopologyEntityDTO.newBuilder();
            JsonFormat.parser().merge(dtoString, entityDtoBuilder);
            topologyDTOBuilders.add(entityDtoBuilder);
        }
        return topologyDTOBuilders;
    }

    /**
     * Load DTOs from a file generated using the hyper-v probe.
     * Move the DTOs through the whole pipe and return the actions generated.
     *
     * @param analysis {@link Analysis} mocked analysis configuration for unit test
     * @return The actions generated from the end2endTest DTOs.
     * @throws IOException If the file load fails.
     * @throws InvalidTopologyException not supposed to happen here
     */
    private AnalysisResults generateEnd2EndActions(Analysis analysis)
                    throws IOException, InvalidTopologyException {
        List<CommonDTO.EntityDTO> probeDTOs = messagesFromJsonFile(
                        "protobuf/messages/discoveredEntities.json", EntityDTO::newBuilder);

        Map<Long, CommonDTO.EntityDTO> map = Maps.newHashMap();
        IntStream.range(0, probeDTOs.size()).forEach(i -> map.put((long)i, probeDTOs.get(i)));
        Map<Long, TopologyEntityDTO> topoDTOs = SdkToTopologyEntityConverter
                        .convertToTopologyEntityDTOs(map).stream()
                        .map(TopologyEntityDTO.Builder::build)
                        .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));

        // Edit topology DTOs as if this is a TP pipeline
        // There are only 2 stages here today
        // 1. change analysis settings to add desired state
        // 2. change historical utilization in topo dtos
        editTopologyDTOMockPipleine(topoDTOs);

        TopologyConverter converter = new TopologyConverter(REALTIME_TOPOLOGY_INFO, true,
                MarketAnalysisUtils.QUOTE_FACTOR, MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR,
                marketPriceTable, ccd, CommodityIndex.newFactory(), tierExcluderFactory,
            consistentScalingHelperFactory);
        Set<TraderTO> economyDTOs = converter.convertToMarket(topoDTOs);
        mockCommsToAdjustForOverhead(analysis, converter);
        final TopologyInfo topologyInfo = TopologyInfo.newBuilder().setTopologyContextId(7L)
                        .setTopologyType(TopologyType.PLAN).setTopologyId(1L).build();

        final AnalysisConfig analysisConfig = AnalysisConfig
                        .newBuilder(MarketAnalysisUtils.QUOTE_FACTOR,
                                        MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR,
                                        SuspensionsThrottlingConfig.DEFAULT, Collections.emptyMap())
                        .setRightsizeLowerWatermark(rightsizeLowerWatermark)
                        .setRightsizeUpperWatermark(rightsizeUpperWatermark)
                        .setMaxPlacementsOverride(maxPlacementIterations)
                        .setUseQuoteCacheDuringSNM(useQuoteCacheDuringSNM).build();
        final Topology topology = TopologyEntitiesHandler.createTopology(economyDTOs, topologyInfo,
                analysisConfig, analysis);
        AnalysisResults results = TopologyEntitiesHandler.performAnalysis(economyDTOs, topologyInfo,
                        analysisConfig, analysis, topology);
        return results;
    }

    /**
     * Mock pipeline processing required for end to end market component test
     * since market component does not have access to TP pipeline code
     *
     * @param map The map of the topology entities.
     */
    private void editTopologyDTOMockPipleine(Map<Long, TopologyEntityDTO> map) {
        for (int i = 0; i < map.size(); i++) {
            TopologyEntityDTO entity = map.get((long)i);
            TopologyEntityDTO.Builder entityBuilder = entity.toBuilder();

            // update analysis settings on entity
            updateDesiredState(entityBuilder);

            // update historical utilization in sold and bought commodities
            updateHistoricalUtilization(entityBuilder, entity);

            // build it again to update values
            entity = entityBuilder.build();
            map.put((long)i, entity);
        }
    }

    /**
     * This method takes as parameter the map of the topology entities and sets all the historical
     * used and peaked values of the commodities to the respective used and peak values.
     *
     * @param entityBuilder TopologyDTO builder which is being modified
     * @param entity {@link TopologyEntityDTO} which is being modified
     */
    private void updateHistoricalUtilization(TopologyEntityDTO.Builder entityBuilder,
                    TopologyEntityDTO entity) {
        try {
            // sold commodities
            List<CommoditySoldDTO.Builder> commSoldBuilder =
                            entityBuilder.getCommoditySoldListBuilderList();
            for (int j = 0; j < commSoldBuilder.size(); j++) {
                CommoditySoldDTO.Builder commSold = commSoldBuilder.get(j);
                commSold.setHistoricalUsed(createHistUsedValue(commSold.getUsed()))
                                .setHistoricalPeak(createHistUsedValue(commSold.getPeak()));
            }
            List<CommoditySoldDTO> soldList = new ArrayList<>();
            for (CommoditySoldDTO.Builder commSold : commSoldBuilder) {
                soldList.add(commSold.build());
            }

            // bought commodities
            List<CommoditiesBoughtFromProvider.Builder> commBoughtFromProviderBuilder =
                            entityBuilder.getCommoditiesBoughtFromProvidersBuilderList();
            for (int j = 0; j < commBoughtFromProviderBuilder.size(); j++) {
                List<CommodityBoughtDTO.Builder> commBoughtBuilder = commBoughtFromProviderBuilder
                                .get(j).getCommodityBoughtBuilderList();
                for (int k = 0; k < commBoughtBuilder.size(); k++) {
                    CommodityBoughtDTO.Builder commBought = commBoughtBuilder.get(k);
                    commBought.setHistoricalUsed(createHistUsedValue(commBought.getUsed()))
                                    .setHistoricalPeak(createHistUsedValue(commBought.getPeak()));
                }
            }
            List<CommoditiesBoughtFromProvider> boughtList = new ArrayList<>();
            for (CommoditiesBoughtFromProvider.Builder commBought : commBoughtFromProviderBuilder) {
                boughtList.add(commBought.build());
            }

            entity = entityBuilder.clearCommoditySoldList().clearCommoditiesBoughtFromProviders()
                            .addAllCommoditySoldList(soldList)
                            .addAllCommoditiesBoughtFromProviders(boughtList).build();
        } catch (IndexOutOfBoundsException ioe) {
            logger.error("Exception trying to add historical utilization for entity "
                            + entity.getDisplayName(), ioe);
        } catch (NullPointerException ne) {
            logger.error("Exception trying to add historical utilization for entity "
                            + entity.getDisplayName(), ne);
        }
    }

    private static HistoricalValues createHistUsedValue(double value) {
        return HistoricalValues.newBuilder().setHistUtilization(value).build();
    }

    /**
     * Update the desired state inside analysis settings before running analysis
     * @param entityBuilder
     */
    private void updateDesiredState(TopologyEntityDTO.Builder entityBuilder) {
        entityBuilder.getAnalysisSettingsBuilder()
                        .setDesiredUtilizationTarget(desiredUtilizationTarget)
                        .setDesiredUtilizationRange(desiredUtilizationRange);
    }

    private long countSoldCommodities(Set<TraderTO> traderDTOs, String pattern) {
        return traderDTOs.stream().map(TraderTO::getCommoditiesSoldList).flatMap(List::stream)
                        .map(CommoditySoldTO::getSpecification)
                        .map(CommoditySpecificationTO::getDebugInfoNeverUseInCode)
                        .filter(key -> key.matches(pattern)).count();
    }

    private long countBoughtCommodities(Set<TraderTO> traderDTOs, String pattern) {
        return traderDTOs.stream().map(TraderTO::getShoppingListsList).flatMap(List::stream)
                        .map(ShoppingListTO::getCommoditiesBoughtList).flatMap(List::stream)
                        .map(CommodityBoughtTO::getSpecification)
                        .map(CommoditySpecificationTO::getDebugInfoNeverUseInCode)
                        .filter(key -> key.matches(pattern)).count();
    }

    /**
     * Load a json file that contains multiple DTOs, one per line.
     *
     * @param fileName the name of the file to load
     * @param builderSupplier The method to create a builder for Msg.
     * @param <Msg> The type of DTO.
     * @return A list of DTOs represented by the file
     * @throws IOException when the file is not found
     */
    public static <Msg extends AbstractMessage> List<Msg> messagesFromJsonFile(
                    @Nonnull final String fileName,
                    @Nonnull final Supplier<AbstractMessage.Builder> builderSupplier)
                    throws IOException {
        URL fileUrl = TopologyEntitiesHandlerTest.class.getClassLoader().getResources(fileName)
                        .nextElement();
        File file = new File(fileUrl.getFile());
        LineProcessor<List<Msg>> callback = new LineProcessor<List<Msg>>() {
            List<Msg> list = Lists.newArrayList();

            @Override
            public boolean processLine(@Nonnull String line) throws IOException {
                AbstractMessage.Builder builder = builderSupplier.get();
                JsonFormat.parser().merge(line, builder);
                return list.add((Msg)builder.build());
            }

            @Override
            public List<Msg> getResult() {
                // TODO Auto-generated method stub
                return list;
            }
        };
        return Files.readLines(file, Charset.defaultCharset(), callback);
    }

    /**
     * Helper method to check that bicliques are created correctly on the {@link TraderTO}.
     *
     * @param traderDTOs {@link TraderTO} to check
     * @param shopTogether true if shoptogether enabled
     */
    private void checkBicliques(final Set<TraderTO> traderDTOs, boolean shopTogether) {
        // Each storage is member of exactly one biclique (check using the 'cliques' property)
        final Set<Integer> stCliqueCounts = traderDTOs.stream()
                        .filter(trader -> trader.getDebugInfoNeverUseInCode().startsWith("STORAGE"))
                        .map(TraderTO::getCliquesCount).collect(Collectors.toSet());
        if (shopTogether) {
            assertFalse(stCliqueCounts.contains(0));
        } else {
            assertEquals(Sets.newHashSet(1), stCliqueCounts);
        }
        // Each PM is member of one or more bicliques (check using the 'cliques' property)
        final Set<Integer> pmCliqueCounts = traderDTOs.stream()
                        .filter(trader -> trader.getDebugInfoNeverUseInCode()
                                        .startsWith("PHYSICAL_MACHINE"))
                        .map(TraderTO::getCliquesCount).collect(Collectors.toSet());
        if (shopTogether) {
            assertEquals(Sets.newHashSet(1), pmCliqueCounts);
        } else {
            assertFalse(pmCliqueCounts.contains(0));
        }

        // All other traders don't have bicliques (check using the 'cliques' property
        final Set<Integer> otherCliqueCounts = traderDTOs.stream().filter(trader -> !trader
                        .getDebugInfoNeverUseInCode().startsWith("PHYSICAL_MACHINE")
                        && !trader.getDebugInfoNeverUseInCode().startsWith("STORAGE"))
                        .map(TraderTO::getCliquesCount).collect(Collectors.toSet());
        assertEquals(Sets.newHashSet(0), otherCliqueCounts);
    }
}
