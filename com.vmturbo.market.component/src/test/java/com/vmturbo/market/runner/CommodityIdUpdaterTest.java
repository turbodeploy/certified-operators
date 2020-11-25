package com.vmturbo.market.runner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.analysis.NumericIDAllocator;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator.TopologyCostCalculatorFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.market.AnalysisRICoverageListener;
import com.vmturbo.market.reservations.InitialPlacementFinder;
import com.vmturbo.market.reserved.instance.analysis.BuyRIImpactAnalysisFactory;
import com.vmturbo.market.runner.AnalysisFactory.AnalysisConfig;
import com.vmturbo.market.runner.cost.MarketPriceTableFactory;
import com.vmturbo.market.runner.cost.MigratedWorkloadCloudCommitmentAnalysisService;
import com.vmturbo.market.runner.wastedfiles.WastedFilesAnalysisEngine;
import com.vmturbo.market.topology.conversions.CommodityTypeAllocator;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper.ConsistentScalingHelperFactory;
import com.vmturbo.market.topology.conversions.MarketAnalysisUtils;
import com.vmturbo.market.topology.conversions.ReversibilitySettingFetcherFactory;
import com.vmturbo.market.topology.conversions.TierExcluder.TierExcluderFactory;
import com.vmturbo.platform.analysis.actions.Activate;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.actions.ProvisionBySupply;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.economy.TraderWithSettings;
import com.vmturbo.platform.analysis.ede.ReplayActions;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;

/**
 * Test {@link CommodityIdUpdater}.
 */
public class CommodityIdUpdaterTest {

    private CommodityIdUpdater commodityIdUpdater;

    private Analysis analysis;

    /**
     * Common code to run before all tests.
     */
    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
        commodityIdUpdater = new CommodityIdUpdater();
        analysis = new Analysis(TopologyInfo.getDefaultInstance(), Collections.emptySet(), mock(GroupMemberRetriever.class),
            Clock.systemUTC(), AnalysisConfig.newBuilder(MarketAnalysisUtils.QUOTE_FACTOR,
            MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR, SuspensionsThrottlingConfig.DEFAULT, Collections.emptyMap(), false).build(),
            mock(TopologyEntityCloudTopologyFactory.class), mock(TopologyCostCalculatorFactory.class),
            mock(MarketPriceTableFactory.class), mock(WastedFilesAnalysisEngine.class),
            mock(BuyRIImpactAnalysisFactory.class), mock(TierExcluderFactory.class),
            mock(AnalysisRICoverageListener.class), mock(ConsistentScalingHelperFactory.class),
            mock(InitialPlacementFinder.class), mock(ReversibilitySettingFetcherFactory.class),
            mock(MigratedWorkloadCloudCommitmentAnalysisService.class), commodityIdUpdater);
    }

    /**
     * Save old commodity id to commodity type mapping and
     * update replay actions with new commodity id to commodity type mapping.
     */
    @Test
    public void testCommodityIdUpdater() {
        // Create a list of commodity types and use CommodityTypeAllocator to allocate commodity id
        final ImmutableList<CommodityType> commodityTypes = ImmutableList.of(
            CommodityType.newBuilder().setType(0).build(),
            CommodityType.newBuilder().setType(1).setKey("1").build(),
            CommodityType.newBuilder().setType(2).build(),
            CommodityType.newBuilder().setType(3).setKey("3").build(),
            CommodityType.newBuilder().setType(4).build(),
            CommodityType.newBuilder().setType(5).setKey("5").build(),
            CommodityType.newBuilder().setType(6).build());

        // First broadcast
        final CommodityTypeAllocator commodityTypeAllocator = new CommodityTypeAllocator(new NumericIDAllocator());
        commodityTypes.forEach(commodityTypeAllocator::topologyToMarketCommodityId);

        final Economy economy = new Economy();
        // Create replay actions
        final Deactivate deactivate = new Deactivate(economy, mock(Trader.class),
            new Basket(new CommoditySpecification(0, 0), new CommoditySpecification(1, 1)));
        final Activate activate = new Activate(economy, mock(Trader.class),
            new Basket(new CommoditySpecification(2, 2), new CommoditySpecification(3, 3)),
            mock(Trader.class), new CommoditySpecification(4, 4));
        final ProvisionBySupply provisionBySupply = new ProvisionBySupply(economy,
            new TraderWithSettings(0, 0, TraderState.ACTIVE, new Basket()),
            new CommoditySpecification(5, 5));
        final ProvisionBySupply provisionBySupplyWontReplay = new ProvisionBySupply(economy,
            new TraderWithSettings(0, 0, TraderState.ACTIVE, new Basket()),
            new CommoditySpecification(6, 6));

        analysis.setReplayActions(new ReplayActions(
            Arrays.asList(activate, provisionBySupply, provisionBySupplyWontReplay),
            Collections.singletonList(deactivate)));
        commodityIdUpdater.saveCommodityIdToCommodityType(commodityTypeAllocator,
            analysis.getReplayActions());

        // Make sure commodity id to commodity type is correct
        final Map<Integer, CommodityType> commodityIdToCommodityType =
            commodityIdUpdater.getCommodityIdToCommodityType();
        assertEquals(commodityTypes.size(), commodityIdToCommodityType.size());
        for (int i = 0; i < commodityTypes.size(); i++) {
            assertEquals(commodityTypes.get(i), commodityIdToCommodityType.get(i));
        }

        // Second broadcast
        final CommodityTypeAllocator newCommodityTypeAllocator = new CommodityTypeAllocator(new NumericIDAllocator());
        // Allocate commodity id in the reversed order and skip one commodity type.
        commodityTypes.reverse().subList(1, commodityTypes.size())
            .forEach(newCommodityTypeAllocator::topologyToMarketCommodityId);
        final int oldSize = newCommodityTypeAllocator.size();

        commodityIdUpdater.updateReplayActions(analysis, newCommodityTypeAllocator);
        final ReplayActions newReplayActions = analysis.getReplayActions();

        // Check if new reply actions are correct
        assertEquals(1, newReplayActions.getDeactivateActions().size());
        final Deactivate newDeactivate = newReplayActions.getDeactivateActions().get(0);
        assertEquals(new Basket(new CommoditySpecification(5, 0), new CommoditySpecification(4, 1)),
            newDeactivate.getTriggeringBasket());

        // provisionBySupplyWontReplay will not be replayed because the commodity type doesn't exist.
        assertEquals(2, newReplayActions.getActions().size());
        final Activate newActivate = (Activate)newReplayActions.getActions().get(0);
        assertEquals(new Basket(new CommoditySpecification(3, 2), new CommoditySpecification(2, 3)),
            newActivate.getTriggeringBasket());
        assertEquals(new CommoditySpecification(1, 4), newActivate.getReason());

        final ProvisionBySupply newProvisionBySupply = (ProvisionBySupply)newReplayActions.getActions().get(1);
        assertEquals(new CommoditySpecification(0, 5), newProvisionBySupply.getReason());

        assertEquals(oldSize, newCommodityTypeAllocator.size());
    }
}
