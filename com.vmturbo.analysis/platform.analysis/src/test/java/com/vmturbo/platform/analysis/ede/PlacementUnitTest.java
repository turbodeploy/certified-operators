package com.vmturbo.platform.analysis.ede;

import static com.vmturbo.platform.analysis.ede.Placement.mergeContextSets;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Context;
import com.vmturbo.platform.analysis.economy.Context.BalanceAccount;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldSettingsTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceCost;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceLimitation;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageTierPriceData;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderSettingsTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderStateTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO.RiskBased;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO.SumOfCommodity;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.topology.Topology;
import com.vmturbo.platform.analysis.translators.ProtobufToAnalysis;
import com.vmturbo.platform.analysis.utilities.PlacementResults;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

public class PlacementUnitTest {

    /**
     * Test the quote computation on current supplier when it is active or inactive.
     */
    @Test
    public void testCurrentQuote() {
        Economy economy = new Economy();
        Trader vm = economy.addTrader(1, TraderState.ACTIVE, new Basket(TestUtils.VCPU),
                new Basket(TestUtils.CPU));
        Trader pm1 = TestUtils.createPM(economy, new ArrayList<>(), 100, 200, true);
        List<Entry<ShoppingList, Market>> movableSlByMarket = economy.getMarketsAsBuyer(vm).entrySet()
                .stream().collect(Collectors.toList());
        ShoppingList sl = movableSlByMarket.get(0).getKey();
        sl.setQuantity(0, 50);
        sl.move(pm1);
        pm1.getCommoditiesSold().get(0).setQuantity(50);
        assertEquals(2, Placement.computeCurrentQuote(economy, movableSlByMarket), TestUtils.FLOATING_POINT_DELTA);

        Trader pm2 = economy.addTrader(TestUtils.PM_TYPE, TraderState.INACTIVE, new Basket(TestUtils.CPU),
                                       new HashSet<>());
        pm2.getCommoditiesSold().get(0).setCapacity(100);
        pm2.getCommoditiesSold().get(0).setQuantity(50);
        sl.move(pm2);
        assertTrue(Double.isInfinite(Placement.computeCurrentQuote(economy, movableSlByMarket)));
    }

    /**
     * Test the quote computation returns the current seller as the best provider even though there is a different
     * provider got evaluated before and returned the same price.
     */
    @Test
    public void testCurrentQuoteEqualsBestQuote() {
        Economy economy = new Economy();
        Trader vm = economy.addTrader(1, TraderState.ACTIVE, new Basket(TestUtils.VCPU),
                new Basket(TestUtils.CPU));
        Trader pm1 = TestUtils.createPM(economy, new ArrayList<>(), 100, 200, true);
        Trader pm2 = TestUtils.createPM(economy, new ArrayList<>(), 100, 200, true);
        List<Entry<ShoppingList, Market>> movableSlByMarket = economy.getMarketsAsBuyer(vm).entrySet()
                .stream().collect(Collectors.toList());
        ShoppingList sl = movableSlByMarket.get(0).getKey();
        sl.setQuantity(0, 50);
        sl.move(pm2);
        pm2.getCommoditiesSold().get(0).setQuantity(50);
        QuoteMinimizer q = Placement.initiateQuoteMinimizer(economy, Lists.newArrayList(pm1, pm2), sl, null, 0);
        assertEquals(sl.getSupplier(), q.getBestSeller());
    }

    public void testGroupLeaderAlwaysMoves_ShopAlone() {
        testGroupLeaderAlwaysMoves(false);
    }

    public void testGroupLeaderAlwaysMoves_ShopTogether() {
        testGroupLeaderAlwaysMoves(true);
    }

    private void testGroupLeaderAlwaysMoves(boolean isShopTogether) {
        Economy economy = new Economy();
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, false);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, false);
        st1.setDebugInfoNeverUseInCode("DS1");
        Trader vm1 = TestUtils.createVM(economy);
        ShoppingList sl1 = TestUtils.createAndPlaceShoppingList(economy,
            Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{60, 0}, pm1);
        sl1.setGroupFactor(2);
        ShoppingList sl2 = TestUtils.createAndPlaceShoppingList(economy,
            Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{100}, st1);
        sl2.setGroupFactor(1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        vm1.getSettings().setIsShopTogether(isShopTogether);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        PlacementResults results = Placement.generatePlacementDecisions(economy, Arrays.asList(sl1, sl2));

        Assert.assertEquals(1, results.getActions().size());
        Action action = results.getActions().get(0);
        Move move = (Move)action;
        assertTrue(move.getSource() == pm1);
        assertTrue(move.getDestination() == pm1);
    }

    /**
     * Test case: pm1 and pm2 are in the same market. They both present in us east and us west
     * regions. st1 and st2 are in the same market. st1 present in us east and st2 present in
     * us west. All storages and hosts are under the same business account. The context combination
     * for the pm and st markets should be {us east, ba}, {us west, ba}.
     */
    @Test
    public void testPopulateContextCombination() {
        Economy economy = new Economy();
        final long usEastId = 100L;
        final long usWestId = 200L;
        final long baId = 1000L;
        final BalanceAccount ba = new BalanceAccount(baId);
        CommoditySpecification cpu = new CommoditySpecification(40);
        CommoditySpecification stAmt = new CommoditySpecification(8);
        Trader pm1 = TestUtils.createTrader(economy, 14, Arrays.asList(0L), Arrays.asList(cpu),
                new double[]{100}, false, false);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader pm2 = TestUtils.createTrader(economy, 14, Arrays.asList(0L), Arrays.asList(cpu),
                new double[]{100}, false, false);
        pm2.setDebugInfoNeverUseInCode("PM2");
        Trader st1 = TestUtils.createTrader(economy, 2, Arrays.asList(0L), Arrays.asList(stAmt),
                new double[]{50}, false, false);
        st1.setDebugInfoNeverUseInCode("ST1");
        Trader st2 = TestUtils.createTrader(economy, 2, Arrays.asList(0L), Arrays.asList(stAmt),
                new double[]{50}, false, false);
        st2.setDebugInfoNeverUseInCode("ST2");
        Trader vm = TestUtils.createVM(economy, "VM");
        TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(cpu), vm, new double[]{90, 0}, pm1);
        TestUtils.createAndPlaceShoppingList(economy, Arrays.asList(stAmt), vm, new double[]{10}, st1);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();
        economy.getTraderWithContextMap().put(pm1, Arrays.asList(new Context(usEastId, usEastId, ba),
                new Context(usWestId, usWestId, ba)));
        economy.getTraderWithContextMap().put(pm2, Arrays.asList(new Context(usEastId, usEastId, ba),
                new Context(usWestId, usWestId, ba)));
        economy.getTraderWithContextMap().put(st1, Arrays.asList(new Context(usEastId, usEastId, ba)));
        economy.getTraderWithContextMap().put(st2, Arrays.asList(new Context(usWestId, usWestId, ba)));
        Set<Context> contexts = Placement.populateContextCombination(economy.getMarketsAsBuyer(vm).values().stream()
                .collect(Collectors.toSet()), economy);
        Assert.assertTrue(contexts.size() == 2);
        contexts.forEach(c -> {
            Assert.assertTrue(c.getRegionId() == usEastId || c.getRegionId() == usWestId);
            Assert.assertTrue(c.getBalanceAccount().getId() == baId);
        });
    }

    /**
     * Test with a vm buying from compute and storage tiers. Both tiers exist in usEast and
     * usWest regions. The compute tier price on {usEast, linux license} is 1.0, {usEast, windows
     * license} is 10.0, {usWest, linux license} is 5.0, {usWest, windows license} is 12.0.
     * The storage tier price for storage amount on usEast is 0.5 per unit, usWest is 1 per unit.
     * The vm asks for linux license will choose compute and storage tiers on usEast.
     */
    @Test
    public void testComputeBestQuoteForMCPWithMultiRegions() {
        long usEast = 111L;
        long usWest = 112L;
        long baId = 10L;
        int linuxLicense = 500;
        int windowsLicense = 700;
        CommoditySpecificationTO stAmtSpecificatinTO = CommoditySpecificationTO.newBuilder()
                .setBaseType(CommodityType.STORAGE_AMOUNT_VALUE)
                .setType(CommodityType.STORAGE_AMOUNT_VALUE).build();
        TraderTO vm = TraderTO.newBuilder().setOid(10000L)
                .setSettings(TraderSettingsTO.newBuilder().setIsShopTogether(true)
                        .setQuoteFunction(QuoteFunctionDTO.newBuilder()
                                .setSumOfCommodity(SumOfCommodity.getDefaultInstance())))
                .setState(TraderStateTO.ACTIVE).setDebugInfoNeverUseInCode("vm")
                .addShoppingLists(ShoppingListTO.newBuilder().setOid(20001L).setMovable(true)
                        .addCommoditiesBought(CommodityBoughtTO.newBuilder().setQuantity(1)
                                .setSpecification(CommoditySpecificationTO.newBuilder()
                                        .setBaseType(CommodityType.CPU_VALUE)
                                        .setType(CommodityType.CPU_VALUE)))
                        .addCommoditiesBought(CommodityBoughtTO.newBuilder().setQuantity(100)
                                .setSpecification(CommoditySpecificationTO.newBuilder()
                                        .setBaseType(CommodityType.LICENSE_ACCESS_VALUE)
                                        .setType(linuxLicense))))
                .addShoppingLists(ShoppingListTO.newBuilder().setOid(20002L).setMovable(true)
                        .addCommoditiesBought(CommodityBoughtTO.newBuilder().setQuantity(50)
                                .setSpecification(stAmtSpecificatinTO)))
                .build();
        TraderTO computeTier1 = TraderTO.newBuilder().setOid(10001L).addAllCliques(Arrays.asList(0L))
                .setState(TraderStateTO.ACTIVE).setDebugInfoNeverUseInCode("computeTier")
                .setSettings(TraderSettingsTO.newBuilder().setCanAcceptNewCustomers(true)
                        .setQuoteFunction(QuoteFunctionDTO.newBuilder()
                        .setRiskBased(RiskBased.newBuilder().setCloudCost(CostDTO.newBuilder()
                                .setComputeTierCost(ComputeTierCostDTO.newBuilder()
                                        .setCouponBaseType(CommodityType.COUPON_VALUE)
                                        .setLicenseCommodityBaseType(CommodityType.LICENSE_ACCESS_VALUE)
                                        .addCostTupleList(CostTuple.newBuilder().setBusinessAccountId(baId)
                                                .setRegionId(usEast).setPrice(10.0)
                                                .setLicenseCommodityType(windowsLicense))
                                        .addCostTupleList(CostTuple.newBuilder().setBusinessAccountId(baId)
                                                .setRegionId(usWest).setPrice(12.0)
                                                .setLicenseCommodityType(windowsLicense))
                                        .addCostTupleList(CostTuple.newBuilder().setBusinessAccountId(baId)
                                                .setRegionId(usEast).setPrice(1.0)
                                                .setLicenseCommodityType(linuxLicense))
                                        .addCostTupleList(CostTuple.newBuilder().setBusinessAccountId(baId)
                                                .setRegionId(usWest).setPrice(5.0)
                                                .setLicenseCommodityType(linuxLicense)))))))
                .addCommoditiesSold(CommoditySoldTO.newBuilder().setCapacity(1000).setQuantity(1)
                        .setSpecification(CommoditySpecificationTO.newBuilder()
                                .setBaseType(CommodityType.CPU_VALUE).setType(CommodityType.CPU_VALUE)))
                .addCommoditiesSold(CommoditySoldTO.newBuilder().setCapacity(1000).setQuantity(1)
                        .setSettings(CommoditySoldSettingsTO.newBuilder()
                                .setPriceFunction(PriceFunctionTO.newBuilder()
                                        .setStandardWeighted(PriceFunctionTO.StandardWeighted
                                                .newBuilder().setWeight(1.0f))))
                        .setSpecification(CommoditySpecificationTO.newBuilder()
                                .setBaseType(CommodityType.LICENSE_ACCESS_VALUE).setType(linuxLicense)))
                .addCommoditiesSold(CommoditySoldTO.newBuilder().setCapacity(1000).setQuantity(1)
                        .setSettings(CommoditySoldSettingsTO.newBuilder()
                                .setPriceFunction(PriceFunctionTO.newBuilder()
                                        .setStandardWeighted(PriceFunctionTO.StandardWeighted
                                                .newBuilder().setWeight(1.0f))))
                        .setSpecification(CommoditySpecificationTO.newBuilder()
                                .setBaseType(CommodityType.LICENSE_ACCESS_VALUE).setType(windowsLicense))).build();

        TraderTO storageTier1 = TraderTO.newBuilder().setOid(10002L).addAllCliques(Arrays.asList(0L))
                .setState(TraderStateTO.ACTIVE).setDebugInfoNeverUseInCode("stTier")
                .setSettings(TraderSettingsTO.newBuilder().setCanAcceptNewCustomers(true)
                        .setQuoteFunction(QuoteFunctionDTO.newBuilder()
                        .setRiskBased(RiskBased.newBuilder().setCloudCost(CostDTO.newBuilder()
                                .setStorageTierCost(StorageTierCostDTO.newBuilder()
                                        .addStorageResourceCost(StorageResourceCost.newBuilder()
                                                .setResourceType(CommoditySpecificationTO.newBuilder()
                                                        .setBaseType(CommodityType.STORAGE_AMOUNT_VALUE)
                                                        .setType(CommodityType.STORAGE_AMOUNT_VALUE))
                                                .addStorageTierPriceData(StorageTierPriceData
                                                        .newBuilder().addCostTupleList(CostTuple
                                                                .newBuilder().setBusinessAccountId(baId)
                                                                .setRegionId(usWest).setPrice(1))
                                                        .addCostTupleList(CostTuple.newBuilder()
                                                                .setBusinessAccountId(baId)
                                                                .setRegionId(usEast)
                                                                .setPrice(0.5))
                                                        .setIsUnitPrice(true)
                                                        .setUpperBound(Double.MAX_VALUE)))
                                        .addStorageResourceLimitation(StorageResourceLimitation.newBuilder()
                                                .setResourceType(stAmtSpecificatinTO).setMaxCapacity(100000)).build())))))
                .addCommoditiesSold(CommoditySoldTO.newBuilder().setCapacity(10000).setQuantity(1)
                        .setSettings(CommoditySoldSettingsTO.newBuilder()
                                .setPriceFunction(PriceFunctionTO.newBuilder()
                                        .setStandardWeighted(PriceFunctionTO.StandardWeighted
                                                .newBuilder().setWeight(1.0f))))
                        .setSpecification(stAmtSpecificatinTO))
                .build();
        Topology topology = new Topology();
        ProtobufToAnalysis.addTrader(topology, computeTier1);
        ProtobufToAnalysis.addTrader(topology, storageTier1);
        Trader vmTrader = ProtobufToAnalysis.addTrader(topology, vm);
        topology.getEconomyForTesting().populateMarketsWithSellersAndMergeConsumerCoverage();
        CliqueMinimizer minimizer = Placement.computeBestQuote(topology.getEconomyForTesting(), vmTrader);
        assertTrue(minimizer != null);
        assertFalse(minimizer.getShoppingListContextMap().isEmpty());

    }

    /**
     * Checks if after context merge, the parent id is set correctly, where applicable. Parent id
     * is needed for fetching cost tuple for CBTP cost table.
     */
    @Test
    public void contextMerger() {
        // Account/pricing id.
        final long accountId = 100L;
        // Id of parent (BillingFamily).
        final long parentId = 200L;
        final long regionId = 300L;
        final long zoneId = 400L;

        // Only CBTP context has the parent id set.
        final Context cbtpContext = new Context(regionId, zoneId,
                new BalanceAccount(accountId, parentId));
        final Context computeContext = new Context(regionId, zoneId,
                new BalanceAccount(accountId));
        final Context storageContext = new Context(regionId, zoneId,
                new BalanceAccount(accountId));

        final Set<Context> contextSet1 = ImmutableSet.of(cbtpContext, computeContext);
        final Set<Context> contextSet2 = ImmutableSet.of(storageContext);
        final Set<Context> resultSet = Stream.of(contextSet1, contextSet2)
                .reduce(mergeContextSets)
                .orElse(Collections.emptySet());

        assertEquals(1, resultSet.size());
        final Context resultContext = resultSet.iterator().next();
        assertEquals(regionId, resultContext.getRegionId());
        assertEquals(zoneId, resultContext.getZoneId());
        assertEquals(accountId, resultContext.getBalanceAccount().getId());
        final Long resultParentId = resultContext.getBalanceAccount().getParentId();
        assertNotNull(resultParentId);
        assertEquals(parentId, resultParentId.longValue());
    }

    /**
     * When MPC use case, it can happen that source and destination contexts only differ by the
     * parentId of the balanceAccount - one has the parentId set and one does not. We want to
     * consider two contexts the same in this situation.
     */
    @Test
    public void testContextMergerSet() {
        // Account/pricing id.
        final long accountId = 100L;
        // Id of parent (BillingFamily).
        final long parentId = 200L;
        final long regionId = 300L;
        final long zoneId = 400L;

        // Context1 has parent ID in balanceAccount, but context2 does not.
        final Context context1 = new Context(regionId, zoneId,
                new BalanceAccount(accountId, parentId));
        final Context context2 = new Context(regionId, zoneId,
                new BalanceAccount(accountId));

        final Set<Context> contextSet1 = ImmutableSet.of(context1);
        final Set<Context> contextSet2 = ImmutableSet.of(context2);
        final Set<Context> resultSet = Stream.of(contextSet1, contextSet2)
                .reduce(mergeContextSets)
                .orElse(Collections.emptySet());

        assertEquals(1, resultSet.size());
        final Context resultContext = resultSet.iterator().next();
        assertEquals(regionId, resultContext.getRegionId());
        assertEquals(zoneId, resultContext.getZoneId());
        assertEquals(accountId, resultContext.getBalanceAccount().getId());
        final Long resultParentId = resultContext.getBalanceAccount().getParentId();
        assertNotNull(resultParentId);
        assertEquals(parentId, resultParentId.longValue());
    }
}
