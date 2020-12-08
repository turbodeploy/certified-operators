package com.vmturbo.market.reservations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer.InitialPlacementCommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.market.reservations.EconomyCaches.EconomyCachesState;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.topology.Topology;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for {@link EconomyCaches}.
 */
public class EconomyCachesTest {

    private static final int PM_TYPE = EntityType.PHYSICAL_MACHINE_VALUE;
    private static final int VM_TYPE = EntityType.VIRTUAL_MACHINE_VALUE;
    private static final int MEM_TYPE = CommodityType.MEM_VALUE;
    private static final int CLUSTER1_COMM_SPEC_TYPE = 300;
    private static final int CLUSTER2_COMM_SPEC_TYPE = 400;
    private static final String cluster1Key = "cluster1";
    private static final String cluster2Key = "cluster2";
    private static final double pmMemCapacity = 100;
    private static final long pm1Oid = 1111L;
    private static final double pm1MemUsed = 20;
    private static final long pm2Oid = 1112L;
    private static final double pm2MemUsed = 30;
    private static final long pm3Oid = 1113L;
    private static final double pm3MemUsed = 10;
    private static final long pm4Oid = 1114L;
    private static final double pm4MemUsed = 20;
    private EconomyCaches economyCaches = new EconomyCaches();
    private static final BiMap<TopologyDTO.CommodityType, Integer> commTypeToSpecMap = HashBiMap.create();

    /**
     * Create the commodity type to spec mapping.
     */
    @BeforeClass
    public static void setUp() {
        commTypeToSpecMap.put(TopologyDTO.CommodityType.newBuilder()
                .setType(CommodityType.MEM_VALUE).build(), MEM_TYPE);
        commTypeToSpecMap.put(TopologyDTO.CommodityType.newBuilder()
                .setType(CommodityType.CLUSTER_VALUE).setKey(cluster1Key).build(),
                CLUSTER1_COMM_SPEC_TYPE);
        commTypeToSpecMap.put(TopologyDTO.CommodityType.newBuilder()
                .setType(CommodityType.CLUSTER_VALUE).setKey(cluster2Key).build(),
                CLUSTER2_COMM_SPEC_TYPE);
    }

    /**
     * Construct an existing reservation with 1 buyer. Pass an economy with 2 pms as the real time
     * economy. Verify that the update method add and applies the existing reservation vm.
     * The existing reservation buyer is placed on pm1, consuming 20 mem.
     * Expected: the update economy should contain 1 vm, 2 pms. Pm1 mem used should include vm's used.
     */
    @Test
    public void testUpdateRealtimeCachedEconomy() {
        long buyerOid = 1234L;
        long buyerSlOid = 1000L;
        long reservationOid = 1L;
        double buyerMemUsed = 20;
        InitialPlacementBuyer buyer = initialPlacementBuyer(buyerOid, buyerSlOid, VM_TYPE, new HashMap() {{
            put(TopologyDTO.CommodityType.newBuilder().setType(MEM_TYPE).build(), buyerMemUsed);
        }});
        Map<Long, List<InitialPlacementBuyer>> existingReservations =  new HashMap() {{
                put(reservationOid, Arrays.asList(buyer));
        }};
        Map<Long, List<InitialPlacementDecision>> buyerPlacements = new HashMap() {{
                put(buyerOid, Arrays.asList(new InitialPlacementDecision(buyerSlOid,
                        Optional.of(pm1Oid), new ArrayList())));
        }};
        Assert.assertTrue(economyCaches.getState() == EconomyCachesState.NOT_READY);
        economyCaches.updateRealtimeCachedEconomy(simpleEconomy(), commTypeToSpecMap,
                buyerPlacements, existingReservations);
        Economy newEconomy =  economyCaches.realtimeCachedEconomy;
        Assert.assertTrue(newEconomy.getTraders().size() == 3);
        Trader trader = newEconomy.getTopology().getTradersByOid().get(buyerOid);
        Assert.assertTrue(trader != null && newEconomy.getMarketsAsBuyer(trader).keySet().stream()
                .anyMatch(sl -> sl.getSupplier().getOid() == pm1Oid));
        Trader pm1 = newEconomy.getTopology().getTradersByOid().get(pm1Oid);
        Assert.assertTrue(pm1.getCommoditiesSold().stream().allMatch(c -> c.getQuantity() == buyerMemUsed + pm1MemUsed));
        Trader pm2 = newEconomy.getTopology().getTradersByOid().get(pm2Oid);
        Assert.assertTrue(pm2.getCommoditiesSold().stream().allMatch(c -> c.getQuantity() ==  pm2MemUsed));
        Assert.assertTrue(economyCaches.getState() == EconomyCachesState.REALTIME_READY);
        Assert.assertTrue(newEconomy.getMarketsAsBuyer(trader).keySet().stream()
                .allMatch(sl -> sl.isMovable() == false));
    }

    /**
     * Construct an existing reservation with 1 buyer. Pass an economy with 2 pms as the historical
     * economy. Verify that the update method rerun the existing reservation vm.
     * The existing reservation buyer was placed on pm2.
     * Expected: the update economy should contain 1 vm, 2 pms. After rerun the vm should be placed
     * on pm1.
     */
    @Test
    public void testUpdateHistoricalCachedEconomyNoBoundary() {
        long buyerOid = 1234L;
        long buyerSlOid = 1000L;
        long reservationOid = 1L;
        double buyerMemUsed = 20;
        InitialPlacementBuyer buyer = initialPlacementBuyer(buyerOid, buyerSlOid, VM_TYPE, new HashMap() {{
            put(TopologyDTO.CommodityType.newBuilder().setType(MEM_TYPE).build(), buyerMemUsed / 1.0);
        }});
        Map<Long, List<InitialPlacementBuyer>> existingReservations =  new HashMap() {{
            put(reservationOid, Arrays.asList(buyer));
        }};
        Map<Long, List<InitialPlacementDecision>> buyerPlacements = new HashMap() {{
            put(buyerOid, Arrays.asList(new InitialPlacementDecision(buyerSlOid,
                    Optional.of(pm2Oid), new ArrayList())));
        }};
        Assert.assertTrue(economyCaches.getState() == EconomyCachesState.NOT_READY);
        Map<Long, List<InitialPlacementDecision>> newPlacements = economyCaches
                .updateHistoricalCachedEconomy(simpleEconomy(), commTypeToSpecMap, buyerPlacements,
                        existingReservations);
        Economy newEconomy =  economyCaches.historicalCachedEconomy;
        Assert.assertTrue(newPlacements.get(buyerOid).stream().allMatch(i -> i.supplier.get() == pm1Oid));
        Assert.assertTrue(newEconomy.getTraders().size() == 3);
        Trader trader = newEconomy.getTopology().getTradersByOid().get(buyerOid);
        Assert.assertTrue(trader != null && newEconomy.getMarketsAsBuyer(trader).keySet().stream()
                .anyMatch(sl -> sl.getSupplier().getOid() == pm1Oid));
        Trader pm1 = newEconomy.getTopology().getTradersByOid().get(pm1Oid);
        Assert.assertTrue(pm1.getCommoditiesSold().stream()
                .allMatch(c -> c.getQuantity() == buyerMemUsed + pm1MemUsed));
        Trader pm2 = newEconomy.getTopology().getTradersByOid().get(pm2Oid);
        Assert.assertTrue(pm2.getCommoditiesSold().stream().allMatch(c -> c.getQuantity() ==  pm2MemUsed));
        Assert.assertTrue(economyCaches.getState() == EconomyCachesState.HISTORICAL_READY);
        Assert.assertTrue(newEconomy.getMarketsAsBuyer(trader).keySet().stream()
                .allMatch(sl -> sl.isMovable() == false));
    }

    /**
     * Construct an existing reservation with 1 buyer. Pass an economy with 4 pms in 2 different clusters
     * as the historical economy. Verify that the update method rerun the existing reservation vm.
     * The existing reservation buyer was placed on pm2.
     * Expected: the update economy should contain 1 vm, 4 pms. After rerun the vm should be placed
     * on pm1. Even though pm4 has the lowest used, but it is not in the same cluster.
     */
    @Test
    public void testUpdateHistoricalCachedEconomyWithClusterBoundary() {
        long buyerOid = 1234L;
        long buyerSlOid = 1000L;
        long reservationOid = 1L;
        double buyerMemUsed = 20;
        InitialPlacementBuyer buyer = initialPlacementBuyer(buyerOid, buyerSlOid, VM_TYPE, new HashMap() {{
            put(TopologyDTO.CommodityType.newBuilder().setType(MEM_TYPE).build(), new Double(buyerMemUsed));
            put(TopologyDTO.CommodityType.newBuilder().setType(CommodityType.CLUSTER_VALUE)
                    .setKey(cluster1Key).build(), 1.0d);
        }});
        Map<Long, List<InitialPlacementBuyer>> existingReservations =  new HashMap() {{
            put(reservationOid, Arrays.asList(buyer));
        }};
        Map<Long, List<InitialPlacementDecision>> buyerPlacements = new HashMap() {{
            put(buyerOid, Arrays.asList(new InitialPlacementDecision(buyerSlOid,
                    Optional.of(pm2Oid), new ArrayList())));
        }};
        Assert.assertTrue(economyCaches.getState() == EconomyCachesState.NOT_READY);
        Map<Long, List<InitialPlacementDecision>> newPlacements = economyCaches
                .updateHistoricalCachedEconomy(economyWithCluster(new double[]{pm1MemUsed, pm2MemUsed,
                        pm3MemUsed, pm4MemUsed}), commTypeToSpecMap, buyerPlacements, existingReservations);
        Economy newEconomy =  economyCaches.historicalCachedEconomy;
        Assert.assertTrue(newPlacements.get(buyerOid).stream().allMatch(i -> i.supplier.get() == pm1Oid));
        Assert.assertTrue(newEconomy.getTraders().size() == 5);
        Trader trader = newEconomy.getTopology().getTradersByOid().get(buyerOid);
        Assert.assertTrue(trader != null && newEconomy.getMarketsAsBuyer(trader).keySet().stream()
                .anyMatch(sl -> sl.getSupplier().getOid() == pm1Oid));
        Trader pm1 = newEconomy.getTopology().getTradersByOid().get(pm1Oid);
        Assert.assertTrue(pm1.getCommoditiesSold().stream()
                .anyMatch(c -> c.getQuantity() == buyerMemUsed + pm1MemUsed));
        Assert.assertTrue(newEconomy.getMarketsAsBuyer(trader).keySet().stream()
                .allMatch(sl -> sl.isMovable() == false));
    }

    /**
     * Construct an existing reservation with 1 buyer. Pass an economy with 2 pms.
     * Verify that the update method rerun the existing reservation vm.
     * The existing reservation buyer was placed on pm2. But now the vm requests more mem than both pms.
     * Expected: the update economy should contain only 2 pms. After rerun the vm should be unplaced.
     */
    @Test
    public void testUpdateHistoricalCachedEconomyReplayFailed() {
        long buyerOid = 1234L;
        long buyerSlOid = 1000L;
        long reservationOid = 1L;
        double buyerMemUsed = 90; // This is more than the pm's mem available
        InitialPlacementBuyer buyer = initialPlacementBuyer(buyerOid, buyerSlOid, VM_TYPE, new HashMap() {{
            put(TopologyDTO.CommodityType.newBuilder().setType(MEM_TYPE).build(), new Double(buyerMemUsed));
        }});
        Map<Long, List<InitialPlacementBuyer>> existingReservations =  new HashMap() {{
            put(reservationOid, Arrays.asList(buyer));
        }};
        Map<Long, List<InitialPlacementDecision>> buyerPlacements = new HashMap() {{
            put(buyerOid, Arrays.asList(new InitialPlacementDecision(buyerSlOid,
                    Optional.of(pm2Oid), new ArrayList())));
        }};
        economyCaches.setState(EconomyCachesState.READY);
        economyCaches.updateRealtimeCachedEconomy(simpleEconomy(), commTypeToSpecMap, buyerPlacements,
                        existingReservations);
        Map<Long, List<InitialPlacementDecision>> newPlacements = economyCaches
                .updateHistoricalCachedEconomy(simpleEconomy(), commTypeToSpecMap, buyerPlacements,
                        existingReservations);
        Economy newEconomy =  economyCaches.historicalCachedEconomy;
        Assert.assertTrue(newPlacements.get(buyerOid).stream()
                .allMatch(i -> i.supplier.equals(Optional.empty())));
        Assert.assertTrue(newEconomy.getTraders().size() == 2);
        Assert.assertTrue(newEconomy.getTraders().stream().allMatch(t -> t.getType() == PM_TYPE));
        Trader pm2 = newEconomy.getTopology().getTradersByOid().get(pm2Oid);
        Assert.assertTrue(pm2.getCommoditiesSold().stream().anyMatch(c -> c.getQuantity() ==  pm2MemUsed));
        Assert.assertFalse(newEconomy.getTopology().getTradersByOid().containsKey(buyerOid));
        Assert.assertFalse(newEconomy.getTopology().getShoppingListOids().inverse().containsKey(buyerSlOid));
    }

    /**
     * Construct an existing reservation with 1 buyer. Pass an economy with 2 pms.
     * First place the buyer on pm2. The call remove method to delete the buyer.
     * Expected: after deletion, the buyer should no longer exist in the economy.
     */
    @Test
    public void testRemoveDeletedTraders() {
        long buyerOid = 1234L;
        long buyerSlOid = 1000L;
        long reservationOid = 1L;
        double buyerMemUsed = 10;
        InitialPlacementBuyer buyer = initialPlacementBuyer(buyerOid, buyerSlOid, VM_TYPE, new HashMap() {{
            put(TopologyDTO.CommodityType.newBuilder().setType(MEM_TYPE).build(), new Double(buyerMemUsed));
        }});
        Map<Long, List<InitialPlacementBuyer>> existingReservations =  new HashMap() {{
            put(reservationOid, Arrays.asList(buyer));
        }};
        Map<Long, List<InitialPlacementDecision>> buyerPlacements = new HashMap() {{
            put(buyerOid, Arrays.asList(new InitialPlacementDecision(buyerSlOid,
                    Optional.of(pm2Oid), new ArrayList())));
        }};
        // Add the existing reservation buyer to the simple economy and find placement for it.
        Map<Long, List<InitialPlacementDecision>> newPlacements = economyCaches
                .updateHistoricalCachedEconomy(simpleEconomy(), commTypeToSpecMap, buyerPlacements,
                        existingReservations);
        Economy newEconomy =  economyCaches.historicalCachedEconomy;
        // Verify that economy now contains the buyer, and it is placed on pm1.
        Assert.assertTrue(newEconomy.getTraders().stream().anyMatch(t -> t.getOid() == buyerOid));
        Assert.assertTrue(newEconomy.getTopology().getTradersByOid().containsKey(buyerOid));
        Assert.assertTrue(newEconomy.getTopology().getShoppingListOids().inverse().containsKey(buyerSlOid));
        Trader pm1 = newEconomy.getTopology().getTradersByOid().get(pm1Oid);
        Assert.assertTrue(pm1.getCommoditiesSold().stream().anyMatch(c -> c.getQuantity()
                == pm1MemUsed + buyerMemUsed));
        // Remove buyer from economy
        economyCaches.removeDeletedTraders(newEconomy, new HashSet(Arrays.asList(buyerOid)));
        // Verify buyer is removed completely from economy.
        Assert.assertFalse(newEconomy.getTraders().stream().anyMatch(t -> t.getOid() == buyerOid));
        Assert.assertFalse(newEconomy.getTopology().getTradersByOid().containsKey(buyerOid));
        Assert.assertFalse(newEconomy.getTopology().getShoppingListOids().inverse().containsKey(buyerSlOid));
        Trader pm1AfterRemove = newEconomy.getTopology().getTradersByOid().get(pm1Oid);
        Assert.assertTrue(pm1AfterRemove.getCommoditiesSold().stream().anyMatch(c -> c.getQuantity()
                == pm1MemUsed));
    }

    /**
     * Construct an existing reservation with 1 buyer. Pass an economy with 4 pms in 2 different
     * clusters as the historical economy. Pass the same economy with 4 pms as the real time economy.
     * The lowest util host is pm3 in cluster 2.
     * Expected: buyer is placed on pm4 in both historical and real time economy caches.
     * The findInitialPlacement returns pm3 as the supplier.
     */
    @Test
    public void testFindInitialPlacementSuccess() {
        long buyerOid = 1234L;
        long buyerSlOid = 1000L;
        double buyerMemUsed = 10;
        InitialPlacementBuyer buyer = initialPlacementBuyer(buyerOid, buyerSlOid, VM_TYPE, new HashMap() {{
            put(TopologyDTO.CommodityType.newBuilder().setType(MEM_TYPE).build(), new Double(buyerMemUsed));
        }});
        // Create a historical economy with no existing reservations. The economy has 4 pms, all of
        // them have used == capacity.
        economyCaches.updateHistoricalCachedEconomy(economyWithCluster(new double[] {pm1MemUsed,
                pm2MemUsed, pm3MemUsed, pm4MemUsed}), commTypeToSpecMap, new HashMap(), new HashMap());
        // Create a real time economy with no existing reservations. The economy has same 4 pms.
        // All of them are low utilized.
        economyCaches.updateRealtimeCachedEconomy(economyWithCluster(new double[] {pm1MemUsed,
                pm2MemUsed, pm3MemUsed, pm4MemUsed}), commTypeToSpecMap, new HashMap(), new HashMap());
        Map<Long, List<InitialPlacementDecision>> result = economyCaches.findInitialPlacement(
                new ArrayList(Arrays.asList(buyer)));

        Assert.assertTrue(result.get(buyerOid).size() == 1);
        Assert.assertTrue(result.get(buyerOid).stream().allMatch(pl -> pl.supplier.get() == pm3Oid));
        Assert.assertTrue(economyCaches.historicalCachedEconomy.getTraders().stream().anyMatch(t ->
                t.getOid() == buyerOid));
        Assert.assertTrue(economyCaches.historicalCachedEconomy.getTopology().getTradersByOid()
                .containsKey(buyerOid));
        Assert.assertTrue(economyCaches.historicalCachedEconomy.getTopology().getShoppingListOids()
                .inverse().containsKey(buyerSlOid));
        Optional<Trader> pm3InHistorical = economyCaches.historicalCachedEconomy.getTraders().stream()
                .filter(t -> t.getOid() == pm3Oid).findFirst();
        int memIndex = pm3InHistorical.get().getBasketSold().indexOf(MEM_TYPE);
        Assert.assertTrue(pm3InHistorical.get().getCommoditiesSold().get(memIndex).getQuantity()
                == buyerMemUsed + pm3MemUsed);
        Assert.assertTrue(economyCaches.realtimeCachedEconomy.getTraders().stream().anyMatch(t ->
                t.getOid() == buyerOid));
        Assert.assertTrue(economyCaches.realtimeCachedEconomy.getTopology().getTradersByOid()
                .containsKey(buyerOid));
        Assert.assertTrue(economyCaches.realtimeCachedEconomy.getTopology().getShoppingListOids()
                .inverse().containsKey(buyerSlOid));
        Optional<Trader> pm3InRealtime = economyCaches.realtimeCachedEconomy.getTraders().stream()
                .filter(t -> t.getOid() == pm3Oid).findFirst();
        Assert.assertTrue(pm3InRealtime.get().getCommoditiesSold().get(memIndex).getQuantity()
                == buyerMemUsed + pm3MemUsed);
    }

    /**
     * Construct an existing reservation with 1 buyer. Pass an economy with 4 pms in 2 different
     * clusters as the historical economy. Pass an economy with 4 pms in 2 different clusters as the
     * real time economy. All 4 pms in historical economy is full.
     * Expected: no buyers in both economy caches. The findInitialPlacement returns a list of
     * {@link InitialPlacementDecision} with empty suppliers.
     */
    @Test
    public void testFindInitialPlacementFailedInHistorical() {
        long buyerOid = 1234L;
        long buyerSlOid = 1000L;
        double buyerMemUsed = 10;
        InitialPlacementBuyer buyer = initialPlacementBuyer(buyerOid, buyerSlOid, VM_TYPE, new HashMap() {{
            put(TopologyDTO.CommodityType.newBuilder().setType(MEM_TYPE).build(), new Double(buyerMemUsed));
        }});
        // Create a historical economy with no existing reservations. The economy has 4 pms, all of
        // them have used == capacity.
        economyCaches.updateHistoricalCachedEconomy(economyWithCluster(new double[] {pmMemCapacity,
                pmMemCapacity, pmMemCapacity, pmMemCapacity}), commTypeToSpecMap, new HashMap(),
                new HashMap());
        // Create a real time economy with no existing reservations. The economy has same 4 pms.
        // All of them are low utilized.
        economyCaches.updateRealtimeCachedEconomy(economyWithCluster(new double[] {pm1MemUsed,
                pm2MemUsed, pm3MemUsed, pm4MemUsed}), commTypeToSpecMap, new HashMap(), new HashMap());
        Map<Long, List<InitialPlacementDecision>> result = economyCaches.findInitialPlacement(
                new ArrayList(Arrays.asList(buyer)));

        Assert.assertTrue(result.get(buyerOid).size() == 1);
        Assert.assertTrue(result.get(buyerOid).stream().allMatch(pl -> !pl.supplier.isPresent()));
        Assert.assertTrue(economyCaches.historicalCachedEconomy.getTraders().stream().noneMatch(t ->
                t.getOid() == buyerOid));
        Assert.assertFalse(economyCaches.historicalCachedEconomy.getTopology().getTradersByOid()
                .containsKey(buyerOid));
        Assert.assertFalse(economyCaches.historicalCachedEconomy.getTopology().getShoppingListOids()
                .inverse().containsKey(buyerSlOid));
        for (Trader trader : economyCaches.historicalCachedEconomy.getTraders()) {
            Assert.assertTrue(trader.getCommoditiesSold().stream().allMatch(c ->
                    c.getQuantity() == pmMemCapacity || c.getQuantity() == 1));
        }
        Assert.assertTrue(economyCaches.realtimeCachedEconomy.getTraders().stream().noneMatch(t ->
                t.getOid() == buyerOid));
        Assert.assertFalse(economyCaches.realtimeCachedEconomy.getTopology().getTradersByOid()
                .containsKey(buyerOid));
        Assert.assertFalse(economyCaches.realtimeCachedEconomy.getTopology().getShoppingListOids()
                .inverse().containsKey(buyerSlOid));
    }

    /**
     * Construct an existing reservation with 1 buyer. Pass an economy with 4 pms in 2 different
     * clusters as the historical economy. Pass an economy with 4 pms in 2 different clusters as the
     * real time economy. Historical economy cluster 2 has lower mem utilization.
     * Real time economy has all 4 pm mem used being full.
     * Expected: no buyers in both economy caches. The findInitialPlacement returns a list of
     * {@link InitialPlacementDecision} with empty suppliers.
     */
    @Test
    public void testFindInitialPlacementFailedInRealtime() {
        long buyerOid = 1234L;
        long buyerSlOid = 1000L;
        double buyerMemUsed = 10;
        InitialPlacementBuyer buyer = initialPlacementBuyer(buyerOid, buyerSlOid, VM_TYPE, new HashMap() {{
            put(TopologyDTO.CommodityType.newBuilder().setType(MEM_TYPE).build(), new Double(buyerMemUsed));
        }});
        // Create a historical economy with no existing reservations. The economy has 4 pms, all of
        // them have low utilization. Among them pm4 has the lowest used.
        economyCaches.updateHistoricalCachedEconomy(economyWithCluster(new double[] {pm1MemUsed,
                pm2MemUsed, pm3MemUsed, pm4MemUsed}), commTypeToSpecMap, new HashMap(), new HashMap());
        // Create a real time economy with no existing reservations. The economy has same 4 pms.
        // All of them are low utilized.
        economyCaches.updateRealtimeCachedEconomy(economyWithCluster(new double[] {pmMemCapacity,
                pmMemCapacity, pmMemCapacity, pmMemCapacity}), commTypeToSpecMap, new HashMap(), new HashMap());
        Map<Long, List<InitialPlacementDecision>> result = economyCaches.findInitialPlacement(
                new ArrayList(Arrays.asList(buyer)));

        Assert.assertTrue(result.get(buyerOid).size() == 1);
        Assert.assertTrue(result.get(buyerOid).stream().allMatch(pl -> !pl.supplier.isPresent()));
        Assert.assertTrue(economyCaches.historicalCachedEconomy.getTraders().stream().noneMatch(t ->
                t.getOid() == buyerOid));
        Assert.assertFalse(economyCaches.historicalCachedEconomy.getTopology().getTradersByOid()
                .containsKey(buyerOid));
        Assert.assertFalse(economyCaches.historicalCachedEconomy.getTopology().getShoppingListOids()
                .inverse().containsKey(buyerSlOid));
        Assert.assertTrue(economyCaches.realtimeCachedEconomy.getTraders().stream().noneMatch(t ->
                t.getOid() == buyerOid));
        Assert.assertFalse(economyCaches.realtimeCachedEconomy.getTopology().getTradersByOid()
                .containsKey(buyerOid));
        Assert.assertFalse(economyCaches.realtimeCachedEconomy.getTopology().getShoppingListOids()
                .inverse().containsKey(buyerSlOid));
        for (Trader trader : economyCaches.historicalCachedEconomy.getTraders()) {
            if (trader.getOid() == pm1Oid) {
                trader.getCommoditiesSold().stream().allMatch(c ->
                        c.getQuantity() == pm1MemUsed || c.getQuantity() == 1);
            }
            if (trader.getOid() == pm2Oid) {
                trader.getCommoditiesSold().stream().allMatch(c ->
                        c.getQuantity() == pm2MemUsed || c.getQuantity() == 1);
            }
            if (trader.getOid() == pm3Oid) {
                trader.getCommoditiesSold().stream().allMatch(c ->
                        c.getQuantity() == pm3MemUsed || c.getQuantity() == 1);
            }
            if (trader.getOid() == pm4Oid) {
                trader.getCommoditiesSold().stream().allMatch(c ->
                        c.getQuantity() == pm4MemUsed || c.getQuantity() == 1);
            }
        }
        for (Trader trader : economyCaches.realtimeCachedEconomy.getTraders()) {
            Assert.assertTrue(trader.getCommoditiesSold().stream().allMatch(c ->
                    c.getQuantity() == pmMemCapacity || c.getQuantity() == 1));
        }
    }

    /**
     * Create a InitialPlacementBuyer list with one object based on given parameters.
     *
     * @param buyerOid buyer oid
     * @param slOid shopping list oid
     * @param entityType the provider entity type
     * @param usedByCommType the used value by commodity type specification.
     * @return 1 InitialPlacementBuyer
     */
    private InitialPlacementBuyer initialPlacementBuyer(long buyerOid, long slOid, int entityType,
            Map<TopologyDTO.CommodityType, Double> usedByCommType) {
        InitialPlacementCommoditiesBoughtFromProvider.Builder pmSl = InitialPlacementCommoditiesBoughtFromProvider
                .newBuilder()
                .setCommoditiesBoughtFromProviderId(slOid);
        CommoditiesBoughtFromProvider.Builder slBuilder = CommoditiesBoughtFromProvider.newBuilder()
                .setProviderEntityType(entityType);
        for (Map.Entry<TopologyDTO.CommodityType, Double> usedByType : usedByCommType.entrySet()) {
            slBuilder.addCommodityBought(CommodityBoughtDTO.newBuilder().setUsed(usedByType.getValue())
                    .setActive(true).setCommodityType(usedByType.getKey()));
        }
        pmSl.setCommoditiesBoughtFromProvider(slBuilder.build());
        InitialPlacementBuyer vm = InitialPlacementBuyer.newBuilder()
                .setBuyerId(buyerOid)
                .setReservationId(1L)
                .addAllInitialPlacementCommoditiesBoughtFromProvider(Arrays.asList(pmSl.build()))
                .build();
        return vm;
    }


    /**
     * Create a simple economy with 2 pm. Both pms have same commodity sold capacity.
     * PM1 mem used 20, capacity 100. PM2 mem used 30, capacity 100.
     *
     * @return economy an economy with traders
     */
    private Economy simpleEconomy() {
        Topology t = new Topology();
        Economy economy = t.getEconomyForTesting();

        Basket basketSoldByPM = new Basket(new CommoditySpecification(MEM_TYPE));
        List<Long> cliques = new ArrayList<>();
        cliques.add(455L);
        Trader pm1 = economy.addTrader(PM_TYPE, TraderState.ACTIVE, basketSoldByPM, cliques);
        pm1.setDebugInfoNeverUseInCode("PM1");
        pm1.getSettings().setCanAcceptNewCustomers(true);
        pm1.setOid(pm1Oid);
        CommoditySold commSold = pm1.getCommoditiesSold().get(0);
        commSold.setCapacity(pmMemCapacity);
        commSold.setQuantity(pm1MemUsed);
        commSold.setPeakQuantity(pm1MemUsed);
        commSold.getSettings().setPriceFunction(PriceFunction.Cache.createStandardWeightedPriceFunction(7.0));

        Trader pm2 = economy.addTrader(PM_TYPE, TraderState.ACTIVE, basketSoldByPM, cliques);
        pm2.setDebugInfoNeverUseInCode("PM2");
        pm2.getSettings().setCanAcceptNewCustomers(true);
        pm2.setOid(pm2Oid);
        CommoditySold commSold2 = pm2.getCommoditiesSold().get(0);
        commSold2.setCapacity(pmMemCapacity);
        commSold2.setQuantity(pm2MemUsed);
        commSold2.setPeakQuantity(pm2MemUsed);
        commSold2.getSettings().setPriceFunction(PriceFunction.Cache.createStandardWeightedPriceFunction(7.0));

        t.getModifiableTraderOids().put(pm1Oid, pm1);
        t.getModifiableTraderOids().put(pm2Oid, pm2);
        return economy;
    }

    /**
     * Create an economy with 4 pms in 2 different clusters. Cluster1 has pm1 and pm2.
     * Cluster2 has pm3 and pm4. All four pm mem capacity is 100.
     *
     * @param fourPMsMemUsed the mem commodity used for each pm.
     * @return economy an economy with traders
     */
    private Economy economyWithCluster(double[] fourPMsMemUsed) {
        Topology t = new Topology();
        Economy economy = t.getEconomyForTesting();
        Basket basketSoldByCluster1 = new Basket(Arrays.asList(new CommoditySpecification(MEM_TYPE),
                new CommoditySpecification(CLUSTER1_COMM_SPEC_TYPE, CommodityType.CLUSTER_VALUE)));
        List<Long> cliques = new ArrayList<>();
        cliques.add(455L);

        Trader pm1 = economy.addTrader(PM_TYPE, TraderState.ACTIVE, basketSoldByCluster1, cliques);
        pm1.setDebugInfoNeverUseInCode("PM1");
        pm1.getSettings().setCanAcceptNewCustomers(true);
        pm1.setOid(pm1Oid);
        int memIndex1 = pm1.getBasketSold().indexOf(MEM_TYPE);
        CommoditySold commSold = pm1.getCommoditiesSold().get(memIndex1);
        commSold.setCapacity(pmMemCapacity);
        commSold.setQuantity(fourPMsMemUsed[0]);
        commSold.setPeakQuantity(fourPMsMemUsed[0]);
        commSold.getSettings().setPriceFunction(PriceFunction.Cache.createStandardWeightedPriceFunction(7.0));
        int clusterIndex1 = pm1.getBasketSold().indexOf(CLUSTER1_COMM_SPEC_TYPE);
        CommoditySold clusterSold1 = pm1.getCommoditiesSold().get(clusterIndex1);
        clusterSold1.setCapacity(pmMemCapacity);
        clusterSold1.setQuantity(1);
        clusterSold1.setPeakQuantity(1);
        clusterSold1.getSettings().setPriceFunction(PriceFunction.Cache.createStandardWeightedPriceFunction(7.0));

        Trader pm2 = economy.addTrader(PM_TYPE, TraderState.ACTIVE, basketSoldByCluster1, cliques);
        pm2.setDebugInfoNeverUseInCode("PM2");
        pm2.getSettings().setCanAcceptNewCustomers(true);
        pm2.setOid(pm2Oid);
        int memIndex2 = pm2.getBasketSold().indexOf(MEM_TYPE);
        CommoditySold commSold2 = pm2.getCommoditiesSold().get(memIndex2);
        commSold2.setCapacity(pmMemCapacity);
        commSold2.setQuantity(fourPMsMemUsed[1]);
        commSold2.setPeakQuantity(fourPMsMemUsed[1]);
        commSold2.getSettings().setPriceFunction(PriceFunction.Cache.createStandardWeightedPriceFunction(7.0));
        int clusterIndex2 = pm2.getBasketSold().indexOf(CLUSTER1_COMM_SPEC_TYPE);
        CommoditySold clusterSold2 = pm2.getCommoditiesSold().get(clusterIndex2);
        clusterSold2.setCapacity(pmMemCapacity);
        clusterSold2.setQuantity(1);
        clusterSold2.setPeakQuantity(1);
        clusterSold2.getSettings().setPriceFunction(PriceFunction.Cache.createStandardWeightedPriceFunction(7.0));

        Basket basketSoldByCluster2 = new Basket(Arrays.asList(new CommoditySpecification(MEM_TYPE),
                new CommoditySpecification(CLUSTER2_COMM_SPEC_TYPE, CommodityType.CLUSTER_VALUE)));
        Trader pm3 = economy.addTrader(PM_TYPE, TraderState.ACTIVE, basketSoldByCluster2, cliques);
        pm3.setDebugInfoNeverUseInCode("PM3");
        pm3.getSettings().setCanAcceptNewCustomers(true);
        pm3.setOid(pm3Oid);
        int memIndex3 = pm3.getBasketSold().indexOf(MEM_TYPE);
        CommoditySold commSold3 = pm3.getCommoditiesSold().get(memIndex3);
        commSold3.setCapacity(pmMemCapacity);
        commSold3.setQuantity(fourPMsMemUsed[2]);
        commSold3.setPeakQuantity(fourPMsMemUsed[2]);
        commSold3.getSettings().setPriceFunction(PriceFunction.Cache.createStandardWeightedPriceFunction(7.0));
        int clusterIndex3 = pm3.getBasketSold().indexOf(CLUSTER2_COMM_SPEC_TYPE);
        CommoditySold clusterSold3 = pm3.getCommoditiesSold().get(clusterIndex3);
        clusterSold3.setCapacity(pmMemCapacity);
        clusterSold3.setQuantity(1);
        clusterSold3.setPeakQuantity(1);
        clusterSold3.getSettings().setPriceFunction(PriceFunction.Cache.createStandardWeightedPriceFunction(7.0));

        Trader pm4 = economy.addTrader(PM_TYPE, TraderState.ACTIVE, basketSoldByCluster2, cliques);
        pm4.setDebugInfoNeverUseInCode("PM4");
        pm4.getSettings().setCanAcceptNewCustomers(true);
        pm4.setOid(pm4Oid);
        int memIndex4 = pm4.getBasketSold().indexOf(MEM_TYPE);
        CommoditySold commSold4 = pm4.getCommoditiesSold().get(memIndex4);
        commSold4.setCapacity(pmMemCapacity);
        commSold4.setQuantity(fourPMsMemUsed[3]);
        commSold4.setPeakQuantity(fourPMsMemUsed[3]);
        commSold4.getSettings().setPriceFunction(PriceFunction.Cache.createStandardWeightedPriceFunction(7.0));
        int clusterIndex4 = pm4.getBasketSold().indexOf(CLUSTER2_COMM_SPEC_TYPE);
        CommoditySold clusterSold4 = pm4.getCommoditiesSold().get(clusterIndex4);
        clusterSold4.setCapacity(pmMemCapacity);
        clusterSold4.setQuantity(1);
        clusterSold4.setPeakQuantity(1);
        clusterSold4.getSettings().setPriceFunction(PriceFunction.Cache.createStandardWeightedPriceFunction(7.0));

        t.getModifiableTraderOids().put(pm1Oid, pm1);
        t.getModifiableTraderOids().put(pm2Oid, pm2);
        t.getModifiableTraderOids().put(pm3Oid, pm3);
        t.getModifiableTraderOids().put(pm4Oid, pm4);
        return economy;
    }
}
