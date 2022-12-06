package com.vmturbo.market.reservations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer.InitialPlacementCommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.pricefunction.PriceFunctionFactory;
import com.vmturbo.platform.analysis.pricefunction.QuoteFunctionFactory;
import com.vmturbo.platform.analysis.topology.Topology;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for {@link EconomyCaches}.
 */
public class EconomyCachesOPTest {
    private static final int PM_TYPE = EntityType.PHYSICAL_MACHINE_VALUE;
    private static final int CLUSTER_TYPE = EntityType.CLUSTER_VALUE;
    private static final int MEM_PROVISIONED_COMM_SPEC = 10;
    private static final int CLUSTER1_COMM_SPEC_TYPE = 301;
    private static final String cluster1Key = "cluster1";
    private static final int CLUSTER2_COMM_SPEC_TYPE = 302;
    private static final String cluster2Key = "cluster2";
    private static final int SEG_COMM_SPEC_TYPE = 303;
    private static final int ACCESS_COMM_SPEC_TYPE_WITH_KEY = 403;
    private static final String segKey = "Segment";
    private static final double pmMemCapacity = 100;
    private static final long pm1Oid = 1111L;
    private static final long pm2Oid = 1112L;
    private static final long pm3Oid = 1113L;
    private static final long pm4Oid = 1114L;
    private static final long pm5Oid = 1115L;
    private static final long pm6Oid = 1116L;
    private static final long cl1Oid = 6001L;
    private static final long cl2Oid = 6002L;

    private static final BiMap<TopologyDTO.CommodityType, Integer> commTypeToSpecMap =
            HashBiMap.create();

    /**
     * Create the commodity type to spec mapping.
     */
    @BeforeClass
    public static void setUp() {
        commTypeToSpecMap.put(TopologyDTO.CommodityType.newBuilder()
                .setType(CommodityType.MEM_PROVISIONED_VALUE)
                .build(), MEM_PROVISIONED_COMM_SPEC);
        commTypeToSpecMap.put(TopologyDTO.CommodityType.newBuilder()
                .setType(CommodityType.ACCESS_VALUE)
                .setKey(StringConstants.FAKE_CLUSTER_ACCESS_COMMODITY_KEY)
                .build(), ACCESS_COMM_SPEC_TYPE_WITH_KEY);
        commTypeToSpecMap.put(TopologyDTO.CommodityType.newBuilder().setType(
                CommodityType.CLUSTER_VALUE).setKey(cluster1Key).build(), CLUSTER1_COMM_SPEC_TYPE);
        commTypeToSpecMap.put(TopologyDTO.CommodityType.newBuilder().setType(
                CommodityType.CLUSTER_VALUE).setKey(cluster2Key).build(), CLUSTER2_COMM_SPEC_TYPE);
        commTypeToSpecMap.put(TopologyDTO.CommodityType.newBuilder().setType(
                CommodityType.SEGMENTATION_VALUE).setKey(segKey).build(), SEG_COMM_SPEC_TYPE);
    }

    /**
     * Create various scenarios for testing.
     */
    @Test
    public void testFindInitialPlacement() {
        // both clusters not full..pick the cluster with less utilization will get placed on pm3
        testFindInitialPlacementWithOP(110, 120, 10, 110, 120, 20, true, true, true, true, true,
                true, pm3Oid, cl1Oid);

        // cluster1 is full.. cluster2 is not full.. placed on pm6 even though pm3 is less utilized
        testFindInitialPlacementWithOP(140, 140, 30, 110, 120, 60, true, true, true, true, true,
                true, pm6Oid, cl2Oid);

        // cluster1 is full.. cluster2 is not full.. placed on pm6 even though pm6 is more than 100% utilized
        // this is because the cluster as a whole is not full yet.
        // pm4 is unavailable due to segmentation commodity to force the move on to pm6.
        testFindInitialPlacementWithOP(140, 140, 30, 10, 140, 130, true, true, true, false, true,
                true, pm6Oid, cl2Oid);
    }

    /**
     * Construct an existing reservation with 1 buyer. Pass an economy with 4 pms in 2 different
     * clusters as the historical economy. Pass the same economy with 4 pms as the real time
     * economy.
     * The lowest util host is pm3 in cluster 2.
     * Expected: buyer is placed on pm4 in both historical and real time economy caches.
     * The findInitialPlacement returns pm3 as the supplier.
     */
    public void testFindInitialPlacementWithOP(double pm1MemUsed, double pm2MemUsed,
            double pm3MemUsed, double pm4MemUsed, double pm5MemUsed, double pm6MemUsed,
            boolean pm1Avialable, boolean pm2Avialable, boolean pm3Avialable, boolean pm4Avialable,
            boolean pm5Avialable, boolean pm6Avialable, Long host, Long cluster) {
        EconomyCachePersistence economyCachePersistenceSpy = Mockito.mock(
                EconomyCachePersistence.class);
        EconomyCaches economyCaches = Mockito.spy(new EconomyCaches(economyCachePersistenceSpy));
        IdentityGenerator.initPrefix(0);
        long buyerOid = 1234L;
        long pmSlOid = 1000L;
        long clSlOid = 1001L;
        double buyerMemUsed = 10;
        economyCaches.getState().setReservationReceived(true);
        InitialPlacementBuyer buyer = initialPlacementBuyer(buyerOid, pmSlOid, clSlOid,
                buyerMemUsed);

        economyCaches.updateHistoricalCachedEconomy(
                economyWithCluster(pm1MemUsed, pm2MemUsed, pm3MemUsed, pm4MemUsed, pm5MemUsed,
                        pm6MemUsed, pm1Avialable, pm2Avialable, pm3Avialable, pm4Avialable,
                        pm5Avialable, pm6Avialable), commTypeToSpecMap, new HashMap(),
                new HashMap());
        // Create a real time economy with no existing reservations. The economy has same 4 pms.
        // All of them are low utilized.
        economyCaches.updateRealtimeCachedEconomy(
                economyWithCluster(pm1MemUsed, pm2MemUsed, pm3MemUsed, pm4MemUsed, pm5MemUsed,
                        pm6MemUsed, pm1Avialable, pm2Avialable, pm3Avialable, pm4Avialable,
                        pm5Avialable, pm6Avialable), commTypeToSpecMap, new HashMap(),
                new HashMap());
        Map<Long, List<InitialPlacementDecision>> result = economyCaches.findInitialPlacement(
                new ArrayList(Arrays.asList(buyer)), new HashMap(), 0,
                TopologyDTO.ReservationMode.NO_GROUPING, TopologyDTO.ReservationGrouping.NONE, 5,
                Collections.emptyList());
        Assert.assertEquals(host, result.get(buyerOid)
                .stream()
                .filter(a -> a.slOid == pmSlOid)
                .findFirst()
                .get().supplier.get());
        Assert.assertEquals(cluster, result.get(buyerOid)
                .stream()
                .filter(a -> a.slOid == clSlOid)
                .findFirst()
                .get().supplier.get());
    }

    /**
     * Create an economy with 6 pms in 2 different clusters. Cluster1 has pm1, pm2 and pm3.
     * Cluster2 has pm4, pm5 and pm6. All six pm mem capacity is 100.
     *
     * @param pMsMemUsed the mem commodity used for each pm.
     * @return economy an economy with traders
     */
    protected static Economy economyWithCluster(double pm1MemUsed, double pm2MemUsed,
            double pm3MemUsed, double pm4MemUsed, double pm5MemUsed, double pm6MemUsed,
            boolean pm1Avialable, boolean pm2Avialable, boolean pm3Avialable, boolean pm4Avialable,
            boolean pm5Avialable, boolean pm6Avialable) {
        Topology t = new Topology();
        Economy economy = t.getEconomyForTesting();

        List<Long> cliques1 = new ArrayList<>();
        cliques1.add(5001L);
        List<Long> cliques2 = new ArrayList<>();
        cliques2.add(5002L);

        Trader pm1 = createHost(economy, cliques1, "PM1", pm1Oid, pm1MemUsed, pm1Avialable);
        Trader pm2 = createHost(economy, cliques1, "PM2", pm2Oid, pm2MemUsed, pm2Avialable);
        Trader pm3 = createHost(economy, cliques1, "PM3", pm3Oid, pm3MemUsed, pm3Avialable);
        Trader pm4 = createHost(economy, cliques2, "PM4", pm4Oid, pm4MemUsed, pm4Avialable);
        Trader pm5 = createHost(economy, cliques2, "PM5", pm5Oid, pm5MemUsed, pm5Avialable);
        Trader pm6 = createHost(economy, cliques2, "PM6", pm6Oid, pm6MemUsed, pm6Avialable);

        Trader cl1 = createCluster(economy, cliques1, "CLUSTER1", cl1Oid,
                pm1MemUsed + pm2MemUsed + pm3MemUsed);
        Trader cl2 = createCluster(economy, cliques2, "CLUSTER2", cl2Oid,
                pm4MemUsed + pm5MemUsed + pm6MemUsed);

        t.getModifiableTraderOids().put(pm1Oid, pm1);
        t.getModifiableTraderOids().put(pm2Oid, pm2);
        t.getModifiableTraderOids().put(pm3Oid, pm3);
        t.getModifiableTraderOids().put(pm4Oid, pm4);
        t.getModifiableTraderOids().put(pm5Oid, pm5);
        t.getModifiableTraderOids().put(pm6Oid, pm6);
        t.getModifiableTraderOids().put(cl1Oid, cl1);
        t.getModifiableTraderOids().put(cl2Oid, cl2);
        return economy;
    }

    protected static Trader createCluster(Economy economy, List<Long> cliques, String clusterName,
            long clusterOid, double usedValue) {
        Basket basketSoldByCluster = new Basket(
                Arrays.asList(new CommoditySpecification(MEM_PROVISIONED_COMM_SPEC),
                        new CommoditySpecification(ACCESS_COMM_SPEC_TYPE_WITH_KEY)));
        Trader cl = economy.addTrader(CLUSTER_TYPE, TraderState.ACTIVE, basketSoldByCluster,
                cliques);
        cl.setDebugInfoNeverUseInCode(clusterName);
        cl.getSettings().setCanAcceptNewCustomers(true);
        cl.getSettings().setQuoteFunction(QuoteFunctionFactory.sumOfCommodityQuoteFunction());
        cl.setOid(clusterOid);
        CommoditySold commSold = cl.getCommoditiesSold().get(0);
        commSold.setCapacity(pmMemCapacity * 3d);
        commSold.setQuantity(usedValue);
        commSold.setPeakQuantity(usedValue);
        commSold.getSettings().setPriceFunction(
                PriceFunctionFactory.createStandardWeightedPriceFunction(1.0));

        CommoditySold segComm = cl.getCommoditiesSold().get(1);
        segComm.setCapacity(1000);
        segComm.setQuantity(0);
        segComm.setPeakQuantity(0);
        segComm.getSettings().setPriceFunction(
                PriceFunctionFactory.createStepPriceFunction(1, 1, Double.POSITIVE_INFINITY));
        return cl;
    }

    protected static Trader createHost(Economy economy, List<Long> cliques, String hostname,
            long hostOid, double usedValue, boolean sellsSeg) {
        Basket basketSoldByHost = new Basket(
                Arrays.asList(new CommoditySpecification(MEM_PROVISIONED_COMM_SPEC),
                        new CommoditySpecification(SEG_COMM_SPEC_TYPE)));
        Trader pm = economy.addTrader(PM_TYPE, TraderState.ACTIVE, basketSoldByHost, cliques);
        pm.setDebugInfoNeverUseInCode(hostname);
        pm.getSettings().setCanAcceptNewCustomers(true);
        pm.getSettings().setQuoteFunction(QuoteFunctionFactory.sumOfCommodityQuoteFunction());
        pm.setOid(hostOid);
        int memIndex = pm.getBasketSold().indexOf(MEM_PROVISIONED_COMM_SPEC);
        CommoditySold commSold = pm.getCommoditiesSold().get(memIndex);
        commSold.setCapacity(pmMemCapacity);
        commSold.setQuantity(usedValue);
        commSold.setPeakQuantity(usedValue);
        commSold.getSettings().setPriceFunction(
                PriceFunctionFactory.createOffsetStandardWeightedPriceFunction(1.0));

        int segIndex = pm.getBasketSold().indexOf(SEG_COMM_SPEC_TYPE);
        CommoditySold segSold = pm.getCommoditiesSold().get(segIndex);
        if (sellsSeg) {
            segSold.setCapacity(pmMemCapacity);
        } else {
            segSold.setCapacity(0.1);
        }
        segSold.setQuantity(0);
        segSold.setPeakQuantity(0);
        segSold.getSettings().setPriceFunction(
                PriceFunctionFactory.createStepPriceFunction(1, 1, Double.POSITIVE_INFINITY));

        return pm;
    }

    /**
     * Create a InitialPlacementBuyer list with one object based on given parameters.
     *
     * @param buyerOid buyer oid
     * @param pmSlOid pm shopping list oid
     * @param clSlOid cluster shopping list oid
     * @param memUsed the mem used by vm
     * @return 1 InitialPlacementBuyer
     */
    protected static InitialPlacementBuyer initialPlacementBuyer(long buyerOid, long pmSlOid,
            long clSlOid, double memUsed) {
        InitialPlacementCommoditiesBoughtFromProvider.Builder pmSl =
                InitialPlacementCommoditiesBoughtFromProvider.newBuilder()
                        .setCommoditiesBoughtFromProviderId(pmSlOid);
        CommoditiesBoughtFromProvider.Builder pmslBuilder =
                CommoditiesBoughtFromProvider.newBuilder().setProviderEntityType(
                        EntityType.PHYSICAL_MACHINE_VALUE);
        pmslBuilder.addCommodityBought(CommodityBoughtDTO.newBuilder()
                .setUsed(memUsed)
                .setActive(true)
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                        .setType(CommodityType.MEM_PROVISIONED_VALUE)
                        .build()));
        pmslBuilder.addCommodityBought(CommodityBoughtDTO.newBuilder()
                .setUsed(1d)
                .setActive(true)
                .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(
                        CommodityType.SEGMENTATION_VALUE).setKey(segKey).build()));
        pmSl.setCommoditiesBoughtFromProvider(pmslBuilder.build());
        InitialPlacementCommoditiesBoughtFromProvider.Builder clSl =
                InitialPlacementCommoditiesBoughtFromProvider.newBuilder()
                        .setCommoditiesBoughtFromProviderId(clSlOid);
        CommoditiesBoughtFromProvider.Builder clslBuilder =
                CommoditiesBoughtFromProvider.newBuilder().setProviderEntityType(
                        EntityType.CLUSTER_VALUE);
        clslBuilder.addCommodityBought(CommodityBoughtDTO.newBuilder()
                .setUsed(memUsed)
                .setActive(true)
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                        .setType(CommodityType.MEM_PROVISIONED_VALUE)
                        .build()));
        clslBuilder.addCommodityBought(CommodityBoughtDTO.newBuilder()
                .setUsed(1)
                .setActive(true)
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                        .setType(CommodityType.ACCESS_VALUE)
                        .setKey(StringConstants.FAKE_CLUSTER_ACCESS_COMMODITY_KEY)
                        .build()));
        clSl.setCommoditiesBoughtFromProvider(clslBuilder.build());
        InitialPlacementBuyer vm = InitialPlacementBuyer.newBuilder()
                .setBuyerId(buyerOid)
                .setReservationId(1L)
                .addAllInitialPlacementCommoditiesBoughtFromProvider(
                        Arrays.asList(pmSl.build(), clSl.build()))
                .build();
        return vm;
    }
}
