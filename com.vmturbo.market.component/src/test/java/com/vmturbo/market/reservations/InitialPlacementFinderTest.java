package com.vmturbo.market.reservations;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.collect.Table;

import org.junit.BeforeClass;
import org.junit.Test;

import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer.InitialPlacementCommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.market.reservations.InitialPlacementFinderResult.FailureInfo;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.topology.Topology;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for InitialPlacementFinder class.
 */
public class InitialPlacementFinderTest {

    private static final int PM_TYPE = EntityType.PHYSICAL_MACHINE_VALUE;
    private static final int VM_TYPE = EntityType.VIRTUAL_MACHINE_VALUE;
    private static final int MEM_TYPE = CommodityType.MEM_VALUE;
    private static final long pm1Oid = 32L;
    private static final long pm2Oid = 33L;
    private static final long vm1Oid = 30L;
    private static final long vmID = 101L;
    private static final long pmSlOid = 111L;
    private static final double quantity = 20;
    private static final Map<TopologyDTO.CommodityType, Integer> commTypeToSpecMap = Maps.newHashMap();

    /**
     * Create the commodity type to spec mapping.
     */
    @BeforeClass
    public static void setUp() {
        commTypeToSpecMap.put(TopologyDTO.CommodityType.newBuilder().setType(CommodityType.MEM_VALUE).build(), MEM_TYPE);
    }

    /**
     * Test InitialPlacementBuyer to TraderTO conversion.
     */
    @Test
    public void testConstructTraderTOs() {
        InitialPlacementFinder pf = new InitialPlacementFinder();
        pf.updateCachedEconomy(getOriginalEconomy(), commTypeToSpecMap, new HashSet<>());
        List<TraderTO> traderTO = pf.constructTraderTOs(getTradersToPlace(vmID, pmSlOid, PM_TYPE,
                MEM_TYPE, 100), commTypeToSpecMap);
        assertTrue(traderTO.size() == 1);
        TraderTO vmTO = traderTO.get(0);
        assertTrue(vmTO.getOid() == vmID);
        ShoppingListTO pmSlTO = vmTO.getShoppingLists(0);
        assertTrue(pmSlTO.getMovable() == true);
        assertTrue(pmSlTO.getOid() == pmSlOid);
        assertTrue(pmSlTO.getCommoditiesBoughtList().get(0).getQuantity() == 100);
        assertTrue(pmSlTO.getCommoditiesBought(0).getPeakQuantity() == 100);
        assertTrue(pmSlTO.getCommoditiesBought(0).getSpecification().getType() == MEM_TYPE);
    }

    /**
     * Test find placement for a reservation entity. The original economy has two hosts.
     * PM1 mem used 25, capacity 100. PM2 mem used 20, capacity 100.
     * Expected: reservation place new vm on PM2. PM2 used becomes 10 + 20 = 30.
     * @throws NoSuchFieldException can not find cachedEconomy
     * @throws IllegalAccessException can not get access to traders in economy
     */
    @Test
    public void testFindPlacement() throws NoSuchFieldException, IllegalAccessException {
        InitialPlacementFinder pf = new InitialPlacementFinder();
        Economy originalEconomy = getOriginalEconomy();
        pf.updateCachedEconomy(originalEconomy, commTypeToSpecMap, new HashSet<>());
        double used = 10;
        Table<Long, Long, InitialPlacementFinderResult> result = pf.findPlacement(getTradersToPlace(vmID, pmSlOid, PM_TYPE,
                MEM_TYPE, used));
        for (Table.Cell<Long, Long, InitialPlacementFinderResult> cell : result.cellSet()) {
            assertTrue(cell.getRowKey() == vmID);
            assertTrue(cell.getColumnKey() == pmSlOid);
            assertTrue(cell.getValue().getProviderOid().get() == pm2Oid);
        }
        Field economy = InitialPlacementFinder.class.getDeclaredField("cachedEconomy");
        economy.setAccessible(true);
        Economy cachedEconomy = (Economy)economy.get(pf);
        Map<Long, Trader> traderOids = cachedEconomy.getTopology().getTradersByOid();
        Trader pm2 = traderOids.get(pm2Oid);
        assertTrue(pm2.getCommoditiesSold().get(0).getQuantity() == quantity + used);
    }

    /**
     * Test initial placement finder failed. The original economy has two hosts.
     * PM1 mem used 25, capacity 100. PM2 mem used 20, capacity 100.
     * Expected: reservation failed with a new reservation VM requesting 100 mem.
     */
    @Test
    public void testInitialPlacementFinderResultWithFailureInfo() {
        InitialPlacementFinder pf = new InitialPlacementFinder();
        Economy originalEconomy = getOriginalEconomy();
        pf.updateCachedEconomy(originalEconomy, commTypeToSpecMap, new HashSet<>());
        List<InitialPlacementBuyer> buyer = getTradersToPlace(vmID, pmSlOid, PM_TYPE, MEM_TYPE, 100);
        Table<Long, Long, InitialPlacementFinderResult> result = pf.findPlacement(buyer);
        List<FailureInfo> failureInfo = result.get(vmID, pmSlOid).getFailureInfoList();
        assertTrue(failureInfo.size() == 1);
        assertTrue(failureInfo.get(0).getCommodityType().getType() == MEM_TYPE);
        assertTrue(failureInfo.get(0).getRequestedAmount() == 100);
        assertTrue(failureInfo.get(0).getClosestSellerOid() == pm2Oid);
        assertTrue(failureInfo.get(0).getMaxQuantity() == 80);
    }

    /**
     * Create a InitialPlacementBuyer list with one object based on given parameters.
     *
     * @param buyerOid buyer oid
     * @param slOid shopping list oid
     * @param entityType the provider entity type
     * @param commodityType commodity type
     * @param used the requested amount
     * @return a list containing 1 InitialPlacementBuyer
     */
    private List<InitialPlacementBuyer> getTradersToPlace(long buyerOid, long slOid, int entityType,
            int commodityType, double used) {
        InitialPlacementCommoditiesBoughtFromProvider pmSl = InitialPlacementCommoditiesBoughtFromProvider
                .newBuilder()
                .setCommoditiesBoughtFromProviderId(slOid)
                .setCommoditiesBoughtFromProvider(CommoditiesBoughtFromProvider.newBuilder()
                        .addCommodityBought(CommodityBoughtDTO.newBuilder().setUsed(used).setActive(true)
                                .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(commodityType)))
                        .setProviderEntityType(entityType))
                .build();
        InitialPlacementBuyer vm = InitialPlacementBuyer.newBuilder()
                .setBuyerId(buyerOid)
                .addAllInitialPlacementCommoditiesBoughtFromProvider(Arrays.asList(pmSl))
                .build();
        return Arrays.asList(vm);
    }



    /**
     * Create a simple economy with 2 pm and 1 vm. Both pms have same commodity sold capacity.
     * PM1 hosts the VM1 thus utilization is higher than PM2.
     * PM1 mem used 25, capacity 100. PM2 mem used 20, capacity 100. VM1 mem used 5.
     *
     * @return economy an economy with traders
     */
    private Economy getOriginalEconomy() {
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
        commSold.setCapacity(100);
        commSold.setQuantity(quantity);
        commSold.setPeakQuantity(30);
        commSold.getSettings().setPriceFunction(PriceFunction.Cache.createStandardWeightedPriceFunction(7.0));

        Trader pm2 = economy.addTrader(PM_TYPE, TraderState.ACTIVE, basketSoldByPM, cliques);
        pm2.setDebugInfoNeverUseInCode("PM2");
        pm2.getSettings().setCanAcceptNewCustomers(true);
        pm2.setOid(pm2Oid);
        CommoditySold commSold2 = pm2.getCommoditiesSold().get(0);
        commSold2.setCapacity(100);
        commSold2.setQuantity(quantity);
        commSold2.setPeakQuantity(30);
        commSold2.getSettings().setPriceFunction(PriceFunction.Cache.createStandardWeightedPriceFunction(7.0));

        Trader vm1 = economy.addTrader(VM_TYPE, TraderState.ACTIVE, new Basket());
        vm1.setDebugInfoNeverUseInCode("VM1");
        vm1.setOid(vm1Oid);
        ShoppingList shoppingList = economy.addBasketBought(vm1, basketSoldByPM);
        shoppingList.setQuantity(0, 5);
        shoppingList.setPeakQuantity(0, 10);
        new Move(economy, shoppingList, pm1).take();

        t.getModifiableTraderOids().put(pm1Oid, pm1);
        t.getModifiableTraderOids().put(pm2Oid, pm2);
        t.getModifiableTraderOids().put(vm1Oid, vm1);
        return economy;
    }

    /**
     * Test partial successful reservation in the findPlacement. The original economy contains VM1, PM1 and PM2.
     * VM1 resides on PM1. PM1 mem used 25, capacity 100. PM2 mem used 20, capacity 100.
     * Expected: new reservation with two VMs has mem used 50 and 100 will fail. The rollbackPlacedTraders() should
     * take care of clean up cachedEconomy so that after reservation it only contains VM1, PM1 and PM2.
     *
     * @throws NoSuchFieldException can not find cachedEconomy
     * @throws IllegalAccessException can not get access to traders in economy
     */
    @Test
    public void testReservationPartialSuccess() throws NoSuchFieldException, IllegalAccessException {
        InitialPlacementFinder pf = new InitialPlacementFinder();
        Economy originalEconomy = getOriginalEconomy();
        pf.updateCachedEconomy(originalEconomy, commTypeToSpecMap, new HashSet<>());
        long vm2Oid = 10002L;
        long vm3Oid = 10003L;
        long vm2SlOid = 20002L;
        long vm3SlOid = 20003L;
        long used1 = 50;
        long used2 = 100;
        List<InitialPlacementBuyer> vm2 = getTradersToPlace(vm2Oid, vm2SlOid, PM_TYPE, MEM_TYPE, used1);
        List<InitialPlacementBuyer> vm3 = getTradersToPlace(vm3Oid, vm3SlOid, PM_TYPE, MEM_TYPE, used2);
        List<InitialPlacementBuyer> vms = new ArrayList<InitialPlacementBuyer>();
        vms.addAll(vm2);
        vms.addAll(vm3);
        Table<Long, Long, InitialPlacementFinderResult> result = pf.findPlacement(vms);
        assertFalse(result.isEmpty());
        Field economy = InitialPlacementFinder.class.getDeclaredField("cachedEconomy");
        economy.setAccessible(true);
        Economy cachedEconomy = (Economy)economy.get(pf);
        Trader pm1 = cachedEconomy.getTopology().getTradersByOid().get(pm1Oid);
        Trader pm2 = cachedEconomy.getTopology().getTradersByOid().get(pm2Oid);
        assertTrue(pm1.getCommoditiesSold().get(0).getQuantity() == 25);
        assertTrue(pm2.getCommoditiesSold().get(0).getQuantity() == 20);
        Trader vmTrader2 = cachedEconomy.getTopology().getTradersByOid().get(vm2Oid);
        assertTrue(cachedEconomy.getMarketsAsBuyer(vmTrader2).keySet().stream().allMatch(sl -> sl.getSupplier() == null));
        Trader vmTrader3 = cachedEconomy.getTopology().getTradersByOid().get(vm3Oid);
        assertTrue(cachedEconomy.getMarketsAsBuyer(vmTrader3).keySet().stream().allMatch(sl -> sl.getSupplier() == null));

    }

    /**
     * Test reservation deletion in the findPlacement. The original economy contains VM1, PM1 and PM2.
     * VM1 resides on PM1. PM1 mem used 25, capacity 100. PM2 mem used 20, capacity 100.
     * Expected: new reservation VM2 with mem used 20 selects PM2. PM2 new mem used is 40.
     * Then deleting VM2, a new reservation VM3 with mem used 10 selects PM2.
     */
    @Test
    public void testReservationDeletionAndAdd() {
        InitialPlacementFinder pf = new InitialPlacementFinder();
        Economy originalEconomy = getOriginalEconomy();
        pf.updateCachedEconomy(originalEconomy, commTypeToSpecMap, new HashSet<>());

        long vm2Oid = 10002L;
        long vm2SlOid = 20002L;
        long vm2Used = 20;
        Table<Long, Long, InitialPlacementFinderResult> vm2Result = pf.findPlacement(
                getTradersToPlace(vm2Oid, vm2SlOid, PM_TYPE, MEM_TYPE, vm2Used));
        assertTrue(vm2Result.get(vm2Oid, vm2SlOid).getProviderOid().get() == pm2Oid);
        // delete the VM1 which stays on PM1, now the utilization of PM1 is lower
        pf.buyersToBeDeleted(Arrays.asList(vm2Oid));
        long vm3Oid = 10003L;
        long vm3SlOid = 20003L;
        long vm3Used = 10;
        Table<Long, Long, InitialPlacementFinderResult> vm3Result = pf.findPlacement(
                getTradersToPlace(vm3Oid, vm3SlOid, PM_TYPE, MEM_TYPE, vm3Used));
        assertTrue(vm3Result.get(vm3Oid, vm3SlOid).getProviderOid().get() == pm2Oid);
    }
}
