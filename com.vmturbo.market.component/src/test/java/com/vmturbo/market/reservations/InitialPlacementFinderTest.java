package com.vmturbo.market.reservations;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Table;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer.InitialPlacementCommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementDTO;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetExistingReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetExistingReservationsResponse;
import com.vmturbo.common.protobuf.plan.ReservationDTOMoles.ReservationServiceMole;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.market.diagnostics.AnalysisDiagnosticsCollector.AnalysisDiagnosticsCollectorFactory;
import com.vmturbo.market.diagnostics.AnalysisDiagnosticsCollector.AnalysisDiagnosticsCollectorFactory.DefaultAnalysisDiagnosticsCollectorFactory;
import com.vmturbo.market.reservations.InitialPlacementFinderResult.FailureInfo;
import com.vmturbo.plan.orchestrator.api.PlanUtils;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.pricefunction.PriceFunctionFactory;
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
    private static final long reservationId = 90000L;
    private static final BiMap<TopologyDTO.CommodityType, Integer> commTypeToSpecMap = HashBiMap.create();
    private final  ReservationServiceMole testReservationService = spy(new ReservationServiceMole());
    private  ReservationServiceBlockingStub reservationServiceBlockingStub;
    AnalysisDiagnosticsCollectorFactory diagsCollectorFactory = Mockito.mock(DefaultAnalysisDiagnosticsCollectorFactory.class);

    /**
     * The grpc server.
     */
    @Rule
    public  GrpcTestServer grpcServer = GrpcTestServer.newServer(testReservationService);

    /**
     * Create the commodity type to spec mapping.
     */
    @BeforeClass
    public static void setUp() {
        commTypeToSpecMap.put(TopologyDTO.CommodityType.newBuilder().setType(CommodityType.MEM_VALUE).build(), MEM_TYPE);
    }

    /**
     * Create the GRPC stub.
     */
    @Before
    public void before() {
        IdentityGenerator.initPrefix(0);
        reservationServiceBlockingStub = ReservationServiceGrpc.newBlockingStub(grpcServer.getChannel());
        when(diagsCollectorFactory.newDiagsCollector(any(), any())).thenReturn(Optional.empty());
    }

    /**
     * Test InitialPlacementBuyer to TraderTO conversion.
     */
    @Test
    public void testConstructTraderTOs() {
        InitialPlacementHandler handler = new InitialPlacementHandler(Mockito.mock(DSLContext.class), reservationServiceBlockingStub,
true, 1, 5, diagsCollectorFactory);
        handler.updateCachedEconomy(getOriginalEconomy(), commTypeToSpecMap, true);
        TraderTO vmTO = (TraderTO)InitialPlacementUtils.constructTraderTO(
                getTradersToPlace(vmID, pmSlOid, PM_TYPE, MEM_TYPE, 100), commTypeToSpecMap,
                new HashMap()).get();
        assertTrue(vmTO.getOid() == vmID);
        ShoppingListTO pmSlTO = vmTO.getShoppingLists(0);
        assertTrue(pmSlTO.getMovable() == true);
        assertTrue(pmSlTO.getOid() == pmSlOid);
        assertEquals(pmSlTO.getCommoditiesBoughtList().get(0).getQuantity(), 100, 0.000001);
        assertEquals(pmSlTO.getCommoditiesBought(0).getPeakQuantity(), 100, 0.000001);
        assertTrue(pmSlTO.getCommoditiesBought(0).getSpecification().getType() == MEM_TYPE);
    }

    /**
     * Test InitialPlacementBuyer to TraderTO conversion error scenario.
     */
    @Test
    public void testConstructTraderTOsWithException() {
        InitialPlacementHandler handler = new InitialPlacementHandler(Mockito.mock(DSLContext.class),
                reservationServiceBlockingStub, true, 1, 5, diagsCollectorFactory);
        handler.updateCachedEconomy(getOriginalEconomy(), commTypeToSpecMap, true);
        // Create Trader with invalid commodity Type 1000.
        Optional<TraderTO> vmTO = InitialPlacementUtils.constructTraderTO(
                getTradersToPlace(vmID, pmSlOid, PM_TYPE, 1000, 100), commTypeToSpecMap,
                new HashMap());
        assertTrue(!vmTO.isPresent());
    }

    /**
     * Test buyersToBeDeleted. Verify the method clears the entry in existingReservations and
     * buyerPlacements.
     */
    @Test
    public void testBuyersToBeDeleted() {
        InitialPlacementHandler handler = new InitialPlacementHandler(Mockito.mock(DSLContext.class),
                reservationServiceBlockingStub, true, 1, 5,
                diagsCollectorFactory);
        InitialPlacementFinder pf = handler.getPlacementFinder();
        pf.existingReservations.put(1L, PlanUtils.setupInitialPlacement(new ArrayList(Arrays
                    .asList(getTradersToPlace(vmID, pmSlOid, PM_TYPE, MEM_TYPE, 10))), 1L));
        pf.buyerPlacements.put(vmID, new ArrayList(Arrays.asList(new InitialPlacementDecision(pmSlOid,
                    Optional.of(pm1Oid), new ArrayList()))));
        handler.buyersToBeDeleted(Arrays.asList(vmID), false);
        assertTrue(pf.existingReservations.isEmpty());
        assertTrue(pf.buyerPlacements.isEmpty());
    }

    /**
     * Test find placement for a reservation entity. The original economy has two hosts.
     * PM1 mem used 25, capacity 100. PM2 mem used 20, capacity 100.
     * Expected: buyer has pm2 as the supplier, existingReservations contains the new reservation
     * and buyerPlacements keeps track of buyer oid and its placement decisions.
     *
     * @throws InterruptedException        if we're interrupted
     * @throws ExecutionException          if failure in asynchronous updation
     */
    @Test
    public void testFindPlacement() throws ExecutionException, InterruptedException {
        InitialPlacementHandler handler = new InitialPlacementHandler(Mockito.mock(DSLContext.class),
                reservationServiceBlockingStub, true, 1, 5,
                diagsCollectorFactory);
        InitialPlacementFinder pf = handler.getPlacementFinder();
        // Create both economy caches using same economy.
        Economy originalEconomy = getOriginalEconomy();
        pf.economyCaches.getState().setReservationReceived(true);
        handler.updateCachedEconomy(originalEconomy, commTypeToSpecMap, true).get();
        handler.updateCachedEconomy(originalEconomy, commTypeToSpecMap, false).get();
        double used = 10;
        Table<Long, Long, InitialPlacementFinderResult> result
            = handler.findPlacement(PlanUtils.setupReservationRequest(Arrays
                .asList(getTradersToPlace(vmID, pmSlOid, PM_TYPE,
                MEM_TYPE, used)), 1L));
        for (Table.Cell<Long, Long, InitialPlacementFinderResult> cell : result.cellSet()) {
            assertTrue(cell.getRowKey() == vmID);
            assertTrue(cell.getColumnKey() == pmSlOid);
            assertTrue(cell.getValue().getProviderOid().get() == pm2Oid);
        }
        assertTrue(pf.existingReservations.size() == 1);
        assertTrue(pf.existingReservations.values().stream()
                .map(InitialPlacementDTO::getInitialPlacementBuyerList)
                .flatMap(List::stream)
                .anyMatch(buyer -> buyer.getBuyerId() == vmID
                        && buyer.getInitialPlacementCommoditiesBoughtFromProviderList()
                        .stream().anyMatch(sl -> sl.getCommoditiesBoughtFromProviderId() == pmSlOid)));
        assertTrue(pf.buyerPlacements.size() == 1);
        assertNotNull(pf.buyerPlacements.get(vmID));
    }

    /**
     * Test initial placement finder failed. The original economy has two hosts.
     * PM1 mem used 25, capacity 100. PM2 mem used 20, capacity 100.
     * Expected: reservation failed with a new reservation VM requesting 100 mem.
     * @throws InterruptedException        if we're interrupted
     * @throws ExecutionException          if failure in asynchronous updation
     */
    @Test
    public void testInitialPlacementFinderResultWithFailureInfo()
            throws ExecutionException, InterruptedException {
        InitialPlacementHandler handler = new InitialPlacementHandler(Mockito.mock(DSLContext.class),
                reservationServiceBlockingStub, true, 0, 5,
                diagsCollectorFactory);
        InitialPlacementFinder pf = handler.getPlacementFinder();
        Economy originalEconomy = getOriginalEconomy();
        handler.updateCachedEconomy(originalEconomy, commTypeToSpecMap, true);
        handler.updateCachedEconomy(originalEconomy, commTypeToSpecMap, false);
        pf.economyCaches.getState().setReservationReceived(true);
        InitialPlacementBuyer buyer = getTradersToPlace(vmID, pmSlOid, PM_TYPE, MEM_TYPE, 100);
        Table<Long, Long, InitialPlacementFinderResult> result
            = handler.findPlacement(PlanUtils.setupReservationRequest(Arrays.asList(buyer), 1L));
        List<FailureInfo> failureInfo = result.get(vmID, pmSlOid).getFailureInfoList();
        assertTrue(failureInfo.size() == 1);
        assertTrue(failureInfo.get(0).getCommodityType().getType() == MEM_TYPE);
        assertEquals(failureInfo.get(0).getRequestedAmount(), 100, 0.000001);
        assertTrue(failureInfo.get(0).getClosestSellerOid() == pm2Oid);
        assertEquals(failureInfo.get(0).getMaxQuantity(), 80, 0.000001);
    }

    /**
     * Test initial placement finder failed. The original economy has two hosts.
     * PM1 mem used 110, capacity 100. PM2 mem used 120, capacity 100.
     * Expected: reservation failed with a new reservation VM requesting 100 mem.
     * Failure info  has maxquantity non negative
     *
     * @throws InterruptedException        if we're interrupted
     * @throws ExecutionException          if failure in asynchronous updation
     */
    @Test
    public void testInitialPlacementFinderResultWithFailureInfoOnOverFlowEconomy()
            throws InterruptedException, ExecutionException {
        InitialPlacementHandler handler = new InitialPlacementHandler(Mockito.mock(DSLContext.class),
                reservationServiceBlockingStub, true, 0, 5,
                diagsCollectorFactory);
        InitialPlacementFinder pf = handler.getPlacementFinder();
        Economy originalEconomy = getOverflowEconomy();
        handler.updateCachedEconomy(originalEconomy, commTypeToSpecMap, true);
        handler.updateCachedEconomy(originalEconomy, commTypeToSpecMap, false);
        pf.economyCaches.getState().setReservationReceived(true);
        InitialPlacementBuyer buyer = getTradersToPlace(vmID, pmSlOid, PM_TYPE, MEM_TYPE, 100);
        Table<Long, Long, InitialPlacementFinderResult> result
            = handler.findPlacement(PlanUtils.setupReservationRequest(Arrays.asList(buyer), 1L));
        List<FailureInfo> failureInfo = result.get(vmID, pmSlOid).getFailureInfoList();
        assertTrue(failureInfo.size() == 1);
        assertTrue(failureInfo.get(0).getCommodityType().getType() == MEM_TYPE);
        assertEquals(failureInfo.get(0).getRequestedAmount(), 100, 0.000001);
        assertTrue(failureInfo.get(0).getClosestSellerOid() == pm1Oid);
        assertEquals(failureInfo.get(0).getMaxQuantity(), 0, 0.000001);
    }

    /**
     * Create a InitialPlacementBuyer list with one object based on given parameters.
     *
     * @param buyerOid buyer oid
     * @param slOid shopping list oid
     * @param entityType the provider entity type
     * @param commodityType commodity type
     * @param used the requested amount
     * @return 1 InitialPlacementBuyer
     */
    private InitialPlacementBuyer getTradersToPlace(long buyerOid, long slOid, int entityType,
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
                .setReservationId(reservationId)
                .addAllInitialPlacementCommoditiesBoughtFromProvider(Arrays.asList(pmSl))
                .build();
        return vm;
    }


    /**
     * Create a simple economy with 2 pm and 1 vm. Both pms have same commodity sold capacity.
     * PM1 mem used 110, capacity 100. PM2 mem used 120, capacity 100.
     *
     * @return economy an economy with traders
     */
    private Economy getOverflowEconomy() {
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
        commSold.setQuantity(110);
        commSold.setPeakQuantity(110);
        commSold.getSettings().setPriceFunction(PriceFunctionFactory.createStandardWeightedPriceFunction(7.0));

        Trader pm2 = economy.addTrader(PM_TYPE, TraderState.ACTIVE, basketSoldByPM, cliques);
        pm2.setDebugInfoNeverUseInCode("PM2");
        pm2.getSettings().setCanAcceptNewCustomers(true);
        pm2.setOid(pm2Oid);
        CommoditySold commSold2 = pm2.getCommoditiesSold().get(0);
        commSold2.setCapacity(100);
        commSold2.setQuantity(120);
        commSold2.setPeakQuantity(120);
        commSold2.getSettings().setPriceFunction(PriceFunctionFactory.createStandardWeightedPriceFunction(7.0));


        t.getModifiableTraderOids().put(pm1Oid, pm1);
        t.getModifiableTraderOids().put(pm2Oid, pm2);
        return economy;
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
        commSold.getSettings().setPriceFunction(PriceFunctionFactory.createStandardWeightedPriceFunction(7.0));

        Trader pm2 = economy.addTrader(PM_TYPE, TraderState.ACTIVE, basketSoldByPM, cliques);
        pm2.setDebugInfoNeverUseInCode("PM2");
        pm2.getSettings().setCanAcceptNewCustomers(true);
        pm2.setOid(pm2Oid);
        CommoditySold commSold2 = pm2.getCommoditiesSold().get(0);
        commSold2.setCapacity(100);
        commSold2.setQuantity(quantity);
        commSold2.setPeakQuantity(30);
        commSold2.getSettings().setPriceFunction(PriceFunctionFactory.createStandardWeightedPriceFunction(7.0));

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
     * Expected: new reservation with VM2 and VM3 has mem used 50 and 100 will fail, existingReservations
     * contains the new reservation and buyerPlacements keeps track of buyer oid and its placement
     * decisions with no supplier present in both VM2 and VM3.
     *
     * @throws InterruptedException        if we're interrupted
     * @throws ExecutionException          if failure in asynchronous updation
     */
    @Test
    public void testReservationPartialSuccess() throws ExecutionException, InterruptedException {

        InitialPlacementHandler handler = new InitialPlacementHandler(Mockito.mock(DSLContext.class),
                reservationServiceBlockingStub, true, 1, 5,
                diagsCollectorFactory);
        InitialPlacementFinder pf = handler.getPlacementFinder();
        Economy originalEconomy = getOriginalEconomy();
        // Create both economy caches using same economy.
        pf.economyCaches.getState().setReservationReceived(true);
        handler.updateCachedEconomy(originalEconomy, commTypeToSpecMap, true);
        handler.updateCachedEconomy(originalEconomy, commTypeToSpecMap, false);
        long vm2Oid = 10002L;
        long vm3Oid = 10003L;
        long vm2SlOid = 20002L;
        long vm3SlOid = 20003L;
        long used1 = 50;
        long used2 = 100;
        InitialPlacementBuyer vm2 = getTradersToPlace(vm2Oid, vm2SlOid, PM_TYPE, MEM_TYPE, used1);
        InitialPlacementBuyer vm3 = getTradersToPlace(vm3Oid, vm3SlOid, PM_TYPE, MEM_TYPE, used2);
        List<InitialPlacementBuyer> vms = new ArrayList<InitialPlacementBuyer>();
        vms.add(vm2);
        vms.add(vm3);
        Table<Long, Long, InitialPlacementFinderResult> result
            = handler.findPlacement(PlanUtils.setupReservationRequest(vms, 1L));
        for (Table.Cell<Long, Long, InitialPlacementFinderResult> cell : result.cellSet()) {
            assertTrue(cell.getRowKey() == vm2Oid || cell.getRowKey() == vm3Oid);
            assertTrue(cell.getColumnKey() == vm2SlOid || cell.getColumnKey() == vm3SlOid);
            assertTrue(!cell.getValue().getProviderOid().isPresent());
        }
        assertTrue(pf.existingReservations.size() == 1);
        assertTrue(pf.existingReservations.values().stream()
            .map(InitialPlacementDTO::getInitialPlacementBuyerList).flatMap(List::stream).count() == 2);
        assertTrue(pf.buyerPlacements.get(vm2Oid).stream().allMatch(pl -> !pl.supplier.isPresent()));
        assertTrue(pf.buyerPlacements.get(vm3Oid).stream().allMatch(pl -> !pl.supplier.isPresent()));
        assertTrue(pf.buyerPlacements.size() == 2);
    }


    /**
     * Test reservation delayed deletion in the findPlacement.
     *
     * @throws InterruptedException        if we're interrupted
     * @throws ExecutionException          if failure in asynchronous updation
     */
    @Test
    public void testDelayedDeletion() throws ExecutionException, InterruptedException {
        InitialPlacementHandler handler = new InitialPlacementHandler(Mockito.mock(DSLContext.class),
                reservationServiceBlockingStub, true, 1, 5,
                diagsCollectorFactory);
        InitialPlacementFinder pf = handler.getPlacementFinder();
        Economy originalEconomy = getOriginalEconomy();
        pf.economyCaches.getState().setReservationReceived(true);
        handler.updateCachedEconomy(originalEconomy, commTypeToSpecMap, true);
        handler.updateCachedEconomy(originalEconomy, commTypeToSpecMap, false);
        // PM1 mem used 25, capacity 100. PM2 mem used 20, capacity 100.
        long vm2Oid = 10002L;
        long vm2SlOid = 20002L;
        long vm2Used = 10;
        InitialPlacementBuyer vm2Buyer = getTradersToPlace(vm2Oid, vm2SlOid, PM_TYPE, MEM_TYPE, vm2Used)
                .toBuilder().setReservationId(900002L).build();
        Table<Long, Long, InitialPlacementFinderResult> vm2Result
            = handler.findPlacement(PlanUtils.setupReservationRequest(Arrays.asList(vm2Buyer), 900002L));
        assertTrue(vm2Result.get(vm2Oid, vm2SlOid).getProviderOid().get() == pm2Oid);
        /*
        Historical:
        PM1 mem used 25, capacity 100. PM2 mem used 30, capacity 100. vm2 placed in PM2
        RealTime:
        PM1 mem used 25, capacity 100. PM2 mem used 30, capacity 100. vm2 placed in PM2
        */
        assertEquals(pf.economyCaches.historicalCachedEconomy.getTraders()
                .get(0).getCommoditiesSold().get(0).getQuantity(), 25d, 0.01);
        assertEquals(pf.economyCaches.historicalCachedEconomy.getTraders()
                .get(1).getCommoditiesSold().get(0).getQuantity(), 30d, 0.01);
        assertEquals(pf.economyCaches.realtimeCachedEconomy.getTraders()
                .get(0).getCommoditiesSold().get(0).getQuantity(), 25d, 0.01);
        assertEquals(pf.economyCaches.realtimeCachedEconomy.getTraders()
                .get(1).getCommoditiesSold().get(0).getQuantity(), 30d, 0.01);

        long vm3Oid = 10003L;
        long vm3SlOid = 20003L;
        long vm3Used = 10;
        InitialPlacementBuyer vm3Buyer = getTradersToPlace(vm3Oid, vm3SlOid, PM_TYPE, MEM_TYPE, vm3Used)
                .toBuilder().setReservationId(900003L).build();
        Table<Long, Long, InitialPlacementFinderResult> vm3Result
            = handler.findPlacement(PlanUtils.setupReservationRequest(Arrays.asList(vm3Buyer), 900003L));
        assertTrue(vm3Result.get(vm3Oid, vm3SlOid).getProviderOid().get() == pm1Oid);
        /*
        Historical:
        PM1 mem used 35, capacity 100. PM2 mem used 30, capacity 100. vm3 placed in PM1
        RealTime:
        PM1 mem used 35, capacity 100. PM2 mem used 30, capacity 100. vm3 placed in PM1
        */

        assertEquals(pf.economyCaches.historicalCachedEconomy.getTraders()
                .get(0).getCommoditiesSold().get(0).getQuantity(), 35d, 0.01);
        assertEquals(pf.economyCaches.historicalCachedEconomy.getTraders()
                .get(1).getCommoditiesSold().get(0).getQuantity(), 30d, 0.01);
        assertEquals(pf.economyCaches.realtimeCachedEconomy.getTraders()
                .get(0).getCommoditiesSold().get(0).getQuantity(), 35d, 0.01);
        assertEquals(pf.economyCaches.realtimeCachedEconomy.getTraders()
                .get(1).getCommoditiesSold().get(0).getQuantity(), 30d, 0.01);
        List vmsTobeDeleted = new ArrayList();
        vmsTobeDeleted.add(vm3Oid);
        // delete the VM3 from PM1, now the utilization of PM1 is lower only in realtime
        pf.buyersToBeDeleted(Arrays.asList(vm3Oid), true);
        vmsTobeDeleted.clear();
        /*
        Historical:
        PM1 mem used 35, capacity 100. PM2 mem used 30, capacity 100. vm3 and vm2 present
        RealTime:
        PM1 mem used 25, capacity 100. PM2 mem used 30, capacity 100. only vm2 is present.
        */
        assertEquals(pf.economyCaches.historicalCachedEconomy.getTraders()
                .get(0).getCommoditiesSold().get(0).getQuantity(), 35d, 0.01);
        assertEquals(pf.economyCaches.historicalCachedEconomy.getTraders()
                .get(1).getCommoditiesSold().get(0).getQuantity(), 30d, 0.01);
        assertEquals(pf.economyCaches.realtimeCachedEconomy.getTraders()
                .get(0).getCommoditiesSold().get(0).getQuantity(), 25d, 0.01);
        assertEquals(pf.economyCaches.realtimeCachedEconomy.getTraders()
                .get(1).getCommoditiesSold().get(0).getQuantity(), 30d, 0.01);
        assertTrue(pf.existingReservations.get(900003L).getInitialPlacementBuyerList().get(0).getDeployed());
        assertTrue(!pf.existingReservations.get(900002L).getInitialPlacementBuyerList().get(0).getDeployed());

        //Next VM will be placed on PM2 in historical and in PM1 in realtime.
        long vm4Oid = 10004L;
        long vm4SlOid = 20004L;
        long vm4Used = 10;
        InitialPlacementBuyer vm4Buyer = getTradersToPlace(vm4Oid, vm4SlOid, PM_TYPE, MEM_TYPE, vm4Used)
                .toBuilder().setReservationId(900004L).build();
        Table<Long, Long, InitialPlacementFinderResult> vm4Result
            = pf.findPlacement(PlanUtils.setupReservationRequest(Arrays.asList(vm4Buyer), 900004L));
        assertTrue(vm4Result.get(vm4Oid, vm4SlOid).getProviderOid().get() == pm1Oid);
         /*
        Historical:
        PM1 mem used 35, capacity 100. PM2 mem used 40, capacity 100. vm3 and vm2 present
        RealTime:
        PM1 mem used 35, capacity 100. PM2 mem used 30, capacity 100. only vm2 is present.
        */
        assertEquals(pf.economyCaches.historicalCachedEconomy.getTraders()
                .get(0).getCommoditiesSold().get(0).getQuantity(), 35d, 0.01);
        assertEquals(pf.economyCaches.historicalCachedEconomy.getTraders()
                .get(1).getCommoditiesSold().get(0).getQuantity(), 40d, 0.01);
        assertEquals(pf.economyCaches.realtimeCachedEconomy.getTraders()
                .get(0).getCommoditiesSold().get(0).getQuantity(), 35d, 0.01);
        assertEquals(pf.economyCaches.realtimeCachedEconomy.getTraders()
                .get(1).getCommoditiesSold().get(0).getQuantity(), 30d, 0.01);
        vmsTobeDeleted.add(vm4Oid);
        // delete vm4 full delete..vm4 delete from pm1 in realtime and pm2 in historical
        pf.buyersToBeDeleted(vmsTobeDeleted, false);
        /*
        Historical:
        PM1 mem used 35, capacity 100. PM2 mem used 30, capacity 100. vm3 and vm2 present
        RealTime:
        PM1 mem used 25, capacity 100. PM2 mem used 30, capacity 100. only vm2 is present.
        */
        assertEquals(pf.economyCaches.historicalCachedEconomy.getTraders()
                .get(0).getCommoditiesSold().get(0).getQuantity(), 35d, 0.01);
        assertEquals(pf.economyCaches.historicalCachedEconomy.getTraders()
                .get(1).getCommoditiesSold().get(0).getQuantity(), 30d, 0.01);
        assertEquals(pf.economyCaches.realtimeCachedEconomy.getTraders()
                .get(0).getCommoditiesSold().get(0).getQuantity(), 25d, 0.01);
        assertEquals(pf.economyCaches.realtimeCachedEconomy.getTraders()
                .get(1).getCommoditiesSold().get(0).getQuantity(), 30d, 0.01);
        // vm4 remains unchanged
        assertTrue(pf.existingReservations.get(900003L).getInitialPlacementBuyerList().get(0).getDeployed());
        assertTrue(!pf.existingReservations.get(900002L).getInitialPlacementBuyerList().get(0).getDeployed());
        assertEquals(pf.existingReservations.size(), 2);

        vmsTobeDeleted.clear();
        vmsTobeDeleted.add(vm3Oid);
        //delete vm3 full delete..vm3 deleted from historical from pm1
        pf.buyersToBeDeleted(vmsTobeDeleted, false);
        /*
        Historical:
        PM1 mem used 25, capacity 100. PM2 mem used 30, capacity 100. vm3 and vm2 present
        RealTime:
        PM1 mem used 25, capacity 100. PM2 mem used 30, capacity 100. only vm2 is present.
        */
        assertEquals(pf.economyCaches.historicalCachedEconomy.getTraders()
                .get(0).getCommoditiesSold().get(0).getQuantity(), 25d, 0.01);
        assertEquals(pf.economyCaches.historicalCachedEconomy.getTraders()
                .get(1).getCommoditiesSold().get(0).getQuantity(), 30d, 0.01);
        assertEquals(pf.economyCaches.realtimeCachedEconomy.getTraders()
                .get(0).getCommoditiesSold().get(0).getQuantity(), 25d, 0.01);
        assertEquals(pf.economyCaches.realtimeCachedEconomy.getTraders()
                .get(1).getCommoditiesSold().get(0).getQuantity(), 30d, 0.01);
        assertEquals(pf.existingReservations.size(), 1);

    }

    /**
     * Test reservation deletion in the findPlacement. The original economy contains VM1, PM1 and PM2.
     * VM1 resides on PM1. PM1 mem used 25, capacity 100. PM2 mem used 20, capacity 100.
     * Expected: new reservation VM2 with mem used 20 selects PM2. PM2 new mem used is 40.
     * Then deleting VM2, a new reservation VM3 with mem used 10 selects PM2.
     *
     * @throws InterruptedException        if we're interrupted
     * @throws ExecutionException          if failure in asynchronous updation
     */
    @Test
    public void testReservationDeletionAndAdd() throws ExecutionException, InterruptedException {
        InitialPlacementHandler handler = new InitialPlacementHandler(Mockito.mock(DSLContext.class),
                reservationServiceBlockingStub, true, 1, 5,
                diagsCollectorFactory);
        InitialPlacementFinder pf = handler.getPlacementFinder();
        Economy originalEconomy = getOriginalEconomy();
        pf.economyCaches.getState().setReservationReceived(true);
        handler.updateCachedEconomy(originalEconomy, commTypeToSpecMap, true);
        handler.updateCachedEconomy(originalEconomy, commTypeToSpecMap, false);

        long vm2Oid = 10002L;
        long vm2SlOid = 20002L;
        long vm2Used = 20;
        Table<Long, Long, InitialPlacementFinderResult> vm2Result
            = handler.findPlacement(PlanUtils.setupReservationRequest(Arrays.asList(
                getTradersToPlace(vm2Oid, vm2SlOid, PM_TYPE, MEM_TYPE, vm2Used)), 1L));
        assertTrue(vm2Result.get(vm2Oid, vm2SlOid).getProviderOid().get() == pm2Oid);
        // delete the VM1 which stays on PM1, now the utilization of PM1 is lower
        pf.buyersToBeDeleted(Arrays.asList(vm2Oid), false);
        long vm3Oid = 10003L;
        long vm3SlOid = 20003L;
        long vm3Used = 10;
        Table<Long, Long, InitialPlacementFinderResult> vm3Result
            = handler.findPlacement(PlanUtils.setupReservationRequest(Arrays.asList(
                getTradersToPlace(vm3Oid, vm3SlOid, PM_TYPE, MEM_TYPE, vm3Used)), 1L));
        assertTrue(vm3Result.get(vm3Oid, vm3SlOid).getProviderOid().get() == pm2Oid);
    }

    /**
     * Test grpc call to QueryExistingReservations from PO.
     * @throws InterruptedException the interrupted exception.
     */
    @Test
    public void testQueryExistingReservations() throws InterruptedException {
        InitialPlacementHandler handler = new InitialPlacementHandler(Mockito.mock(DSLContext.class),
                reservationServiceBlockingStub, true, 1, 5,
                diagsCollectorFactory);
        InitialPlacementDTO initialPlacement = PlanUtils.setupInitialPlacement(new ArrayList(Arrays
            .asList(getTradersToPlace(vmID, pmSlOid, PM_TYPE, MEM_TYPE, 100))), 1L);
        GetExistingReservationsRequest request = GetExistingReservationsRequest
                .newBuilder().build();
        GetExistingReservationsResponse response = GetExistingReservationsResponse
                .newBuilder().addInitialPlacement(initialPlacement).build();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        doAnswer(i -> {
            countDownLatch.countDown();
            return response;
        }).when(testReservationService).getExistingReservations(request);
        handler.getPlacementFinder().queryExistingReservations(120);
        countDownLatch.await();

        verify(testReservationService, times(1)).getExistingReservations(request);
    }
}
