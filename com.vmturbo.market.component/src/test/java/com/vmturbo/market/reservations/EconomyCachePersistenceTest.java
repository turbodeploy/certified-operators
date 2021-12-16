package com.vmturbo.market.reservations;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import org.apache.commons.lang.ArrayUtils;
import org.jooq.DSLContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementDTO;
import com.vmturbo.common.protobuf.plan.ReservationDTOMoles.ReservationServiceMole;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.market.component.db.Market;
import com.vmturbo.market.component.db.tables.EconomyCache;
import com.vmturbo.market.component.db.tables.records.EconomyCacheRecord;
import com.vmturbo.market.db.MarketDbEndpointConfig;
import com.vmturbo.market.reservations.EconomyCachePersistenceTest.TestMarketDbEndpointConfig;
import com.vmturbo.plan.orchestrator.api.PlanUtils;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.protobuf.EconomyCacheDTOs.EconomyCacheDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointCompleter;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.DbEndpointTestRule;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * Unit tests for EconomyCachePersistence.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {TestMarketDbEndpointConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_CLASS)
@TestPropertySource(properties = {"sqlDialect=MARIADB"})
public class EconomyCachePersistenceTest {

    @Autowired(required = false)
    private TestMarketDbEndpointConfig dbEndpointConfig;

    /**
     * Test rule to use {@link com.vmturbo.sql.utils.DbEndpoint}s in test.
     */
    @ClassRule
    public static DbEndpointTestRule dbEndpointTestRule = new DbEndpointTestRule("market");

    /**
     * Rule to manage feature flag enablement to make sure FeatureFlagManager store is set up.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule().testAllCombos(
            FeatureFlags.POSTGRES_PRIMARY_DB);

    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Market.MARKET);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    private static String cluster1Key = "cluster1";
    private static String cluster2Key = "cluster2";
    private static final int CLUSTER1_COMM_SPEC_TYPE = 300;
    private static final int CLUSTER2_COMM_SPEC_TYPE = 400;
    private static final int MEM_TYPE = CommodityType.MEM_VALUE;
    private DSLContext dsl;
    private EconomyCachePersistence persistence;
    private static final BiMap<TopologyDTO.CommodityType, Integer> commTypeToSpecMap = HashBiMap.create();
    private ReservationServiceBlockingStub reservationServiceBlockingStub;

    /**
     * Set up the commodity map.
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
     * Common code before every test.
     *
     * @throws SQLException if there is db error
     * @throws UnsupportedDialectException if the dialect is not supported
     * @throws InterruptedException if interrupted
    */
    @Before
    public void setUpBefore()
            throws SQLException, UnsupportedDialectException, InterruptedException {
        if (FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled()) {
            dbEndpointTestRule.addEndpoints(dbEndpointConfig.marketEndpoint());
            dsl = dbEndpointConfig.marketEndpoint().dslContext();
        } else {
            dsl = dbConfig.getDslContext();
        }
        persistence = new EconomyCachePersistence(dsl);
        IdentityGenerator.initPrefix(0);
    }

    /**
     * Verify the EconomyCachePersistence.saveEconomyCache trigger the db write.
     */
    @Test
    public void testSaveEconomyCache() {
        EconomyCachePersistence spy = Mockito.spy(persistence);
        spy.saveEconomyCache(EconomyCachesTest.simpleEconomy(), commTypeToSpecMap, true);
        Mockito.verify(spy, Mockito.times(1)).convertToProto(Mockito.any(), Mockito.any());
        Mockito.verify(spy, Mockito.times(1)).writeToEconomyCacheTable(Mockito.anyList(), Mockito.eq(true));
    }

    /**
     * Test the convertToProto method on a simple economy with only 2 pm traders.
     */
    @Test
    public void testConvertToProto() {
        EconomyCacheDTO economyCacheDTO = persistence.convertToProto(EconomyCachesTest.simpleEconomy(), commTypeToSpecMap);

        Assert.assertEquals(commTypeToSpecMap.size(), economyCacheDTO.getCommTypeEntryCount());
        Assert.assertEquals(EconomyCachesTest.simpleEconomy().getTraders().size(), economyCacheDTO.getTradersCount());
    }

    /**
     * Test the writeToEconomyCacheTable by persisting a string object and loads it back.
     */
    @Test
    public void testWriteToEconomyCacheTable() {
        List<List<Byte>> chunkList = new ArrayList();
        String obj = "TestWriteToDB";
        Byte[] test = ArrayUtils.toObject(obj.getBytes());
        chunkList.add(Arrays.asList(test));
        persistence.writeToEconomyCacheTable(chunkList, true);

        byte[] result = new byte[obj.getBytes().length];
        List<EconomyCacheRecord> record = dsl.selectFrom(EconomyCache.ECONOMY_CACHE).where(
                EconomyCache.ECONOMY_CACHE.ID.greaterThan(0)).orderBy(
                EconomyCache.ECONOMY_CACHE.ID.sortAsc()).fetch();
        int index = 0;
        for (EconomyCacheRecord r : record) {
            if (r != null) {
                byte[] info = r.getValue(EconomyCache.ECONOMY_CACHE.ECONOMY);
                for (int i = 0; i < info.length; i++) {
                    result[index] = info[i];
                    index++;
                }
            }
        }

        // Confirm that the content loaded back from db is the same as original string object.
        Assert.assertTrue(obj.equals(new String(result, StandardCharsets.UTF_8)));
    }

    /**
     * Test updateHistoricalEconomyCache with an economy that has 1 reservation buyer placed.
     */
    @Test
    public void testUpdateHistoricalEconomyCacheWithPersistence() {
        ReservationServiceMole testReservationService = spy(new ReservationServiceMole());
        GrpcTestServer grpcServer = GrpcTestServer.newServer(testReservationService);
        try {
            grpcServer.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
        ReservationServiceBlockingStub stub = ReservationServiceGrpc.newBlockingStub(grpcServer.getChannel());
        InitialPlacementFinder finder = new InitialPlacementFinder(dsl, stub, true, 2, 5);
        final long buyer1Oid = 1234L;
        final long buyerSl1Oid = 1000L;
        final long reservation1Oid = 1L;
        final double buyerMemUsed = 20;
        final double pm1MemUsed = 10;
        final double pm2MemUsed = 20;
        final double pm3MemUsed = 30;
        final double pm4MemUsed = 40;
        // Construct the historical economy with 4 pms in 2 different clusters. Cluster1 has pm1 and pm2.
        // Cluster2 has pm3 and pm4. All four pm mem capacity is 100. A reservation with oid 1L has 1 buyer
        // already finds cluster1's pm1 as the provider.
        InitialPlacementBuyer buyer1 = EconomyCachesTest.initialPlacementBuyer(buyer1Oid, buyerSl1Oid,
                EntityType.VIRTUAL_MACHINE_VALUE, new HashMap() {{
            put(TopologyDTO.CommodityType.newBuilder().setType(CommodityType.MEM_VALUE).build(),
                    new Double(buyerMemUsed));
            put(TopologyDTO.CommodityType.newBuilder().setType(CommodityType.CLUSTER_VALUE)
                    .setKey("cluster1").build(), 1.0d);
        }});
        List<InitialPlacementBuyer> buyerList = new ArrayList();
        buyerList.add(buyer1);
        Map<Long, InitialPlacementDTO> existingReservations =  new HashMap() {{
            put(reservation1Oid, PlanUtils.setupInitialPlacement(buyerList, reservation1Oid));
        }};
        Map<Long, List<InitialPlacementDecision>> buyerPlacements = new HashMap() {{
            put(buyer1Oid, Arrays.asList(new InitialPlacementDecision(buyerSl1Oid,
                    Optional.of(1112L), new ArrayList())));
        }};
        finder.economyCaches.updateHistoricalCachedEconomy(EconomyCachesTest.economyWithCluster(
                new double[]{pm1MemUsed, pm2MemUsed, pm3MemUsed, pm4MemUsed}), commTypeToSpecMap,
                buyerPlacements, existingReservations);
        // Assuming the PO returns a reservation with oid 2L that has 1 buyer. The buyer has pm4 as
        // the provider.
        final long buyer2Oid = 2234L;
        final long buyerSl2Oid = 2000L;
        final long reservation2Oid = 2L;
        InitialPlacementBuyer buyer2 = EconomyCachesTest.initialPlacementBuyer(buyer2Oid, buyerSl2Oid,
                EntityType.VIRTUAL_MACHINE_VALUE, new HashMap() {{
            put(TopologyDTO.CommodityType.newBuilder().setType(CommodityType.MEM_VALUE).build(),
                    new Double(buyerMemUsed));
            put(TopologyDTO.CommodityType.newBuilder().setType(CommodityType.CLUSTER_VALUE)
                    .setKey("cluster2").build(), 1.0d);
        }});
        List<InitialPlacementBuyer> newBuyerList = new ArrayList();
        newBuyerList.add(buyer2);
        finder.buyerPlacements = new HashMap() {{
            put(buyer2Oid, Arrays.asList(new InitialPlacementDecision(buyerSl2Oid, Optional.of(1114L), new ArrayList())));
        }};
        finder.existingReservations = new HashMap() {{
            put(reservation2Oid, PlanUtils.setupInitialPlacement(newBuyerList, reservation2Oid));
        }};
        finder.economyCaches.getState().setReservationReceived(true);
        // Restore economy cache would eliminate any previously placed buyers from the loaded
        finder.restoreEconomyCaches(180);

        Assert.assertTrue(finder.economyCaches.historicalCachedEconomy.getTraders().size() == 5);
        Assert.assertTrue(finder.economyCaches.historicalCachedEconomy.getTraders().stream()
                .filter(t -> !InitialPlacementUtils.PROVIDER_ENTITY_TYPES.contains(t.getType()))
                .allMatch(t -> t.getOid() == buyer2Oid));
        Trader pm4 = finder.economyCaches.historicalCachedEconomy.getTraders().stream()
                .filter(t -> t.getOid() == 1114L).findFirst().get();
        Assert.assertEquals(pm4MemUsed + buyerMemUsed, pm4.getCommoditiesSold()
                .get(pm4.getBasketSold().indexOf(CommodityType.MEM_VALUE)).getQuantity(), 0.001);
    }

    /**
     * Workaround for {@link MarketDbEndpointConfig} (remove conditional annotation), since
     * it's conditionally initialized based on {@link com.vmturbo.components.common.featureflags.FeatureFlags#POSTGRES_PRIMARY_DB}. When we
     * test all combinations of it using {@link com.vmturbo.test.utils.FeatureFlagTestRule}, first it's false, so
     * {@link MarketDbEndpointConfig} is not created; then second it's true,
     * {@link MarketDbEndpointConfig} is created, but the endpoint inside is also eagerly
     * initialized due to the same FF, which results in several issues like: it doesn't go through
     * DbEndpointTestRule, making call to auth to get root password, etc.
     */
    @Configuration
    public static class TestMarketDbEndpointConfig extends MarketDbEndpointConfig {
        @Override
        public DbEndpointCompleter endpointCompleter() {
            // prevent actual completion of the DbEndpoint
            DbEndpointCompleter dbEndpointCompleter = spy(super.endpointCompleter());
            doNothing().when(dbEndpointCompleter).setEnvironment(any());
            return dbEndpointCompleter;
        }
    }
}
