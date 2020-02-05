package com.vmturbo.cost.component.reserved.instance;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.api.enums.EntityState;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByTopologyRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByTopologyResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtCountByTemplateResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtCountRequest;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc.ReservedInstanceBoughtServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList.ComputeTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.repository.api.RepositoryClient;

/**
 * Test the ReservedInstanceBoughtRpcService.
 */
public class ReservedInstanceBoughtRpcServiceTest {

    private ReservedInstanceBoughtStore reservedInstanceBoughtStore =
            mock(ReservedInstanceBoughtStore.class);

    private EntityReservedInstanceMappingStore reservedInstanceMappingStore =
            mock(EntityReservedInstanceMappingStore.class);

    private RepositoryClient repositoryClient = mock(RepositoryClient.class);

    private SupplyChainServiceBlockingStub supplyChainService;

    private final Long realtimeTopologyContextId = 777777L;

    private ReservedInstanceSpecStore reservedInstanceSpecStore =
            mock(ReservedInstanceSpecStore.class);

    private PriceTableStore priceTableStore = mock(PriceTableStore.class);

    private ReservedInstanceBoughtRpcService service = new ReservedInstanceBoughtRpcService(
                reservedInstanceBoughtStore,
               reservedInstanceMappingStore, repositoryClient, supplyChainService,
               realtimeTopologyContextId, priceTableStore, reservedInstanceSpecStore);

    /**
     * Set up a test GRPC server.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(service);

    private ReservedInstanceBoughtServiceBlockingStub client;

    // Constants as defined in EntityType
    //private static final int REGION_VALUE = 54;
    //private static final int AVAILABILITY_ZONE_VALUE = 55;
    //private static final int BUSINESS_ACCOUNT_VALUE = 28;

    //private static final String REGION = "Region";
    //private static final String AVAILABILITY_ZONE = "AvailabilityZone";
    //private static final String BUSINESS_ACCOUNT = "BusinessAccount";

    private static final Long VM1_OID = 123L;
    private static final Long VM2_OID = 456L;
    private static final Long AZ1_OID = 789L;
    private static final Long AZ2_OID = 123456L;
    private static final Long REGION1_OID = 789456L;
    private static final Long REGION2_OID = 456789L;
    private static final Long BA1_OID = 7L;
    private static final Long BA2_OID = 77L;
    private static final Long BAMASTER_OID = 777L;
    private static final long RI_SPEC_ID = 2222L;
    private static final long TIER_ID = 3333L;
    private static final long RI_BOUGHT_COUNT = 4L;
    private static final double TIER_PRICE = 0.41;
    private static final double delta = 0.01;
    private static final long RI_BOUGHT_ID_3 = 8002L;
    private static final long RI_SPEC_ID_3 = 2224L;

    /**
     * Initialize instances for each test.
     */
    @Before
    public void setup() {
        client = ReservedInstanceBoughtServiceGrpc.newBlockingStub(grpcServer.getChannel());
        supplyChainService = SupplyChainServiceGrpc.newBlockingStub(grpcServer.getChannel());
        repositoryClient = new RepositoryClient(grpcServer.getChannel());
        when(reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(any()))
                .thenReturn(Collections.singletonList(createRiBought()));
        when(reservedInstanceSpecStore.getReservedInstanceSpecByIds(any()))
                .thenReturn(Collections.singletonList(createRiSpec()));
        when(reservedInstanceBoughtStore.getReservedInstanceCountByRISpecIdMap(any()))
                .thenReturn(Collections.singletonMap(RI_SPEC_ID, RI_BOUGHT_COUNT));
        mockPricedTableStore(priceTableStore);
    }

    private void mockPricedTableStore(final PriceTableStore priceTableStore) {
        final PriceTable priceTable = PriceTable.newBuilder()
                .putOnDemandPriceByRegionId(REGION1_OID,
                        OnDemandPriceTable.newBuilder().putComputePricesByTierId(TIER_ID,
                                ComputeTierPriceList.newBuilder()
                                        .setBasePrice(ComputeTierConfigPrice.newBuilder()
                                                .addPrices(Price.newBuilder()
                                                        .setPriceAmount(CurrencyAmount.newBuilder()
                                                                .setAmount(TIER_PRICE)
                                                                .build()))
                                                .build())
                                        .build())
                                .build())
                .build();
        when(priceTableStore.getMergedPriceTable()).thenReturn(priceTable);
    }

    private ReservedInstanceSpec createRiSpec() {
        return ReservedInstanceSpec.newBuilder()
                .setId(RI_SPEC_ID)
                .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                        .setRegionId(REGION1_OID)
                        .setTierId(TIER_ID)
                        .build())
                .build();
    }

    private ReservedInstanceBought createRiBought() {
        return ReservedInstanceBought.newBuilder()
                .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                        .setReservedInstanceSpec(RI_SPEC_ID)
                        .build())
                .build();
    }

    /**
     * Test getting all business account/subscription OIDs belonging to a Billing Family.
     */
    @Test
    public void testGetRelatedBusinessAccountAccountOrSubscriptionOids() {
         List<ConnectedEntity> connectedEntities = new ArrayList<>();
         final ConnectedEntity connectedEntityAccount1 =  ConnectedEntity.newBuilder()
                         .setConnectedEntityId(7L)
                         .setConnectedEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                         .build();
         final ConnectedEntity connectedEntityAccount2 = ConnectedEntity.newBuilder()
                         .setConnectedEntityId(77L)
                         .setConnectedEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                         .build();

         connectedEntities.add(connectedEntityAccount1);
         connectedEntities.add(connectedEntityAccount2);
         final TopologyEntityDTO topologyEntityAccount1 =   TopologyEntityDTO.newBuilder()
                         .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                         .setOid(7L)
                         .build();
         final TopologyEntityDTO topologyEntityAccount2 =   TopologyEntityDTO.newBuilder()
                         .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                         .setOid(77L)
                         .build();
         final TopologyEntityDTO topologyEntityAccountMaster =   TopologyEntityDTO.newBuilder()
                         .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                         .setOid(777L)
                         .addAllConnectedEntityList(connectedEntities)
                         .build();
         List<TopologyEntityDTO> allBusinessAccounts = new ArrayList<>();
         allBusinessAccounts.add(topologyEntityAccount1);
         allBusinessAccounts.add(topologyEntityAccount2);
         allBusinessAccounts.add(topologyEntityAccountMaster);

         List<Long> scopeIds = new ArrayList<>();
         // Sub-account to related accounts association.
         scopeIds.add(7L);
         assertEquals(3, repositoryClient
                         .parseRelatedBusinessAccountOrSubscriptionOids(scopeIds,
                                                                        allBusinessAccounts)
                         .size());

         scopeIds.clear();
         // master account to sub-accounts association.
         scopeIds.add(777L);
         assertEquals(3, repositoryClient
                         .parseRelatedBusinessAccountOrSubscriptionOids(scopeIds,
                                                                        allBusinessAccounts)
                         .size());
     }

    /**
     * Test retrieval of a VM's relationship to cloud scopes such as Availability Zone,
     * Region and Billing Family Business Accounts / Subscriptions.
     * VM --> AZ.
     * AZ --> REgion.
     * BA --> Billing Family accounts including master and sub-accounts.
     */
    @Test
    public void testParseCloudScopes() {
        Set<Long> baSet = new HashSet<>();
        baSet.add(BA1_OID);
        baSet.add(BA2_OID);
        baSet.add(BAMASTER_OID);
        Set<Long> azSet = new HashSet<>();
        azSet.add(AZ1_OID);
        azSet.add(AZ2_OID);
        Set<Long> regionSet = new HashSet<>();
        regionSet.add(REGION1_OID);
        regionSet.add(REGION2_OID);

        // Mocked up GetSupplyChainResponse
        final List<SupplyChainNode> supplyChainNodes = new ArrayList<>();
        MemberList membersBa = MemberList.newBuilder()
                        .addAllMemberOids(baSet).build();
        SupplyChainNode scnBA = SupplyChainNode.newBuilder()
                        .setEntityType("BusinessAccount")
                        .putMembersByState(EntityState.ACTIVE.ordinal(), membersBa)
                        .build();
        supplyChainNodes.add(scnBA);

        MemberList membersAz = MemberList.newBuilder()
                        .addAllMemberOids(azSet).build();
        SupplyChainNode scnAZ = SupplyChainNode.newBuilder()
                        .setEntityType("AvailabilityZone")
                        .putMembersByState(EntityState.ACTIVE.ordinal(), membersAz)
                        .build();
        supplyChainNodes.add(scnAZ);

        MemberList membersRegion = MemberList.newBuilder()
                        .addAllMemberOids(regionSet).build();
        SupplyChainNode scnRegion = SupplyChainNode.newBuilder()
                        .setEntityType("Region")
                        .putMembersByState(EntityState.ACTIVE.ordinal(), membersRegion)
                        .build();
        supplyChainNodes.add(scnRegion);

        SupplyChain supplyChain = SupplyChain.newBuilder()
                        .addAllSupplyChainNodes(supplyChainNodes)
                        .build();

        final GetSupplyChainResponse response = GetSupplyChainResponse.newBuilder()
                        .setSupplyChain(supplyChain)
                        .build();

        final Map<EntityType, Set<Long>> topologyMap =
            repositoryClient.parseSupplyChainResponseToEntityOidsMap(response.getSupplyChain());

        assertTrue(topologyMap.size() > 0);
        // There should be 2 AZ's, 2 Regions and 3 Business Accounts associated with
        // the Billing Family, for the 2 VMs in scope.
        assertEquals(2, topologyMap.get(EntityType.REGION).size());
        assertEquals(2, topologyMap.get(EntityType.AVAILABILITY_ZONE).size());
        assertEquals(3, topologyMap.get(EntityType.BUSINESS_ACCOUNT).size());

    }

    /**
     * Test that ReservedInstanceBought instance has onDemandCost set.
     */
    @Test
    public void testOnDemandCostSetRIBought() {
        final GetReservedInstanceBoughtByFilterRequest request =
                GetReservedInstanceBoughtByFilterRequest.newBuilder().build();
        final GetReservedInstanceBoughtByFilterResponse response =
                client.getReservedInstanceBoughtByFilter(request);
        Assert.assertNotNull(response);
        final List<ReservedInstanceBought> reservedInstancesBought =
                response.getReservedInstanceBoughtsList();
        Assert.assertFalse(reservedInstancesBought.isEmpty());
        final ReservedInstanceBought riBought = reservedInstancesBought.iterator().next();
        Assert.assertTrue(riBought.getReservedInstanceBoughtInfo().getReservedInstanceDerivedCost().hasOnDemandRatePerHour());
        final double onDemandCost = riBought.getReservedInstanceBoughtInfo()
                .getReservedInstanceDerivedCost().getOnDemandRatePerHour()
                .getAmount();
        Assert.assertEquals(TIER_PRICE, onDemandCost, delta);
    }

    /**
     * Test that GetReservedInstanceBoughtCountByTemplate request returns correct bought count by
     * tier id.
     */
    @Test
    public void testGetReservedInstanceBoughtCountByTemplateType() {
        final GetReservedInstanceBoughtCountRequest request =
                GetReservedInstanceBoughtCountRequest.newBuilder().build();
        final GetReservedInstanceBoughtCountByTemplateResponse response = client
                .getReservedInstanceBoughtCountByTemplateType(request);
        Assert.assertNotNull(response);
        final Map<Long, Long> riBoughtCountByTierId = response.getReservedInstanceCountMapMap();
        Assert.assertFalse(riBoughtCountByTierId.isEmpty());
        Assert.assertEquals(TIER_ID, riBoughtCountByTierId.keySet().iterator().next(), delta);
        Assert.assertEquals(RI_BOUGHT_COUNT, riBoughtCountByTierId.get(TIER_ID), delta);
    }

    /**
     * Tests the {{@link ReservedInstanceBoughtRpcService#getReservedInstanceBoughtByTopology}}
     * method.
     */
    @Test
    public void testGetReservedInstanceBoughtByTopology() {
        // ARRANGE
        GetReservedInstanceBoughtByTopologyRequest request =
            GetReservedInstanceBoughtByTopologyRequest.newBuilder()
                .setTopologyType(TopologyDTO.TopologyType.REALTIME)
                .build();

        // Set up what RI bought store returns
        ReservedInstanceBought riBought = ReservedInstanceBought.newBuilder()
            .setId(RI_BOUGHT_ID_3)
            .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                .setReservedInstanceSpec(RI_SPEC_ID_3)
                .setStartTime(Instant.now().minus(600, ChronoUnit.DAYS).toEpochMilli())
                .setStartTime(Instant.now().plus(495, ChronoUnit.DAYS).toEpochMilli())
                .build())
            .build();

        when(reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(any()))
            .thenReturn(ImmutableList.of(riBought));

        // ACT
        GetReservedInstanceBoughtByTopologyResponse response = client.getReservedInstanceBoughtByTopology(request);

        // ASSERT
        assertThat(response.getReservedInstanceBoughtCount(), is(1));
        ReservedInstanceBought ri = response.getReservedInstanceBought(0);
        assertThat(ri.getId(), is(RI_BOUGHT_ID_3));
    }
}
