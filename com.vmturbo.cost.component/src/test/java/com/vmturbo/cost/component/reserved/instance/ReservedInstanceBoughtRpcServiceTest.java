package com.vmturbo.cost.component.reserved.instance;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.enums.EntityState;
import com.vmturbo.common.protobuf.cost.BuyReservedInstanceServiceGrpc;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtCountByTemplateResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtCountRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtForAnalysisRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtForAnalysisResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtForScopeRequest;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.CostMoles.BuyReservedInstanceServiceMole;
import com.vmturbo.common.protobuf.cost.CostMoles.PlanReservedInstanceServiceMole;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc.PlanReservedInstanceServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc.ReservedInstanceBoughtServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
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

    final PlanReservedInstanceServiceMole planReservedInstanceServiceMole = spy(PlanReservedInstanceServiceMole.class);

    private RepositoryClient repositoryClient;

    private final Long realtimeTopologyContextId = 777777L;

    private ReservedInstanceBoughtServiceBlockingStub client;

    private static final Long AZ1_OID = 789L;
    private static final Long AZ2_OID = 123456L;
    private static final Long REGION1_OID = 789456L;
    private static final Long REGION2_OID = 456789L;
    private static final Long BA1_OID = 7L;
    private static final Long BA2_OID = 77L;
    private static final Long BAMASTER_OID = 777L;
    private static final long RI_SPEC_ID = 2222L;
    private static final long RI_SPEC_WINDOWS = 3333L;
    private static final long TIER_ID = 3333L;
    private static final long RI_BOUGHT_COUNT = 4L;
    private static final double TIER_PRICE = 0.41;
    private static final double LICENSE_PRICE = 0.21;
    private static final OSType WINDOWS_OS_TYPE = OSType.WINDOWS;
    private static final double delta = 0.01;
    private static final long RI_BOUGHT_ID_3 = 8002L;
    private static final long RI_SPEC_ID_3 = 2224L;

    private static final ReservedInstanceBoughtInfo RI_INFO_1 = ReservedInstanceBoughtInfo.newBuilder()
                    .setBusinessAccountId(123L)
                    .setProbeReservedInstanceId("bar")
                    .setReservedInstanceSpec(101L)
                    .setAvailabilityZoneId(100L)
                    .setNumBought(10)
                    .setReservedInstanceBoughtCost(ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost.newBuilder()
                                    .setFixedCost(CloudCostDTO.CurrencyAmount.newBuilder().setAmount(0))
                                    .setRecurringCostPerHour(CloudCostDTO.CurrencyAmount.newBuilder().setAmount(0.25)))
                    .setDisplayName("t101.small")
                    .build();

    private static final ReservedInstanceBoughtInfo RI_INFO_2 = ReservedInstanceBoughtInfo.newBuilder()
                    .setBusinessAccountId(456L)
                    .setProbeReservedInstanceId("foo")
                    .setReservedInstanceSpec(102L)
                    .setAvailabilityZoneId(100L)
                    .setNumBought(20)
                    .setReservedInstanceBoughtCost(ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost
                                    .newBuilder()
                                    .setFixedCost(CloudCostDTO.CurrencyAmount.newBuilder().setAmount(15))
                                    .setRecurringCostPerHour(CloudCostDTO.CurrencyAmount.newBuilder().setAmount(0.25)))
                    .setDisplayName("t102.large")
                    .build();

    private static final ReservedInstanceBoughtInfo RI_INFO_3 = ReservedInstanceBoughtInfo.newBuilder()
                    .setBusinessAccountId(456L)
                    .setProbeReservedInstanceId("foo")
                    .setReservedInstanceSpec(103L)
                    .setAvailabilityZoneId(100L)
                    .setNumBought(20)
                    .setReservedInstanceBoughtCost(ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost
                                    .newBuilder()
                                    .setFixedCost(CloudCostDTO.CurrencyAmount.newBuilder().setAmount(15))
                                    .setRecurringCostPerHour(CloudCostDTO.CurrencyAmount.newBuilder().setAmount(0.25)))
                    .setDisplayName("m3.large")
                    .build();

    private static final List<ReservedInstanceSpec> RESERVED_INSTANCE_SPECS =
            ImmutableList.of(createRiSpec(OSType.LINUX, RI_SPEC_ID),
                    createRiSpec(WINDOWS_OS_TYPE, RI_SPEC_WINDOWS));

    /**
     * Initialize instances for each test.
     *
     * @throws IOException in case of server start exception
     */
    @Before
    public void setup() throws IOException {

        final SupplyChainServiceMole supplyChainServiceMole = spy(SupplyChainServiceMole.class);
        final BuyReservedInstanceServiceMole buyReservedInstanceServiceMole =
                spy(BuyReservedInstanceServiceMole.class);

        final GrpcTestServer grpcTestServer =
                GrpcTestServer.newServer(buyReservedInstanceServiceMole,
                        planReservedInstanceServiceMole, supplyChainServiceMole);
        grpcTestServer.start();

        final SupplyChainServiceBlockingStub supplyChainService =
                SupplyChainServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
        repositoryClient =
                spy(new RepositoryClient(grpcTestServer.getChannel(), realtimeTopologyContextId));
        final ReservedInstanceSpecStore reservedInstanceSpecStore =
                mock(ReservedInstanceSpecStore.class);
        when(reservedInstanceSpecStore.getReservedInstanceSpecByIds(any())).thenReturn(
                RESERVED_INSTANCE_SPECS);
        when(reservedInstanceBoughtStore.getReservedInstanceCountByRISpecIdMap(any())).thenReturn(
                Collections.singletonMap(RI_SPEC_ID, RI_BOUGHT_COUNT));
        final PriceTableStore priceTableStore = mock(PriceTableStore.class);
        mockPricedTableStore(priceTableStore);

        final EntityReservedInstanceMappingStore reservedInstanceMappingStore =
                mock(EntityReservedInstanceMappingStore.class);

        PlanReservedInstanceServiceBlockingStub planReservedInstanceService =
                PlanReservedInstanceServiceGrpc.newBlockingStub(grpcTestServer.getChannel());

        final ReservedInstanceBoughtRpcService service =
                new ReservedInstanceBoughtRpcService(reservedInstanceBoughtStore,
                        reservedInstanceMappingStore, repositoryClient, supplyChainService,
                        planReservedInstanceService, realtimeTopologyContextId, priceTableStore,
                        reservedInstanceSpecStore, BuyReservedInstanceServiceGrpc.newBlockingStub(
                        grpcTestServer.getChannel()));

        final GrpcTestServer grpcServer = GrpcTestServer.newServer(service);
        grpcServer.start();
        client = ReservedInstanceBoughtServiceGrpc.newBlockingStub(grpcServer.getChannel());
    }

    private void mockPricedTableStore(final PriceTableStore priceTableStore) {
        final PriceTable priceTable = PriceTable.newBuilder()
                .putOnDemandPriceByRegionId(REGION1_OID,
                        OnDemandPriceTable.newBuilder().putComputePricesByTierId(TIER_ID,
                                ComputeTierPriceList.newBuilder()
                                        .setBasePrice(ComputeTierConfigPrice.newBuilder()
                                                .setGuestOsType(OSType.LINUX)
                                                .addPrices(Price.newBuilder()
                                                        .setPriceAmount(CurrencyAmount.newBuilder()
                                                                .setAmount(TIER_PRICE))))
                                        .addPerConfigurationPriceAdjustments(ComputeTierConfigPrice
                                                .newBuilder()
                                                .setGuestOsType(WINDOWS_OS_TYPE)
                                                .addPrices(Price.newBuilder()
                                                        .setPriceAmount(CurrencyAmount.newBuilder()
                                                                .setAmount(LICENSE_PRICE))))
                                        .build())
                                .build())
                .build();
        when(priceTableStore.getMergedPriceTable()).thenReturn(priceTable);
    }

    private static ReservedInstanceSpec createRiSpec(final OSType osType, final long riSpecId) {
        return ReservedInstanceSpec.newBuilder()
                .setId(riSpecId)
                .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                        .setRegionId(REGION1_OID)
                        .setOs(osType)
                        .setTierId(TIER_ID)
                        .build())
                .build();
    }

    private ReservedInstanceBought createRiBought(final long riSpecId) {
        return ReservedInstanceBought.newBuilder()
                .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                        .setReservedInstanceSpec(riSpecId))
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

         Boolean shared = true;
         // Sub-account to related accounts association.
         assertEquals(3, RepositoryClient.getFilteredScopeBusinessAccountOids(
                 Sets.newHashSet(7L),
                 allBusinessAccounts).size());
         // master account to sub-accounts association.
         assertEquals(3, repositoryClient.getFilteredScopeBusinessAccountOids(
                 Sets.newHashSet(777L),
                 allBusinessAccounts).size());
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
        when(reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(any()))
                .thenReturn(Collections.singletonList(createRiBought(RI_SPEC_ID)));
        final GetReservedInstanceBoughtByFilterRequest request =
                GetReservedInstanceBoughtByFilterRequest.newBuilder().build();
        final GetReservedInstanceBoughtByFilterResponse response =
                client.getReservedInstanceBoughtByFilter(request);
        Assert.assertNotNull(response);
        final List<ReservedInstanceBought> reservedInstancesBought =
                response.getReservedInstanceBoughtsList();
        Assert.assertFalse(reservedInstancesBought.isEmpty());
        final ReservedInstanceBought riBought = reservedInstancesBought.iterator().next();
        Assert.assertTrue(riBought.getReservedInstanceBoughtInfo().getReservedInstanceDerivedCost()
                .hasOnDemandRatePerHour());
        final double onDemandCost = riBought.getReservedInstanceBoughtInfo()
                .getReservedInstanceDerivedCost().getOnDemandRatePerHour()
                .getAmount();
        Assert.assertEquals(TIER_PRICE, onDemandCost, delta);
    }

    /**
     * Test that ReservedInstanceBought instance has onDemandCost set which includes the License
     * cost.
     */
    @Test
    public void testOnDemandCostWithLicenseSetRIBought() {
        // given
        when(reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(any()))
               .thenReturn(Collections.singletonList(createRiBought(RI_SPEC_WINDOWS)));
        final GetReservedInstanceBoughtByFilterRequest request =
                GetReservedInstanceBoughtByFilterRequest.newBuilder().build();

        // when
        final GetReservedInstanceBoughtByFilterResponse response =
                client.getReservedInstanceBoughtByFilter(request);

        // then
        final List<ReservedInstanceBought> reservedInstancesBought =
                response.getReservedInstanceBoughtsList();
        final ReservedInstanceBought riBought = reservedInstancesBought.iterator().next();
        final double onDemandCost = riBought.getReservedInstanceBoughtInfo()
                .getReservedInstanceDerivedCost().getOnDemandRatePerHour()
                .getAmount();
        Assert.assertEquals(TIER_PRICE + LICENSE_PRICE, onDemandCost, delta);
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
     * Tests the {{@link ReservedInstanceBoughtRpcService#getReservedInstanceBoughtForAnalysis}}
     * method.
     */
    @Test
    public void testGetReservedInstanceBoughtByTopology() {
        // ARRANGE
        GetReservedInstanceBoughtForAnalysisRequest request =
                GetReservedInstanceBoughtForAnalysisRequest.newBuilder()
                        .setTopologyInfo(TopologyInfo.newBuilder()
                                .setTopologyType(TopologyDTO.TopologyType.REALTIME)
                                .build())
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
        GetReservedInstanceBoughtForAnalysisResponse response = client.getReservedInstanceBoughtForAnalysis(request);

        // ASSERT
        assertThat(response.getReservedInstanceBoughtCount(), is(1));
        ReservedInstanceBought ri = response.getReservedInstanceBought(0);
        assertThat(ri.getId(), is(RI_BOUGHT_ID_3));
    }

    /**
     * Tests retrieval of RIs/Coupons included in OCP plans.
     */
    @Test
    public void testGetIncludedReservedInstanceBought() {

        // The plan added RIs above have Region ID == 101L and 102L, and below are all the
        // RIs in scope.
        Set<Long> scopeIds = new HashSet<>();
        scopeIds.add(101L);
        scopeIds.add(102L);
        scopeIds.add(103L);

        Mockito.doReturn(Collections.emptyList()).when(repositoryClient).getAllBusinessAccounts(anyLong());


        // setup what's expected to be returned on going through the real-time path.
        // When getSaved is false, the real-time RIs are fetched, which is all the RIs in scope.
        final List<ReservedInstanceBought> allReservedInstanceBought =
                        Arrays.asList(ReservedInstanceBought.newBuilder()
                                      .setReservedInstanceBoughtInfo(RI_INFO_1).build(),
                    ReservedInstanceBought.newBuilder().setReservedInstanceBoughtInfo(RI_INFO_2)
                                    .build(),
                    ReservedInstanceBought.newBuilder().setReservedInstanceBoughtInfo(RI_INFO_3)
                                    .build());

        when(reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(any())).thenReturn(
                allReservedInstanceBought);

        List<ReservedInstanceBought> riBought2 = client
                        .getReservedInstanceBoughtForScope(
                                GetReservedInstanceBoughtForScopeRequest.newBuilder()
                                        .addAllScopeSeedOids(scopeIds)
                                        .build())
                        .getReservedInstanceBoughtList();
        assertEquals(3, riBought2.size());
    }

}
