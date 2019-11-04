package com.vmturbo.cost.component.reserved.instance;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.grpc.Channel;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.api.enums.EntityState;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.GetEntityReservedInstanceCoverageRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetEntityReservedInstanceCoverageResponse;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc.ReservedInstanceBoughtServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.api.RepositoryClient;

public class ReservedInstanceBoughtRpcServiceTest {

    private ReservedInstanceBoughtStore reservedInstanceBoughtStore =
            mock(ReservedInstanceBoughtStore.class);

    private EntityReservedInstanceMappingStore reservedInstanceMappingStore =
            mock(EntityReservedInstanceMappingStore.class);

    private RepositoryClient repositoryClient = mock(RepositoryClient.class);

    private SupplyChainServiceBlockingStub supplyChainService;

    private final Long realtimeTopologyContextId = 777777L;

    private ReservedInstanceBoughtRpcService service = new ReservedInstanceBoughtRpcService(
                reservedInstanceBoughtStore,
               reservedInstanceMappingStore, repositoryClient, supplyChainService,
               realtimeTopologyContextId);

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

    @Before
    public void setup() {
        client = ReservedInstanceBoughtServiceGrpc.newBlockingStub(grpcServer.getChannel());
        supplyChainService = SupplyChainServiceGrpc.newBlockingStub(grpcServer.getChannel());
        repositoryClient = new RepositoryClient(grpcServer.getChannel());
    }

    @Test
    public void testGetRiCoverage() {
        final Map<Long, EntityReservedInstanceCoverage> coverageMap =
                ImmutableMap.of(7L, EntityReservedInstanceCoverage.getDefaultInstance());
        when(reservedInstanceMappingStore.getEntityRiCoverage())
                .thenReturn(coverageMap);
        final GetEntityReservedInstanceCoverageResponse response =
            client.getEntityReservedInstanceCoverage(GetEntityReservedInstanceCoverageRequest.getDefaultInstance());

        assertThat(response.getCoverageByEntityIdMap(), is(coverageMap));
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
     *
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
        Set<Long> RegionSet = new HashSet<>();
        RegionSet.add(REGION1_OID);
        RegionSet.add(REGION2_OID);

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
                        .addAllMemberOids(RegionSet).build();
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

        final Map<EntityType, Set<Long>> topologyMap = repositoryClient.parseSupplyChainResponse(response,
                                                                                              777777L);

        assertTrue(topologyMap.size() > 0);
        // There should be 2 AZ's, 2 Regions and 3 Business Accounts associated with
        // the Billing Family, for the 2 VMs in scope.
        assertEquals(2, topologyMap.get(EntityType.REGION).size());
        assertEquals(2, topologyMap.get(EntityType.AVAILABILITY_ZONE).size());
        assertEquals(3, topologyMap.get(EntityType.BUSINESS_ACCOUNT).size());

    }
}
