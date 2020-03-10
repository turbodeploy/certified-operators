package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;


import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.component.db.tables.records.ComputeTierTypeHourlyByWeekRecord;
import com.vmturbo.cost.component.reserved.instance.ComputeTierDemandStatsStore;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalysisScope;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstancePurchaseConstraints;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.ImmutableReservedInstanceSpecData;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.RegionalRIMatcherCache;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.RegionalRIMatcherCacheFactory;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.ReservedInstanceSpecMatcher;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.ReservedInstanceSpecMatcher.ReservedInstanceSpecData;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.ReservedInstanceData.Platform;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

/**
 * This class tests methods in the ReservedInstanceSpecStore class.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=cost"})
public class RIBuyAnalysisContextProviderTest {

    private static final long REGION_ID = 111;
    private static final Long MASTER_ACCOUNT_1_OID = 222L;
    private static final Long TIER_ID = 333L;
    private static final Long ACCOUNT_ID = 444L;
    private static final long ZONE_ID = 666L;

    private static final Map<Long, TopologyEntityDTO> entityMap = getEntityMap();
    private static final long VM_ID = 101L;
    private static final long MASTER_VM_ID = 102L;
    private static final long SPEC_ID = 777L;
    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    private static final long CONTEXT_ID = 9999L;

    private Flyway flyway;

    private DSLContext dsl;

    private final RepositoryServiceMole repositoryService = spy(new RepositoryServiceMole());
    private final RegionalRIMatcherCache matcherCache = mock(RegionalRIMatcherCache.class);
    private final ReservedInstanceSpecMatcher matcher = mock(ReservedInstanceSpecMatcher.class);

    private final GrpcTestServer grpcServer = GrpcTestServer.newServer(repositoryService);

    /**
     * Setup each test.
     * @throws Exception due to database operations.
     */
    @Before
    public void setup() throws Exception {

        flyway = dbConfig.flyway();
        dsl = dbConfig.dsl();
        flyway.clean();
        flyway.migrate();
        grpcServer.start();

        when(repositoryService.retrieveTopologyEntities(any()))
                .thenReturn(Arrays.asList(PartialEntityBatch.newBuilder()
                        .addAllEntities(entityMap.values()
                                .stream()
                                .map(e -> PartialEntity.newBuilder()
                                        .setFullEntity(e)
                                        .build())
                                .collect(Collectors.toList()))
                        .build()));

        ReservedInstanceSpecInfo info = ReservedInstanceSpecInfo.newBuilder()
                .setTierId(TIER_ID)
                .setRegionId(REGION_ID)
                .setOs(CloudCostDTO.OSType.LINUX)
                .setTenancy(Tenancy.DEFAULT)
                .setSizeFlexible(false).build();
        ReservedInstanceSpec spec = ReservedInstanceSpec.newBuilder()
                .setReservedInstanceSpecInfo(info)
                .setId(SPEC_ID).build();
        ReservedInstanceSpecData data =
                ImmutableReservedInstanceSpecData.builder().computeTier(entityMap.get(TIER_ID))
                        .reservedInstanceSpec(spec).couponsPerInstance(1).build();
        when(matcher.matchToPurchasingRISpecData(any(TopologyEntityDTO.class),
                any(TopologyEntityDTO.class), any(OSType.class), any(Tenancy.class)))
                .thenReturn(Optional.of(data));
        when(matcherCache.getOrCreateRISpecMatchForRegion(REGION_ID)).thenReturn(matcher);
    }

    /**
     * Teardown after test finishes.
     */
    @After
    public void teardown() {
        flyway.clean();
    }

    /**
     * Test the compute analysis contexts with the scope containing accounts.
     */
    @Test
    public void testComputeAnalysisContexts() {

        ComputeTierDemandStatsStore computeTierDemandStatsStore =
                new ComputeTierDemandStatsStore(dsl, 100, 100);

        ComputeTierTypeHourlyByWeekRecord record1 = setupComputeTierTypeHourlyByWeekRecord(REGION_ID, ACCOUNT_ID, TIER_ID);
        ComputeTierTypeHourlyByWeekRecord record2 = setupComputeTierTypeHourlyByWeekRecord(ZONE_ID, MASTER_ACCOUNT_1_OID, TIER_ID);

        computeTierDemandStatsStore.persistComputeTierDemandStats(ImmutableList.of(record1, record2), false);
        RepositoryServiceBlockingStub repositoryClient = RepositoryServiceGrpc.newBlockingStub(grpcServer.getChannel());

        TopologyEntityCloudTopologyFactory cloudTopologyFactory = new DefaultTopologyEntityCloudTopologyFactory(mock(GroupMemberRetriever.class));

        RegionalRIMatcherCacheFactory regionalRIMatcherCacheFactory = mock(RegionalRIMatcherCacheFactory.class);
        when(regionalRIMatcherCacheFactory.createNewCache(any(CloudTopology.class),
                any(Map.class))).thenReturn(matcherCache);


        final StartBuyRIAnalysisRequest startBuyRIAnalysisRequest = StartBuyRIAnalysisRequest.newBuilder()
                .addAllAccounts(Lists.newArrayList(ACCOUNT_ID, MASTER_ACCOUNT_1_OID)).build();
        ReservedInstanceAnalysisScope scope = new ReservedInstanceAnalysisScope(startBuyRIAnalysisRequest);
        RIBuyAnalysisContextProvider contextProvider =
                new RIBuyAnalysisContextProvider(computeTierDemandStatsStore, repositoryClient,
                        cloudTopologyFactory, regionalRIMatcherCacheFactory, CONTEXT_ID,
                        true);


        final RIBuyAnalysisContextInfo contexts =
                contextProvider.computeAnalysisContexts(scope, null);
        Assert.assertNotNull(contexts);
        Assert.assertNotNull(contexts.regionalContexts());
        Assert.assertTrue(contexts.regionalContexts().size() == 2);
        contexts.regionalContexts().stream().forEach(context ->
            Assert.assertTrue(context.demandClusters().size() == 1));
    }

    /**
     * Test the compute analysis contexts with the scope containing a billing family.
     */
    @Test
    public void testComputeAnalysisContextsWithBillingFamily() {

        ComputeTierDemandStatsStore computeTierDemandStatsStore =
                new ComputeTierDemandStatsStore(dsl, 100, 100);

        ComputeTierTypeHourlyByWeekRecord record1 = setupComputeTierTypeHourlyByWeekRecord(REGION_ID, ACCOUNT_ID, TIER_ID);
        ComputeTierTypeHourlyByWeekRecord record2 = setupComputeTierTypeHourlyByWeekRecord(ZONE_ID, MASTER_ACCOUNT_1_OID, TIER_ID);

        computeTierDemandStatsStore.persistComputeTierDemandStats(ImmutableList.of(record1, record2), false);
        RepositoryServiceBlockingStub repositoryClient = RepositoryServiceGrpc.newBlockingStub(grpcServer.getChannel());

        GroupMemberRetriever groupMemberRetriever = mockBillingFamilySetup();


        TopologyEntityCloudTopologyFactory cloudTopologyFactory = new DefaultTopologyEntityCloudTopologyFactory(groupMemberRetriever);

        RegionalRIMatcherCacheFactory regionalRIMatcherCacheFactory = mock(RegionalRIMatcherCacheFactory.class);
        when(regionalRIMatcherCacheFactory.createNewCache(any(CloudTopology.class),
                any(Map.class))).thenReturn(matcherCache);


        final StartBuyRIAnalysisRequest startBuyRIAnalysisRequest = StartBuyRIAnalysisRequest.newBuilder()
                .addAllAccounts(Lists.newArrayList(ACCOUNT_ID, MASTER_ACCOUNT_1_OID)).build();
        ReservedInstanceAnalysisScope scope = new ReservedInstanceAnalysisScope(startBuyRIAnalysisRequest);
        RIBuyAnalysisContextProvider contextProvider =
                new RIBuyAnalysisContextProvider(computeTierDemandStatsStore, repositoryClient,
                        cloudTopologyFactory, regionalRIMatcherCacheFactory, CONTEXT_ID,
                        true);


        final RIBuyAnalysisContextInfo contexts =
                contextProvider.computeAnalysisContexts(scope, null);
        Assert.assertNotNull(contexts);
        Assert.assertNotNull(contexts.regionalContexts());
        Assert.assertTrue(contexts.regionalContexts().size() == 1);
        Assert.assertTrue(contexts.regionalContexts().get(0).demandClusters().size() == 2);
    }

    private GroupMemberRetriever mockBillingFamilySetup() {
        GroupMemberRetriever groupMemberRetriever = mock(GroupMemberRetriever.class);
        final GroupAndMembers billingFamily = mock(GroupAndMembers.class);
        when(billingFamily.entities()).thenReturn(ImmutableList.of(VM_ID, MASTER_VM_ID));
        Grouping group = Grouping.newBuilder().setDefinition(GroupDefinition.newBuilder().setDisplayName("TEST_BILLING_FAMILY").build()).build();
        when(billingFamily.group()).thenReturn(group);
        when(billingFamily.members()).thenReturn(ImmutableList.of(ACCOUNT_ID, MASTER_ACCOUNT_1_OID));
        final List<GroupAndMembers> billingFamilygroups = ImmutableList.of(billingFamily);
        when(groupMemberRetriever.getMembersForGroup(any(List.class))).thenReturn(ImmutableList.of(ACCOUNT_ID, MASTER_ACCOUNT_1_OID));
        when(groupMemberRetriever.getGroupsWithMembers(any(GetGroupsRequest.class))).thenReturn(billingFamilygroups);
        return groupMemberRetriever;
    }


    private ComputeTierTypeHourlyByWeekRecord setupComputeTierTypeHourlyByWeekRecord(final long regionZoneId,
                                                                                     final long accountId,
                                                                                     final long tierId) {
        ComputeTierTypeHourlyByWeekRecord record = new ComputeTierTypeHourlyByWeekRecord();
        record.setRegionOrZoneId(regionZoneId);
        record.setAccountId(accountId);
        record.setComputeTierId(tierId);
        record.setCountFromProjectedTopology(new BigDecimal(1.0d));
        record.setCountFromSourceTopology(new BigDecimal(1.0d));
        record.setPlatform(Byte.valueOf(Platform.LINUX.getValue() + ""));
        record.setDay(Byte.valueOf("1"));
        record.setHour(Byte.valueOf("0"));
        record.setTenancy(Byte.valueOf(Tenancy.DEFAULT_VALUE + ""));
        return record;
    }

    private static Map<Long, TopologyEntityDTO> getEntityMap() {
        Map<Long, TopologyEntityDTO> entityMap = new HashMap<>();
        // build up a topology in which az is owned by region,
        // vm connectedTo az and consumes computeTier,
        // computeTier connectedTo region,
        // ba connectedTo vm
        TopologyEntityDTO az = TopologyEntityDTO.newBuilder()
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                .setOid(ZONE_ID)
                .build();
        TopologyEntityDTO region = TopologyEntityDTO.newBuilder()
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setEntityType(EntityType.REGION_VALUE)
                .setOid(REGION_ID)
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectionType(ConnectionType.OWNS_CONNECTION)
                        .setConnectedEntityId(ZONE_ID)
                        .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE))
                .build();
        TopologyEntityDTO ba = TopologyEntityDTO.newBuilder()
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .setOid(ACCOUNT_ID)
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(VM_ID)
                        .setConnectedEntityType(EntityType.VIRTUAL_MACHINE_VALUE))
                .build();
        TopologyEntityDTO ma = TopologyEntityDTO.newBuilder()
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .setOid(MASTER_ACCOUNT_1_OID)
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(MASTER_VM_ID)
                        .setConnectedEntityType(EntityType.VIRTUAL_MACHINE_VALUE))
                .build();

        TopologyEntityDTO computeTier = TopologyEntityDTO.newBuilder()
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                .setOid(TIER_ID)
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(REGION_ID)
                        .setConnectedEntityType(EntityType.REGION_VALUE))
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setComputeTier(ComputeTierInfo.newBuilder().setNumCoupons(10)))
                .build();
        TopologyEntityDTO vm = TopologyEntityDTO.newBuilder()
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(VM_ID)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(TIER_ID)
                        .setProviderEntityType(EntityType.COMPUTE_TIER_VALUE))
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(ZONE_ID)
                        .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE))
                .build();
        TopologyEntityDTO vm2 = TopologyEntityDTO.newBuilder()
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(MASTER_VM_ID)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(TIER_ID)
                        .setProviderEntityType(EntityType.COMPUTE_TIER_VALUE))
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(ZONE_ID)
                        .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE))
                .build();
        entityMap.put(ZONE_ID, az);
        entityMap.put(REGION_ID, region);
        entityMap.put(ACCOUNT_ID, ba);
        entityMap.put(MASTER_ACCOUNT_1_OID, ma);
        entityMap.put(TIER_ID, computeTier);
        entityMap.put(VM_ID, vm);
        entityMap.put(MASTER_VM_ID, vm2);
        return entityMap;
    }
}
