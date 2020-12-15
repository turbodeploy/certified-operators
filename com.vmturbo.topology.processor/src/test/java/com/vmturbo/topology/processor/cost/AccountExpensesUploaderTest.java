package com.vmturbo.topology.processor.cost;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo.TierExpenses;
import com.vmturbo.common.protobuf.cost.Cost.ChecksumResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetAccountExpensesChecksumRequest;
import com.vmturbo.common.protobuf.cost.Cost.UploadAccountExpensesRequest;
import com.vmturbo.common.protobuf.cost.Cost.UploadAccountExpensesResponse;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc.RIAndExpenseUploadServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc.RIAndExpenseUploadServiceImplBase;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.kvstore.MapKeyValueStore;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.NonMarketDTO.CostDataDTO;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO.CloudServiceData;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO.CloudServiceData.BillingData;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO.NonMarketEntityType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.TargetCostData;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.identity.IdentityService;
import com.vmturbo.topology.processor.identity.services.HeuristicsMatcher;
import com.vmturbo.topology.processor.identity.storage.IdentityDatabaseStore;
import com.vmturbo.topology.processor.identity.storage.IdentityServiceInMemoryUnderlyingStore;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.probes.ProbeInfoCompatibilityChecker;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingEntityData;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;

/**
 *
 */
public class AccountExpensesUploaderTest {
    private static final Logger logger = LogManager.getLogger();

    private static final long TOPOLOGY_ID = 123456L;

    private static final long PROBE_ID_AWS_DISCOVERY = 1L;
    private static final long PROBE_ID_AWS_BILLING = 2L;
    private static final long TARGET_ID_AWS_DISCOVERY_1 = 1L;
    private static final long TARGET_ID_AWS_BILLING_1 = 2L;
    private static final long TARGET_ID_AWS_BILLING_2 = 3L;

    private static final long ACCOUNT_ID_1 = 11L;
    private static final long ACCOUNT_ID_2 = 22L;
    private static final String ACCOUNT_NAME_1 = "account-1";
    private static final String ACCOUNT_NAME_2 = "account-2";
    private static final String ACCOUNT_NAME_3 = "account-3";

    private static final long USAGE_DATE_1 = 1576800000000L;
    private static final long USAGE_DATE_2 = 1576886400000L;

    private static final String CLOUD_SERVICE_NAME_1 = "aws::CS::AmazonEC2";
    private static final String CLOUD_SERVICE_NAME_2 = "aws::CS::AmazonS3";
    private static final String CLOUD_SERVICE_NAME_3 = "azure::CS::Storage";

    private static final long CLOUD_SERVICE_ID_1 = 3L;
    private static final long CLOUD_SERVICE_ID_2 = 4L;

    private static final double COST_AMOUNT_1 = 0.1d;
    private static final double COST_AMOUNT_2 = 0.2d;
    private static final float COST_AMOUNT_FLOAT = 0.5f;

    private static final String AWS_CS_ID_1 = "aws::account-1::CS::AmazonEC2";
    private static final String AWS_CS_ID_2 = "aws::account-2::CS::AmazonEC2";
    private static final String AWS_CS_ID_3 = "aws::account-3::CS::AmazonEC2";
    private static final String AZURE_CS_ID_1 = "azure::CS::Storage::26080bd2-d98f-4420-a737-9de8";
    private static final String BAD_CS_ID =
            "azure::CS::Storage::BUSINESS_ACCOUNT::26080bd2-d98f-4420-a737-9de8";
    private static final String DATABASE_SERVER_1 = "843D1F6C-12FC-3EC2-9DDF-24C719ED2E20";
    private static final String COMPUTE_TIER_1 = "aws::VMPROFILE::t2.nano";
    private static final String COMPUTE_TIER_2 = "aws::VMPROFILE::m4.large";

    // simple KV store for the test identity provider
    private KeyValueStore keyValueStore = new MapKeyValueStore();
    {
        keyValueStore.put("id/probes/AWS", "11");
        keyValueStore.put("id/probes/Azure", "21");
    }

    private IdentityProvider identityProvider = new IdentityProviderImpl(
        new IdentityService(new IdentityServiceInMemoryUnderlyingStore(
            Mockito.mock(IdentityDatabaseStore.class), 10),
            new HeuristicsMatcher()), keyValueStore, new ProbeInfoCompatibilityChecker(), 0L);

    private final Discovery discoveryTopology =
            new Discovery(PROBE_ID_AWS_DISCOVERY, TARGET_ID_AWS_DISCOVERY_1, identityProvider);
    private final Discovery discoveryBilling1 =
            new Discovery(PROBE_ID_AWS_BILLING, TARGET_ID_AWS_BILLING_1, identityProvider);
    private final Discovery discoveryBilling2 =
            new Discovery(PROBE_ID_AWS_BILLING, TARGET_ID_AWS_BILLING_1, identityProvider);

    private StitchingContext mockStitchingContext = Mockito.mock(StitchingContext.class);

    // test GRPC server
    private final TestCostService costServiceSpy = spy(new TestCostService());

    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(costServiceSpy);

    // test cost component client
    private RIAndExpenseUploadServiceBlockingStub costServiceClient;

    //private TargetStore targetStore = Mockito.mock(TargetStore.class);
    private Map<Long, SDKProbeType> probeTypeMap;

    // test data
    private Map<Long, TargetCostData> costDataByTargetId;

    // object to be tested
    AccountExpensesUploader accountExpensesUploader;

    @Before
    public void setup() {
        costServiceClient = RIAndExpenseUploadServiceGrpc.newBlockingStub(server.getChannel());
        accountExpensesUploader = spy(new AccountExpensesUploader(costServiceClient, 60, Clock.systemUTC()));

        probeTypeMap = new HashMap<>();
        probeTypeMap.put(TARGET_ID_AWS_DISCOVERY_1, SDKProbeType.AWS);
        probeTypeMap.put(TARGET_ID_AWS_BILLING_1, SDKProbeType.AWS_BILLING);

        discoveryTopology.success();
        discoveryBilling1.success();
        discoveryBilling2.success();

        // set up the test discovered data cache
        costDataByTargetId = new HashMap<>();
        costDataByTargetId.put(TARGET_ID_AWS_DISCOVERY_1,
                createEmptyTargetCostData(TARGET_ID_AWS_DISCOVERY_1, discoveryTopology));

        List<NonMarketEntityDTO> nonMarketEntityDTOs = createNonMarketEntityDTOs();
        List<CostDataDTO> costDataDTOS = createCostDataDTOs();
        costDataByTargetId.put(TARGET_ID_AWS_BILLING_1,
                createTargetCostData(TARGET_ID_AWS_BILLING_1, discoveryBilling1,
                        nonMarketEntityDTOs, costDataDTOS));

        setupMockStitchingContext();
    }

    private TargetCostData createEmptyTargetCostData(long targetId, Discovery discovery) {
        TargetCostData costDataTopology = new TargetCostData();
        costDataTopology.targetId = targetId;
        costDataTopology.discovery = discovery;
        costDataTopology.cloudServiceEntities = Collections.emptyList();
        costDataTopology.costDataDTOS = Collections.emptyList();
        return costDataTopology;
    }

    private TargetCostData createTargetCostData(long targetId, Discovery discovery,
                                                List<NonMarketEntityDTO> nonMarketEntityDTOs,
                                                List<CostDataDTO> costDataDTOS) {
        TargetCostData costDataTopology = new TargetCostData();
        costDataTopology.targetId = targetId;
        costDataTopology.discovery = discovery;
        costDataTopology.cloudServiceEntities = nonMarketEntityDTOs;
        costDataTopology.costDataDTOS = costDataDTOS;
        return costDataTopology;
    }

    private List<CostDataDTO> createCostDataDTOs() {
        List<CostDataDTO> costDataDTOS = new ArrayList<>();
        costDataDTOS.add(CostDataDTO.newBuilder()
                .setAccountId(ACCOUNT_NAME_1).setId(AWS_CS_ID_1)
                .setEntityType(EntityType.CLOUD_SERVICE).setCost(COST_AMOUNT_FLOAT)
                .setAppliesProfile(false)
                .build());
        costDataDTOS.add(CostDataDTO.newBuilder()
                .setAccountId(ACCOUNT_NAME_2).setId(AWS_CS_ID_2)
                .setEntityType(EntityType.CLOUD_SERVICE).setCost(COST_AMOUNT_FLOAT)
                .setAppliesProfile(false)
                .build());
        costDataDTOS.add(CostDataDTO.newBuilder()
                .setAccountId(ACCOUNT_NAME_1).setId(DATABASE_SERVER_1)
                .setEntityType(EntityType.DATABASE_SERVER).setCost(COST_AMOUNT_FLOAT)
                .setAppliesProfile(false)
                .build());
        costDataDTOS.add(CostDataDTO.newBuilder()
                .setAccountId(ACCOUNT_NAME_1).setId(COMPUTE_TIER_1)
                .setEntityType(EntityType.VIRTUAL_MACHINE).setCost(COST_AMOUNT_FLOAT)
                .setAppliesProfile(false)
                .build());
        return costDataDTOS;
    }

     private List<NonMarketEntityDTO> createNonMarketEntityDTOs() {
         List<NonMarketEntityDTO> nmes = new ArrayList<>();
         nmes.add(NonMarketEntityDTO.newBuilder()
                 .setEntityType(NonMarketEntityType.CLOUD_SERVICE)
                 .setId(AWS_CS_ID_1).setDisplayName("AWS EC2")
                 .setCloudServiceData(CloudServiceData.newBuilder()
                         .setAccountId(ACCOUNT_NAME_1)
                         .setCloudProvider("AWS")
                         .setBillingData(BillingData.newBuilder()))
                 .build());
         nmes.add(NonMarketEntityDTO.newBuilder()
                 .setEntityType(NonMarketEntityType.CLOUD_SERVICE)
                 .setId(AWS_CS_ID_2).setDisplayName("AWS EC2")
                 .build());
         nmes.add(NonMarketEntityDTO.newBuilder() // we expect the account to be ignored
                 .setEntityType(NonMarketEntityType.ACCOUNT)
                 .setId("nme-account-1").setDisplayName("Non-Market-Entity Account 1")
                 .build());
         return nmes;
     }

     private void setupMockStitchingContext() {
         // set up the mock stitching context
         long now = System.currentTimeMillis();

         ImmutableMap.Builder<EntityType, Map<Long, List<TopologyStitchingEntity>>> entitiesByEntityTypeAndTarget =
                 ImmutableMap.builder();

         // CLOUD SERVICES
         TopologyStitchingEntity cloudService1 = new TopologyStitchingEntity(StitchingEntityData.newBuilder(
                 EntityDTO.newBuilder()
                         .setEntityType(EntityType.CLOUD_SERVICE)
                         .setId(CLOUD_SERVICE_NAME_1))
                 .oid(CLOUD_SERVICE_ID_1)
                 .targetId(TARGET_ID_AWS_BILLING_1)
                 .lastUpdatedTime(now)
                 .build());
         TopologyStitchingEntity cloudService2 = new TopologyStitchingEntity(StitchingEntityData.newBuilder(
                 EntityDTO.newBuilder()
                         .setEntityType(EntityType.CLOUD_SERVICE)
                         .setId(CLOUD_SERVICE_NAME_2))
                 .oid(CLOUD_SERVICE_ID_2)
                 .targetId(TARGET_ID_AWS_BILLING_1)
                 .lastUpdatedTime(now)
                 .build());

         entitiesByEntityTypeAndTarget.put(EntityType.CLOUD_SERVICE,
                 ImmutableMap.of(TARGET_ID_AWS_BILLING_1, Arrays.asList(cloudService1, cloudService2)));
         Mockito.when(mockStitchingContext.getEntitiesOfType(EntityType.CLOUD_SERVICE)).thenAnswer(
                 invocationOnMock -> Stream.of(cloudService1, cloudService2)
         );

         // BUSINESS ACCOUNTS
         // we will see the biz accounts discovered multiple times -- some in discovery and some in
         // billing
         TopologyStitchingEntity businessAccount1 = new TopologyStitchingEntity(
                 StitchingEntityData.newBuilder(
                     EntityDTO.newBuilder()
                             .setEntityType(EntityType.BUSINESS_ACCOUNT)
                             .setId(ACCOUNT_NAME_1))
                     .oid(ACCOUNT_ID_1)
                     .targetId(TARGET_ID_AWS_DISCOVERY_1)
                     .lastUpdatedTime(now)
                     .build());
         TopologyStitchingEntity businessAccount2 = new TopologyStitchingEntity(
                 StitchingEntityData.newBuilder(
                     EntityDTO.newBuilder()
                             .setEntityType(EntityType.BUSINESS_ACCOUNT)
                             .setId(ACCOUNT_NAME_2))
                     .oid(ACCOUNT_ID_2)
                     .targetId(TARGET_ID_AWS_DISCOVERY_1)
                     .lastUpdatedTime(now)
                     .build());
         TopologyStitchingEntity businessAccount3 = new TopologyStitchingEntity(
                 StitchingEntityData.newBuilder(
                     EntityDTO.newBuilder()
                             .setEntityType(EntityType.BUSINESS_ACCOUNT)
                             .setId(ACCOUNT_NAME_1))
                     .oid(ACCOUNT_ID_1)
                     .targetId(TARGET_ID_AWS_BILLING_1)
                     .lastUpdatedTime(now)
                     .build());
         TopologyStitchingEntity businessAccount4 = new TopologyStitchingEntity(
                 StitchingEntityData.newBuilder(
                     EntityDTO.newBuilder()
                             .setEntityType(EntityType.BUSINESS_ACCOUNT)
                             .setId(ACCOUNT_NAME_2))
                     .oid(ACCOUNT_ID_2)
                     .targetId(TARGET_ID_AWS_BILLING_1)
                     .lastUpdatedTime(now)
                     .build());

         entitiesByEntityTypeAndTarget.put(EntityType.BUSINESS_ACCOUNT,
                 ImmutableMap.of(TARGET_ID_AWS_BILLING_1, Arrays.asList(
                         businessAccount1, businessAccount2, businessAccount3, businessAccount4)));
         Mockito.when(mockStitchingContext.getEntitiesOfType(EntityType.BUSINESS_ACCOUNT)).thenAnswer(
                 invocationOnMock -> Stream.of(
                         businessAccount1, businessAccount2, businessAccount3, businessAccount4)
         );

         // COMPUTE TIERS
         TopologyStitchingEntity computeTier1 = new TopologyStitchingEntity(
                 StitchingEntityData.newBuilder(
                         EntityDTO.newBuilder()
                                 .setEntityType(EntityType.COMPUTE_TIER)
                                 .setId(COMPUTE_TIER_1))
                         .oid(21)
                         .targetId(TARGET_ID_AWS_DISCOVERY_1)
                         .lastUpdatedTime(now)
                         .build());
         TopologyStitchingEntity computeTier2 = new TopologyStitchingEntity(
                 StitchingEntityData.newBuilder(
                     EntityDTO.newBuilder()
                             .setEntityType(EntityType.COMPUTE_TIER)
                             .setId(COMPUTE_TIER_2))
                     .oid(22)
                     .targetId(TARGET_ID_AWS_DISCOVERY_1)
                     .lastUpdatedTime(now)
                     .build());

         entitiesByEntityTypeAndTarget.put(EntityType.COMPUTE_TIER,
                 ImmutableMap.of(TARGET_ID_AWS_DISCOVERY_1, Arrays.asList(computeTier1, computeTier2)));
         Mockito.when(mockStitchingContext.getEntitiesOfType(EntityType.COMPUTE_TIER)).thenAnswer(
                 invocationOnMock -> Stream.of(computeTier1, computeTier2)
         );

         // DATABASE TIERS
         Mockito.when(mockStitchingContext.getEntitiesOfType(EntityType.DATABASE_TIER)).thenAnswer(
                 invocationOnMock -> Stream.empty());

         // REGIONS
         Mockito.when(mockStitchingContext.getEntitiesOfType(EntityType.REGION)).thenAnswer(
                 invocationOnMock -> Stream.empty());

         // AVAILABILITY ZONES
         Mockito.when(mockStitchingContext.getEntitiesOfType(EntityType.AVAILABILITY_ZONE)).thenAnswer(
                 invocationOnMock -> Stream.empty());

         // RESERVED INSTANCES
         Mockito.when(mockStitchingContext.getEntitiesOfType(EntityType.RESERVED_INSTANCE)).thenAnswer(
                 invocationOnMock -> Stream.empty());

         // VIRTUAL MACHINES
         Mockito.when(mockStitchingContext.getEntitiesOfType(EntityType.VIRTUAL_MACHINE)).thenAnswer(
                 invocationOnMock -> Stream.empty());

         // STORAGE TIERS
         Mockito.when(mockStitchingContext.getEntitiesOfType(EntityType.STORAGE_TIER)).thenAnswer(
                 invocationOnMock -> Stream.empty());

         // DATABASE SERVER TIERS
         Mockito.when(mockStitchingContext.getEntitiesOfType(EntityType.DATABASE_SERVER_TIER)).thenAnswer(
                 invocationOnMock -> Stream.empty());

         Mockito.when(mockStitchingContext.getEntitiesByEntityTypeAndTarget())
                 .thenReturn(entitiesByEntityTypeAndTarget.build());

         Mockito.when(mockStitchingContext.getEntitiesOfType(EntityType.SERVICE_PROVIDER)).thenAnswer(
                 invocationOnMock -> Stream.empty());
     }

    /**
     * Test the creation of account expenses, based on the billing information, accounts and
     * topology entities that were discovered by the probes and stitched in the TP.
     */
    @Test
    public void testCreateAccountExpenses() {

        CloudEntitiesMap cloudEntitiesMap = new CloudEntitiesMap(mockStitchingContext, probeTypeMap);

        // create the account expenses
        Map<Long, AccountExpenses.Builder> accountExpensesByAccountOid =
                accountExpensesUploader.createAccountExpenses(cloudEntitiesMap,
                        mockStitchingContext, costDataByTargetId);

        // check account 1
        AccountExpenses.Builder awsAccount1Expenses = accountExpensesByAccountOid.get(ACCOUNT_ID_1);
        // we should see that account 1 has one service expense for 0.5.
        Assert.assertEquals(1, awsAccount1Expenses.getAccountExpensesInfo()
                .getServiceExpensesCount());
        Assert.assertEquals(COST_AMOUNT_FLOAT, awsAccount1Expenses.getAccountExpensesInfo()
                .getServiceExpenses(0).getExpenses().getAmount(), 0);
        // it should also have two tier expenses with a total cost of 1.0
        List<TierExpenses> tierExpenses = awsAccount1Expenses.getAccountExpensesInfo()
                .getTierExpensesList();
        Assert.assertEquals(1, tierExpenses.size());
        Assert.assertEquals(COST_AMOUNT_FLOAT, tierExpenses.stream()
                .mapToDouble(expenses -> expenses.getExpenses().getAmount())
                .sum(), 0);

        // account 2 should also have a single service expense of 0.5, but no tier expenses
        AccountExpenses.Builder awsAccount2Expenses = accountExpensesByAccountOid.get(ACCOUNT_ID_2);
        Assert.assertEquals(1, awsAccount2Expenses.getAccountExpensesInfo().getServiceExpensesCount());
        Assert.assertEquals(COST_AMOUNT_FLOAT, awsAccount2Expenses.getAccountExpensesInfo()
                .getServiceExpenses(0).getExpenses().getAmount(), 0);
        Assert.assertEquals(0, awsAccount2Expenses.getAccountExpensesInfo().getTierExpensesCount());
    }

    /**
     * Test that the account expenses are created and sent to the cost component, when all the
     * relevant information (billing data, accounts, cloud services) was discovered.
     */
    @Test
    public void testUploadAccountExpenses() {
        TopologyInfo topologyInfo = TopologyInfo.newBuilder().setTopologyId(TOPOLOGY_ID).build();
        CloudEntitiesMap cloudEntitiesMap = new CloudEntitiesMap(mockStitchingContext, probeTypeMap);

        // call uploadAccountExpenses twice - should create expenses and send them only once.
        accountExpensesUploader.uploadAccountExpenses(costDataByTargetId, topologyInfo,
                mockStitchingContext, cloudEntitiesMap);
        accountExpensesUploader.uploadAccountExpenses(costDataByTargetId, topologyInfo,
                mockStitchingContext, cloudEntitiesMap);

        Mockito.verify(accountExpensesUploader, Mockito.times(1))
                .createAccountExpenses(cloudEntitiesMap, mockStitchingContext, costDataByTargetId);
        Mockito.verify(costServiceSpy, Mockito.times(1))
                .uploadAccountExpenses(any(UploadAccountExpensesRequest.class), any());
    }

    /**
     * Test that the account expenses are uploaded even if some expenses were already uploaded in
     * the last hour (test lastUploadTime per target).
     */
    @Test
    public void testUploadAccountExpensesWithNewExpenses() {
        TopologyInfo topologyInfo = TopologyInfo.newBuilder().setTopologyId(TOPOLOGY_ID).build();
        CloudEntitiesMap cloudEntitiesMap = new CloudEntitiesMap(mockStitchingContext, probeTypeMap);

        // call uploadAccountExpenses with the discovered expenses
        accountExpensesUploader.uploadAccountExpenses(costDataByTargetId, topologyInfo,
                mockStitchingContext, cloudEntitiesMap);

        // make sure that the expenses were uploaded to the cost component
        Mockito.verify(accountExpensesUploader, Mockito.times(1))
                .createAccountExpenses(cloudEntitiesMap, mockStitchingContext, costDataByTargetId);
        Mockito.verify(costServiceSpy, Mockito.times(1))
                .uploadAccountExpenses(any(UploadAccountExpensesRequest.class), any());

        // new expenses discovered
        costDataByTargetId.put(TARGET_ID_AWS_BILLING_2,
                createTargetCostData(TARGET_ID_AWS_BILLING_2, discoveryBilling2,
                        Collections.singletonList(NonMarketEntityDTO.newBuilder()
                                .setEntityType(NonMarketEntityType.CLOUD_SERVICE)
                                .setId(AWS_CS_ID_3).setDisplayName("AWS EC2")
                                .setCloudServiceData(CloudServiceData.newBuilder()
                                        .setAccountId(ACCOUNT_NAME_3)
                                        .setCloudProvider("AWS")
                                        .setBillingData(BillingData.newBuilder()))
                                .build()),
                        Collections.singletonList(CostDataDTO.newBuilder()
                                .setAccountId(ACCOUNT_NAME_2).setId(AWS_CS_ID_2)
                                .setEntityType(EntityType.CLOUD_SERVICE).setCost(COST_AMOUNT_FLOAT)
                                .setAppliesProfile(false)
                                .build())));

        // upload expenses again, since TARGET_ID_AWS_BILLING_2 upload time is not initialized yet
        accountExpensesUploader.uploadAccountExpenses(costDataByTargetId, topologyInfo,
                mockStitchingContext, cloudEntitiesMap);

        Mockito.verify(accountExpensesUploader, Mockito.times(2))
                .createAccountExpenses(cloudEntitiesMap, mockStitchingContext, costDataByTargetId);
        Mockito.verify(costServiceSpy, Mockito.times(2))
                .uploadAccountExpenses(any(UploadAccountExpensesRequest.class), any());
    }

    /**
     * Test that account expenses are not being created and sent to cost component if no expenses
     * were discovered.
     */
    @Test
    public void testUploadAccountExpensesNoExpenses() {
        Map<Long, TargetCostData> emptyCostDataByTargetId = new HashMap<>();
        emptyCostDataByTargetId.put(TARGET_ID_AWS_DISCOVERY_1,
                createEmptyTargetCostData(TARGET_ID_AWS_DISCOVERY_1, discoveryTopology));
        TopologyInfo topologyInfo = TopologyInfo.newBuilder().setTopologyId(TOPOLOGY_ID).build();
        CloudEntitiesMap cloudEntitiesMap = new CloudEntitiesMap(mockStitchingContext, probeTypeMap);

        accountExpensesUploader.uploadAccountExpenses(emptyCostDataByTargetId, topologyInfo,
                mockStitchingContext, cloudEntitiesMap);

        Mockito.verify(accountExpensesUploader, Mockito.times(0))
                .shouldSkipProcessingExpenses(any());
        Mockito.verify(accountExpensesUploader, Mockito.times(0))
                .createAccountExpenses(any(), any(), any());
        Mockito.verify(costServiceSpy, Mockito.times(0))
                .uploadAccountExpenses(any(UploadAccountExpensesRequest.class), any());
    }

    /**
     * Test that account expenses are not being created and sent to cost component if no cloud
     * services were discovered.
     */
    @Test
    public void testUploadAccountExpensesNoTopologyEntities() {
        TopologyInfo topologyInfo = TopologyInfo.newBuilder().setTopologyId(TOPOLOGY_ID).build();
        StitchingContext emptyStitchingContext = Mockito.mock(StitchingContext.class);
        when(emptyStitchingContext.getEntitiesByEntityTypeAndTarget()).thenReturn(ImmutableMap.of());
        Mockito.when(emptyStitchingContext.getEntitiesOfType(any(EntityType.class))).thenAnswer(
                invocationOnMock -> Stream.empty());
        CloudEntitiesMap cloudEntitiesMap = new CloudEntitiesMap(emptyStitchingContext, probeTypeMap);

        accountExpensesUploader.uploadAccountExpenses(costDataByTargetId, topologyInfo,
                emptyStitchingContext, cloudEntitiesMap);

        Mockito.verify(accountExpensesUploader, Mockito.times(0))
                .shouldSkipProcessingExpenses(any());
        Mockito.verify(accountExpensesUploader, Mockito.times(0))
                .createAccountExpenses(any(), any(), any());
        Mockito.verify(costServiceSpy, Mockito.times(0))
                .uploadAccountExpenses(any(UploadAccountExpensesRequest.class), any());
    }

    /**
     * Check that the same account expenses in different order have the same checksum.
     */
    @Test
    public void testAccountExpensesChecksumSingleService() {

        AccountExpenses.Builder expenses1 = createAccountExpenses(ACCOUNT_ID_1, USAGE_DATE_1,
                ImmutableMap.of(CLOUD_SERVICE_ID_1, COST_AMOUNT_1));
        AccountExpenses.Builder expenses2 = createAccountExpenses(ACCOUNT_ID_2, USAGE_DATE_2,
                ImmutableMap.of(CLOUD_SERVICE_ID_1, COST_AMOUNT_2));

        UploadAccountExpensesRequest.Builder requestBuilder1 = UploadAccountExpensesRequest.newBuilder();
        requestBuilder1.addAccountExpenses(expenses1);
        requestBuilder1.addAccountExpenses(expenses2);

        UploadAccountExpensesRequest.Builder requestBuilder2 = UploadAccountExpensesRequest.newBuilder();
        requestBuilder2.addAccountExpenses(expenses2);
        requestBuilder2.addAccountExpenses(expenses1);

        long checksum1 = accountExpensesUploader
                .calcAccountExpensesUploadRequestChecksum(requestBuilder1.build());
        long checksum2 = accountExpensesUploader
                .calcAccountExpensesUploadRequestChecksum(requestBuilder2.build());

        Assert.assertEquals(checksum1, checksum2);
    }

    /**
     * Check that the same account expenses with service expenses in different order has the same checksum.
     */
    @Test
    public void testAccountExpensesChecksumMultipleServices() {

        // The service expenses order is different in these 2 account expenses.
        AccountExpenses.Builder expenses1 = createAccountExpenses(ACCOUNT_ID_1, USAGE_DATE_1,
                ImmutableMap.of(CLOUD_SERVICE_ID_1, COST_AMOUNT_1, CLOUD_SERVICE_ID_2, COST_AMOUNT_1));
        AccountExpenses.Builder expenses2 = createAccountExpenses(ACCOUNT_ID_1, USAGE_DATE_1,
                ImmutableMap.of(CLOUD_SERVICE_ID_2, COST_AMOUNT_1, CLOUD_SERVICE_ID_1, COST_AMOUNT_1));

        UploadAccountExpensesRequest.Builder requestBuilder1 = UploadAccountExpensesRequest.newBuilder();
        requestBuilder1.addAccountExpenses(expenses1);

        UploadAccountExpensesRequest.Builder requestBuilder2 = UploadAccountExpensesRequest.newBuilder();
        requestBuilder2.addAccountExpenses(expenses2);

        long checksum1 = accountExpensesUploader
                .calcAccountExpensesUploadRequestChecksum(requestBuilder1.build());
        long checksum2 = accountExpensesUploader
                .calcAccountExpensesUploadRequestChecksum(requestBuilder2.build());

        Assert.assertEquals(checksum1, checksum2);
    }

    /**
     * Check that account expenses with different usage dates don't have the same checksum.
     */
    @Test
    public void testAccountExpensesChecksumDifferentUsageDate() {

        Map<Long, Double> serviceIdToAmount = ImmutableMap.of(CLOUD_SERVICE_ID_1, COST_AMOUNT_1);

        // The usage date is different in these 2 account expenses.
        AccountExpenses.Builder expenses1 = createAccountExpenses(ACCOUNT_ID_1, USAGE_DATE_1,
                serviceIdToAmount);
        AccountExpenses.Builder expenses2 = createAccountExpenses(ACCOUNT_ID_1, USAGE_DATE_2,
                serviceIdToAmount);

        UploadAccountExpensesRequest.Builder requestBuilder1 = UploadAccountExpensesRequest.newBuilder();
        requestBuilder1.addAccountExpenses(expenses1);

        UploadAccountExpensesRequest.Builder requestBuilder2 = UploadAccountExpensesRequest.newBuilder();
        requestBuilder2.addAccountExpenses(expenses2);

        long checksum1 = accountExpensesUploader
                .calcAccountExpensesUploadRequestChecksum(requestBuilder1.build());
        long checksum2 = accountExpensesUploader
                .calcAccountExpensesUploadRequestChecksum(requestBuilder2.build());

        Assert.assertNotEquals(checksum1, checksum2);
    }

    private AccountExpenses.Builder createAccountExpenses(long accountId, long usageDate,
                                                          Map<Long, Double> serviceIdToAmount) {
        AccountExpenses.AccountExpensesInfo.Builder accountExpensesInfoBuilder =
                createAccountExpenseInfo(serviceIdToAmount);
        return AccountExpenses.newBuilder()
                .setAssociatedAccountId(accountId)
                .setExpensesDate(usageDate)
                .setAccountExpensesInfo(accountExpensesInfoBuilder.build());
    }

    private AccountExpensesInfo.Builder createAccountExpenseInfo(final Map<Long, Double> serviceIdToAmount) {
        List<AccountExpenses.AccountExpensesInfo.ServiceExpenses> serviceExpenses =
                serviceIdToAmount.entrySet().stream().map(pair ->
                        AccountExpenses.AccountExpensesInfo.ServiceExpenses.newBuilder()
                                .setAssociatedServiceId(pair.getKey())
                                .setExpenses(CurrencyAmount.newBuilder()
                                        .setAmount(pair.getValue())
                                        .setCurrency(840)
                                        .build()).build()).collect(Collectors.toList());
        return AccountExpenses.AccountExpensesInfo.newBuilder().addAllServiceExpenses(serviceExpenses);
    }

    @Test
    public void testSanitizeCloudServiceId() {
        final String sanitizedAwsId =
                accountExpensesUploader.sanitizeCloudServiceId(AWS_CS_ID_1);
        final String sanitizedAzureId =
                accountExpensesUploader.sanitizeCloudServiceId(AZURE_CS_ID_1);
        final String sanitizedBadCsId =
                accountExpensesUploader.sanitizeCloudServiceId(BAD_CS_ID);

        Assert.assertEquals(CLOUD_SERVICE_NAME_1, sanitizedAwsId);
        Assert.assertEquals(CLOUD_SERVICE_NAME_3, sanitizedAzureId);
        Assert.assertEquals(BAD_CS_ID, sanitizedBadCsId);
    }

    public static class TestCostService extends RIAndExpenseUploadServiceImplBase {
        @Override
        public void uploadAccountExpenses(final UploadAccountExpensesRequest request,
                                           final StreamObserver<UploadAccountExpensesResponse> responseObserver) {
            logger.info("upload account expenses called.");
            responseObserver.onNext(UploadAccountExpensesResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }

        @Override
        public void getAccountExpensesChecksum(final GetAccountExpensesChecksumRequest request,
                                          final StreamObserver<ChecksumResponse> responseObserver) {
            logger.info("upload account expenses called.");
            responseObserver.onNext(ChecksumResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }
    }
}
