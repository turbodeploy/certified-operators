package com.vmturbo.topology.processor.cost;

import static org.mockito.Mockito.spy;

import java.time.Clock;
import java.util.ArrayList;
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
import com.vmturbo.common.protobuf.cost.Cost.UploadAccountExpensesRequest;
import com.vmturbo.common.protobuf.cost.Cost.UploadAccountExpensesResponse;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc.RIAndExpenseUploadServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc.RIAndExpenseUploadServiceImplBase;
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
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
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

    private static final long PROBE_ID_AWS_DISCOVERY = 1;
    private static final long PROBE_ID_AWS_BILLING = 2;
    private static final long TARGET_ID_AWS_DISCOVERY_1 = 1;
    private static final long TARGET_ID_AWS_BILLING_1 = 2;

    private static final long ACCOUNT_ID_1 = 1L;
    private static final long ACCOUNT_ID_2 = 2L;
    private static final long USAGE_DATE_1 = 1576800000000L;
    private static final long USAGE_DATE_2 = 1576886400000L;
    private static final long CLOUD_SERVICE_ID_1 = 3L;
    private static final long CLOUD_SERVICE_ID_2 = 4L;
    private static final double COST_AMOUNT_1 = 100d;
    private static final double COST_AMOUNT_2 = 200d;

    private static final String AWS_CS_ID = "aws::192821421245::CS::AmazonS3";
    private static final String AZURE_CS_ID = "azure::CS::Storage::26080bd2-d98f-4420-a737-9de8";
    private static final String BAD_CS_ID =
            "azure::CS::Storage::BUSINESS_ACCOUNT::26080bd2-d98f-4420-a737-9de8";

    // simple KV store for the test identity provider
    private KeyValueStore keyValueStore = new MapKeyValueStore();
    {
        keyValueStore.put("id/probes/AWS", "11");
        keyValueStore.put("id/probes/Azure", "21");
    }

    private IdentityProvider identityProvider = new IdentityProviderImpl(
        new IdentityService(new IdentityServiceInMemoryUnderlyingStore(
            Mockito.mock(IdentityDatabaseStore.class)),
            new HeuristicsMatcher()), keyValueStore, new ProbeInfoCompatibilityChecker(), 0L);

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

        probeTypeMap = new HashMap<>();
        probeTypeMap.put(TARGET_ID_AWS_DISCOVERY_1, SDKProbeType.AWS);
        probeTypeMap.put(TARGET_ID_AWS_BILLING_1, SDKProbeType.AWS_BILLING);

        costServiceClient = RIAndExpenseUploadServiceGrpc.newBlockingStub(server.getChannel());

        accountExpensesUploader = new AccountExpensesUploader(costServiceClient, 0, Clock.systemUTC());

        // create some discovery cost data for the uploader to cache
        Discovery discoveryTopology = new Discovery(PROBE_ID_AWS_DISCOVERY, TARGET_ID_AWS_DISCOVERY_1, identityProvider);
        discoveryTopology.success();
        Discovery discoveryBilling = new Discovery(PROBE_ID_AWS_BILLING, TARGET_ID_AWS_BILLING_1, identityProvider);
        discoveryBilling.success();

        // create some NME's
        List<NonMarketEntityDTO> nmes = new ArrayList<>();
        nmes.add(NonMarketEntityDTO.newBuilder()
                .setEntityType(NonMarketEntityType.CLOUD_SERVICE)
                .setId("aws::account-1::CS::AmazonEC2").setDisplayName("AWS EC2")
                .setCloudServiceData(CloudServiceData.newBuilder()
                        .setAccountId("account-1")
                        .setCloudProvider("AWS")
                        .setBillingData(BillingData.newBuilder()))
                .build());
        nmes.add(NonMarketEntityDTO.newBuilder()
                .setEntityType(NonMarketEntityType.CLOUD_SERVICE)
                .setId("aws::account-2::CS::AmazonEC2").setDisplayName("AWS EC2")
                .build());
        nmes.add(NonMarketEntityDTO.newBuilder() // we expect the account to be ignored
                .setEntityType(NonMarketEntityType.ACCOUNT)
                .setId("nme-account-1").setDisplayName("Non-Market-Entity Account 1")
                .build());

        // create some cost data dtos
        List<CostDataDTO> costDataDTOS = new ArrayList<>();
        costDataDTOS.add(CostDataDTO.newBuilder()
                .setAccountId("account-1").setId("aws::account-1::CS::AmazonEC2")
                .setEntityType(EntityType.CLOUD_SERVICE).setCost(0.5f)
                .setAppliesProfile(false)
                .build());
        costDataDTOS.add(CostDataDTO.newBuilder()
                .setAccountId("account-2").setId("aws::account-2::CS::AmazonEC2")
                .setEntityType(EntityType.CLOUD_SERVICE).setCost(0.5f)
                .setAppliesProfile(false)
                .build());
        costDataDTOS.add(CostDataDTO.newBuilder()
                .setAccountId("account-1").setId("843D1F6C-12FC-3EC2-9DDF-24C719ED2E20")
                .setEntityType(EntityType.DATABASE_SERVER).setCost(0.5f)
                .setAppliesProfile(false)
                .build());
        costDataDTOS.add(CostDataDTO.newBuilder()
                .setAccountId("account-1").setId("aws::VMPROFILE::t2.nano")
                .setEntityType(EntityType.VIRTUAL_MACHINE).setCost(0.5f)
                .setAppliesProfile(false)
                .build());

        // set up the test discovered data cache
        costDataByTargetId = new HashMap<>();
        TargetCostData costDataTopology = new TargetCostData();
        costDataTopology.targetId = TARGET_ID_AWS_DISCOVERY_1;
        costDataTopology.discovery = discoveryTopology;
        costDataTopology.cloudServiceEntities = Collections.emptyList();
        costDataTopology.costDataDTOS = Collections.emptyList();
        costDataByTargetId.put(TARGET_ID_AWS_DISCOVERY_1, costDataTopology);


        TargetCostData costDataBilling = new TargetCostData();
        costDataBilling.targetId = TARGET_ID_AWS_BILLING_1;
        costDataBilling.discovery = discoveryBilling;
        costDataBilling.cloudServiceEntities = nmes;
        costDataBilling.costDataDTOS = costDataDTOS;
        costDataByTargetId.put(TARGET_ID_AWS_BILLING_1, costDataBilling);

        // set up the mock stitching context
        long now = System.currentTimeMillis();
        Mockito.when(mockStitchingContext.getEntitiesOfType(EntityType.CLOUD_SERVICE)).thenAnswer(
                invocationOnMock -> Stream.of(new TopologyStitchingEntity(StitchingEntityData.newBuilder(
                        EntityDTO.newBuilder()
                                .setEntityType(EntityType.CLOUD_SERVICE)
                                .setId("aws::CS::AmazonEC2"))
                                .oid(1)
                                .targetId(TARGET_ID_AWS_BILLING_1)
                                .lastUpdatedTime(now)
                                .build()),
                        new TopologyStitchingEntity(StitchingEntityData.newBuilder(
                                EntityDTO.newBuilder()
                                        .setEntityType(EntityType.CLOUD_SERVICE)
                                        .setId("aws::CS::AmazonS3"))
                                .oid(2)
                                .targetId(TARGET_ID_AWS_BILLING_1)
                                .lastUpdatedTime(now)
                                .build())
                )
        );
        // we will see the biz accounts discovered multiple times -- some in discovery and some in
        // billing
        Mockito.when(mockStitchingContext.getEntitiesOfType(EntityType.BUSINESS_ACCOUNT)).thenAnswer(
                invocationOnMock -> Stream.of(
                        new TopologyStitchingEntity(StitchingEntityData.newBuilder(
                            EntityDTO.newBuilder()
                                .setEntityType(EntityType.BUSINESS_ACCOUNT)
                                .setId("account-1"))
                                .oid(11)
                                .targetId(TARGET_ID_AWS_DISCOVERY_1)
                                .lastUpdatedTime(now)
                                .build()),
                        new TopologyStitchingEntity(StitchingEntityData.newBuilder(
                            EntityDTO.newBuilder()
                                .setEntityType(EntityType.BUSINESS_ACCOUNT)
                                .setId("account-2"))
                                .oid(12)
                                .targetId(TARGET_ID_AWS_DISCOVERY_1)
                                .lastUpdatedTime(now)
                                .build()),
                        new TopologyStitchingEntity(StitchingEntityData.newBuilder(
                                EntityDTO.newBuilder()
                                        .setEntityType(EntityType.BUSINESS_ACCOUNT)
                                        .setId("account-1"))
                                .oid(11)
                                .targetId(TARGET_ID_AWS_BILLING_1)
                                .lastUpdatedTime(now)
                                .build()),
                        new TopologyStitchingEntity(StitchingEntityData.newBuilder(
                                EntityDTO.newBuilder()
                                        .setEntityType(EntityType.BUSINESS_ACCOUNT)
                                        .setId("account-2"))
                                .oid(12)
                                .targetId(TARGET_ID_AWS_BILLING_1)
                                .lastUpdatedTime(now)
                                .build())
                )
        );
        Mockito.when(mockStitchingContext.getEntitiesOfType(EntityType.COMPUTE_TIER)).thenAnswer(
                invocationOnMock -> Stream.of(new TopologyStitchingEntity(StitchingEntityData.newBuilder(
                        EntityDTO.newBuilder()
                                .setEntityType(EntityType.COMPUTE_TIER)
                                .setId("aws::VMPROFILE::t2.nano"))
                                .oid(21)
                                .targetId(TARGET_ID_AWS_DISCOVERY_1)
                                .lastUpdatedTime(now)
                                .build()),
                        new TopologyStitchingEntity(StitchingEntityData.newBuilder(
                                EntityDTO.newBuilder()
                                        .setEntityType(EntityType.COMPUTE_TIER)
                                        .setId("aws::VMPROFILE::m4.large"))
                                .oid(22)
                                .targetId(TARGET_ID_AWS_DISCOVERY_1)
                                .lastUpdatedTime(now)
                                .build())
                )
        );
        Mockito.when(mockStitchingContext.getEntitiesOfType(EntityType.DATABASE_TIER)).thenAnswer(
                invocationOnMock -> Stream.of(new TopologyStitchingEntity(StitchingEntityData.newBuilder(
                        EntityDTO.newBuilder()
                                .setEntityType(EntityType.DATABASE_TIER)
                                .setId("843D1F6C-12FC-3EC2-9DDF-24C719ED2E20"))
                                .oid(31)
                                .targetId(TARGET_ID_AWS_DISCOVERY_1)
                                .lastUpdatedTime(now)
                                .build()),
                        new TopologyStitchingEntity(StitchingEntityData.newBuilder(
                                EntityDTO.newBuilder()
                                        .setEntityType(EntityType.DATABASE_TIER)
                                        .setId("BCD04F61-7B65-3C03-A660-03C0C08125AA"))
                                .oid(32)
                                .targetId(TARGET_ID_AWS_DISCOVERY_1)
                                .lastUpdatedTime(now)
                                .build())
                )
        );
        Mockito.when(mockStitchingContext.getEntitiesOfType(EntityType.REGION)).thenAnswer(
                invocationOnMock -> Stream.of(new TopologyStitchingEntity(StitchingEntityData.newBuilder(
                        EntityDTO.newBuilder()
                                .setEntityType(EntityType.REGION)
                                .setId("aws::us-east-2::DC::us-east-2"))
                                .oid(41)
                                .targetId(TARGET_ID_AWS_DISCOVERY_1)
                                .lastUpdatedTime(now)
                                .build()),
                        new TopologyStitchingEntity(StitchingEntityData.newBuilder(
                                EntityDTO.newBuilder()
                                        .setEntityType(EntityType.REGION)
                                        .setId("aws::ap-south-1::DC::ap-south-1"))
                                .oid(42)
                                .targetId(TARGET_ID_AWS_DISCOVERY_1)
                                .lastUpdatedTime(now)
                                .build())
                )
        );
        Mockito.when(mockStitchingContext.getEntitiesOfType(EntityType.AVAILABILITY_ZONE)).thenAnswer(
                invocationOnMock -> Stream.of(new TopologyStitchingEntity(StitchingEntityData.newBuilder(
                        EntityDTO.newBuilder()
                                .setEntityType(EntityType.AVAILABILITY_ZONE)
                                .setId("aws::ap-south-1::PM::ap-south-1b"))
                                .oid(51)
                                .targetId(TARGET_ID_AWS_DISCOVERY_1)
                                .lastUpdatedTime(now)
                                .build()),
                        new TopologyStitchingEntity(StitchingEntityData.newBuilder(
                                EntityDTO.newBuilder()
                                        .setEntityType(EntityType.AVAILABILITY_ZONE)
                                        .setId("aws::us-east-2::PM::us-east-2a"))
                                .oid(52)
                                .targetId(TARGET_ID_AWS_DISCOVERY_1)
                                .lastUpdatedTime(now)
                                .build())
                )
        );

        Mockito.when(mockStitchingContext.getEntitiesOfType(EntityType.RESERVED_INSTANCE)).thenAnswer(
                invocationOnMock -> Stream.empty());
        Mockito.when(mockStitchingContext.getEntitiesOfType(EntityType.VIRTUAL_MACHINE)).thenAnswer(
                invocationOnMock -> Stream.of(new TopologyStitchingEntity(StitchingEntityData.newBuilder(
                        EntityDTO.newBuilder()
                                .setEntityType(EntityType.VIRTUAL_MACHINE)
                                .setId("aws::ap-south-1::VM::i-0d4769e1080b462fa")
                                .setProfileId("aws::VMPROFILE::t2.nano"))
                        .oid(201)
                        .targetId(TARGET_ID_AWS_DISCOVERY_1)
                        .lastUpdatedTime(now)
                        .build())
                )
        );
        Mockito.when(mockStitchingContext.getEntitiesOfType(EntityType.STORAGE_TIER)).thenAnswer(
                invocationOnMock -> Stream.empty());
        Mockito.when(mockStitchingContext.getEntitiesOfType(EntityType.DATABASE_SERVER_TIER)).thenAnswer(
                invocationOnMock -> Stream.empty());
    }

    @Test
    public void testAccountExpenses() {

        CloudEntitiesMap cloudEntitiesMap = new CloudEntitiesMap(mockStitchingContext, probeTypeMap);

        // check the account expenses
        Map<Long,AccountExpenses.Builder> accountExpensesByAccountOid
                = accountExpensesUploader.createAccountExpenses(cloudEntitiesMap, mockStitchingContext, costDataByTargetId);

        // check account 1
        AccountExpenses.Builder awsAccount1Expenses = accountExpensesByAccountOid.get(11L);
        // we should see that account 1 has one service expense for 0.5.
        Assert.assertEquals(1, awsAccount1Expenses.getAccountExpensesInfo().getServiceExpensesCount());
        Assert.assertEquals(0.5, awsAccount1Expenses.getAccountExpensesInfo().getServiceExpenses(0).getExpenses().getAmount(), 0);
        // it should also have two tier expenses with a total cost of 1.0
        List<TierExpenses> tierExpenses = awsAccount1Expenses.getAccountExpensesInfo().getTierExpensesList();
        Assert.assertEquals(2, tierExpenses.size());
        Assert.assertEquals(1.0, tierExpenses.stream()
                .mapToDouble(expenses -> expenses.getExpenses().getAmount())
                .sum(), 0);

        // account 2 should also have a single service expense of 0.5, but no tier expenses
        AccountExpenses.Builder awsAccount2Expenses = accountExpensesByAccountOid.get(12L);
        Assert.assertEquals(1, awsAccount2Expenses.getAccountExpensesInfo().getServiceExpensesCount());
        Assert.assertEquals(0.5, awsAccount2Expenses.getAccountExpensesInfo().getServiceExpenses(0).getExpenses().getAmount(), 0);
        Assert.assertEquals(0, awsAccount2Expenses.getAccountExpensesInfo().getTierExpensesCount());
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
                .setExpenseReceivedTimestamp(usageDate)
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
                accountExpensesUploader.sanitizeCloudServiceId(AWS_CS_ID);
        final String sanitizedAzureId =
                accountExpensesUploader.sanitizeCloudServiceId(AZURE_CS_ID);
        final String sanitizedBadCsId =
                accountExpensesUploader.sanitizeCloudServiceId(BAD_CS_ID);

        Assert.assertEquals("aws::CS::AmazonS3", sanitizedAwsId);
        Assert.assertEquals("azure::CS::Storage", sanitizedAzureId);
        Assert.assertEquals("azure::CS::Storage::BUSINESS_ACCOUNT::26080bd2-d98f-4420-a737-9de8",
                sanitizedBadCsId);
    }

    public static class TestCostService extends RIAndExpenseUploadServiceImplBase {
        @Override
        public void uploadAccountExpenses(final UploadAccountExpensesRequest request,
                                           final StreamObserver<UploadAccountExpensesResponse> responseObserver) {
            logger.info("upload account expenses called.");
            responseObserver.onNext(UploadAccountExpensesResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }
    }
}
