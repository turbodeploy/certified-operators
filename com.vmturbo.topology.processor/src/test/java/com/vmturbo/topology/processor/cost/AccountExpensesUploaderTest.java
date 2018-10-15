package com.vmturbo.topology.processor.cost;

import static org.mockito.Mockito.spy;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses;
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
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.TargetCostData;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.identity.IdentityService;
import com.vmturbo.topology.processor.identity.services.HeuristicsMatcher;
import com.vmturbo.topology.processor.identity.storage.IdentityDatabaseStore;
import com.vmturbo.topology.processor.identity.storage.IdentityServiceInMemoryUnderlyingStore;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
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

    // simple KV store for the test identity provider
    private KeyValueStore keyValueStore = new MapKeyValueStore();
    {
        keyValueStore.put("id/probes/AWS", "11");
        keyValueStore.put("id/probes/Azure", "21");
    }

    private IdentityProvider identityProvider = new IdentityProviderImpl(
            new IdentityService(new IdentityServiceInMemoryUnderlyingStore(
                    Mockito.mock(IdentityDatabaseStore.class)), new HeuristicsMatcher()), keyValueStore, 0L);

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
