package com.vmturbo.topology.processor.cost;

import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import com.google.protobuf.TextFormat;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceScopeInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.AccountRICoverageUpload;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage.RICoverageSource;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataResponse;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc.RIAndExpenseUploadServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc.RIAndExpenseUploadServiceImplBase;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.kvstore.MapKeyValueStore;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.BusinessAccountData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ReservedInstanceData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ReservedInstanceData.InstanceTenancy;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ReservedInstanceData.OfferingClass;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ReservedInstanceData.OfferingType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ReservedInstanceData.Platform;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ReservedInstanceData.ReservedInstanceAppliedScope;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ReservedInstanceData.ReservedInstanceAppliedScope.MultipleAccountsReservedInstanceScope;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.PaymentOption;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.TargetCostData;
import com.vmturbo.topology.processor.cost.RICostDataUploader.RICostComponentData;
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
 * Unit tests for {@link RICostDataUploader}.
 */
public class RIDataUploaderTest {
    private static final Logger logger = LogManager.getLogger();

    private static final long PROBE_ID_AWS_DISCOVERY_1 = 1;
    private static final long TARGET_ID_AWS_DISCOVERY_1 = 1;
    private static final long TARGET_ID_AWS_BILLING_1 = 2;
    private static final long TARGET_ID_AZURE_DISCOVERY_1 = 3;
    private static final long USAGE_TIMESTAMP = 1591574400000L;

    // simple KV store for the test identity provider
    private KeyValueStore keyValueStore = new MapKeyValueStore();

    {
        keyValueStore.put("id/probes/AWS", "11");
        keyValueStore.put("id/probes/Azure", "21");
    }

    private IdentityProvider identityProvider = new IdentityProviderImpl(
            new IdentityService(new IdentityServiceInMemoryUnderlyingStore(
                    Mockito.mock(IdentityDatabaseStore.class)),
                    new HeuristicsMatcher()),
            keyValueStore,
            new ProbeInfoCompatibilityChecker(), 0L);

    private StitchingContext mockStitchingContext = Mockito.mock(StitchingContext.class);

    // test GRPC server
    private final TestCostService costServiceSpy = spy(new TestCostService());

    /**
     * The {@link GrpcTestServer}.
     */
    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(costServiceSpy);

    private Map<Long, SDKProbeType> probeTypeMap;

    // test data
    private Map<Long, TargetCostData> costDataByTargetId;

    // object to be tested
    private RICostDataUploader riCostDataUploader;

    /**
     * Setup.
     *
     * @throws IOException not expected.
     */
    @Before
    public void setup() throws IOException {

        probeTypeMap = new HashMap<>();
        probeTypeMap.put(TARGET_ID_AWS_DISCOVERY_1, SDKProbeType.AWS);
        probeTypeMap.put(TARGET_ID_AWS_BILLING_1, SDKProbeType.AWS_BILLING);
        probeTypeMap.put(TARGET_ID_AZURE_DISCOVERY_1, SDKProbeType.AZURE_EA);

        // test cost component client
        final RIAndExpenseUploadServiceBlockingStub costServiceClient = RIAndExpenseUploadServiceGrpc.newBlockingStub(server.getChannel());

        riCostDataUploader = new RICostDataUploader(costServiceClient, 0, Clock.systemUTC(), true,
                true);

        // create some discovery cost data for the uploader to cache
        Discovery discovery = new Discovery(PROBE_ID_AWS_DISCOVERY_1, TARGET_ID_AWS_DISCOVERY_1, identityProvider);
        discovery.success();

        costDataByTargetId = new HashMap<>();
        final TargetCostData costData = new TargetCostData();
        costData.targetId = TARGET_ID_AWS_BILLING_1;
        costData.discovery = discovery;
        final DiscoveryResponse.Builder awsBillingDiscoveryResponse = loadDiscoveryResponseFromTextFormatFile(
                "protobuf/messages/ri_cost_data_uploader/AWS_Billing_EngDevBilling-2020.06.09.16.40.35.688-FULL.txt");
        costData.cloudServiceEntities = awsBillingDiscoveryResponse
                .getNonMarketEntityDTOList();
        costData.costDataDTOS = awsBillingDiscoveryResponse.getCostDTOList();
        costDataByTargetId.put(TARGET_ID_AWS_BILLING_1, costData);

        // set up the mock stitching context
        long now = System.currentTimeMillis();

        Mockito.when(mockStitchingContext.getEntitiesOfType(EntityType.CLOUD_SERVICE)).thenAnswer(
                invocationOnMock -> Stream.empty());

        Mockito.when(mockStitchingContext.getEntitiesOfType(EntityType.BUSINESS_ACCOUNT)).thenAnswer(
                invocationOnMock -> Stream.of(new TopologyStitchingEntity(StitchingEntityData.newBuilder(
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
                                        .setId("192821421245").setBusinessAccountData(
                                        BusinessAccountData.newBuilder().setDataDiscovered(true)))
                                .oid(192821421245L)
                                .targetId(TARGET_ID_AWS_DISCOVERY_1)
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
                invocationOnMock -> Stream.of(new TopologyStitchingEntity(StitchingEntityData.newBuilder(
                        EntityDTO.newBuilder()
                                .setEntityType(EntityType.RESERVED_INSTANCE)
                                .setId("aws::us-east-1::RI::18bc975d-54a6-4622-929e-2d9a232766aa")
                                .setDisplayName("RI display name")
                                .setReservedInstanceData(ReservedInstanceData.newBuilder()
                                        .setReservedInstanceId("1ac0b0f5-ff53-4d64-aac5-c5cf674cce77")
                                        .setStartTime(0)
                                        .setNumberOfCoupons(10)
                                        .setNumberOfCouponsUsed(1)
                                        .setFixedCost(1)
                                        .setUsageCost(2)
                                        .setRecurringCost(3)
                                        .setRegion("aws::ap-south-1::DC::ap-south-1")
                                        .setInstanceCount(1)
                                        .setOfferingClass(OfferingClass.STANDARD)
                                        .setOfferingType(OfferingType.NO_UPFRONT)
                                        .setDuration(DiscoveredCloudCostUploader.MILLIS_PER_YEAR)
                                        .setInstanceTenancy(InstanceTenancy.DEFAULT)
                                        .setAvailabilityZone("aws::ap-south-1::PM::ap-south-1b")
                                        .setPlatform(Platform.LINUX)
                                        .setPlatformFlexible(true)
                                        .setRelatedProfileId("aws::VMPROFILE::t2.nano")
                                        .setPurchasingAccountId("account-1")
                                        .setAppliedScope(ReservedInstanceAppliedScope.newBuilder()
                                                .setMultipleAccountsReservedInstanceScope(
                                                        MultipleAccountsReservedInstanceScope.newBuilder()
                                                                .addAccountId("account-1")
                                                                .addAccountId("192821421245")))
                                        .setReservationOrderId("orderID-1")
                                        .setInstanceSizeFlexible(true)))
                                .oid(101)
                                .targetId(TARGET_ID_AWS_DISCOVERY_1)
                                .lastUpdatedTime(now)
                                .build()),
                        new TopologyStitchingEntity(StitchingEntityData.newBuilder(
                                EntityDTO.newBuilder()
                                        .setEntityType(EntityType.RESERVED_INSTANCE)
                                        .setId("aws::ca-central-1::RI::921378bc-5142-44c5-84d6-d4569ea26b00")
                                        .setReservedInstanceData(ReservedInstanceData.newBuilder()
                                                .setReservedInstanceId("921378bc-5142-44c5-84d6-d4569ea26b00")
                                                .setStartTime(0)
                                                .setPurchasingAccountId("192821421245")
                                                .setNumberOfCoupons(16)
                                                .setNumberOfCouponsUsed(4)
                                                .setFixedCost(10)
                                                .setUsageCost(12)
                                                .setRecurringCost(13)
                                                .setRegion("aws::ap-south-1::DC::ap-south-1")
                                                .setInstanceCount(2)
                                                .setOfferingClass(OfferingClass.CONVERTIBLE)
                                                .setOfferingType(OfferingType.ALL_UPFRONT)
                                                .setDuration(3 * DiscoveredCloudCostUploader.MILLIS_PER_YEAR)
                                                .setInstanceTenancy(InstanceTenancy.DEDICATED)
                                                //.setAvailabilityZone("aws::ap-south-1::PM::ap-south-1b")
                                                .setPlatform(Platform.WINDOWS)
                                                .setRelatedProfileId("aws::VMPROFILE::m4.large")
                                                .setInstanceSizeFlexible(true)))
                                .oid(102)
                                .targetId(TARGET_ID_AWS_DISCOVERY_1)
                                .lastUpdatedTime(now)
                                .build()),
                        new TopologyStitchingEntity(StitchingEntityData.newBuilder(
                                EntityDTO.newBuilder()
                                        .setEntityType(EntityType.RESERVED_INSTANCE)
                                        .setId("aws::ca-central-2::RI::921378bc-5142-44c5-84d6-d4569ea26b11")
                                        .setReservedInstanceData(ReservedInstanceData.newBuilder()
                                                .setReservedInstanceId("921378bc-5142-44c5-84d6-d4569ea26b11")
                                                .setStartTime(0)
                                                .setNumberOfCoupons(16)
                                                .setNumberOfCouponsUsed(4)
                                                .setFixedCost(10)
                                                .setUsageCost(12)
                                                .setRecurringCost(13)
                                                .setRegion("aws::ap-south-1::DC::ap-south-1")
                                                .setInstanceCount(1)
                                                .setOfferingClass(OfferingClass.CONVERTIBLE)
                                                .setOfferingType(OfferingType.ALL_UPFRONT)
                                                .setDuration(3 * DiscoveredCloudCostUploader.MILLIS_PER_YEAR)
                                                .setInstanceTenancy(InstanceTenancy.DEDICATED)
                                                .setAvailabilityZone("aws::ap-south-1::PM::ap-south-1b")
                                                .setPlatform(Platform.WINDOWS)
                                                .setRelatedProfileId("aws::VMPROFILE::m4.large")
                                                .setInstanceSizeFlexible(true)))
                                .oid(103)
                                .targetId(TARGET_ID_AWS_DISCOVERY_1)
                                .lastUpdatedTime(now)
                                .build()),
                        new TopologyStitchingEntity(StitchingEntityData.newBuilder(
                                EntityDTO.newBuilder()
                                        .setEntityType(EntityType.RESERVED_INSTANCE)
                                        .setId("azure::96c54bd2-e4a2-4105-9a4b-c3ab28bdef1a::RESERVED_INSTAN5B351A")
                                        .setReservedInstanceData(ReservedInstanceData.newBuilder()
                                                .setReservedInstanceId("c991a277-c5ec-48e6-9a70-c36a910ccd5d")
                                                .setStartTime(0)
                                                .setNumberOfCoupons(16)
                                                .setNumberOfCouponsUsed(4)
                                                .setFixedCost(10)
                                                .setUsageCost(12)
                                                .setRecurringCost(13)
                                                .setRegion("azure::australiaeast::DC::australiaeast")
                                                .setInstanceCount(1)
                                                .setOfferingClass(OfferingClass.CONVERTIBLE)
                                                .setOfferingType(OfferingType.ALL_UPFRONT)
                                                .setDuration(3 * DiscoveredCloudCostUploader.MILLIS_PER_YEAR)
                                                .setInstanceTenancy(InstanceTenancy.DEFAULT)
                                                .setPlatform(Platform.UNKNOWN)
                                                .setRelatedProfileId("azure::VMPROFILE::Standard_B2ms")
                                                .setPlatformFlexible(false)
                                                .setInstanceSizeFlexible(true)))
                                .oid(104)
                                .targetId(TARGET_ID_AZURE_DISCOVERY_1)
                                .lastUpdatedTime(now)
                                .build()),
                        new TopologyStitchingEntity(StitchingEntityData.newBuilder(
                                EntityDTO.newBuilder()
                                        .setEntityType(EntityType.RESERVED_INSTANCE)
                                        .setId("azure::96c54bd2-e4a2-4105-9a4b-c3ab28bdef1a::RESERVED_INSTAN5B351A")
                                        .setReservedInstanceData(ReservedInstanceData.newBuilder()
                                                .setReservedInstanceId("c991a277-c5ec-48e6-9a70-c36a910ccd5d")
                                                .setStartTime(0)
                                                .setNumberOfCoupons(16)
                                                .setNumberOfCouponsUsed(4)
                                                .setFixedCost(10)
                                                .setUsageCost(12)
                                                .setRecurringCost(13)
                                                .setRegion("azure::australiaeast::DC::australiaeast")
                                                .setInstanceCount(1)
                                                .setOfferingClass(OfferingClass.CONVERTIBLE)
                                                .setOfferingType(OfferingType.ALL_UPFRONT)
                                                .setDuration(2 * DiscoveredCloudCostUploader.MILLIS_PER_YEAR)
                                                .setInstanceTenancy(InstanceTenancy.DEFAULT)
                                                .setPlatform(Platform.UNKNOWN)
                                                .setRelatedProfileId("azure::VMPROFILE::Standard_B2ms")
                                                .setPlatformFlexible(true)
                                                .setInstanceSizeFlexible(true)))
                                .oid(105)
                                .targetId(TARGET_ID_AZURE_DISCOVERY_1)
                                .lastUpdatedTime(now)
                                .build()),
                        new TopologyStitchingEntity(StitchingEntityData.newBuilder(
                                EntityDTO.newBuilder()
                                        .setEntityType(EntityType.RESERVED_INSTANCE)
                                        .setId("azure::96c54bd2-e4a2-4105-9a4b-c3ab28bdef1a::RESERVED_INSTAN5B351D")
                                        .setReservedInstanceData(ReservedInstanceData.newBuilder()
                                                .setReservedInstanceId("c991a277-c5ec-48e6-9a70-c36a910ccd5d")
                                                .setStartTime(0)
                                                .setNumberOfCoupons(16)
                                                .setNumberOfCouponsUsed(4)
                                                .setFixedCost(10)
                                                .setUsageCost(12)
                                                .setRecurringCost(13)
                                                .setRegion("azure::australiaeast::DC::australiaeast")
                                                .setInstanceCount(1)
                                                .setOfferingClass(OfferingClass.CONVERTIBLE)
                                                .setOfferingType(OfferingType.ALL_UPFRONT)
                                                //this shud be a valid Duration
                                                .setDuration(Duration.ofMillis(DiscoveredCloudCostUploader.MILLIS_PER_YEAR)
                                                        .plusHours(23).toMillis())
                                                .setInstanceTenancy(InstanceTenancy.DEFAULT)
                                                .setPlatform(Platform.UNKNOWN)
                                                .setRelatedProfileId("azure::VMPROFILE::Standard_B2ms")
                                                .setPlatformFlexible(true)
                                                .setInstanceSizeFlexible(true)))
                                .oid(108)
                                .targetId(TARGET_ID_AZURE_DISCOVERY_1)
                                .lastUpdatedTime(now)
                                .build()),
                        new TopologyStitchingEntity(StitchingEntityData.newBuilder(
                                EntityDTO.newBuilder()
                                        .setEntityType(EntityType.RESERVED_INSTANCE)
                                        .setId("azure::96c54bd2-e4a2-4105-9a4b-c3ab28bdef1a::RESERVED_INSTAN5B351E")
                                        .setReservedInstanceData(ReservedInstanceData.newBuilder()
                                                .setReservedInstanceId("c991a277-c5ec-48e6-9a70-c36a910ccd5d")
                                                .setStartTime(0)
                                                .setNumberOfCoupons(16)
                                                .setNumberOfCouponsUsed(4)
                                                .setFixedCost(10)
                                                .setUsageCost(12)
                                                .setRecurringCost(13)
                                                .setRegion("azure::australiaeast::DC::australiaeast")
                                                .setInstanceCount(1)
                                                .setOfferingClass(OfferingClass.CONVERTIBLE)
                                                .setOfferingType(OfferingType.ALL_UPFRONT)
                                                .setDuration(
                                                        Duration.ofMillis(DiscoveredCloudCostUploader.MILLIS_PER_YEAR)
                                                                .plusDays(12).toMillis())
                                                .setInstanceTenancy(InstanceTenancy.DEFAULT)
                                                .setPlatform(Platform.UNKNOWN)
                                                .setRelatedProfileId("azure::VMPROFILE::Standard_B2ms")
                                                .setPlatformFlexible(true)
                                                .setInstanceSizeFlexible(true)))
                                .oid(109)
                                .targetId(TARGET_ID_AZURE_DISCOVERY_1)
                                .lastUpdatedTime(now)
                                .build())

                )
        );
        Mockito.when(mockStitchingContext.getEntitiesOfType(EntityType.VIRTUAL_MACHINE)).thenAnswer(
                invocationOnMock -> Stream.of(new TopologyStitchingEntity(StitchingEntityData.newBuilder(
                        EntityDTO.newBuilder()
                                .setEntityType(EntityType.VIRTUAL_MACHINE)
                                .setId("aws::us-east-1::VM::i-0b85cb6231b4fa0a2"))
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
        Mockito.when(mockStitchingContext.getEntitiesOfType(EntityType.SERVICE_PROVIDER)).thenAnswer(
                invocationOnMock -> Stream.empty());

    }

    /**
     * Test for {@link RICostDataUploader#createRICostComponentData}.
     */
    @Test
    public void testRIData() {
        final CloudEntitiesMap cloudEntitiesMap = new CloudEntitiesMap(mockStitchingContext,
                probeTypeMap);
        final RICostComponentData riData = riCostDataUploader.createRICostComponentData(
                mockStitchingContext, cloudEntitiesMap, costDataByTargetId);

        Assert.assertEquals(2, riData.riSpecs.size());
        Assert.assertEquals(2, riData.riBoughtByLocalId.size());
        // Verify an RI Spec -- the data should have come from the stitching entities rather than
        // the nme's.

        final ReservedInstanceSpecInfo riSpecInfo = riData.riSpecs.stream().filter(
                spec -> spec.getId() == 0).findFirst().get().getReservedInstanceSpecInfo();
        Assert.assertEquals(PaymentOption.NO_UPFRONT, riSpecInfo.getType().getPaymentOption());
        Assert.assertEquals(Tenancy.DEFAULT, riSpecInfo.getTenancy());
        Assert.assertEquals(OSType.LINUX, riSpecInfo.getOs());
        // compute tier should be tier2.nano = oid 21
        Assert.assertEquals(21, riSpecInfo.getTierId());
        // region should be ap-south-1
        Assert.assertEquals(42, riSpecInfo.getRegionId());
        // verify size flexibility
        Assert.assertTrue(riSpecInfo.getSizeFlexible());

        final ReservedInstanceBoughtInfo reservedInstanceBoughtInfo = riData.riBoughtByLocalId.get(
                "aws::us-east-1::RI::18bc975d-54a6-4622-929e-2d9a232766aa")
                .getReservedInstanceBoughtInfo();
        Assert.assertEquals(ReservedInstanceBoughtInfo.newBuilder()
                .setBusinessAccountId(11)
                .setProbeReservedInstanceId(
                        "aws::us-east-1::RI::18bc975d-54a6-4622-929e-2d9a232766aa")
                .setStartTime(0)
                .setNumBought(1)
                .setAvailabilityZoneId(51)
                .setReservedInstanceSpec(0)
                .setReservedInstanceBoughtCost(ReservedInstanceBoughtCost.newBuilder()
                        .setFixedCost(CurrencyAmount.newBuilder().setAmount(1.0))
                        .setUsageCostPerHour(CurrencyAmount.newBuilder().setAmount(2.0))
                        .setRecurringCostPerHour(CurrencyAmount.newBuilder().setAmount(3.0)))
                .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder()
                        .setNumberOfCoupons(10)
                        .setNumberOfCouponsUsed(516.3844453333334))
                .setDisplayName("RI display name")
                .setReservationOrderId("orderID-1")
                .setReservedInstanceScopeInfo(ReservedInstanceScopeInfo.newBuilder()
                        .setShared(false)
                        .addApplicableBusinessAccountId(11L)
                        .addApplicableBusinessAccountId(192821421245L))
                .build(), reservedInstanceBoughtInfo);

        // verify that RI coverage info was consumed correctly.
        Assert.assertEquals(1, riData.entityLevelReservedInstanceCoverages.size());
        final EntityRICoverageUpload.Builder entityRICoverageUpload =
                riData.entityLevelReservedInstanceCoverages.get(0);
        Assert.assertEquals(EntityRICoverageUpload.newBuilder()
                .setEntityId(201)
                .setTotalCouponsRequired(16.0)
                .addCoverage(Coverage.newBuilder()
                        .setReservedInstanceId(101)
                        .setProbeReservedInstanceId(
                                "aws::us-east-1::RI::18bc975d-54a6-4622-929e-2d9a232766aa")
                        .setCoveredCoupons(15.333333333333334)
                        .setRiCoverageSource(RICoverageSource.BILLING)
                        .setUsageStartTimestamp(USAGE_TIMESTAMP)
                        .setUsageEndTimestamp(USAGE_TIMESTAMP))
                .build(), entityRICoverageUpload.build());

        Assert.assertTrue(riData.riSpecs.stream()
                .anyMatch(spec -> spec.getReservedInstanceSpecInfo().getPlatformFlexible()));

        // verify that account ID for the second spec is assigned based on target default account
        final long accountId2 = riData.riBoughtByLocalId.get(
                "aws::ca-central-1::RI::921378bc-5142-44c5-84d6-d4569ea26b00")
                .getReservedInstanceBoughtInfo()
                .getBusinessAccountId();
        Assert.assertEquals(192821421245L, accountId2);
        Assert.assertEquals(1, riData.accountLevelReservedInstanceCoverages.size());
        final AccountRICoverageUpload.Builder accountRICoverageUpload =
                riData.accountLevelReservedInstanceCoverages.get(0);
        Assert.assertEquals(AccountRICoverageUpload.newBuilder()
                .setAccountId(192821421245L)
                .addCoverage(Coverage.newBuilder()
                        .setProbeReservedInstanceId(
                                "aws::ca-central-1::RI::846667fa-4e04-46c0-a032-9f720a1d48c9")
                        .setCoveredCoupons(64.0)
                        .setRiCoverageSource(RICoverageSource.BILLING)
                        .setUsageStartTimestamp(USAGE_TIMESTAMP)
                        .setUsageEndTimestamp(USAGE_TIMESTAMP))
                .addCoverage(Coverage.newBuilder()
                        .setProbeReservedInstanceId(
                                "aws::us-west-2::RI::200d8957-4ff4-423a-a9f3-9af578e7c4cb")
                        .setCoveredCoupons(8.0)
                        .setRiCoverageSource(RICoverageSource.BILLING)
                        .setUsageStartTimestamp(USAGE_TIMESTAMP)
                        .setUsageEndTimestamp(USAGE_TIMESTAMP))
                .addCoverage(Coverage.newBuilder()
                        .setProbeReservedInstanceId(
                                "aws::eu-west-3::RI::2462e73d-1aba-4bd5-8abc-60ec2ac033cf")
                        .setCoveredCoupons(3.0)
                        .setRiCoverageSource(RICoverageSource.BILLING)
                        .setUsageStartTimestamp(USAGE_TIMESTAMP)
                        .setUsageEndTimestamp(USAGE_TIMESTAMP))
                .addCoverage(Coverage.newBuilder()
                        .setProbeReservedInstanceId(
                                "aws::ca-central-1::RI::cfb751ed-c4e3-4420-80ef-493188b40c07")
                        .setCoveredCoupons(16.0)
                        .setRiCoverageSource(RICoverageSource.BILLING)
                        .setUsageStartTimestamp(USAGE_TIMESTAMP)
                        .setUsageEndTimestamp(USAGE_TIMESTAMP))
                .addCoverage(Coverage.newBuilder()
                        .setReservedInstanceId(101)
                        .setProbeReservedInstanceId(
                                "aws::us-east-1::RI::18bc975d-54a6-4622-929e-2d9a232766aa")
                        .setCoveredCoupons(516.3844453333334)
                        .setRiCoverageSource(RICoverageSource.BILLING)
                        .setUsageStartTimestamp(USAGE_TIMESTAMP)
                        .setUsageEndTimestamp(USAGE_TIMESTAMP))
                .build(), accountRICoverageUpload.build());
    }

    /**
     * Test implementation of {@link RIAndExpenseUploadServiceImplBase}.
     */
    private static class TestCostService extends RIAndExpenseUploadServiceImplBase {
        @Override
        public void uploadRIData(final UploadRIDataRequest request,
                                           final StreamObserver<UploadRIDataResponse> responseObserver) {
            logger.info("upload account expenses called.");
            responseObserver.onNext(UploadRIDataResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }
    }

    private static DiscoveryResponse.Builder loadDiscoveryResponseFromTextFormatFile(
            final String fileName) throws IOException {
        final DiscoveryResponse.Builder discoveryResponseBuilder = DiscoveryResponse.newBuilder();
        TextFormat.getParser().merge(
                new InputStreamReader(RIDataUploaderTest.class.getClassLoader().getResources(
                        fileName).nextElement().openStream()), discoveryResponseBuilder);
        return discoveryResponseBuilder;
    }
}
