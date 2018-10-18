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

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataResponse;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc.RIAndExpenseUploadServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc.RIAndExpenseUploadServiceImplBase;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.kvstore.MapKeyValueStore;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ReservedInstanceData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ReservedInstanceData.InstanceTenancy;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ReservedInstanceData.OfferingClass;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ReservedInstanceData.OfferingType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ReservedInstanceData.Platform;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO.CloudServiceData;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO.CloudServiceData.BillingData;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO.NonMarketEntityType;
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
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingEntityData;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;

/**
 *
 */
public class RIDataUploaderTest {
    private static final Logger logger = LogManager.getLogger();

    private static final long PROBE_ID_AWS_DISCOVERY_1 = 1;
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
    RICostDataUploader riCostDataUploader;

    @Before
    public void setup() {

        probeTypeMap = new HashMap<>();
        probeTypeMap.put(TARGET_ID_AWS_DISCOVERY_1, SDKProbeType.AWS);
        probeTypeMap.put(TARGET_ID_AWS_BILLING_1, SDKProbeType.AWS_BILLING);

        costServiceClient = RIAndExpenseUploadServiceGrpc.newBlockingStub(server.getChannel());

        riCostDataUploader = new RICostDataUploader(costServiceClient, 0, Clock.systemUTC());

        // create some discovery cost data for the uploader to cache
        Discovery discovery = new Discovery(PROBE_ID_AWS_DISCOVERY_1, TARGET_ID_AWS_DISCOVERY_1, identityProvider);
        discovery.success();

        // create some NME's
        List<NonMarketEntityDTO> nmes = new ArrayList<>();
        nmes.add(NonMarketEntityDTO.newBuilder()
                .setEntityType(NonMarketEntityType.CLOUD_SERVICE)
                .setId("aws::account-1::CS::AmazonEC2").setDisplayName("AWS EC2")
                .setCloudServiceData(CloudServiceData.newBuilder()
                        .setAccountId("account-1")
                        .setCloudProvider("AWS")
                        .setBillingData(BillingData.newBuilder()
                                .addReservedInstances(0, EntityDTO.newBuilder()
                                        .setEntityType(EntityType.RESERVED_INSTANCE)
                                        .setId("aws::ap-south-1::RI::1ac0b0f5-ff53-4d64-aac5-c5cf674cce77")
                                        .setReservedInstanceData(ReservedInstanceData.newBuilder()
                                                .setReservedInstanceId("1ac0b0f5-ff53-4d64-aac5-c5cf674cce77")
                                                .setStartTime(-1) // these properties should be ignored
                                                .setDuration(-1)
                                                .setInstanceTenancy(InstanceTenancy.DEDICATED)
                                                .setOfferingClass(OfferingClass.CONVERTIBLE)
                                                .setOfferingType(OfferingType.ALL_UPFRONT)
                                                .setPlatform(Platform.RHEL)
                                                .setNumberOfCoupons(-1)
                                                .setNumberOfCouponsUsed(-1)
                                                .setFixedCost(-1)
                                                .setRecurringCost(-1)
                                                .setUsageCost(-1)
                                                .setRegion("ap-south-1")
                                                .setInstanceCount(-1)
                                                .setRelatedProfileId("VPM-1")))
                                .addVirtualMachines(0, EntityDTO.newBuilder()
                                        .setEntityType(EntityType.VIRTUAL_MACHINE)
                                        .setId("aws::ap-south-1::VM::i-0d4769e1080b462fa")
                                        .addCommoditiesSold(CommodityDTO.newBuilder()
                                                .setCommodityType(CommodityType.COUPON)
                                                .setCapacity(2))
                                        .addCommoditiesBought(CommodityBought.newBuilder()
                                                .setProviderId("aws::ap-south-1::RI::1ac0b0f5-ff53-4d64-aac5-c5cf674cce77")
                                                .setProviderType(EntityType.RESERVED_INSTANCE)
                                                .addBought(CommodityDTO.newBuilder()
                                                        .setCommodityType(CommodityType.COUPON)
                                                        .setUsed(2))))))
                .build());
        nmes.add(NonMarketEntityDTO.newBuilder()
                .setEntityType(NonMarketEntityType.CLOUD_SERVICE)
                .setId("aws::account-2::CS::AmazonEC2").setDisplayName("AWS EC2")
                .build());
        nmes.add(NonMarketEntityDTO.newBuilder()
                .setEntityType(NonMarketEntityType.ACCOUNT)
                .setId("nme-account-1").setDisplayName("Non-Market-Entity Account 1")
                .build());

        costDataByTargetId = new HashMap<>();
        TargetCostData costData = new TargetCostData();
        costData.targetId = TARGET_ID_AWS_BILLING_1;
        costData.discovery = discovery;
        costData.cloudServiceEntities = nmes;
        costData.costDataDTOS = Collections.emptyList();
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
                                        .setId("account-2"))
                                .oid(12)
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
                                .setId("aws::ap-south-1::RI::1ac0b0f5-ff53-4d64-aac5-c5cf674cce77")
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
                                        .setRelatedProfileId("aws::VMPROFILE::t2.nano")))
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
                                                .setNumberOfCoupons(16)
                                                .setNumberOfCouponsUsed(4)
                                                .setFixedCost(10)
                                                .setUsageCost(12)
                                                .setRecurringCost(13)
                                                .setRegion("aws::ap-south-1::DC::ap-south-1")
                                                .setInstanceCount(2)
                                                .setOfferingClass(OfferingClass.CONVERTIBLE)
                                                .setOfferingType(OfferingType.ALL_UPFRONT)
                                                .setDuration(2 * DiscoveredCloudCostUploader.MILLIS_PER_YEAR)
                                                .setInstanceTenancy(InstanceTenancy.DEDICATED)
                                                //.setAvailabilityZone("aws::ap-south-1::PM::ap-south-1b")
                                                .setPlatform(Platform.WINDOWS)
                                                .setRelatedProfileId("aws::VMPROFILE::m4.large")))
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
                                                .setDuration(2 * DiscoveredCloudCostUploader.MILLIS_PER_YEAR)
                                                .setInstanceTenancy(InstanceTenancy.DEDICATED)
                                                .setAvailabilityZone("aws::ap-south-1::PM::ap-south-1b")
                                                .setPlatform(Platform.WINDOWS)
                                                .setRelatedProfileId("aws::VMPROFILE::m4.large")))
                                .oid(103)
                                .targetId(TARGET_ID_AWS_DISCOVERY_1)
                                .lastUpdatedTime(now)
                                .build())
                )
        );
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
    public void testRIData() {
        CloudEntitiesMap cloudEntitiesMap = new CloudEntitiesMap(mockStitchingContext, probeTypeMap);

        RICostComponentData riData = riCostDataUploader.createRICostComponentData(
                mockStitchingContext, cloudEntitiesMap, costDataByTargetId);

        // there should be 3 RI bought but only 2 specs -- two of the RI should have mapped to the
        // same spec instance
        Assert.assertEquals(2, riData.riSpecs.size());
        Assert.assertEquals(3, riData.riBoughtByLocalId.size());
        // Verify an RI Spec -- the data should have come from the stitching entities rather than
        // the nme's.

        ReservedInstanceSpecInfo riSpecInfo = riData.riSpecs.stream()
                .filter(spec -> spec.getId() == 0)
                .findFirst()
                .get().getReservedInstanceSpecInfo();
        Assert.assertEquals(PaymentOption.NO_UPFRONT, riSpecInfo.getType().getPaymentOption());
        Assert.assertEquals(Tenancy.DEFAULT, riSpecInfo.getTenancy());
        Assert.assertEquals(OSType.LINUX, riSpecInfo.getOs());
        // compute tier should be tier2.nano = oid 21
        Assert.assertEquals(21, riSpecInfo.getTierId());
        // region should be ap-south-1
        Assert.assertEquals(42, riSpecInfo.getRegionId());

        // Verify some RI Bought
        // we'll verify a few fields
        ReservedInstanceBoughtInfo boughtInfo = riData.riBoughtByLocalId
                .get("aws::ap-south-1::RI::1ac0b0f5-ff53-4d64-aac5-c5cf674cce77").getReservedInstanceBoughtInfo();
        // these fields should come from the discovery entities, and not the billing NME's
        Assert.assertEquals(51, boughtInfo.getAvailabilityZoneId());
        Assert.assertEquals(1, boughtInfo.getNumBought());
        // we expect this RI to have mapped to spec 0
        Assert.assertEquals(0, boughtInfo.getReservedInstanceSpec());

        Assert.assertEquals(1.0, boughtInfo.getReservedInstanceBoughtCost().getFixedCost().getAmount(), 0);
        Assert.assertEquals(2.0, boughtInfo.getReservedInstanceBoughtCost().getUsageCostPerHour().getAmount(), 0);
        Assert.assertEquals(3.0, boughtInfo.getReservedInstanceBoughtCost().getRecurringCostPerHour().getAmount(), 0);

        Assert.assertEquals(10, boughtInfo.getReservedInstanceBoughtCoupons().getNumberOfCoupons());

        // verify that RI coverage info was consumed correctly.
        Assert.assertEquals(1, riData.riCoverages.size());
        EntityReservedInstanceCoverage.Builder riCoverage = riData.riCoverages.get(0);
        // VM 201 should be covered by 2 coupons
        Assert.assertEquals(201, riCoverage.getEntityId());
        Assert.assertEquals(2, riCoverage.getTotalCouponsRequired(), 0);
        // should be consuming 2 coupons from RI 101
        Assert.assertEquals("aws::ap-south-1::RI::1ac0b0f5-ff53-4d64-aac5-c5cf674cce77", riCoverage.getCoverage(0).getProbeReservedInstanceId());
        Assert.assertEquals(2, riCoverage.getCoverage(0).getCoveredCoupons(), 0);

        // also verify that the account id and coupons used were backfilled into the RI Bought info
        // based on some data from coverage

        // verify that num used was calculated and should be = num bought. This requires the RI
        // coverage data to be mined properly.
        Assert.assertEquals(2, boughtInfo.getReservedInstanceBoughtCoupons().getNumberOfCouponsUsed(), 0);

        // assignment of business account id verifies that the coverage mapping is working
        Assert.assertEquals(11, boughtInfo.getBusinessAccountId());
    }

    public static class TestCostService extends RIAndExpenseUploadServiceImplBase {
        @Override
        public void uploadRIData(final UploadRIDataRequest request,
                                           final StreamObserver<UploadRIDataResponse> responseObserver) {
            logger.info("upload account expenses called.");
            responseObserver.onNext(UploadRIDataResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }
    }
}
