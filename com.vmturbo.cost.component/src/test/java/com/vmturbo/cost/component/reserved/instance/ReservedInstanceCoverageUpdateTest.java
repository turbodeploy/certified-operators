package com.vmturbo.cost.component.reserved.instance;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.notification.CostNotificationSender;
import com.vmturbo.cost.component.reserved.instance.coverage.analysis.SupplementalRICoverageAnalysis;
import com.vmturbo.cost.component.reserved.instance.coverage.analysis.SupplementalRICoverageAnalysisFactory;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

public class ReservedInstanceCoverageUpdateTest {
    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Cost.COST);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    private DSLContext dsl = dbConfig.getDslContext();

    private EntityReservedInstanceMappingStore entityReservedInstanceMappingStore =
            mock(EntityReservedInstanceMappingStore.class);

    private ReservedInstanceUtilizationStore reservedInstanceUtilizationStore =
            mock(ReservedInstanceUtilizationStore.class);

    private ReservedInstanceCoverageStore reservedInstanceCoverageStore =
            mock(ReservedInstanceCoverageStore.class);

    private ReservedInstanceCoverageValidatorFactory reservedInstanceCoverageValidatorFactory =
            mock(ReservedInstanceCoverageValidatorFactory.class);

    private ReservedInstanceCoverageValidator reservedInstanceCoverageValidator =
            mock(ReservedInstanceCoverageValidator.class);

    private SupplementalRICoverageAnalysisFactory supplementalRICoverageAnalysisFactory =
            mock(SupplementalRICoverageAnalysisFactory.class);
    private SupplementalRICoverageAnalysis supplementalRICoverageAnalysis =
            mock(SupplementalRICoverageAnalysis.class);

    private CostNotificationSender costNotificationSender =
            mock(CostNotificationSender.class);

    @Captor
    private ArgumentCaptor<List<EntityRICoverageUpload>> entityRIMappingStoreCoverageCaptor;

    @Captor ArgumentCaptor<List<ServiceEntityReservedInstanceCoverageRecord>> riCoverageStoreCoverageCaptor;

    private ReservedInstanceCoverageUpdate reservedInstanceCoverageUpdate;

    private static final long AWS_TARGET_ID = 77777L;

    private static final Origin AWS_ORIGIN = Origin.newBuilder()
            .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                    .putDiscoveredTargetData(AWS_TARGET_ID, PerTargetEntityInformation.getDefaultInstance()))
            .build();

    private final TopologyEntityDTO AZ = TopologyEntityDTO.newBuilder()
            .setOid(8L)
            .setDisplayName("this is available")
            .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
            .setOrigin(AWS_ORIGIN)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .build();

    private final TopologyEntityDTO REGION = TopologyEntityDTO.newBuilder()
            .setOid(9L)
            .setDisplayName("region")
            .setEntityType(EntityType.REGION_VALUE)
            .setOrigin(AWS_ORIGIN)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityType(AZ.getEntityType())
                    .setConnectedEntityId(AZ.getOid())
                    .setConnectionType(ConnectionType.OWNS_CONNECTION))
            .build();

    private final TopologyEntityDTO COMPUTE_TIER = TopologyEntityDTO.newBuilder()
            .setOid(99L)
            .setDisplayName("r3.xlarge")
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setComputeTier(ComputeTierInfo.newBuilder()
                            .setNumCoupons(50)))
            .setEntityType(EntityType.COMPUTE_TIER_VALUE)
            .setOrigin(AWS_ORIGIN)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityType(REGION.getEntityType())
                    .setConnectedEntityId(REGION.getOid())
                    .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
            .build();

    private final EntityRICoverageUpload entityRICoverageOne =
            EntityRICoverageUpload.newBuilder()
                    .setEntityId(123L)
                    .setTotalCouponsRequired(100)
                    .addCoverage(Coverage.newBuilder()
                            .setReservedInstanceId(11)
                            .setCoveredCoupons(10))
                    .addCoverage(Coverage.newBuilder()
                            .setReservedInstanceId(12)
                            .setCoveredCoupons(10))
                    .build();

    private final TopologyEntityDTO VMOne = TopologyEntityDTO.newBuilder()
            .setOid(123L)
            .setDisplayName("bar")
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setOrigin(AWS_ORIGIN)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(COMPUTE_TIER.getOid())
                    .setProviderEntityType(COMPUTE_TIER.getEntityType()))
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setVirtualMachine(VirtualMachineInfo.newBuilder()
                            .setGuestOsInfo(OS.newBuilder()
                                .setGuestOsType(OSType.LINUX)
                                .setGuestOsName(OSType.LINUX.name()))
                            .setTenancy(Tenancy.DEFAULT)))
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityType(REGION.getEntityType())
                    .setConnectedEntityId(REGION.getOid())
                    .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
            .build();

    private final TopologyEntityDTO VMTwo = TopologyEntityDTO.newBuilder()
            .setOid(124L)
            .setDisplayName("foo")
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setOrigin(AWS_ORIGIN)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(COMPUTE_TIER.getOid())
                    .setProviderEntityType(COMPUTE_TIER.getEntityType()))
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setVirtualMachine(VirtualMachineInfo.newBuilder()
                            .setGuestOsInfo(OS.newBuilder()
                                .setGuestOsType(OSType.LINUX)
                                .setGuestOsName(OSType.LINUX.name()))
                            .setTenancy(Tenancy.DEFAULT)))
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityType(AZ.getEntityType())
                    .setConnectedEntityId(AZ.getOid())
                    .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
            .build();

    private final TopologyEntityDTO BUSINESS_ACCOUNT = TopologyEntityDTO.newBuilder()
            .setOid(125L)
            .setDisplayName("businessAccount")
            .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
            .setOrigin(AWS_ORIGIN)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(VMOne.getOid())
                    .setConnectedEntityType(VMOne.getEntityType())
                    .setConnectionType(ConnectionType.OWNS_CONNECTION))
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(VMTwo.getOid())
                    .setConnectedEntityType(VMTwo.getEntityType())
                    .setConnectionType(ConnectionType.OWNS_CONNECTION))
            .build();

    private final Map<Long, TopologyEntityDTO> topology = ImmutableMap.<Long, TopologyEntityDTO>builder()
            .put(VMOne.getOid(), VMOne)
            .put(VMTwo.getOid(), VMTwo)
            .put(AZ.getOid(), AZ)
            .put(COMPUTE_TIER.getOid(), COMPUTE_TIER)
            .put(REGION.getOid(), REGION)
            .put(BUSINESS_ACCOUNT.getOid(), BUSINESS_ACCOUNT)
            .build();

    private final float DELTA = 0.000001f;

    /**
     * It's more "unit-testy" to mock the factory and the CloudTopology it produces, but it's a bit
     * of work to mock all the different methods on the CloudTopology to connect the entities
     * together, so we use a "real" topology factory for now.
     */
    private TopologyEntityCloudTopologyFactory cloudTopologyFactory =
            new DefaultTopologyEntityCloudTopologyFactory(mock(GroupMemberRetriever.class));

    @Before
    public void setup() throws CommunicationException {

        MockitoAnnotations.initMocks(this);

        reservedInstanceCoverageUpdate = new ReservedInstanceCoverageUpdate(dsl,
                entityReservedInstanceMappingStore, reservedInstanceUtilizationStore,
                reservedInstanceCoverageStore, reservedInstanceCoverageValidatorFactory,
                supplementalRICoverageAnalysisFactory, costNotificationSender,
                120);

        when(reservedInstanceCoverageValidatorFactory.newValidator(any()))
                .thenReturn(reservedInstanceCoverageValidator);
        when(supplementalRICoverageAnalysisFactory.createCoverageAnalysis(any(), anyList()))
                .thenReturn(supplementalRICoverageAnalysis);
    }

    @Test
    public void testCreateServiceEntityReservedInstanceCoverageRecords() {
        final TopologyEntityCloudTopology cloudTopology = cloudTopologyFactory.newCloudTopology(topology.values().stream());
        final List<ServiceEntityReservedInstanceCoverageRecord> records =
                reservedInstanceCoverageUpdate.createServiceEntityReservedInstanceCoverageRecords(
                        Lists.newArrayList(entityRICoverageOne), cloudTopology);
        assertEquals(2L, records.size());
        final ServiceEntityReservedInstanceCoverageRecord firstRecord =
                records.stream()
                        .filter(record -> record.getId() == 123)
                        .findFirst()
                        .get();
        final ServiceEntityReservedInstanceCoverageRecord secondRecord =
                records.stream()
                        .filter(record -> record.getId() == 124)
                        .findFirst()
                        .get();
        assertEquals(125, firstRecord.getBusinessAccountId());
        assertEquals(125, secondRecord.getBusinessAccountId());
        assertEquals(9, firstRecord.getRegionId());
        assertEquals(9, secondRecord.getRegionId());
        assertEquals(0, firstRecord.getAvailabilityZoneId());
        assertEquals(8, secondRecord.getAvailabilityZoneId());
        assertEquals(100, firstRecord.getTotalCoupons(), DELTA);
        // this should match the compute tier capacity
        assertEquals(50, secondRecord.getTotalCoupons(), DELTA);
        assertEquals(20, firstRecord.getUsedCoupons(), DELTA);
        assertEquals(0, secondRecord.getUsedCoupons(), DELTA);
    }

    @Test
    public void testUpdateAllEntityRICoverageIntoDB() {
        // Input setup
        final long topologyId = 123456789L;
        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                .setTopologyId(topologyId)
                .build();
        final TopologyEntityCloudTopology cloudTopology =
                cloudTopologyFactory.newCloudTopology(topology.values().stream());

        final List<EntityRICoverageUpload> coverageInput =
                ImmutableList.of(entityRICoverageOne);

        // mock setup
        when(reservedInstanceCoverageValidator.validateCoverageUploads(eq(coverageInput)))
                .thenReturn(coverageInput);

        when(supplementalRICoverageAnalysis.createCoverageRecordsFromSupplementalAllocation())
                .thenReturn(coverageInput);

        // setup SUT
        reservedInstanceCoverageUpdate.storeEntityRICoverageOnlyIntoCache(topologyId,
                coverageInput);

        // invoke SUT
        reservedInstanceCoverageUpdate.updateAllEntityRICoverageIntoDB(topologyInfo,
                cloudTopology);


        // setup captors
        verify(entityReservedInstanceMappingStore).updateEntityReservedInstanceMapping(
                any(), entityRIMappingStoreCoverageCaptor.capture());
        verify(reservedInstanceCoverageStore).updateReservedInstanceCoverageStore(
                any(), riCoverageStoreCoverageCaptor.capture());
        verify(reservedInstanceUtilizationStore).updateReservedInstanceUtilization(
                any());


        // asserts
        List<EntityRICoverageUpload> entityRIMappingStoreInput =
                entityRIMappingStoreCoverageCaptor.getValue();
        assertThat(entityRIMappingStoreInput.size(), equalTo(1));
        final EntityRICoverageUpload entityRICoverageUpload = entityRIMappingStoreInput.get(0);
        // verify the capacity is overridden to match the compute tier
        assertThat(entityRICoverageUpload.getTotalCouponsRequired(), equalTo(100.0));

        List<ServiceEntityReservedInstanceCoverageRecord> coverageRecords =
                riCoverageStoreCoverageCaptor.getValue();
        assertThat(coverageRecords.size(), equalTo(2));
    }

    /**
     * Test {@link ReservedInstanceCoverageUpdate#updateAllEntityRICoverageIntoDB(TopologyInfo, CloudTopology)}
     * in which the riUploadCache is empty (it should pull records from the {@link EntityReservedInstanceMappingStore}
     */
    @Test
    public void testUpdateAllEntityRICoverageIntoDB_emptyCache() {
        // Input setup
        final long topologyId = 123456789L;
        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                .setTopologyId(topologyId)
                .build();
        final TopologyEntityCloudTopology cloudTopology =
                cloudTopologyFactory.newCloudTopology(topology.values().stream());

        final List<EntityRICoverageUpload> coverageInput =
                ImmutableList.of(entityRICoverageOne);

        // mock setup
        when(reservedInstanceCoverageValidator.validateCoverageUploads(eq(coverageInput)))
                .thenReturn(coverageInput);

        when(supplementalRICoverageAnalysis.createCoverageRecordsFromSupplementalAllocation())
                .thenReturn(coverageInput);
        when(entityReservedInstanceMappingStore.getRICoverageByEntity())
                .thenReturn(ImmutableMap.of(
                        entityRICoverageOne.getEntityId(),
                        ImmutableSet.copyOf(entityRICoverageOne.getCoverageList())));


        // invoke SUT
        reservedInstanceCoverageUpdate.updateAllEntityRICoverageIntoDB(topologyInfo,
                cloudTopology);

        // setup captors
        verify(entityReservedInstanceMappingStore).updateEntityReservedInstanceMapping(
                any(), entityRIMappingStoreCoverageCaptor.capture());
        verify(reservedInstanceCoverageStore).updateReservedInstanceCoverageStore(
                any(), riCoverageStoreCoverageCaptor.capture());
        verify(reservedInstanceUtilizationStore).updateReservedInstanceUtilization(
                any());


        // asserts
        List<EntityRICoverageUpload> entityRIMappingStoreInput =
                entityRIMappingStoreCoverageCaptor.getValue();
        assertThat(entityRIMappingStoreInput.size(), equalTo(1));
        final EntityRICoverageUpload entityRICoverageUpload = entityRIMappingStoreInput.get(0);
        // verify the capacity is overridden to match the compute tier
        assertThat(entityRICoverageUpload.getTotalCouponsRequired(), equalTo(100.0));

        List<ServiceEntityReservedInstanceCoverageRecord> coverageRecords =
                riCoverageStoreCoverageCaptor.getValue();
        assertThat(coverageRecords.size(), equalTo(2));
    }
}
