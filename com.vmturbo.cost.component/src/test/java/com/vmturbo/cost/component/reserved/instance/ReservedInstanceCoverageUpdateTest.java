package com.vmturbo.cost.component.reserved.instance;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;

public class ReservedInstanceCoverageUpdateTest {

    private DSLContext dsl = mock(DSLContext.class);

    private EntityReservedInstanceMappingStore entityReservedInstanceMappingStore =
            mock(EntityReservedInstanceMappingStore.class);

    private ReservedInstanceUtilizationStore reservedInstanceUtilizationStore =
            mock(ReservedInstanceUtilizationStore.class);

    private ReservedInstanceCoverageStore reservedInstanceCoverageStore =
            mock(ReservedInstanceCoverageStore.class);

    private ReservedInstanceCoverageUpdate reservedInstanceCoverageUpdate;

    private static final long AWS_TARGET_ID = 77777L;
    private static final long AWS_PROBE_ID = 87778L;

    private static final Origin AWS_ORIGIN = Origin.newBuilder()
            .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                    .addDiscoveringTargetIds(AWS_TARGET_ID))
            .build();

    private TargetInfo awsTargetInfo;

    private ProbeInfo awsProbeInfo;

    private TopologyProcessor topologyProcessorClient = mock(TopologyProcessor.class);

    private final TopologyEntityDTO AZ = TopologyEntityDTO.newBuilder()
            .setOid(8L)
            .setDisplayName("this is available")
            .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
            .setOrigin(AWS_ORIGIN)
            .build();

    private final TopologyEntityDTO REGION = TopologyEntityDTO.newBuilder()
            .setOid(9L)
            .setDisplayName("region")
            .setEntityType(EntityType.REGION_VALUE)
            .setOrigin(AWS_ORIGIN)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityType(AZ.getEntityType())
                    .setConnectedEntityId(AZ.getOid())
                    .setConnectionType(ConnectionType.OWNS_CONNECTION))
            .build();

    private final TopologyEntityDTO COMPUTE_TIER = TopologyEntityDTO.newBuilder()
            .setOid(99L)
            .setDisplayName("r3.xlarge")
            .setEntityType(EntityType.COMPUTE_TIER_VALUE)
            .setOrigin(AWS_ORIGIN)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityType(REGION.getEntityType())
                    .setConnectedEntityId(REGION.getOid())
                    .setConnectionType(ConnectionType.NORMAL_CONNECTION))
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

    private final TopologyEntityDTO VMOne =
            TopologyEntityDTO.newBuilder()
                    .setOid(123L)
                    .setDisplayName("bar")
                    .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .setOrigin(AWS_ORIGIN)
                    .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                            .setProviderId(COMPUTE_TIER.getOid())
                            .setProviderEntityType(COMPUTE_TIER.getEntityType()))
                    .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                            .setVirtualMachine(VirtualMachineInfo.newBuilder()
                                    .setGuestOsType(OSType.LINUX)
                                    .setTenancy(Tenancy.DEFAULT)))
                    .addConnectedEntityList(ConnectedEntity.newBuilder()
                            .setConnectedEntityType(REGION.getEntityType())
                            .setConnectedEntityId(REGION.getOid()))
                    .build();

    private final TopologyEntityDTO VMTwo =
            TopologyEntityDTO.newBuilder()
                    .setOid(124L)
                    .setDisplayName("foo")
                    .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .setOrigin(AWS_ORIGIN)
                    .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                            .setProviderId(COMPUTE_TIER.getOid())
                            .setProviderEntityType(COMPUTE_TIER.getEntityType()))
                    .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                            .setVirtualMachine(VirtualMachineInfo.newBuilder()
                                    .setGuestOsType(OSType.LINUX)
                                    .setTenancy(Tenancy.DEFAULT)))
                    .addConnectedEntityList(ConnectedEntity.newBuilder()
                            .setConnectedEntityType(AZ.getEntityType())
                            .setConnectedEntityId(AZ.getOid()))
                    .build();

    private final TopologyEntityDTO BUSINESS_ACCOUNT = TopologyEntityDTO.newBuilder()
            .setOid(125L)
            .setDisplayName("businessAccount")
            .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
            .setOrigin(AWS_ORIGIN)
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
            new DefaultTopologyEntityCloudTopologyFactory(topologyProcessorClient);

    @Before
    public void setup() throws CommunicationException {
        reservedInstanceCoverageUpdate = new ReservedInstanceCoverageUpdate(dsl,
                entityReservedInstanceMappingStore, reservedInstanceUtilizationStore,
                reservedInstanceCoverageStore, 120);
        awsProbeInfo = mock(ProbeInfo.class);
        when(awsProbeInfo.getType()).thenReturn(SDKProbeType.AWS.getProbeType());
        when(awsProbeInfo.getId()).thenReturn(AWS_PROBE_ID);
        awsTargetInfo = mock(TargetInfo.class);
        when(awsTargetInfo.getId()).thenReturn(AWS_TARGET_ID);
        when(awsTargetInfo.getProbeId()).thenReturn(AWS_PROBE_ID);
        when(topologyProcessorClient.getAllTargets()).thenReturn(Collections.singleton(awsTargetInfo));
        when(topologyProcessorClient.getAllProbes()).thenReturn(Collections.singleton(awsProbeInfo));
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
        assertEquals(32, secondRecord.getTotalCoupons(), DELTA);
        assertEquals(20, firstRecord.getUsedCoupons(), DELTA);
        assertEquals(0, secondRecord.getUsedCoupons(), DELTA);
    }
}
