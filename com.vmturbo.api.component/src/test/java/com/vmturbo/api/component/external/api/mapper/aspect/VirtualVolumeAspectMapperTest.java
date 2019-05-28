package com.vmturbo.api.component.external.api.mapper.aspect;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.api.dto.entityaspect.VirtualDiskApiDTO;
import com.vmturbo.api.dto.entityaspect.VirtualDisksAspectApiDTO;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.VirtualVolumeFileDescriptor;

/**
 * Unit tests for the {@link VirtualVolumeAspectMapper}.
 */
public class VirtualVolumeAspectMapperTest {

    // aws
    private Long vmId1 = 11L;
    private Long volumeId1 = 21L;
    private Long volumeId2 = 22L;
    private Long storageTierId1 = 31L;
    private Long zoneId1 = 41L;
    private Long regionId1 = 51L;
    private String vmName1 = "testVM1";
    private String volumeName1 = "vol-123";
    private String volumeName2 = "vol-234";
    private String storageTierName1 = "GP2";

    // azure
    private Long vmId2 = 12L;
    private Long volumeId3 = 23L;
    private Long storageTierId2 = 32L;
    private Long regionId2 = 52L;
    private String vmName2 = "testVM2";
    private String volumeName3 = "vol-345";
    private String storageTierName2 = "UNMANAGED_STANDARD";

    // VCenter
    private Long volumeId4 = 24L;
    private String volumeName4 = "volume4";
    private Long wastedVolumeId1 = 25L;
    private String wastedVolumeDisplayName = "wastedVolumeForStorage1";
    private Long storageId = 33L;
    private String storageDisplayName = "storage1";
    private String pathFile1 = "file3";
    private String pathFile2 = "file4";
    private Long sizeFile1 = 3000L;
    private Long sizeFile2 = 4000L;
    private Long timeFile1 = 300L;
    private Long timeFile2 = 400L;

    // aws entities:
    // vm1 --> volume1, vm1 --> storageTier1
    // volume1 and volume2 --> zone1, storageTier1
    // storageTier1 --> region1
    private TopologyEntityDTO vm1 = TopologyEntityDTO.newBuilder()
            .setOid(vmId1)
            .setDisplayName(vmName1)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                    .setConnectedEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                    .setConnectedEntityId(volumeId1)
                    .build())
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(storageTierId1)
                    .setVolumeId(volumeId1)
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                            .setCommodityType(CommodityType.newBuilder().setType(
                                    CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE).build())
                            .setUsed(50)
                            .build())
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                            .setCommodityType(CommodityType.newBuilder().setType(
                                    CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE).build())
                            .setUsed(100)
                            .build())
                    .build())
            .build();

    private TopologyEntityDTO volume1 = TopologyEntityDTO.newBuilder()
            .setOid(volumeId1)
            .setDisplayName(volumeName1)
            .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                    .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                    .setConnectedEntityId(zoneId1)
                    .build())
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                    .setConnectedEntityType(EntityType.STORAGE_TIER_VALUE)
                    .setConnectedEntityId(storageTierId1)
                    .build())
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setVirtualVolume(VirtualVolumeInfo.newBuilder()
                            .setStorageAccessCapacity(100)
                            .setStorageAmountCapacity(1000)
                            .build())
                    .build())
            .build();

    private TopologyEntityDTO volume2 = TopologyEntityDTO.newBuilder()
            .setOid(volumeId2)
            .setDisplayName(volumeName2)
            .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                    .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                    .setConnectedEntityId(zoneId1)
                    .build())
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                    .setConnectedEntityType(EntityType.STORAGE_TIER_VALUE)
                    .setConnectedEntityId(storageTierId1)
                    .build())
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setVirtualVolume(VirtualVolumeInfo.newBuilder()
                            .setStorageAccessCapacity(200)
                            .setStorageAmountCapacity(2000)
                            .build()))
            .build();

    private TopologyEntityDTO storageTier1 = TopologyEntityDTO.newBuilder()
            .setOid(storageTierId1)
            .setDisplayName(storageTierName1)
            .setEntityType(EntityType.STORAGE_TIER_VALUE)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                    .setConnectedEntityType(EntityType.REGION_VALUE)
                    .setConnectedEntityId(regionId1)
                    .build())
            .build();

    private TopologyEntityDTO region1 = TopologyEntityDTO.newBuilder()
            .setOid(regionId1)
            .setDisplayName("aws-US East")
            .setEntityType(EntityType.REGION_VALUE)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectionType(ConnectionType.OWNS_CONNECTION)
                    .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                    .setConnectedEntityId(zoneId1)
                    .build())
            //            .addConnectedEntityList(ConnectedEntity.newBuilder()
            //                    .setConnectionType(ConnectionType.OWNS_CONNECTION)
            //                    .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
            //                    .setConnectedEntityId(zoneId2)
            //                    .build())
            .build();

    // azure entities:
    // vm2 --> volume3, vm2 --> storageTier2
    // volume1 and volume2 --> zone1, storageTier1
    // storageTier1 --> region1
    private TopologyEntityDTO vm2 = TopologyEntityDTO.newBuilder()
            .setOid(vmId2)
            .setDisplayName(vmName2)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                    .setConnectedEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                    .setConnectedEntityId(volumeId3)
                    .build())
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(storageTierId2)
                    .setVolumeId(volumeId3)
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                            .setCommodityType(CommodityType.newBuilder().setType(
                                    CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE).build())
                            .setUsed(150)
                            .build())
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                            .setCommodityType(CommodityType.newBuilder().setType(
                                    CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE).build())
                            .setUsed(500)
                            .build())
                    .build())
            .build();

    private TopologyEntityDTO volume3 = TopologyEntityDTO.newBuilder()
            .setOid(volumeId3)
            .setDisplayName(volumeName3)
            .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                    .setConnectedEntityType(EntityType.STORAGE_TIER_VALUE)
                    .setConnectedEntityId(storageTierId2)
                    .build())
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                    .setConnectedEntityType(EntityType.REGION_VALUE)
                    .setConnectedEntityId(regionId2)
                    .build())
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setVirtualVolume(VirtualVolumeInfo.newBuilder()
                            .setStorageAccessCapacity(300)
                            .setStorageAmountCapacity(3000)
                            .build()))
            .build();

    private TopologyEntityDTO storageTier2 = TopologyEntityDTO.newBuilder()
            .setOid(storageTierId2)
            .setDisplayName(storageTierName2)
            .setEntityType(EntityType.STORAGE_TIER_VALUE)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                    .setConnectedEntityType(EntityType.REGION_VALUE)
                    .setConnectedEntityId(regionId2)
                    .build())
            .build();

    private TopologyEntityDTO region2 = TopologyEntityDTO.newBuilder()
            .setOid(regionId2)
            .setDisplayName("azure-US East")
            .setEntityType(EntityType.REGION_VALUE)
            .build();

    private TopologyEntityDTO storage1 = TopologyEntityDTO.newBuilder()
        .setOid(storageId)
        .setDisplayName(storageDisplayName)
        .setEntityType(EntityType.STORAGE_VALUE)
        .build();

    private TopologyEntityDTO volume4 = TopologyEntityDTO.newBuilder()
        .setOid(volumeId4)
        .setDisplayName(volumeName4)
        .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectionType(ConnectionType.NORMAL_CONNECTION)
            .setConnectedEntityType(EntityType.STORAGE_VALUE)
            .setConnectedEntityId(storageId)
            .build())
        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
            .setVirtualVolume(VirtualVolumeInfo.newBuilder()
                .addFiles(VirtualVolumeFileDescriptor.newBuilder()
                    .setPath("file1")
                    .setSizeKb(1000)
                    .build())
                .addFiles(VirtualVolumeFileDescriptor.newBuilder()
                    .setPath("file2")
                    .setSizeKb(2000)
                    .build())
                .build()))
        .build();

    private TopologyEntityDTO wastedFilesVolume = TopologyEntityDTO.newBuilder()
        .setOid(wastedVolumeId1)
        .setDisplayName(wastedVolumeDisplayName)
        .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectionType(ConnectionType.NORMAL_CONNECTION)
            .setConnectedEntityType(EntityType.STORAGE_VALUE)
            .setConnectedEntityId(storageId)
            .build())
        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
            .setVirtualVolume(VirtualVolumeInfo.newBuilder()
                .addFiles(VirtualVolumeFileDescriptor.newBuilder()
                    .setPath(pathFile1)
                    .setSizeKb(sizeFile1)
                    .setModificationTimeMs(timeFile1)
                    .build())
                .addFiles(VirtualVolumeFileDescriptor.newBuilder()
                    .setPath(pathFile2)
                    .setSizeKb(sizeFile2)
                    .setModificationTimeMs(timeFile2)
                    .build())
                .build()))
        .build();

    private VirtualVolumeAspectMapper volumeAspectMapper;

    @Before
    public void setup() throws Exception {
        // init mapper
        SearchServiceMole searchServiceSpy = Mockito.spy(new SearchServiceMole());
        GrpcTestServer grpcServer = GrpcTestServer.newServer(searchServiceSpy);
        grpcServer.start();
        SearchServiceBlockingStub searchRpc = SearchServiceGrpc.newBlockingStub(grpcServer.getChannel());
        CostServiceBlockingStub costRpc = CostServiceGrpc.newBlockingStub(grpcServer.getChannel());
        volumeAspectMapper = spy(new VirtualVolumeAspectMapper(searchRpc, costRpc));
    }

    @Test
    public void testMapStorageTiers() {
        when(volumeAspectMapper.traverseAndGetEntities(String.valueOf(storageTierId1),
            TraversalDirection.CONNECTED_FROM, UIEntityType.VIRTUAL_VOLUME.apiStr()))
            .thenReturn(Lists.newArrayList(volume1, volume2));

        when(volumeAspectMapper.traverseAndGetEntities(String.valueOf(storageTierId2),
            TraversalDirection.CONNECTED_FROM, UIEntityType.VIRTUAL_VOLUME.apiStr()))
            .thenReturn(Lists.newArrayList(volume3));

        when(volumeAspectMapper.traverseAndGetEntities(String.valueOf(storageTierId1),
            TraversalDirection.PRODUCES, UIEntityType.VIRTUAL_MACHINE.apiStr()))
            .thenReturn(Lists.newArrayList(vm1));

        when(volumeAspectMapper.traverseAndGetEntities(String.valueOf(storageTierId2),
            TraversalDirection.PRODUCES, UIEntityType.VIRTUAL_MACHINE.apiStr()))
            .thenReturn(Lists.newArrayList(vm2));

        when(volumeAspectMapper.searchTopologyEntityDTOs(Sets.newHashSet(regionId1, regionId2), null))
            .thenReturn(Lists.newArrayList(region1, region2));

        VirtualDisksAspectApiDTO aspect = (VirtualDisksAspectApiDTO) volumeAspectMapper.mapEntitiesToAspect(
            Lists.newArrayList(storageTier1, storageTier2));

        assertEquals(3, aspect.getVirtualDisks().size());

        // check attached vm for volumes:
        // volume1 is attached to vm1, volume2 is unattached volume, volume3 is attached to vm2
        VirtualDiskApiDTO volumeAspect1 = null;
        VirtualDiskApiDTO volumeAspect2 = null;
        VirtualDiskApiDTO volumeAspect3 = null;
        for (VirtualDiskApiDTO virtualDiskApiDTO : aspect.getVirtualDisks()) {
            if (virtualDiskApiDTO.getDisplayName().equals(volumeName1)) {
                volumeAspect1 = virtualDiskApiDTO;
            } else if (virtualDiskApiDTO.getDisplayName().equals(volumeName2)) {
                volumeAspect2 = virtualDiskApiDTO;
            } else if (virtualDiskApiDTO.getDisplayName().equals(volumeName3)) {
                volumeAspect3 = virtualDiskApiDTO;
            }
        }
        assertNotNull(volumeAspect1);
        assertNotNull(volumeAspect2);
        assertNotNull(volumeAspect3);
        assertEquals(String.valueOf(vmId1), volumeAspect1.getAttachedVirtualMachine().getUuid());
        assertNull(volumeAspect2.getAttachedVirtualMachine());
        assertEquals(String.valueOf(vmId2), volumeAspect3.getAttachedVirtualMachine().getUuid());

        // check datacenter
        assertEquals(String.valueOf(regionId1), volumeAspect1.getDataCenter().getUuid());
        assertEquals(String.valueOf(regionId1), volumeAspect2.getDataCenter().getUuid());
        assertEquals(String.valueOf(regionId2), volumeAspect3.getDataCenter().getUuid());

        // check storage tier
        assertEquals(storageTierName1, volumeAspect1.getTier());
        assertEquals(storageTierName1, volumeAspect2.getTier());
        assertEquals(storageTierName2, volumeAspect3.getTier());

        // check stats for different volumes
        assertEquals(2, volumeAspect1.getStats().size());
        volumeAspect1.getStats().forEach(statApiDTO -> {
            if (statApiDTO.getName().equals(CommodityTypeUnits.STORAGE_ACCESS.getMixedCase())) {
                assertEquals(50, statApiDTO.getValue(), 0);
                assertEquals(100, statApiDTO.getCapacity().getTotal(), 0);
                assertEquals(String.valueOf(storageTierId1), statApiDTO.getRelatedEntity().getUuid());
            } else if (statApiDTO.getName().equals(CommodityTypeUnits.STORAGE_AMOUNT.getMixedCase())) {
                assertEquals(100, statApiDTO.getValue(), 0);
                assertEquals(1000, statApiDTO.getCapacity().getTotal(), 0);
                assertEquals(String.valueOf(storageTierId1), statApiDTO.getRelatedEntity().getUuid());
            }
        });

        assertEquals(2, volumeAspect2.getStats().size());
        volumeAspect2.getStats().forEach(statApiDTO -> {
            if (statApiDTO.getName().equals(CommodityTypeUnits.STORAGE_ACCESS.getMixedCase())) {
                assertEquals(0, statApiDTO.getValue(), 0);
                assertEquals(200, statApiDTO.getCapacity().getTotal(), 0);
                assertEquals(String.valueOf(storageTierId1), statApiDTO.getRelatedEntity().getUuid());
            } else if (statApiDTO.getName().equals(CommodityTypeUnits.STORAGE_AMOUNT.getMixedCase())) {
                assertEquals(0, statApiDTO.getValue(), 0);
                assertEquals(2000, statApiDTO.getCapacity().getTotal(), 0);
                assertEquals(String.valueOf(storageTierId1), statApiDTO.getRelatedEntity().getUuid());
            }
        });

        assertEquals(2, volumeAspect3.getStats().size());
        volumeAspect3.getStats().forEach(statApiDTO -> {
            if (statApiDTO.getName().equals(CommodityTypeUnits.STORAGE_ACCESS.getMixedCase())) {
                assertEquals(150, statApiDTO.getValue(), 0);
                assertEquals(300, statApiDTO.getCapacity().getTotal(), 0);
                assertEquals(String.valueOf(storageTierId2), statApiDTO.getRelatedEntity().getUuid());
            } else if (statApiDTO.getName().equals(CommodityTypeUnits.STORAGE_AMOUNT.getMixedCase())) {
                assertEquals(500, statApiDTO.getValue(), 0);
                assertEquals(3000, statApiDTO.getCapacity().getTotal(), 0);
                assertEquals(String.valueOf(storageTierId2), statApiDTO.getRelatedEntity().getUuid());
            }
        });
    }

    @Test
    public void testMapStorage() {
        when(volumeAspectMapper.traverseAndGetEntities(String.valueOf(storageId),
            TraversalDirection.CONNECTED_FROM, UIEntityType.VIRTUAL_VOLUME.apiStr()))
            .thenReturn(Lists.newArrayList(volume4, wastedFilesVolume));

        when(volumeAspectMapper.searchTopologyEntityDTOs(Sets.newHashSet(volumeId4, wastedVolumeId1), null))
            .thenReturn(Lists.newArrayList(volume4, wastedFilesVolume));

        when(volumeAspectMapper.traverseAndGetEntityCount(volumeId4.toString(),
            TraversalDirection.CONNECTED_FROM,
            UIEntityType.VIRTUAL_MACHINE.apiStr())).thenReturn(1);

        when(volumeAspectMapper.traverseAndGetEntityCount(wastedVolumeId1.toString(),
            TraversalDirection.CONNECTED_FROM,
            UIEntityType.VIRTUAL_MACHINE.apiStr())).thenReturn(0);

        VirtualDisksAspectApiDTO aspect = (VirtualDisksAspectApiDTO) volumeAspectMapper.mapEntitiesToAspect(
            Lists.newArrayList(storage1));

        assertEquals(2, aspect.getVirtualDisks().size());

        // check the virtual disks for each file on the wasted storage
        VirtualDiskApiDTO volumeAspect1 = null;
        VirtualDiskApiDTO volumeAspect2 = null;
        for (VirtualDiskApiDTO virtualDiskApiDTO : aspect.getVirtualDisks()) {
            if (virtualDiskApiDTO.getDisplayName().equals(pathFile1)) {
                volumeAspect1 = virtualDiskApiDTO;
            } else if (virtualDiskApiDTO.getDisplayName().equals(pathFile2)) {
                volumeAspect2 = virtualDiskApiDTO;
            }
        }
        assertNotNull(volumeAspect1);
        assertNotNull(volumeAspect2);
        assertEquals(String.valueOf(storageId), volumeAspect1.getProvider().getUuid());
        assertNull(volumeAspect2.getAttachedVirtualMachine());
        assertEquals(String.valueOf(storageId), volumeAspect2.getProvider().getUuid());

        // check stats for different volumes
        assertEquals(1, volumeAspect1.getStats().size());
        assertEquals(pathFile1, volumeAspect1.getDisplayName());
        assertEquals(sizeFile1 / 1024.0D, volumeAspect1.getStats().get(0).getValue(), 0.1D);
        assertEquals((long) timeFile1, volumeAspect1.getLastModified());

        assertEquals(1, volumeAspect2.getStats().size());
        assertEquals(pathFile2, volumeAspect2.getDisplayName());
        assertEquals(sizeFile2 / 1024.0D, volumeAspect2.getStats().get(0).getValue(), 0.1D);
        assertEquals((long) timeFile2, volumeAspect2.getLastModified());
    }
}
