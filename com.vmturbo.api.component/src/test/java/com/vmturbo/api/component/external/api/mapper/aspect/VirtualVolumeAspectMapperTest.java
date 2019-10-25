package com.vmturbo.api.component.external.api.mapper.aspect;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.function.Function;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entityaspect.VirtualDiskApiDTO;
import com.vmturbo.api.dto.entityaspect.VirtualDisksAspectApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.CostMoles.CostServiceMole;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.StorageInfo;
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
    private static final Long volumeId4 = 24L;
    private static final String volumeName4 = "volume4";
    private static final Long wastedVolumeId1 = 25L;
    // wastedVolume oid for wasted storage attached to storage that is set to have
    // wasted files ignored
    private static final Long wastedVolumeId2 = 26L;
    private static final String wastedVolumeDisplayName = "wastedVolumeForStorage1";
    private static final String wastedVolume2DisplayName = "wastedVolumeForStorage2";
    private static final Long storageId = 33L;
    // storage oid for storage with ignoreWastedFiles == true
    private static final Long storageId2 = 34L;
    private static final String storageDisplayName = "storage1";
    private static final String storage2DisplayName = "storage2";
    private static final String pathFile1 = "file3";
    private static final String pathFile2 = "file4";
    private static final Long sizeFile1 = 3000L;
    private static final Long sizeFile2 = 4000L;
    private static final Long timeFile1 = 300L;
    private static final Long timeFile2 = 400L;

    private static final String[] wastedFiles = { pathFile1, pathFile2 };
    private static final String[] wastedFiles2 = { "file5", "file6" };
    private static final long[] wastedFileSizes = { sizeFile1, sizeFile2 };
    private static final long[] wastedFileModTimes = { timeFile1, timeFile2 };

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
            .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
            .setConnectedEntityId(zoneId1)
            .build())
        .addConnectedEntityList(ConnectedEntity.newBuilder()
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
            .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
            .setConnectedEntityId(zoneId1)
            .build())
        .addConnectedEntityList(ConnectedEntity.newBuilder()
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
            .setConnectedEntityType(EntityType.REGION_VALUE)
            .setConnectedEntityId(regionId1)
            .build())
        .build();

    private ApiPartialEntity region1 = ApiPartialEntity.newBuilder()
        .setOid(regionId1)
        .setDisplayName("aws-US East")
        .setEntityType(EntityType.REGION_VALUE)
        .addConnectedTo(RelatedEntity.newBuilder()
            .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
            .setOid(zoneId1)
            .build())
        .build();

    private ApiPartialEntity zone1 = ApiPartialEntity.newBuilder()
            .setOid(zoneId1)
            .setDisplayName("aws-ap-northeast-1a")
            .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
            .build();

    private ApiPartialEntity storageTierPartialEntity = ApiPartialEntity.newBuilder()
            .setOid(storageTierId1)
            .setDisplayName(storageDisplayName)
            .setEntityType(EntityType.STORAGE_TIER_VALUE)
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
            .setConnectedEntityType(EntityType.STORAGE_TIER_VALUE)
            .setConnectedEntityId(storageTierId2)
            .build())
        .addConnectedEntityList(ConnectedEntity.newBuilder()
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
            .setConnectedEntityType(EntityType.REGION_VALUE)
            .setConnectedEntityId(regionId2)
            .build())
        .build();

    private ApiPartialEntity region2 = ApiPartialEntity.newBuilder()
            .setOid(regionId2)
            .setDisplayName("azure-US East")
            .setEntityType(EntityType.REGION_VALUE)
            .build();

    private TopologyEntityDTO storage1 = TopologyEntityDTO.newBuilder()
        .setOid(storageId)
        .setDisplayName(storageDisplayName)
        .setEntityType(EntityType.STORAGE_VALUE)
        .build();

    private TopologyEntityDTO storage2 = TopologyEntityDTO.newBuilder()
        .setOid(storageId2)
        .setDisplayName(storage2DisplayName)
        .setEntityType(EntityType.STORAGE_VALUE)
        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
            .setStorage(StorageInfo.newBuilder()
                .setIgnoreWastedFiles(true)
                .build())
            .build())
        .build();

    private final Long volumeConnectedZoneId = 102L;
    private final String volumeConnectedZoneDisplayName = "zone1";

    private ApiPartialEntity volumeConnectedZone = ApiPartialEntity.newBuilder()
            .setOid(volumeConnectedZoneId)
            .setDisplayName(volumeConnectedZoneDisplayName)
            .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
            .build();

    private final Long volumeConnectedBusinessAccountId = 103L;
    private final String volumeConnectedBusinessAccountDisplayName = "businessAccount1";

    private ServiceEntityApiDTO volumeConnectedBusinessAccount = new ServiceEntityApiDTO();

    ServiceEntityApiDTO storageTierSEApiDTO = new ServiceEntityApiDTO();

    private final Long virtualVolumeId = 100L;
    private final String virtualVolumeDisplayName = "volume1";

    private final int storageAccessCapacity = 512000;
    private final int storageAmountCapacityInMB = 2 * 1024;
    private final String snapshotId = "snap-vv1";

    private Function<EnvironmentType, TopologyEntityDTO> getVirtualVolume = (envType) -> TopologyEntityDTO.newBuilder()
            .setOid(virtualVolumeId)
            .setDisplayName(virtualVolumeDisplayName)
            .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
            .setEnvironmentType(envType)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(volumeConnectedZoneId)
                    .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                    .build())
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityType(EntityType.STORAGE_TIER_VALUE)
                    .setConnectedEntityId(storageTierId1)
                    .build())
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                    .setConnectedEntityId(volumeConnectedBusinessAccountId)
                    .build())
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setVirtualVolume(VirtualVolumeInfo.newBuilder()
                            .setStorageAccessCapacity(storageAccessCapacity)
                            .setStorageAmountCapacity(storageAmountCapacityInMB)
                            .setSnapshotId(snapshotId)
                            .build()))
            .build();

    /**
     * Test mapOnVolume for Cloud Volume.
     */
    @Test
    public void testMapVolumeCloud() {
        storageTierSEApiDTO.setUuid(storageTierId1.toString());
        storageTierSEApiDTO.setDisplayName(storageDisplayName);

        volumeConnectedBusinessAccount.setUuid(volumeConnectedBusinessAccountId.toString());
        volumeConnectedBusinessAccount.setDisplayName(volumeConnectedBusinessAccountDisplayName);

        doAnswer(invocation -> {
            SearchParameters param = invocation.getArgumentAt(0, SearchParameters.class);
            if (param.equals(SearchProtoUtil.neighborsOfType(virtualVolumeId, TraversalDirection.CONNECTED_FROM, UIEntityType.VIRTUAL_MACHINE))) {
                return ApiTestUtils.mockSearchFullReq(Lists.newArrayList(vm1));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(volumeConnectedZoneId, TraversalDirection.CONNECTED_FROM, UIEntityType.REGION))) {
                return ApiTestUtils.mockSearchReq(Lists.newArrayList(volumeConnectedZone));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(virtualVolumeId, TraversalDirection.CONNECTED_FROM, UIEntityType.BUSINESS_ACCOUNT))) {
                return ApiTestUtils.mockSearchSEReq(Lists.newArrayList(volumeConnectedBusinessAccount));
            } else {
                throw new IllegalArgumentException(param.toString());
            }
        }).when(repositoryApi).newSearchRequest(any(SearchParameters.class));

        RepositoryApi.MultiEntityRequest storageTierRequest = ApiTestUtils.mockMultiSEReq(Lists.newArrayList(storageTierSEApiDTO));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(storageTierId1))).thenReturn(storageTierRequest);

        VirtualDisksAspectApiDTO aspect = (VirtualDisksAspectApiDTO)volumeAspectMapper.mapEntitiesToAspect(
                Lists.newArrayList(getVirtualVolume.apply(EnvironmentType.CLOUD)));

        assertEquals(1, aspect.getVirtualDisks().size());

        // check the virtual disks for each file on the wasted storage
        VirtualDiskApiDTO volumeAspect = null;
        for (VirtualDiskApiDTO virtualDiskApiDTO : aspect.getVirtualDisks()) {
            if (virtualDiskApiDTO.getDisplayName().equals(virtualVolumeDisplayName)) {
                volumeAspect = virtualDiskApiDTO;
            }
        }

        assertNotNull(volumeAspect);
        assertEquals(String.valueOf(virtualVolumeId), volumeAspect.getUuid());
        assertEquals(String.valueOf(storageTierId1), volumeAspect.getProvider().getUuid());
        assertEquals(String.valueOf(volumeConnectedZoneId), volumeAspect.getDataCenter().getUuid());
        assertEquals(String.valueOf(volumeConnectedBusinessAccountId), volumeAspect.getBusinessAccount().getUuid());

        // check stats for volume
        java.util.List<StatApiDTO> stats = volumeAspect.getStats();
        assertEquals(2, stats.size());
        java.util.Optional<StatApiDTO> statApiDTOStorageAccess = stats.stream().filter(stat -> stat.getName() == "StorageAccess").findFirst();
        assertEquals(statApiDTOStorageAccess.get().getCapacity().getAvg().longValue(), storageAccessCapacity);
        java.util.Optional<StatApiDTO> statApiDTOStorageAmount = stats.stream().filter(stat -> stat.getName() == "StorageAmount").findFirst();
        assertEquals(storageAmountCapacityInMB / 1024F, statApiDTOStorageAmount.get().getCapacity().getAvg().longValue(), 0.00001);
        assertEquals(VirtualVolumeAspectMapper.CLOUD_STORAGE_AMOUNT_UNIT, statApiDTOStorageAmount.get().getUnits());

        assertEquals(volumeAspect.getSnapshotId(), snapshotId);
        assertEquals(virtualVolumeDisplayName, volumeAspect.getDisplayName());

        assertEquals(String.valueOf(vmId1), volumeAspect.getAttachedVirtualMachine().getUuid());
    }

    /**
     * test MapOnVolume method for on-perm volume.
     */
    @Test
    public void testMapVolumeOnPerm() {
        storageTierSEApiDTO.setUuid(storageTierId1.toString());
        storageTierSEApiDTO.setDisplayName(storageDisplayName);

        volumeConnectedBusinessAccount.setUuid(volumeConnectedBusinessAccountId.toString());
        volumeConnectedBusinessAccount.setDisplayName(volumeConnectedBusinessAccountDisplayName);

        doAnswer(invocation -> {
            SearchParameters param = invocation.getArgumentAt(0, SearchParameters.class);
            if (param.equals(SearchProtoUtil.neighborsOfType(virtualVolumeId, TraversalDirection.CONNECTED_FROM, UIEntityType.VIRTUAL_MACHINE))) {
                return ApiTestUtils.mockSearchFullReq(Lists.newArrayList(vm1));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(volumeConnectedZoneId, TraversalDirection.CONNECTED_FROM, UIEntityType.REGION))) {
                return ApiTestUtils.mockSearchReq(Lists.newArrayList(volumeConnectedZone));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(virtualVolumeId, TraversalDirection.CONNECTED_FROM, UIEntityType.BUSINESS_ACCOUNT))) {
                return ApiTestUtils.mockSearchSEReq(Lists.newArrayList(volumeConnectedBusinessAccount));
            } else {
                throw new IllegalArgumentException(param.toString());
            }
        }).when(repositoryApi).newSearchRequest(any(SearchParameters.class));

        RepositoryApi.MultiEntityRequest storageTierRequest = ApiTestUtils.mockMultiSEReq(Lists.newArrayList(storageTierSEApiDTO));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(storageTierId1))).thenReturn(storageTierRequest);

        VirtualDisksAspectApiDTO aspect = (VirtualDisksAspectApiDTO) volumeAspectMapper.mapEntitiesToAspect(
            Lists.newArrayList(getVirtualVolume.apply(EnvironmentType.ON_PREM)));

        assertEquals(1, aspect.getVirtualDisks().size());

        // check the virtual disks for each file on the wasted storage
        VirtualDiskApiDTO volumeAspect = null;
        for (VirtualDiskApiDTO virtualDiskApiDTO : aspect.getVirtualDisks()) {
            if (virtualDiskApiDTO.getDisplayName().equals(virtualVolumeDisplayName)) {
                volumeAspect = virtualDiskApiDTO;
            }
        }

        assertNotNull(volumeAspect);
        assertEquals(String.valueOf(virtualVolumeId), volumeAspect.getUuid());
        assertEquals(String.valueOf(storageTierId1), volumeAspect.getProvider().getUuid());
        assertEquals(String.valueOf(volumeConnectedZoneId), volumeAspect.getDataCenter().getUuid());
        assertEquals(String.valueOf(volumeConnectedBusinessAccountId), volumeAspect.getBusinessAccount().getUuid());

        // check stats for volume
        java.util.List<StatApiDTO> stats = volumeAspect.getStats();
        assertEquals(2, stats.size());
        java.util.Optional<StatApiDTO> statApiDTOStorageAccess = stats.stream().filter(stat -> stat.getName() == "StorageAccess").findFirst();
        assertEquals(statApiDTOStorageAccess.get().getCapacity().getAvg().longValue(), storageAccessCapacity);
        java.util.Optional<StatApiDTO> statApiDTOStorageAmount = stats.stream().filter(stat -> stat.getName() == "StorageAmount").findFirst();
        assertEquals(storageAmountCapacityInMB, statApiDTOStorageAmount.get().getCapacity().getAvg().longValue());
        assertEquals(CommodityTypeUnits.STORAGE_AMOUNT.getUnits(), statApiDTOStorageAmount.get().getUnits());

        assertEquals(volumeAspect.getSnapshotId(), snapshotId);
        assertEquals(virtualVolumeDisplayName, volumeAspect.getDisplayName());

        assertEquals(String.valueOf(vmId1), volumeAspect.getAttachedVirtualMachine().getUuid());
    }

    private static final TopologyEntityDTO volume4 = TopologyEntityDTO.newBuilder()
        .setOid(volumeId4)
        .setDisplayName(volumeName4)
        .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
        .addConnectedEntityList(ConnectedEntity.newBuilder()
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

    private static final TopologyEntityDTO wastedFilesVolume = createWastedFilesVolume(wastedVolumeId1,
        wastedVolumeDisplayName, storageId, wastedFiles, wastedFileSizes, wastedFileModTimes);

    private static final TopologyEntityDTO wastedFilesVolume2 = createWastedFilesVolume(wastedVolumeId2,
        wastedVolume2DisplayName, storageId2, wastedFiles2, wastedFileSizes, wastedFileModTimes);

    private VirtualVolumeAspectMapper volumeAspectMapper;

    private RepositoryApi repositoryApi = mock(RepositoryApi.class);

    private CostServiceMole costServiceMole = spy(new CostServiceMole());

    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(costServiceMole);

    @Before
    public void setup() throws Exception {
        // init mapper
        CostServiceBlockingStub costRpc = CostServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
        volumeAspectMapper = spy(new VirtualVolumeAspectMapper(costRpc, repositoryApi));
    }

    @Test
    public void testMapStorageTiers() {
        volumeConnectedBusinessAccount.setUuid(volumeConnectedBusinessAccountId.toString());
        volumeConnectedBusinessAccount.setDisplayName(volumeConnectedBusinessAccountDisplayName);

        doAnswer(invocation -> {
            SearchParameters param = invocation.getArgumentAt(0, SearchParameters.class);
            if (param.equals(SearchProtoUtil.neighborsOfType(storageTierId1, TraversalDirection.CONNECTED_FROM, UIEntityType.VIRTUAL_VOLUME))) {
                return ApiTestUtils.mockSearchFullReq(Lists.newArrayList(volume1, volume2));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(storageTierId2, TraversalDirection.CONNECTED_FROM, UIEntityType.VIRTUAL_VOLUME))) {
                return ApiTestUtils.mockSearchFullReq(Lists.newArrayList(volume3));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(storageTierId1, TraversalDirection.PRODUCES, UIEntityType.VIRTUAL_MACHINE))) {
                return ApiTestUtils.mockSearchFullReq(Lists.newArrayList(vm1));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(storageTierId2, TraversalDirection.PRODUCES, UIEntityType.VIRTUAL_MACHINE))) {
                return ApiTestUtils.mockSearchFullReq(Lists.newArrayList(vm2));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(volumeId1, TraversalDirection.CONNECTED_FROM, UIEntityType.BUSINESS_ACCOUNT))) {
                return ApiTestUtils.mockSearchSEReq(Lists.newArrayList(volumeConnectedBusinessAccount));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(volumeId2, TraversalDirection.CONNECTED_FROM, UIEntityType.BUSINESS_ACCOUNT))) {
                return ApiTestUtils.mockSearchSEReq(Lists.newArrayList(volumeConnectedBusinessAccount));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(volumeId3, TraversalDirection.CONNECTED_FROM, UIEntityType.BUSINESS_ACCOUNT))) {
                return ApiTestUtils.mockSearchSEReq(Lists.newArrayList(volumeConnectedBusinessAccount));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(volumeId4, TraversalDirection.CONNECTED_FROM, UIEntityType.BUSINESS_ACCOUNT))) {
                return ApiTestUtils.mockSearchSEReq(Lists.newArrayList(volumeConnectedBusinessAccount));
            } else {
                throw new IllegalArgumentException(param.toString());
            }
        }).when(repositoryApi).newSearchRequest(any(SearchParameters.class));

        MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(region1, region2));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(regionId1, regionId2))).thenReturn(req);

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
        volumeConnectedBusinessAccount.setUuid(volumeConnectedBusinessAccountId.toString());
        volumeConnectedBusinessAccount.setDisplayName(volumeConnectedBusinessAccountDisplayName);

        doAnswer(invocation -> {
            SearchParameters param = invocation.getArgumentAt(0, SearchParameters.class);
            if (param.equals(SearchProtoUtil.neighborsOfType(storageId, TraversalDirection.CONNECTED_FROM, UIEntityType.VIRTUAL_VOLUME))) {
                return ApiTestUtils.mockSearchFullReq(Lists.newArrayList(volume4, wastedFilesVolume));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(volumeId4, TraversalDirection.CONNECTED_FROM, UIEntityType.VIRTUAL_MACHINE))) {
                return ApiTestUtils.mockSearchCountReq(1);
            } else if (param.equals(SearchProtoUtil.neighborsOfType(wastedVolumeId1, TraversalDirection.CONNECTED_FROM, UIEntityType.VIRTUAL_MACHINE))) {
                return ApiTestUtils.mockSearchCountReq(0);
            } else if (param.equals(SearchProtoUtil.neighborsOfType(volumeId4, TraversalDirection.CONNECTED_FROM, UIEntityType.BUSINESS_ACCOUNT))) {
                return ApiTestUtils.mockSearchSEReq(Lists.newArrayList(volumeConnectedBusinessAccount));
            } else {
                throw new IllegalArgumentException(param.toString());
            }
        }).when(repositoryApi).newSearchRequest(any(SearchParameters.class));

        verify(repositoryApi, never()).newSearchRequest(SearchProtoUtil.neighborsOfType(storageId2, TraversalDirection.CONNECTED_FROM, UIEntityType.VIRTUAL_VOLUME));
        verify(repositoryApi, never()).newSearchRequest(SearchProtoUtil.neighborsOfType(storageId, TraversalDirection.CONNECTED_FROM, UIEntityType.VIRTUAL_VOLUME));

        MultiEntityRequest req = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList(volume4, wastedFilesVolume));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(volumeId4, wastedVolumeId1))).thenReturn(req);

        VirtualDisksAspectApiDTO aspect = (VirtualDisksAspectApiDTO) volumeAspectMapper.mapEntitiesToAspect(
            Lists.newArrayList(storage1, storage2));

        // if ignoreWastedFiles for storage2 was honored, we should never search for storage2
        verify(repositoryApi, never()).newSearchRequest(SearchProtoUtil.neighborsOfType(storageId2,
            TraversalDirection.CONNECTED_FROM, UIEntityType.VIRTUAL_VOLUME));

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

    private static TopologyEntityDTO createWastedFilesVolume(long oid,
                                                      String displayName,
                                                      long connectedStorage,
                                                      String[] paths,
                                                      long[] sizes,
                                                      long[] modificationTimes) {
        VirtualVolumeInfo.Builder vviBuilder = VirtualVolumeInfo.newBuilder();
        for (int i = 0; i < paths.length; i++) {
            vviBuilder.addFiles(VirtualVolumeFileDescriptor.newBuilder()
                .setPath(paths[i])
                .setSizeKb(sizes[i])
                .setModificationTimeMs(modificationTimes[i])
                .build());
        }
        return TopologyEntityDTO.newBuilder()
            .setOid(oid)
            .setDisplayName(displayName)
            .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                .setConnectedEntityType(EntityType.STORAGE_VALUE)
                .setConnectedEntityId(connectedStorage)
                .build())
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setVirtualVolume(vviBuilder.build()))
            .build();
    }
}
