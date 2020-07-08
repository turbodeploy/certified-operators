package com.vmturbo.api.component.external.api.mapper.aspect;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
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
import com.vmturbo.common.protobuf.stats.Stats.GetMostRecentStatResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatHistoricalEpoch;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.StorageInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
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
    private Long azureVmId = 12L;
    private Long azureVolumeId = 23L;
    private Long azureStorageTierId = 32L;
    private Long azureRegionId = 52L;
    private float azureVolumeIoThroughput = 30;
    private String azureVmName = "testAzureVM";
    private String azureVolumeName = "azureVolume";
    private String azureStorageTierName = "UNMANAGED_STANDARD";

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
    // vm1 --> volume1
    // volume1 and volume2 --> zone1, storageTier1
    // region1 --> zone --> storageTier1
    // zone1 --> vm1
    private final TopologyEntityDTO vm1 = TopologyEntityDTO.newBuilder()
        .setOid(vmId1)
        .setDisplayName(vmName1)
        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
        .addConnectedEntityList(ConnectedEntity.newBuilder()
                .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                .setConnectedEntityId(zoneId1)
                .build())
        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                .setProviderId(volumeId1)
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

    private final TopologyEntityDTO volume1 = TopologyEntityDTO.newBuilder()
        .setOid(volumeId1)
        .setDisplayName(volumeName1)
        .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
            .setConnectedEntityId(zoneId1)
            .build())
        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setVirtualVolume(VirtualVolumeInfo.getDefaultInstance()))
        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderEntityType(EntityType.STORAGE_TIER_VALUE)
                .setProviderId(storageTierId1)
                .build())
        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT.getNumber())
                        .build())
                .setCapacity(1000)
                .build())
        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.STORAGE_ACCESS.getNumber())
                        .build())
                .setCapacity(100)
                .build())
        .build();

    private final TopologyEntityDTO volume2 = TopologyEntityDTO.newBuilder()
        .setOid(volumeId2)
        .setDisplayName(volumeName2)
        .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
            .setConnectedEntityId(zoneId1)
            .build())
        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setVirtualVolume(VirtualVolumeInfo.getDefaultInstance()))
        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderEntityType(EntityType.STORAGE_TIER_VALUE)
                .setProviderId(storageTierId1)
                .build())
        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT.getNumber())
                        .build())
                .setCapacity(2000)
                .build())
        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.STORAGE_ACCESS.getNumber())
                        .build())
                .setCapacity(200)
                .build())
        .build();

    private final TopologyEntityDTO storageTier1 = TopologyEntityDTO.newBuilder()
        .setOid(storageTierId1)
        .setDisplayName(storageTierName1)
        .setEntityType(EntityType.STORAGE_TIER_VALUE)
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityId(regionId1)
            .setConnectedEntityType(EntityType.REGION_VALUE)
            .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
        .build();

    private final ApiPartialEntity region1 = ApiPartialEntity.newBuilder()
        .setOid(regionId1)
        .setDisplayName("aws-US East")
        .setEntityType(EntityType.REGION_VALUE)
        .addConnectedTo(RelatedEntity.newBuilder()
                            .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                            .setOid(zoneId1)
                            .build())
        .addConnectedTo(RelatedEntity.newBuilder()
                            .setEntityType(EntityType.STORAGE_TIER_VALUE)
                            .setOid(storageTierId1)
                            .build())
        .build();

    // azure entities:
    // azureVm --> azureVolume, azureVm --> azureStorageTier
    // volume1 and volume2 --> zone1, storageTier1
    // region1 --> storageTier1, azureVm
    private final TopologyEntityDTO azureVm = TopologyEntityDTO.newBuilder()
            .setOid(azureVmId)
            .setDisplayName(azureVmName)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(azureRegionId)
                    .setConnectedEntityType(EntityType.REGION_VALUE)
                    .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                    .setProviderId(azureVolumeId)
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

    private final TopologyEntityDTO azureVolume = TopologyEntityDTO.newBuilder()
        .setOid(azureVolumeId)
        .setDisplayName(azureVolumeName)
        .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityType(EntityType.REGION_VALUE)
            .setConnectedEntityId(azureRegionId)
            .build())
        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setVirtualVolume(VirtualVolumeInfo.getDefaultInstance()))
        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderEntityType(EntityType.STORAGE_TIER.getNumber())
                .setProviderId(azureStorageTierId)
                .build())
        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT.getNumber())
                        .build())
                .setCapacity(3000)
                .build())
        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.STORAGE_ACCESS.getNumber())
                        .build())
                .setCapacity(300)
                .build())
        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.IO_THROUGHPUT.getNumber())
                        .build())
                .setCapacity(azureVolumeIoThroughput)
                .build())
        .build();

    private final TopologyEntityDTO azureStorageTier = TopologyEntityDTO.newBuilder()
        .setOid(azureStorageTierId)
        .setDisplayName(azureStorageTierName)
        .setEntityType(EntityType.STORAGE_TIER_VALUE)
        .addConnectedEntityList(ConnectedEntity.newBuilder()
                                    .setConnectedEntityId(azureRegionId)
                                    .setConnectedEntityType(EntityType.REGION_VALUE)
                                    .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
        .build();

    private final ApiPartialEntity azureRegion = ApiPartialEntity.newBuilder()
            .setOid(azureRegionId)
            .setDisplayName("azure-US East")
            .setEntityType(EntityType.REGION_VALUE)
            .build();

    private final TopologyEntityDTO storage1 = TopologyEntityDTO.newBuilder()
        .setOid(storageId)
        .setDisplayName(storageDisplayName)
        .setEntityType(EntityType.STORAGE_VALUE)
        .build();

    private final TopologyEntityDTO storage2 = TopologyEntityDTO.newBuilder()
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

    private final  ApiPartialEntity volumeConnectedZone = ApiPartialEntity.newBuilder()
            .setOid(volumeConnectedZoneId)
            .setDisplayName(volumeConnectedZoneDisplayName)
            .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
            .build();

    private final Long volumeConnectedBusinessAccountId = 103L;
    private final String volumeConnectedBusinessAccountDisplayName = "businessAccount1";

    private final ServiceEntityApiDTO volumeConnectedBusinessAccount = new ServiceEntityApiDTO();

    private final ServiceEntityApiDTO storageTierSEApiDTO = new ServiceEntityApiDTO();

    private final Long virtualVolumeId = 100L;
    private final String virtualVolumeDisplayName = "volume1";

    private final int storageAccessCapacity = 512000;
    private final int storageAmountCapacityInMB = 2 * 1024;
    private final String snapshotId = "snap-vv1";

    private ApiPartialEntity region = ApiPartialEntity.newBuilder().setOid(azureRegionId)
            .setEntityType(EntityType.REGION_VALUE)
            .addConnectedTo(RelatedEntity.newBuilder().setOid(volumeConnectedZoneId)
                    .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE).build()).build();

    private final BiFunction<Long, EnvironmentType, TopologyEntityDTO> getVirtualVolumeWithId = (virtualVolumeId, envType) -> TopologyEntityDTO.newBuilder()
            .setOid(virtualVolumeId)
            .setDisplayName(virtualVolumeDisplayName)
            .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
            .setEnvironmentType(envType)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(volumeConnectedZoneId)
                    .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                    .build())
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderEntityType(EntityType.STORAGE_TIER_VALUE)
                    .setProviderId(storageTierId1)
                    .build())
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setVirtualVolume(VirtualVolumeInfo.newBuilder()
                            .setSnapshotId(snapshotId)
                            .setAttachmentState(AttachmentState.ATTACHED)
                            .setEncryption(true)
                            .build()))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT.getNumber())
                            .build())
                    .setCapacity(storageAmountCapacityInMB)
                    .build())
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.STORAGE_ACCESS.getNumber())
                            .build())
                    .setCapacity(storageAccessCapacity)
                    .build())
            .build();

    private final Function<EnvironmentType, TopologyEntityDTO> getVirtualVolume = environmentType -> getVirtualVolumeWithId.apply(virtualVolumeId, environmentType);

    private final Supplier<TopologyEntityDTO> getAzureVirtualVolume = () -> TopologyEntityDTO.newBuilder()
        .setOid(azureVolumeId)
        .setDisplayName(azureVolumeName)
        .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
        .setEnvironmentType(EnvironmentType.CLOUD)
        .addConnectedEntityList(ConnectedEntity.newBuilder()
            .setConnectedEntityId(azureRegionId)
            .setConnectedEntityType(EntityType.REGION_VALUE)
            .build())
        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderEntityType(EntityType.STORAGE_TIER_VALUE)
            .setProviderId(azureStorageTierId)
            .build())
        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
            .setVirtualVolume(VirtualVolumeInfo.newBuilder()
                .setSnapshotId(snapshotId)
                .setAttachmentState(AttachmentState.ATTACHED)
                .setEncryption(true)
                .build()))
        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT.getNumber())
                .build())
            .setCapacity(storageAmountCapacityInMB)
            .build())
        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.STORAGE_ACCESS.getNumber())
                .build())
            .setCapacity(storageAccessCapacity)
            .build())
        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.IO_THROUGHPUT.getNumber())
                .build())
            .setCapacity(azureVolumeIoThroughput)
            .build())
        .build();

    /**
     * Test mapVirtualVolume for multiple volume with same storage tier.
     * Two volumes, connected to the same VM, same storage tier.  Each of them should have all the
     * information.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testMapCloudVolumes() throws Exception {
        storageTierSEApiDTO.setUuid(storageTierId1.toString());
        storageTierSEApiDTO.setDisplayName(storageDisplayName);

        volumeConnectedBusinessAccount.setUuid(volumeConnectedBusinessAccountId.toString());
        volumeConnectedBusinessAccount.setDisplayName(volumeConnectedBusinessAccountDisplayName);

        doAnswer(invocation -> {
            SearchParameters param = invocation.getArgumentAt(0, SearchParameters.class);
            if (param.equals(SearchProtoUtil.neighborsOfType(virtualVolumeId, TraversalDirection.PRODUCES, ApiEntityType.VIRTUAL_MACHINE))) {
                return ApiTestUtils.mockSearchFullReq(Lists.newArrayList(vm1));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(virtualVolumeId + 1, TraversalDirection.PRODUCES, ApiEntityType.VIRTUAL_MACHINE))) {
                return ApiTestUtils.mockSearchFullReq(Lists.newArrayList(vm1));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(volumeConnectedZoneId, TraversalDirection.OWNED_BY, ApiEntityType.REGION))) {
                return ApiTestUtils.mockSearchReq(Lists.newArrayList(region));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(virtualVolumeId, TraversalDirection.OWNED_BY, ApiEntityType.BUSINESS_ACCOUNT))) {
                return ApiTestUtils.mockSearchSEReq(Lists.newArrayList(volumeConnectedBusinessAccount));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(virtualVolumeId + 1, TraversalDirection.OWNED_BY, ApiEntityType.BUSINESS_ACCOUNT))) {
                return ApiTestUtils.mockSearchSEReq(Lists.newArrayList(volumeConnectedBusinessAccount));
            } else if (param.equals(SearchParameters.newBuilder().setStartingFilter(SearchProtoUtil.entityTypeFilter(ApiEntityType.REGION.apiStr())).build())) {
                return ApiTestUtils.mockSearchReq(Lists.newArrayList(region));
            } else {
                throw new IllegalArgumentException(param.toString());
            }
        }).when(repositoryApi).newSearchRequest(any(SearchParameters.class));

        final List<MinimalEntity> storageTierMinEntities = ImmutableList.of(
                MinimalEntity.newBuilder().setOid(storageTierId1).setDisplayName(storageDisplayName).build()
                );
        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(storageTierMinEntities);
        when(repositoryApi.entitiesRequest(eq(Sets.newHashSet(storageTierId1)))).thenReturn(req);

        VirtualDisksAspectApiDTO aspect = (VirtualDisksAspectApiDTO)volumeAspectMapper.mapEntitiesToAspect(
            Lists.newArrayList(getVirtualVolume.apply(EnvironmentType.CLOUD), getVirtualVolumeWithId.apply(virtualVolumeId + 1, EnvironmentType.CLOUD)));

        assertEquals(2, aspect.getVirtualDisks().size());

        // check the virtual disks for each file on the wasted storage
        VirtualDiskApiDTO volumeAspect = null;
        for (VirtualDiskApiDTO virtualDiskApiDTO : aspect.getVirtualDisks()) {
            if (virtualDiskApiDTO.getDisplayName().equals(virtualVolumeDisplayName)) {
                volumeAspect = virtualDiskApiDTO;

                assertNotNull(volumeAspect);
                assertThat(volumeAspect.getUuid(), anyOf(is(String.valueOf(virtualVolumeId)), is(String.valueOf(virtualVolumeId + 1))));
                assertEquals(String.valueOf(storageTierId1), volumeAspect.getProvider().getUuid());
                assertEquals(String.valueOf(azureRegionId), volumeAspect.getDataCenter().getUuid());
                assertEquals(String.valueOf(volumeConnectedBusinessAccountId), volumeAspect.getBusinessAccount().getUuid());

                // check stats for volume
                java.util.List<StatApiDTO> stats = volumeAspect.getStats();
                assertEquals(3, stats.size());
                java.util.Optional<StatApiDTO> statApiDTOStorageAccess = stats.stream().filter(stat -> stat.getName() == "StorageAccess").findFirst();
                assertEquals(statApiDTOStorageAccess.get().getCapacity().getAvg().longValue(), storageAccessCapacity);
                java.util.Optional<StatApiDTO> statApiDTOStorageAmount = stats.stream().filter(stat -> stat.getName() == "StorageAmount").findFirst();
                assertEquals(storageAmountCapacityInMB / 1024F, statApiDTOStorageAmount.get().getCapacity().getAvg().longValue(), 0.00001);
                assertEquals(VirtualVolumeAspectMapper.CLOUD_STORAGE_AMOUNT_UNIT, statApiDTOStorageAmount.get().getUnits());

                assertEquals(snapshotId, volumeAspect.getSnapshotId());
                assertEquals(AttachmentState.ATTACHED.name(), volumeAspect.getAttachmentState());
                assertEquals("Enabled", volumeAspect.getEncryption());
                assertEquals(virtualVolumeDisplayName, volumeAspect.getDisplayName());

                assertEquals(String.valueOf(vmId1), volumeAspect.getAttachedVirtualMachine().getUuid());
            }
        }
    }

    /**
     * Test mapOnVolume for Cloud Volume.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testMapVolumeCloud() throws Exception {
        storageTierSEApiDTO.setUuid(storageTierId1.toString());
        storageTierSEApiDTO.setDisplayName(storageDisplayName);

        volumeConnectedBusinessAccount.setUuid(volumeConnectedBusinessAccountId.toString());
        volumeConnectedBusinessAccount.setDisplayName(volumeConnectedBusinessAccountDisplayName);

        doAnswer(invocation -> {
            SearchParameters param = invocation.getArgumentAt(0, SearchParameters.class);
            if (param.equals(SearchProtoUtil.neighborsOfType(virtualVolumeId, TraversalDirection.PRODUCES, ApiEntityType.VIRTUAL_MACHINE))) {
                return ApiTestUtils.mockSearchFullReq(Lists.newArrayList(vm1));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(volumeConnectedZoneId, TraversalDirection.OWNED_BY, ApiEntityType.REGION))) {
                return ApiTestUtils.mockSearchReq(Lists.newArrayList(region));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(virtualVolumeId, TraversalDirection.OWNED_BY, ApiEntityType.BUSINESS_ACCOUNT))) {
                return ApiTestUtils.mockSearchSEReq(Lists.newArrayList(volumeConnectedBusinessAccount));
            } else if (param.equals(SearchParameters.newBuilder().setStartingFilter(SearchProtoUtil.entityTypeFilter(ApiEntityType.REGION.apiStr())).build())) {
                return ApiTestUtils.mockSearchReq(Lists.newArrayList(region));
            } else {
                throw new IllegalArgumentException(param.toString());
            }
        }).when(repositoryApi).newSearchRequest(any(SearchParameters.class));

        RepositoryApi.MultiEntityRequest storageTierRequest = ApiTestUtils.mockMultiSEReq(Lists.newArrayList(storageTierSEApiDTO));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(storageTierId1))).thenReturn(storageTierRequest);

        final List<MinimalEntity> storageTierMinEntities = ImmutableList.of(
                MinimalEntity.newBuilder().setOid(storageTierId1).setDisplayName(storageDisplayName).build()
        );
        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(storageTierMinEntities);
        when(repositoryApi.entitiesRequest(eq(Sets.newHashSet(storageTierId1)))).thenReturn(req);

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
        assertEquals(String.valueOf(azureRegionId), volumeAspect.getDataCenter().getUuid());
        assertEquals(String.valueOf(volumeConnectedBusinessAccountId), volumeAspect.getBusinessAccount().getUuid());

        // check stats for volume
        java.util.List<StatApiDTO> stats = volumeAspect.getStats();
        assertEquals(3, stats.size());
        java.util.Optional<StatApiDTO> statApiDTOStorageAccess = stats.stream().filter(stat -> stat.getName() == "StorageAccess").findFirst();
        assertEquals(statApiDTOStorageAccess.get().getCapacity().getAvg().longValue(), storageAccessCapacity);
        java.util.Optional<StatApiDTO> statApiDTOStorageAmount = stats.stream().filter(stat -> stat.getName() == "StorageAmount").findFirst();
        assertEquals(storageAmountCapacityInMB / 1024F, statApiDTOStorageAmount.get().getCapacity().getAvg().longValue(), 0.00001);
        assertEquals(VirtualVolumeAspectMapper.CLOUD_STORAGE_AMOUNT_UNIT, statApiDTOStorageAmount.get().getUnits());

        assertEquals(snapshotId, volumeAspect.getSnapshotId());
        assertEquals(AttachmentState.ATTACHED.name(), volumeAspect.getAttachmentState());
        assertEquals("Enabled", volumeAspect.getEncryption());
        assertEquals(virtualVolumeDisplayName, volumeAspect.getDisplayName());

        assertEquals(String.valueOf(vmId1), volumeAspect.getAttachedVirtualMachine().getUuid());
    }

    /**
     * Test Azure Volume Mapping.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testMapVolumeAzure() throws Exception {
        storageTierSEApiDTO.setUuid(azureStorageTierId.toString());
        storageTierSEApiDTO.setDisplayName(azureStorageTierName);

        volumeConnectedBusinessAccount.setUuid(volumeConnectedBusinessAccountId.toString());
        volumeConnectedBusinessAccount.setDisplayName(volumeConnectedBusinessAccountDisplayName);

        doAnswer(invocation -> {
            SearchParameters param = invocation.getArgumentAt(0, SearchParameters.class);
            if (param.equals(SearchProtoUtil.neighborsOfType(azureVolumeId, TraversalDirection.PRODUCES, ApiEntityType.VIRTUAL_MACHINE))) {
                return ApiTestUtils.mockSearchFullReq(Lists.newArrayList(azureVm));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(azureVolumeId, TraversalDirection.CONNECTED_FROM, ApiEntityType.STORAGE_TIER))) {
                return ApiTestUtils.mockSearchFullReq(Lists.newArrayList(azureStorageTier));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(azureVolumeId, TraversalDirection.OWNED_BY, ApiEntityType.BUSINESS_ACCOUNT))) {
                return ApiTestUtils.mockSearchSEReq(Lists.newArrayList(volumeConnectedBusinessAccount));
            } else if (param.equals(SearchParameters.newBuilder().setStartingFilter(SearchProtoUtil.entityTypeFilter(ApiEntityType.REGION.apiStr())).build())) {
                return ApiTestUtils.mockSearchReq(Lists.newArrayList(region));
            } else {
                throw new IllegalArgumentException(param.toString());
            }
        }).when(repositoryApi).newSearchRequest(any(SearchParameters.class));

        final RepositoryApi.MultiEntityRequest storageTierRequest = ApiTestUtils.mockMultiSEReq(Lists.newArrayList(storageTierSEApiDTO));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(azureStorageTierId))).thenReturn(storageTierRequest);
        final RepositoryApi.SingleEntityRequest regionRequest = ApiTestUtils.mockSingleEntityRequest(azureRegion);
        when(repositoryApi.entityRequest(azureRegionId)).thenReturn(regionRequest);

        final List<MinimalEntity> storageTierMinEntities = ImmutableList.of(
                MinimalEntity.newBuilder().setOid(azureStorageTierId).setDisplayName(azureVolumeName).build()
        );
        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(storageTierMinEntities);
        when(repositoryApi.entitiesRequest(eq(Sets.newHashSet(azureStorageTierId)))).thenReturn(req);

        final VirtualDisksAspectApiDTO aspect = (VirtualDisksAspectApiDTO)volumeAspectMapper.mapEntitiesToAspect(
            Lists.newArrayList(getAzureVirtualVolume.get()));

        assertEquals(1, aspect.getVirtualDisks().size());

        // check the virtual disks for each file on the wasted storage
        VirtualDiskApiDTO volumeAspect = null;
        for (VirtualDiskApiDTO virtualDiskApiDTO : aspect.getVirtualDisks()) {
            if (virtualDiskApiDTO.getDisplayName().equals(azureVolumeName)) {
                volumeAspect = virtualDiskApiDTO;
            }
        }

        assertNotNull(volumeAspect);
        assertEquals(String.valueOf(azureVolumeId), volumeAspect.getUuid());
        assertEquals(String.valueOf(azureStorageTierId), volumeAspect.getProvider().getUuid());
        assertEquals(String.valueOf(azureRegionId), volumeAspect.getDataCenter().getUuid());
        assertEquals(String.valueOf(volumeConnectedBusinessAccountId), volumeAspect.getBusinessAccount().getUuid());

        // check stats for volume
        java.util.List<StatApiDTO> stats = volumeAspect.getStats();
        assertEquals(3, stats.size());
        java.util.Optional<StatApiDTO> statApiDTOStorageAccess = stats.stream().filter(stat -> stat.getName() == "StorageAccess").findFirst();
        assertEquals(statApiDTOStorageAccess.get().getCapacity().getAvg().longValue(), storageAccessCapacity);
        java.util.Optional<StatApiDTO> statApiDTOStorageAmount = stats.stream().filter(stat -> stat.getName() == "StorageAmount").findFirst();
        assertEquals(storageAmountCapacityInMB / 1024F, statApiDTOStorageAmount.get().getCapacity().getAvg().longValue(), 0.00001);
        assertEquals(VirtualVolumeAspectMapper.CLOUD_STORAGE_AMOUNT_UNIT, statApiDTOStorageAmount.get().getUnits());
        java.util.Optional<StatApiDTO> statApiDTOIoThroughput = stats.stream()
                .filter(stat -> CommodityTypeUnits.IO_THROUGHPUT.getMixedCase().equals(stat.getName()))
                .findFirst();
        assertTrue(statApiDTOIoThroughput.isPresent());
        assertEquals(azureVolumeIoThroughput,
                statApiDTOIoThroughput.get().getCapacity().getAvg().longValue(), 0.00001);

        assertEquals(snapshotId, volumeAspect.getSnapshotId());
        assertEquals(AttachmentState.ATTACHED.name(), volumeAspect.getAttachmentState());
        assertEquals("Enabled", volumeAspect.getEncryption());
        assertEquals(azureVolumeName, volumeAspect.getDisplayName());

        assertEquals(String.valueOf(azureVmId), volumeAspect.getAttachedVirtualMachine().getUuid());
    }

    /**
     * test MapOnVolume method for on-perm volume.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testMapVolumeOnPrem() throws Exception {
        storageTierSEApiDTO.setUuid(storageTierId1.toString());
        storageTierSEApiDTO.setDisplayName(storageDisplayName);

        volumeConnectedBusinessAccount.setUuid(volumeConnectedBusinessAccountId.toString());
        volumeConnectedBusinessAccount.setDisplayName(volumeConnectedBusinessAccountDisplayName);

        doAnswer(invocation -> {
            SearchParameters param = invocation.getArgumentAt(0, SearchParameters.class);
            if (param.equals(SearchProtoUtil.neighborsOfType(virtualVolumeId, TraversalDirection.PRODUCES, ApiEntityType.VIRTUAL_MACHINE))) {
                return ApiTestUtils.mockSearchFullReq(Lists.newArrayList(vm1));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(azureVolumeId, TraversalDirection.PRODUCES, ApiEntityType.VIRTUAL_MACHINE))) {
                return ApiTestUtils.mockSearchFullReq(Lists.newArrayList(azureVm));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(volumeConnectedZoneId, TraversalDirection.OWNED_BY, ApiEntityType.REGION))) {
                return ApiTestUtils.mockSearchReq(Lists.newArrayList(volumeConnectedZone));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(virtualVolumeId, TraversalDirection.OWNED_BY, ApiEntityType.BUSINESS_ACCOUNT))) {
                return ApiTestUtils.mockSearchSEReq(Lists.newArrayList(volumeConnectedBusinessAccount));
            } else if (param.equals(SearchParameters.newBuilder().setStartingFilter(SearchProtoUtil.entityTypeFilter(ApiEntityType.REGION.apiStr())).build())) {
                return ApiTestUtils.mockSearchReq(Lists.newArrayList(region));
            } else {
                throw new IllegalArgumentException(param.toString());
            }
        }).when(repositoryApi).newSearchRequest(any(SearchParameters.class));

        RepositoryApi.MultiEntityRequest storageTierRequest = ApiTestUtils.mockMultiSEReq(Lists.newArrayList(storageTierSEApiDTO));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(storageTierId1))).thenReturn(storageTierRequest);

        final List<MinimalEntity> storageTierMinEntities = ImmutableList.of(
                MinimalEntity.newBuilder().setOid(storageTierId1).setDisplayName(storageDisplayName).build()
        );
        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(storageTierMinEntities);
        when(repositoryApi.entitiesRequest(eq(Sets.newHashSet(storageTierId1)))).thenReturn(req);

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
        assertEquals(String.valueOf(azureRegionId), volumeAspect.getDataCenter().getUuid());
        assertEquals(String.valueOf(volumeConnectedBusinessAccountId), volumeAspect.getBusinessAccount().getUuid());

        // check stats for volume
        java.util.List<StatApiDTO> stats = volumeAspect.getStats();
        assertEquals(3, stats.size());
        java.util.Optional<StatApiDTO> statApiDTOStorageAccess = stats.stream().filter(stat -> stat.getName() == "StorageAccess").findFirst();
        assertEquals(statApiDTOStorageAccess.get().getCapacity().getAvg().longValue(), storageAccessCapacity);
        java.util.Optional<StatApiDTO> statApiDTOStorageAmount = stats.stream().filter(stat -> stat.getName() == "StorageAmount").findFirst();
        assertEquals(storageAmountCapacityInMB, statApiDTOStorageAmount.get().getCapacity().getAvg().longValue());
        assertEquals(CommodityTypeUnits.STORAGE_AMOUNT.getUnits(), statApiDTOStorageAmount.get().getUnits());

        assertEquals(snapshotId, volumeAspect.getSnapshotId());
        assertEquals(AttachmentState.ATTACHED.name(), volumeAspect.getAttachmentState());
        assertEquals("Enabled", volumeAspect.getEncryption());
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

    private StatsHistoryServiceMole statsHistoryServiceMole = spy(new StatsHistoryServiceMole());

    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(costServiceMole);

    @Rule
    public GrpcTestServer grpcTestHistoryServer = GrpcTestServer.newServer(statsHistoryServiceMole);

    @Before
    public void setup() {
        // init mapper
        CostServiceBlockingStub costRpc = CostServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
        final StatsHistoryServiceBlockingStub historyRpc =
                StatsHistoryServiceGrpc.newBlockingStub(grpcTestHistoryServer.getChannel());
        volumeAspectMapper = spy(new VirtualVolumeAspectMapper(costRpc, repositoryApi, historyRpc,
            5, 10));
    }

    @Test
    public void testMapStorageTiers() throws Exception {
        volumeConnectedBusinessAccount.setUuid(volumeConnectedBusinessAccountId.toString());
        volumeConnectedBusinessAccount.setDisplayName(volumeConnectedBusinessAccountDisplayName);

        doAnswer(invocation -> {
            SearchParameters param = invocation.getArgumentAt(0, SearchParameters.class);
           if (param.equals(SearchProtoUtil.neighborsOfType(storageTierId1, TraversalDirection.PRODUCES, ApiEntityType.VIRTUAL_VOLUME))) {
                return ApiTestUtils.mockSearchFullReq(Lists.newArrayList(volume1, volume2));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(azureStorageTierId, TraversalDirection.PRODUCES, ApiEntityType.VIRTUAL_VOLUME))) {
                return ApiTestUtils.mockSearchFullReq(Lists.newArrayList(azureVolume));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(storageTierId1, TraversalDirection.PRODUCES, ApiEntityType.VIRTUAL_MACHINE))) {
                return ApiTestUtils.mockSearchFullReq(Lists.newArrayList(vm1));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(azureStorageTierId, TraversalDirection.PRODUCES, ApiEntityType.VIRTUAL_MACHINE))) {
                return ApiTestUtils.mockSearchFullReq(Lists.newArrayList(azureVm));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(volumeId1, TraversalDirection.OWNED_BY, ApiEntityType.BUSINESS_ACCOUNT))) {
                return ApiTestUtils.mockSearchSEReq(Lists.newArrayList(volumeConnectedBusinessAccount));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(volumeId2, TraversalDirection.OWNED_BY, ApiEntityType.BUSINESS_ACCOUNT))) {
                return ApiTestUtils.mockSearchSEReq(Lists.newArrayList(volumeConnectedBusinessAccount));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(azureVolumeId, TraversalDirection.OWNED_BY, ApiEntityType.BUSINESS_ACCOUNT))) {
                return ApiTestUtils.mockSearchSEReq(Lists.newArrayList(volumeConnectedBusinessAccount));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(volumeId4, TraversalDirection.OWNED_BY, ApiEntityType.BUSINESS_ACCOUNT))) {
                return ApiTestUtils.mockSearchSEReq(Lists.newArrayList(volumeConnectedBusinessAccount));
            } else {
                throw new IllegalArgumentException(param.toString());
            }
        }).when(repositoryApi).newSearchRequest(any(SearchParameters.class));

        doAnswer(invocation -> ApiTestUtils.mockSearchIdReq(Collections.singleton(regionId1)))
            .when(repositoryApi).getRegion(Collections.singleton(storageTierId1));
        doAnswer(invocation -> ApiTestUtils.mockSearchIdReq(Collections.singleton(azureRegionId)))
            .when(repositoryApi).getRegion(Collections.singleton(azureStorageTierId));

        doAnswer(invocation -> ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(region1, azureRegion)))
            .when(repositoryApi).entitiesRequest(Sets.newHashSet(regionId1, azureRegionId));
        doAnswer(invocation -> ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(region1)))
            .when(repositoryApi).entitiesRequest(Sets.newHashSet(regionId1));
        doAnswer(invocation -> ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(azureRegion)))
            .when(repositoryApi).entitiesRequest(Sets.newHashSet(azureRegionId));

        VirtualDisksAspectApiDTO aspect = (VirtualDisksAspectApiDTO) volumeAspectMapper.mapEntitiesToAspect(
            Lists.newArrayList(storageTier1, azureStorageTier));

        assertEquals(3, aspect.getVirtualDisks().size());

        // check attached vm for volumes:
        // volume1 is attached to vm1, volume2 is unattached volume, azureVolume is attached to azureVm
        VirtualDiskApiDTO volumeAspect1 = null;
        VirtualDiskApiDTO volumeAspect2 = null;
        VirtualDiskApiDTO volumeAspect3 = null;
        for (VirtualDiskApiDTO virtualDiskApiDTO : aspect.getVirtualDisks()) {
            if (virtualDiskApiDTO.getDisplayName().equals(volumeName1)) {
                volumeAspect1 = virtualDiskApiDTO;
            } else if (virtualDiskApiDTO.getDisplayName().equals(volumeName2)) {
                volumeAspect2 = virtualDiskApiDTO;
            } else if (virtualDiskApiDTO.getDisplayName().equals(azureVolumeName)) {
                volumeAspect3 = virtualDiskApiDTO;
            }
        }
        assertNotNull(volumeAspect1);
        assertNotNull(volumeAspect2);
        assertNotNull(volumeAspect3);
        assertNotNull(volumeAspect1.getAttachedVirtualMachine());
        assertEquals(String.valueOf(vmId1), volumeAspect1.getAttachedVirtualMachine().getUuid());
        assertNull(volumeAspect2.getAttachedVirtualMachine());
        assertNotNull(volumeAspect3.getAttachedVirtualMachine());
        assertEquals(String.valueOf(azureVmId), volumeAspect3.getAttachedVirtualMachine().getUuid());

        // check datacenter
        assertEquals(String.valueOf(regionId1), volumeAspect1.getDataCenter().getUuid());
        assertEquals(String.valueOf(regionId1), volumeAspect2.getDataCenter().getUuid());
        assertEquals(String.valueOf(azureRegionId), volumeAspect3.getDataCenter().getUuid());

        // check storage tier
        assertEquals(storageTierName1, volumeAspect1.getTier());
        assertEquals(storageTierName1, volumeAspect2.getTier());
        assertEquals(azureStorageTierName, volumeAspect3.getTier());

        // check stats for different volumes
        assertEquals(3, volumeAspect1.getStats().size());
        volumeAspect1.getStats().forEach(statApiDTO -> {
            if (statApiDTO.getName().equals(CommodityTypeUnits.STORAGE_ACCESS.getMixedCase())) {
                assertEquals(50, statApiDTO.getValue(), 0);
                assertEquals(100, statApiDTO.getCapacity().getTotal(), 0);
                assertEquals(String.valueOf(storageTierId1), statApiDTO.getRelatedEntity().getUuid());
            } else if (statApiDTO.getName().equals(CommodityTypeUnits.STORAGE_AMOUNT.getMixedCase())) {
                assertEquals(100, statApiDTO.getValue(), 0);
                assertEquals(1000, statApiDTO.getCapacity().getTotal(), 0);
                assertEquals(String.valueOf(storageTierId1), statApiDTO.getRelatedEntity().getUuid());
            } else if (statApiDTO.getName().equals(CommodityTypeUnits.IO_THROUGHPUT.getMixedCase())) {
                assertEquals(0, statApiDTO.getCapacity().getTotal(), 0);
                assertEquals(String.valueOf(storageTierId1), statApiDTO.getRelatedEntity().getUuid());
            }
        });

        assertEquals(3, volumeAspect2.getStats().size());
        volumeAspect2.getStats().forEach(statApiDTO -> {
            if (statApiDTO.getName().equals(CommodityTypeUnits.STORAGE_ACCESS.getMixedCase())) {
                assertEquals(0, statApiDTO.getValue(), 0);
                assertEquals(200, statApiDTO.getCapacity().getTotal(), 0);
                assertEquals(String.valueOf(storageTierId1), statApiDTO.getRelatedEntity().getUuid());
            } else if (statApiDTO.getName().equals(CommodityTypeUnits.STORAGE_AMOUNT.getMixedCase())) {
                assertEquals(0, statApiDTO.getValue(), 0);
                assertEquals(2000, statApiDTO.getCapacity().getTotal(), 0);
                assertEquals(String.valueOf(storageTierId1), statApiDTO.getRelatedEntity().getUuid());
            } else if (statApiDTO.getName().equals(CommodityTypeUnits.IO_THROUGHPUT.getMixedCase())) {
                assertEquals(0, statApiDTO.getValue(), 0);
                assertEquals(0, statApiDTO.getCapacity().getTotal(), 0);
                assertEquals(String.valueOf(storageTierId1), statApiDTO.getRelatedEntity().getUuid());
            }
        });

        assertEquals(3, volumeAspect3.getStats().size());
        volumeAspect3.getStats().forEach(statApiDTO -> {
            if (statApiDTO.getName().equals(CommodityTypeUnits.STORAGE_ACCESS.getMixedCase())) {
                assertEquals(150, statApiDTO.getValue(), 0);
                assertEquals(300, statApiDTO.getCapacity().getTotal(), 0);
                assertEquals(String.valueOf(azureStorageTierId), statApiDTO.getRelatedEntity().getUuid());
            } else if (statApiDTO.getName().equals(CommodityTypeUnits.STORAGE_AMOUNT.getMixedCase())) {
                assertEquals(500, statApiDTO.getValue(), 0);
                assertEquals(3000, statApiDTO.getCapacity().getTotal(), 0);
                assertEquals(String.valueOf(azureStorageTierId), statApiDTO.getRelatedEntity().getUuid());
            } else if (statApiDTO.getName().equals(CommodityTypeUnits.IO_THROUGHPUT.getMixedCase())) {
                assertEquals(0, statApiDTO.getValue(), 0);
                assertEquals(azureVolumeIoThroughput, statApiDTO.getCapacity().getTotal(), 0);
                assertEquals(String.valueOf(azureStorageTierId), statApiDTO.getRelatedEntity().getUuid());
            }
        });
    }

    @Test
    public void testMapOneToManyAspectsForStorageTier() {
        // ARRANGE
        final VirtualDiskApiDTO disk1 = new VirtualDiskApiDTO();
        final ServiceEntityApiDTO tier1 = new ServiceEntityApiDTO();
        tier1.setUuid(String.valueOf(1));
        disk1.setProvider(tier1);

        final VirtualDiskApiDTO disk2 = new VirtualDiskApiDTO();
        final ServiceEntityApiDTO tier2 = new ServiceEntityApiDTO();
        tier2.setUuid(String.valueOf(2));
        disk2.setProvider(tier2);

        final VirtualDisksAspectApiDTO aspect = new VirtualDisksAspectApiDTO();
        aspect.setVirtualDisks(ImmutableList.of(disk1, disk2));

        // ACT
        final Map<String, EntityAspect> aspectMap = volumeAspectMapper.mapOneToManyAspects(
                Collections.singletonList(storageTier1), aspect);

        // ASSERT
        assertNotNull(aspectMap);
        assertEquals(2, aspectMap.size());

        final EntityAspect aspect1 = aspectMap.get(String.valueOf(1));
        assertTrue(aspect1 instanceof VirtualDisksAspectApiDTO);
        final VirtualDisksAspectApiDTO vdAspect1 = (VirtualDisksAspectApiDTO)aspect1;
        assertEquals(1, vdAspect1.getVirtualDisks().size());
        assertEquals(String.valueOf(1),
                vdAspect1.getVirtualDisks().iterator().next().getProvider().getUuid());

        final EntityAspect aspect2 = aspectMap.get(String.valueOf(2));
        assertTrue(aspect2 instanceof VirtualDisksAspectApiDTO);
        final VirtualDisksAspectApiDTO vdAspect2 = (VirtualDisksAspectApiDTO)aspect2;
        assertEquals(1, vdAspect2.getVirtualDisks().size());
        assertEquals(String.valueOf(2),
                vdAspect2.getVirtualDisks().iterator().next().getProvider().getUuid());
    }

    /**
     * Test that if the current time is less than the snapshot time returned by
     * HistoryRpcService, then numDaysUnattached and lastAttachedVm are not populated.
     */
    @Test
    public void testUnattachedVolumeInvalidSnapshotTime() throws Exception {
        // given
        final TopologyEntityDTO unattachedVolume = createUnattachedVolume();
        when(statsHistoryServiceMole.getMostRecentStat(any()))
                .thenReturn(createMostRecentStatsResponse("vm-123",
                        System.currentTimeMillis() + TimeUnit.DAYS.toMillis(3),
                        StatHistoricalEpoch.HOUR));
        stubRepositoryApi();
        // when
        final VirtualDisksAspectApiDTO aspect = (VirtualDisksAspectApiDTO)volumeAspectMapper
                .mapEntitiesToAspect(Collections.singletonList(unattachedVolume));

        // then
        Assert.assertNotNull(aspect);
        Assert.assertFalse(aspect.getVirtualDisks().isEmpty());
        final VirtualDiskApiDTO virtualDiskApiDTO = aspect.getVirtualDisks().iterator().next();
        Assert.assertNull(virtualDiskApiDTO.getNumDaysUnattached());
        Assert.assertNull(virtualDiskApiDTO.getLastAttachedVm());
    }

    /**
     * Test that numDaysUnattached and lastAttachedVm are being set correctly for the following
     * valid conditions:
     * 1. HistoryRpcService response contains the epoch and snapshot time values.
     * 2. The snapshot time returned by historyRpcService is a date in the past (< current time).
     *
     * @throws Exception on exception occurred
     */
    @Test
    public void testUnattachedVolumePropertiesValidData() throws Exception {
        // given
        final String lastAttachedVmName = "vm-11111";
        final TopologyEntityDTO unattachedVolume = createUnattachedVolume();
        when(statsHistoryServiceMole.getMostRecentStat(any()))
                .thenReturn(createMostRecentStatsResponse(lastAttachedVmName,
                        System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3),
                        StatHistoricalEpoch.HOUR));
        stubRepositoryApi();
        // when
        final VirtualDisksAspectApiDTO aspect = (VirtualDisksAspectApiDTO)volumeAspectMapper
                .mapEntitiesToAspect(Collections.singletonList(unattachedVolume));

        // then
        Assert.assertNotNull(aspect);
        Assert.assertFalse(aspect.getVirtualDisks().isEmpty());
        final VirtualDiskApiDTO virtualDiskApiDTO = aspect.getVirtualDisks().iterator().next();
        Assert.assertEquals(lastAttachedVmName, virtualDiskApiDTO.getLastAttachedVm());
        Assert.assertEquals("3 days", virtualDiskApiDTO.getNumDaysUnattached());
    }

    /**
     * Test that if the HistoryRpcService response is empty, then the numDaysUnattached and
     * lastAttachedVm fields are not set.
     *
     * @throws Exception on exception occurred
     */
    @Test
    public void testUnattachedVolumeNoHistory() throws Exception {
        // given
        final TopologyEntityDTO unattachedVolume = createUnattachedVolume();
        when(statsHistoryServiceMole.getMostRecentStat(any()))
                .thenReturn(createEmptyMostRecentStatsResponse());
        stubRepositoryApi();
        // when
        final VirtualDisksAspectApiDTO aspect = (VirtualDisksAspectApiDTO)volumeAspectMapper
                .mapEntitiesToAspect(Collections.singletonList(unattachedVolume));

        // then
        Assert.assertNotNull(aspect);
        Assert.assertFalse(aspect.getVirtualDisks().isEmpty());
        final VirtualDiskApiDTO virtualDiskApiDTO = aspect.getVirtualDisks().iterator().next();
        Assert.assertNull(virtualDiskApiDTO.getLastAttachedVm());
        Assert.assertNull(virtualDiskApiDTO.getNumDaysUnattached());
    }

    /**
     * Test that if the HistoryRpcService response contains the Month StatHistoricalEpoch, then the
     * numDaysUnattached has a '+' suffix.
     *
     * @throws Exception on exception occurred
     */
    @Test
    public void testUnattachedVolumeHistoryMonthEpoch() throws Exception {
        // given
        final TopologyEntityDTO unattachedVolume = createUnattachedVolume();
        when(statsHistoryServiceMole.getMostRecentStat(any()))
                .thenReturn(createMostRecentStatsResponse("vm-1111", 123L,
                        StatHistoricalEpoch.MONTH));
        stubRepositoryApi();
        // when
        final VirtualDisksAspectApiDTO aspect = (VirtualDisksAspectApiDTO)volumeAspectMapper
                .mapEntitiesToAspect(Collections.singletonList(unattachedVolume));

        // then
        Assert.assertNotNull(aspect);
        final VirtualDiskApiDTO virtualDiskApiDTO = aspect.getVirtualDisks().iterator().next();
        assertTrue(virtualDiskApiDTO.getNumDaysUnattached().endsWith("+ days"));
    }

    /**
     * Test that if the HistoryRpcService response does not have contain entity display name, then
     * lastAttachedVm is not set.
     *
     * @throws Exception on exception occurred
     */
    @Test
    public void testUnattachedVolumeInfoNoVmInfo() throws Exception {
        // given
        final TopologyEntityDTO unattachedVolume = createUnattachedVolume();
        when(statsHistoryServiceMole.getMostRecentStat(any()))
                .thenReturn(createMostRecentStatsResponse(null, 123L,
                        StatHistoricalEpoch.MONTH));
        stubRepositoryApi();
        // when
        final VirtualDisksAspectApiDTO aspect = (VirtualDisksAspectApiDTO)volumeAspectMapper
                .mapEntitiesToAspect(Collections.singletonList(unattachedVolume));

        // then
        Assert.assertNotNull(aspect);
        final VirtualDiskApiDTO virtualDiskApiDTO = aspect.getVirtualDisks().iterator().next();
        Assert.assertNull(virtualDiskApiDTO.getLastAttachedVm());
    }

    /**
     * Test that the attachment history is not retrieved for bulk calls to mapEntitiesToAspect.
     *
     * @throws Exception on exception occurred
     */
    @Test
    public void testUnattachedVolumeHistoryRpcSkippedForBulk() throws Exception {
        // given
        final TopologyEntityDTO unattachedVolume = createUnattachedVolume();
        when(statsHistoryServiceMole.getMostRecentStat(any()))
                .thenReturn(createMostRecentStatsResponse("vm-1111", 123L,
                        StatHistoricalEpoch.DAY));
        stubRepositoryApi();

        // when
        final VirtualDisksAspectApiDTO aspect = (VirtualDisksAspectApiDTO)volumeAspectMapper
                .mapEntitiesToAspect(Arrays.asList(unattachedVolume, unattachedVolume));

        // then
        verify(statsHistoryServiceMole, never()).getMostRecentStat(any());
        Assert.assertNotNull(aspect);
        final VirtualDiskApiDTO virtualDiskApiDTO = aspect.getVirtualDisks().iterator().next();
        Assert.assertNull(virtualDiskApiDTO.getNumDaysUnattached());
        Assert.assertNull(virtualDiskApiDTO.getLastAttachedVm());
    }

    private GetMostRecentStatResponse createEmptyMostRecentStatsResponse() {
        return GetMostRecentStatResponse.newBuilder().build();
    }

    private GetMostRecentStatResponse createMostRecentStatsResponse(final String vmDisplayName,
                                                                    final long snapshotTime,
                                                                    final StatHistoricalEpoch
                                                                            epoch) {
        final GetMostRecentStatResponse.Builder response = GetMostRecentStatResponse.newBuilder()
                .setSnapshotDate(snapshotTime)
                .setEpoch(epoch);
        if (vmDisplayName != null) {
            response.setEntityDisplayName(vmDisplayName);
        }
        return response.build();
    }

    private TopologyEntityDTO createUnattachedVolume() {
        final long volId = 77777L;
        return TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                .setOid(volId)
                .setDisplayName("random_vol")
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setVirtualVolume(VirtualVolumeInfo.newBuilder()
                                .setAttachmentState(AttachmentState.UNATTACHED))
                        .build())
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderEntityType(EntityType.STORAGE_TIER_VALUE)
                        .setProviderId(storageTierId1)
                        .build())
                .build();
    }

    private void stubRepositoryApi() throws Exception {
        final SearchRequest searchRequest = mock(SearchRequest.class);
        when(searchRequest.getFullEntities()).thenReturn(Stream.of()).thenReturn(Stream.of());
        when(searchRequest.getEntities()).thenReturn(Stream.of()).thenReturn(Stream.of());
        when(repositoryApi.newSearchRequest(any())).thenReturn(searchRequest);
        final MultiEntityRequest multiEntityRequest = mock(MultiEntityRequest.class);
        when(multiEntityRequest.getSEMap()).thenReturn(Collections.emptyMap())
                .thenReturn(Collections.emptyMap());
        when(repositoryApi.entitiesRequest(any())).thenReturn(multiEntityRequest);

        final List<MinimalEntity> storageTierMinEntities = ImmutableList.of(
                MinimalEntity.newBuilder().setOid(77777L).setDisplayName("random_vol").build()
        );
        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(storageTierMinEntities);
        when(repositoryApi.entitiesRequest(eq(Sets.newHashSet(storageTierId1)))).thenReturn(req);
    }

    @Test
    public void testMapStorage() throws Exception {
        volumeConnectedBusinessAccount.setUuid(volumeConnectedBusinessAccountId.toString());
        volumeConnectedBusinessAccount.setDisplayName(volumeConnectedBusinessAccountDisplayName);

        doAnswer(invocation -> {
            SearchParameters param = invocation.getArgumentAt(0, SearchParameters.class);
            if (param.equals(SearchProtoUtil.neighborsOfType(storageId, TraversalDirection.CONNECTED_FROM, ApiEntityType.VIRTUAL_VOLUME))) {
                return ApiTestUtils.mockSearchFullReq(Lists.newArrayList(volume4, wastedFilesVolume));
            } else if (param.equals(SearchProtoUtil.neighborsOfType(volumeId4, TraversalDirection.CONNECTED_FROM, ApiEntityType.VIRTUAL_MACHINE))) {
                return ApiTestUtils.mockSearchCountReq(1);
            } else if (param.equals(SearchProtoUtil.neighborsOfType(wastedVolumeId1, TraversalDirection.CONNECTED_FROM, ApiEntityType.VIRTUAL_MACHINE))) {
                return ApiTestUtils.mockSearchCountReq(0);
            } else if (param.equals(SearchProtoUtil.neighborsOfType(volumeId4, TraversalDirection.CONNECTED_FROM, ApiEntityType.BUSINESS_ACCOUNT))) {
                return ApiTestUtils.mockSearchSEReq(Lists.newArrayList(volumeConnectedBusinessAccount));
            } else {
                throw new IllegalArgumentException(param.toString());
            }
        }).when(repositoryApi).newSearchRequest(any(SearchParameters.class));

        verify(repositoryApi, never()).newSearchRequest(SearchProtoUtil.neighborsOfType(storageId2, TraversalDirection.CONNECTED_FROM, ApiEntityType.VIRTUAL_VOLUME));
        verify(repositoryApi, never()).newSearchRequest(SearchProtoUtil.neighborsOfType(storageId, TraversalDirection.CONNECTED_FROM, ApiEntityType.VIRTUAL_VOLUME));

        MultiEntityRequest req = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList(volume4, wastedFilesVolume));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(volumeId4, wastedVolumeId1))).thenReturn(req);

        VirtualDisksAspectApiDTO aspect = (VirtualDisksAspectApiDTO) volumeAspectMapper.mapEntitiesToAspect(
            Lists.newArrayList(storage1, storage2));

        // if ignoreWastedFiles for storage2 was honored, we should never search for storage2
        verify(repositoryApi, never()).newSearchRequest(SearchProtoUtil.neighborsOfType(storageId2,
            TraversalDirection.CONNECTED_FROM, ApiEntityType.VIRTUAL_VOLUME));

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
