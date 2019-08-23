package com.vmturbo.api.component.external.api.mapper.aspect;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.STEntityAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.VirtualDiskApiDTO;
import com.vmturbo.api.dto.entityaspect.VirtualDisksAspectApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord.StatValue;
import com.vmturbo.common.protobuf.cost.Cost.EntityFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsRequest;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.components.common.mapping.EnvironmentTypeMapper;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.VirtualVolumeFileDescriptor;

public class VirtualVolumeAspectMapper implements IAspectMapper {

    private static final Logger logger = LogManager.getLogger();
    // use for storage tier value for non cloud entities
    private static final String UNKNOWN = "unknown";

    private final CostServiceBlockingStub costServiceRpc;

    private final RepositoryApi repositoryApi;

    public VirtualVolumeAspectMapper(@Nonnull final CostServiceBlockingStub costServiceRpc,
                                     @Nonnull final RepositoryApi repositoryApi) {
        this.costServiceRpc = costServiceRpc;
        this.repositoryApi = repositoryApi;
    }

    @Override
    public boolean supportsGroup() {
        return true;
    }

    @Override
    public @Nonnull String getAspectName() {
        return StringConstants.VIRTUAL_VOLUME_ASPECT_NAME;
    }

    @Override
    public EntityAspect mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
        STEntityAspectApiDTO aspect = new STEntityAspectApiDTO();
        aspect.setDisplayName(entity.getDisplayName());
        aspect.setName(String.valueOf(entity.getOid()));
        return mapEntitiesToAspect(Lists.newArrayList(entity));
    }

    @Override
    public EntityAspect mapEntitiesToAspect(@Nonnull List<TopologyEntityDTO> entities) {
        if (entities.size() == 0) {
            return null;
        }

        final int entityType = entities.get(0).getEntityType();
        if (entityType == EntityType.STORAGE_TIER_VALUE) {
            return mapStorageTiers(entities);
        } else if (entityType == EntityType.VIRTUAL_MACHINE_VALUE) {
            return mapVirtualMachines(entities);
        } else if (entityType == EntityType.STORAGE_VALUE) {
            return mapStorages(entities);
        }
        return null;
    }

    /**
     * Create VirtualVolumeAspect for volumes related to a list of storage tiers.
     */
    private EntityAspect mapStorageTiers(@Nonnull List<TopologyEntityDTO> entities) {
        final Map<Long, TopologyEntityDTO> storageTierById = Maps.newHashMap();
        final Set<Long> storageTierIds = Sets.newHashSet();
        final Set<Long> regionIds = Sets.newHashSet();

        for (TopologyEntityDTO storageTier : entities) {
            storageTierById.put(storageTier.getOid(), storageTier);
            storageTierIds.add(storageTier.getOid());
            regionIds.addAll(storageTier.getConnectedEntityListList().stream()
                .filter(connectedEntity -> connectedEntity.getConnectedEntityType() ==
                        EntityType.REGION_VALUE)
                .map(ConnectedEntity::getConnectedEntityId)
                .collect(Collectors.toSet()));
        }

        // entities are a list of storage tiers, find all volumes connected to these storage tiers
        List<TopologyEntityDTO> volumes = storageTierIds.stream()
            .flatMap(id -> repositoryApi.newSearchRequest(
                SearchProtoUtil.neighborsOfType(id, TraversalDirection.CONNECTED_FROM, UIEntityType.VIRTUAL_VOLUME))
                    .getFullEntities())
            .collect(Collectors.toList());

        // find all regions for the given storage tiers
        final Map<Long, ApiPartialEntity> regionByZoneId = Maps.newHashMap();
        final Map<Long, ApiPartialEntity> regionById = Maps.newHashMap();
        repositoryApi.entitiesRequest(regionIds).getEntities().forEach(region -> {
            regionById.put(region.getOid(), region);
            region.getConnectedToList().forEach(connectedEntity -> {
                if (connectedEntity.getEntityType() == EntityType.AVAILABILITY_ZONE_VALUE) {
                    regionByZoneId.put(connectedEntity.getOid(), region);
                }
            });
        });

        // create mapping from volume id to storage tier and region
        final Map<Long, ServiceEntityApiDTO> storageTierByVolumeId = Maps.newHashMap();
        final Map<Long, ApiPartialEntity> regionByVolumeId = Maps.newHashMap();
        volumes.forEach(volume -> {
            Long volumeId = volume.getOid();
            volume.getConnectedEntityListList().forEach(connectedEntity -> {
                int connectedEntityType = connectedEntity.getConnectedEntityType();
                Long connectedEntityId = connectedEntity.getConnectedEntityId();
                if (connectedEntityType == EntityType.STORAGE_TIER_VALUE) {
                    storageTierByVolumeId.put(volumeId, ServiceEntityMapper.toBasicEntity(storageTierById.get(connectedEntityId)));
                } else if (connectedEntityType == EntityType.AVAILABILITY_ZONE_VALUE) {
                    // if zone exists, find region based on zone id (for aws, volume is connected to az)
                    regionByVolumeId.put(volumeId, regionByZoneId.get(connectedEntityId));
                } else if (connectedEntityType == EntityType.REGION_VALUE) {
                    // if no zone, get region directly (for azure, volume is connected to region)
                    regionByVolumeId.put(volumeId, regionById.get(connectedEntityId));
                }
            });
        });

        // get cost stats for all volumes
        Map<Long, StatApiDTO> volumeCostStatById = getVolumeCostStats(volumes, null);

        // get all VMs consuming given storage tiers
        List<TopologyEntityDTO> vms = storageTierIds.stream()
            .flatMap(id -> repositoryApi.newSearchRequest(
                SearchProtoUtil.neighborsOfType(id, TraversalDirection.PRODUCES, UIEntityType.VIRTUAL_MACHINE)).getFullEntities())
            .collect(Collectors.toList());

        final Map<Long, TopologyEntityDTO> vmByVolumeId = Maps.newHashMap();
        vms.forEach(vm ->
            vm.getConnectedEntityListList().forEach(connectedEntity -> {
                if (connectedEntity.getConnectedEntityType() == EntityType.VIRTUAL_VOLUME_VALUE) {
                    vmByVolumeId.put(connectedEntity.getConnectedEntityId(), vm);
                }
            })
        );

        final List<VirtualDiskApiDTO> virtualDisks = volumes.stream()
            .map(volume -> convert(volume, vmByVolumeId, storageTierByVolumeId,
                    regionByVolumeId, volumeCostStatById))
            .collect(Collectors.toList());

        if (virtualDisks.isEmpty()) {
            return null;
        }

        final VirtualDisksAspectApiDTO aspect = new VirtualDisksAspectApiDTO();
        aspect.setVirtualDisks(virtualDisks);
        return aspect;
    }

    /**
     * Create VirtualVolumeAspect for volumes related to a list of virtual machines.
     */
    @Nullable
    private EntityAspect mapVirtualMachines(@Nonnull List<TopologyEntityDTO> vmDTOs) {
        Map<Long, List<VirtualDiskApiDTO>> volumeAspectsByVMId = mapVirtualMachines(vmDTOs, null);
        if (volumeAspectsByVMId.isEmpty()) {
            return null;
        }
        final VirtualDisksAspectApiDTO aspect = new VirtualDisksAspectApiDTO();
        aspect.setVirtualDisks(volumeAspectsByVMId.values().stream()
            .flatMap(List::stream).collect(Collectors.toList()));
        return aspect;
    }

    private Map<Long, List<VirtualDiskApiDTO>> mapVirtualMachines(@Nonnull List<TopologyEntityDTO> vms,
                                                                  @Nullable Long topologyContextId) {
        // mapping from volume id to vm
        final Map<Long, TopologyEntityDTO> vmByVolumeId = Maps.newHashMap();
        // mapping from zone id to region
        final Map<Long, ApiPartialEntity> regionByZoneId = Maps.newHashMap();
        // mapping from volume id to storage tier
        final Map<Long, ServiceEntityApiDTO> storageTierByVolumeId = Maps.newHashMap();
        // mapping from volume id to region entity
        final Map<Long, ApiPartialEntity> regionByVolumeId = Maps.newHashMap();

        // mapping from volume id to vm
        vms.forEach(vm ->
            vm.getConnectedEntityListList().forEach(connectedEntity -> {
                if (connectedEntity.getConnectedEntityType() == EntityType.VIRTUAL_VOLUME_VALUE) {
                    vmByVolumeId.put(connectedEntity.getConnectedEntityId(), vm);
                }
            })
        );

        // fetch all the regions and create mapping from region id to region
        final Map<Long, ApiPartialEntity> regionById = fetchRegions();
        regionById.values().forEach(region ->
            region.getConnectedToList().stream()
                .filter(connectedEntity -> connectedEntity.getEntityType() == EntityType.AVAILABILITY_ZONE_VALUE)
                .forEach(connectedEntity -> regionByZoneId.put(connectedEntity.getOid(), region))
        );

        // fetch all the storage tiers and create mapping from tier id to tier
        final Map<Long, ServiceEntityApiDTO> storageTierById = fetchStorageTiers();

        // fetch all related volumes
        final List<TopologyEntityDTO> volumes = repositoryApi.entitiesRequest(vmByVolumeId.keySet())
            .contextId(topologyContextId)
            .getFullEntities()
            .collect(Collectors.toList());
        volumes.forEach(volume -> {
            volume.getConnectedEntityListList().forEach(connectedEntity -> {
                int connectedEntityType = connectedEntity.getConnectedEntityType();
                Long connectedEntityId = connectedEntity.getConnectedEntityId();
                if (connectedEntityType == EntityType.REGION_VALUE) {
                    // volume connected to region (azure)
                    regionByVolumeId.put(volume.getOid(), regionById.get(connectedEntityId));
                } else if (connectedEntityType == EntityType.AVAILABILITY_ZONE_VALUE) {
                    // volume connected to zone (aws)
                    regionByVolumeId.put(volume.getOid(), regionByZoneId.get(connectedEntityId));
                } else if (connectedEntityType == EntityType.STORAGE_TIER_VALUE) {
                    storageTierByVolumeId.put(volume.getOid(), storageTierById.get(connectedEntityId));
                }
            });
        });

        // get cost stats for all volumes
        final Map<Long, StatApiDTO> volumeCostStatById = getVolumeCostStats(volumes, topologyContextId);

        // convert to VirtualDiskApiDTO
        final Map<Long, List<VirtualDiskApiDTO>> volumeAspectsByVMId = new HashMap<>();
        volumes.forEach(volume -> {
            VirtualDiskApiDTO volumeAspect = convert(volume, vmByVolumeId, storageTierByVolumeId,
                regionByVolumeId, volumeCostStatById);
            Long vmId = vmByVolumeId.get(volume.getOid()).getOid();
            volumeAspectsByVMId.computeIfAbsent(vmId, k -> Lists.newArrayList()).add(volumeAspect);
        });
        return volumeAspectsByVMId;
    }

    public Map<Long, List<VirtualDiskApiDTO>> mapVirtualMachines(@Nonnull Set<Long> vmIds,
                                                                 final long topologyContextId) {
        // fetch vms from given topology, for example: a plan projected topology
        final List<TopologyEntityDTO> vms = repositoryApi.entitiesRequest(vmIds)
            .contextId(topologyContextId)
            .getFullEntities()
            .collect(Collectors.toList());
        return mapVirtualMachines(vms, topologyContextId);
    }

    /**
     * Create the VirtualVolumeAspect for the wasted files associated with a list of storages.
     *
     * @param storages list of storages to base the VirtualVolumeAspect on.
     * @return VirtualVolumeAspect based on wasted files volumes associated with the list of
     * storages.
     */
    private EntityAspect mapStorages(@Nonnull List<TopologyEntityDTO> storages) {
        List<VirtualDiskApiDTO> virtualDisks = new ArrayList<>();
        // for each storage, find the wasted file virtual volume by getting all volumes in the
        // storage's ConnectedFrom and keeping only the virtual volume that has 0 VirtualMachines
        // in its ConnectedFrom relationship.  Get the files on each wasted volume and convert them
        // into VirtualDiskApiDTOs.  Take the list of VirtualDiskApiDTOs and stick them into the
        // EntityAspect we're returning.
        storages.forEach(storage ->
            repositoryApi.newSearchRequest(SearchProtoUtil.neighborsOfType(
                    storage.getOid(), TraversalDirection.CONNECTED_FROM, UIEntityType.VIRTUAL_VOLUME)).getFullEntities()
                .filter(topoEntity ->
                    repositoryApi.newSearchRequest(SearchProtoUtil.neighborsOfType(
                        topoEntity.getOid(),
                        TraversalDirection.CONNECTED_FROM,
                        UIEntityType.VIRTUAL_MACHINE)).count() == 0)
                .filter(TopologyEntityDTO::hasTypeSpecificInfo)
                .map(TopologyEntityDTO::getTypeSpecificInfo)
                .filter(TypeSpecificInfo::hasVirtualVolume)
                .map(TypeSpecificInfo::getVirtualVolume)
                .flatMap(virtualVolInfo -> virtualVolInfo.getFilesList().stream())
                .map(fileDescriptor -> fileToDiskApiDto(storage, fileDescriptor))
                .forEach(virtualDisks::add));
        final VirtualDisksAspectApiDTO aspect = new VirtualDisksAspectApiDTO();
        aspect.setVirtualDisks(virtualDisks);
        return aspect;
    }

    /**
     * Retrieve cost for volumes and create cost StatApiDTO for each volume.
     *
     * @param volumes list of volumes to get cost for
     * @return map of cost StatApiDTO for each volume id
     */
    private Map<Long, StatApiDTO> getVolumeCostStats(@Nonnull List<TopologyEntityDTO> volumes,
                                                     @Nullable Long topologyContextId) {
        final Set<Long> cloudVolumeIds = volumes.stream()
            .filter(IAspectMapper::isCloudEntity)
            .map(TopologyEntityDTO::getOid)
            .collect(Collectors.toSet());

        if (cloudVolumeIds.isEmpty()) {
            return Collections.emptyMap();
        }

        final GetCloudCostStatsRequest.Builder request = GetCloudCostStatsRequest.newBuilder()
            .setEntityFilter(EntityFilter.newBuilder()
                .addAllEntityId(cloudVolumeIds)
                .build());
        if (topologyContextId != null) {
            // get projected cost
            long now = Instant.now().toEpochMilli();
            request.setStartDate(now);
            request.setEndDate(now);
        }

        try {
            final List<CloudCostStatRecord> cloudStatRecords = costServiceRpc.getCloudCostStats(request.build())
                .getCloudStatRecordList();
            return cloudStatRecords.stream()
                .flatMap(cloudStatRecord -> cloudStatRecord.getStatRecordsList().stream())
                .collect(Collectors.toMap(StatRecord::getAssociatedEntityId, record -> {
                    // cost stats
                    StatApiDTO costStat = new StatApiDTO();
                    costStat.setName(StringConstants.COST_PRICE);
                    costStat.setUnits(record.getUnits());

                    StatValueApiDTO valueDTO = new StatValueApiDTO();
                    StatValue value = record.getValues();
                    valueDTO.setAvg(value.getAvg());
                    valueDTO.setMin(value.getMin());
                    valueDTO.setMax(value.getMax());
                    valueDTO.setTotal(value.getTotal());
                    costStat.setValues(valueDTO);
                    costStat.setValue(value.getAvg());

                    return costStat;
                }));
        } catch (StatusRuntimeException e) {
            logger.error("Error when getting cost for volumes: ", e);
            return Collections.emptyMap();
        }
    }

    /**
     * Fetch all the regions and create mapping from region id to region. It only fetch from real
     * time topology since it should be same in plan topology.
     */
    private Map<Long, ApiPartialEntity> fetchRegions() {
        return repositoryApi.newSearchRequest(SearchParameters.newBuilder()
                .setStartingFilter(SearchProtoUtil.entityTypeFilter(UIEntityType.REGION.apiStr()))
                .build())
            .getEntities()
            .collect(Collectors.toMap(ApiPartialEntity::getOid, Function.identity()));
    }

    /**
     * Fetch all the storage tiers and create mapping from tier id to tier. It only fetch from real
     * time topology since it should be same in plan topology.
     */
    private Map<Long, ServiceEntityApiDTO> fetchStorageTiers() {
        return repositoryApi.newSearchRequest(SearchParameters.newBuilder()
                .setStartingFilter(SearchProtoUtil.entityTypeFilter(UIEntityType.STORAGE_TIER.apiStr()))
                .build())
            .getMinimalEntities()
            .collect(Collectors.toMap(MinimalEntity::getOid, ServiceEntityMapper::toBasicEntity));
    }

    /**
     * Convert a {@link VirtualVolumeFileDescriptor} for a file into a {@link VirtualDiskApiDTO}.
     *
     * @param storage The storage on which the file resides.
     * @param file The file information.
     * @return VirtualDiskApiDTO with the basic information for the file and stats giving the size
     * of the file, the path, and the last modified time.
     */
    private VirtualDiskApiDTO fileToDiskApiDto(TopologyEntityDTO storage,
                                               VirtualVolumeFileDescriptor file) {
        VirtualDiskApiDTO retVal = new VirtualDiskApiDTO();
        retVal.setDisplayName(file.getPath());
        retVal.setEnvironmentType(EnvironmentType.ONPREM);
        retVal.setProvider(ServiceEntityMapper.toBasicEntity(storage));
        retVal.setLastModified(file.getModificationTimeMs());
        retVal.setTier(UNKNOWN);
        // storage amount stats

        retVal.setStats(Collections.singletonList(createStatApiDTO(
            CommodityTypeUnits.STORAGE_AMOUNT.getMixedCase(),
            CommodityTypeUnits.STORAGE_AMOUNT.getUnits(), file.getSizeKb() / 1024F,
            file.getSizeKb() / 1024F, ServiceEntityMapper.toBasicEntity(storage), file.getPath())));
        return retVal;
    }

    /**
     * Convert the given volume into VirtualDiskApiDTO.
     *
     * @param volume the volume entity to convert
     * @param vmByVolumeId mapping from volume id to vm
     * @param storageTierByVolumeId mapping from volume id to storage tier
     * @param regionByVolumeId mapping from volume id to region
     * @param volumeCostStatById mapping from volume id to its cost stat
     * @return VirtualDiskApiDTO representing the volume
     */
    private VirtualDiskApiDTO convert(@Nonnull TopologyEntityDTO volume,
            @Nonnull Map<Long, TopologyEntityDTO> vmByVolumeId,
            @Nonnull Map<Long, ServiceEntityApiDTO> storageTierByVolumeId,
            @Nonnull Map<Long, ApiPartialEntity> regionByVolumeId,
            @Nonnull Map<Long, StatApiDTO> volumeCostStatById) {
        Long volumeId = volume.getOid();
        // the VirtualDiskApiDTO to return
        VirtualDiskApiDTO virtualDiskApiDTO = new VirtualDiskApiDTO();
        virtualDiskApiDTO.setUuid(String.valueOf(volumeId));
        virtualDiskApiDTO.setDisplayName(volume.getDisplayName());
        EnvironmentTypeMapper.fromXLToApi(volume.getEnvironmentType()).ifPresent(
            environmentType -> virtualDiskApiDTO.setEnvironmentType(environmentType));

        // find region for the volume and set it in VirtualDiskApiDTO
        final ApiPartialEntity region = regionByVolumeId.get(volume.getOid());
        if (region != null) {
            virtualDiskApiDTO.setDataCenter(ServiceEntityMapper.toBasicEntity(region));
        }
        // set storage tier
        final ServiceEntityApiDTO storageTier = storageTierByVolumeId.get(volumeId);
        if (storageTier != null) {
            // set tier
            virtualDiskApiDTO.setTier(storageTier.getDisplayName());
            // set storage tier as provider
            virtualDiskApiDTO.setProvider(storageTier);
        }

        // set attached VM (uuid + displayName)
        TopologyEntityDTO vmDTO = vmByVolumeId.get(volume.getOid());

        // commodity used
        float storageAmountUsed = 0.0f;
        float storageAccessUsed = 0.0f;
        // if vmDTO is not null, it means attached volume; if null, then it is unattached volume, used is 0
        if (vmDTO != null) {
            // set attached vm
            BaseApiDTO vm = new BaseApiDTO();
            vm.setUuid(String.valueOf(vmDTO.getOid()));
            vm.setDisplayName(vmDTO.getDisplayName());
            virtualDiskApiDTO.setAttachedVirtualMachine(ServiceEntityMapper.toBasicEntity(vmDTO));

            for (CommoditiesBoughtFromProvider cbfp : vmDTO.getCommoditiesBoughtFromProvidersList()) {
                if (cbfp.getVolumeId() == volume.getOid()) {
                    for (CommodityBoughtDTO cb : cbfp.getCommodityBoughtList()) {
                        if (cb.getCommodityType().getType() == CommodityType.STORAGE_AMOUNT_VALUE) {
                            storageAmountUsed = (float)cb.getUsed();
                        } else if (cb.getCommodityType().getType() == CommodityType.STORAGE_ACCESS_VALUE) {
                            storageAccessUsed = (float)cb.getUsed();
                        }
                    }
                }
            }
        }

        // commodity capacity
        float storageAmountCapacity = 0.0f;
        float storageAccessCapacity = 0.0f;
        if (volume.hasTypeSpecificInfo() && volume.getTypeSpecificInfo().hasVirtualVolume()) {
            VirtualVolumeInfo volumeInfo = volume.getTypeSpecificInfo().getVirtualVolume();
            storageAmountCapacity = volumeInfo.getStorageAmountCapacity();
            storageAccessCapacity = volumeInfo.getStorageAccessCapacity();
        }

        // set following stats:
        //     cost: the cost of the volume, retrieved from cost component
        //     storage amount: set the used (comes from VM bought) and capacity (from volume info)
        //     storage access: set the used (comes from VM bought) and capacity (from volume info)
        // todo: currently we don't need to go through stats API since we have everything. If in the
        // future we need to support "stats/{volumeId}", we may need to move this logic to stats API
        List<StatApiDTO> statDTOs = Lists.newArrayList();
        // cost stats
        if (volumeCostStatById.containsKey(volume.getOid())) {
            statDTOs.add(volumeCostStatById.get(volume.getOid()));
        }
        // storage amount stats
        statDTOs.add(createStatApiDTO(CommodityTypeUnits.STORAGE_AMOUNT.getMixedCase(),
                CommodityTypeUnits.STORAGE_AMOUNT.getUnits(), storageAmountUsed,
                storageAmountCapacity, storageTier, volume.getDisplayName()));
        // storage access stats
        statDTOs.add(createStatApiDTO(CommodityTypeUnits.STORAGE_ACCESS.getMixedCase(),
                CommodityTypeUnits.STORAGE_ACCESS.getUnits(), storageAccessUsed,
                storageAccessCapacity, storageTier, volume.getDisplayName()));
        virtualDiskApiDTO.setStats(statDTOs);

        return virtualDiskApiDTO;
    }

    /**
     * Helper method to create StatApiDTO for given stats.
     */
    private StatApiDTO createStatApiDTO(@Nonnull String statName,
            @Nonnull String statUnit,
            float used,
            float capacity,
            @Nullable ServiceEntityApiDTO relatedEntity,
            @Nullable String volumeName) {
        StatApiDTO statApiDTO = new StatApiDTO();
        statApiDTO.setName(statName);
        statApiDTO.setUnits(statUnit);

        // used
        StatValueApiDTO valueDTO = new StatValueApiDTO();
        valueDTO.setAvg(used);
        valueDTO.setMin(used);
        valueDTO.setMax(used);
        valueDTO.setTotal(used);
        statApiDTO.setValues(valueDTO);
        statApiDTO.setValue(used);

        // capacity
        StatValueApiDTO capacityDTO = new StatValueApiDTO();
        capacityDTO.setAvg(capacity);
        capacityDTO.setMin(capacity);
        capacityDTO.setMax(capacity);
        capacityDTO.setTotal(capacity);
        statApiDTO.setCapacity(capacityDTO);

        // related entity
        if (relatedEntity != null) {
            statApiDTO.setRelatedEntity(relatedEntity);

        }

        // filters
        List<StatFilterApiDTO> filters = Lists.newArrayList();
        StatFilterApiDTO filter1 = new StatFilterApiDTO();
        filter1.setType("key");
        filter1.setValue(volumeName);
        filters.add(filter1);

        StatFilterApiDTO filter2 = new StatFilterApiDTO();
        filter2.setType("relation");
        filter2.setValue("bought");
        filters.add(filter2);
        statApiDTO.setFilters(filters);

        return statApiDTO;
    }
}
