package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.component.external.api.mapper.SearchMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.UIEntityType;
import com.vmturbo.api.dto.BaseApiDTO;
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
import com.vmturbo.common.protobuf.search.Search.CountEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.EntityCountResponse;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchTopologyEntityDTOsRequest;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.VirtualVolumeFileDescriptor;

public class VirtualVolumeAspectMapper implements IAspectMapper {

    private static final Logger logger = LogManager.getLogger();
    // use for storage tier value for non cloud entities
    private static final String UNKNOWN = "unknown";

    private final SearchServiceBlockingStub searchServiceRpc;

    private final CostServiceBlockingStub costServiceRpc;

    public VirtualVolumeAspectMapper(@Nonnull final SearchServiceBlockingStub searchServiceRpc,
                                     @Nonnull final CostServiceBlockingStub costServiceRpc) {
        this.searchServiceRpc = searchServiceRpc;
        this.costServiceRpc = costServiceRpc;
    }

    @Override
    public boolean supportsGroup() {
        return true;
    }

    @Override
    public @Nonnull String getAspectName() {
        return "virtualDisksAspect";
    }

    @Override
    public EntityAspect mapEntityToAspect(@Nonnull TopologyEntityDTO entity) {
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
        final List<String> storageTierIds = Lists.newArrayList();
        final Set<Long> regionIds = Sets.newHashSet();

        for (TopologyEntityDTO storageTier : entities) {
            storageTierById.put(storageTier.getOid(), storageTier);
            storageTierIds.add(String.valueOf(storageTier.getOid()));
            regionIds.addAll(storageTier.getConnectedEntityListList().stream()
                    .filter(connectedEntity -> connectedEntity.getConnectedEntityType() ==
                            EntityType.REGION_VALUE)
                    .map(ConnectedEntity::getConnectedEntityId)
                    .collect(Collectors.toSet()));
        }

        // entities are a list of storage tiers, find all volumes connected to these storage tiers
        List<TopologyEntityDTO> volumes = storageTierIds.stream()
                .flatMap(id -> traverseAndGetEntities(id, TraversalDirection.CONNECTED_FROM,
                        UIEntityType.VIRTUAL_VOLUME.getValue()).stream())
                .collect(Collectors.toList());

        // find all regions for the given storage tiers
        List<TopologyEntityDTO> regions = searchTopologyEntityDTOs(regionIds);
        final Map<Long, TopologyEntityDTO> regionByZoneId = Maps.newHashMap();
        final Map<Long, TopologyEntityDTO> regionById = Maps.newHashMap();
        regions.forEach(region -> {
            regionById.put(region.getOid(), region);
            region.getConnectedEntityListList().forEach(connectedEntity -> {
                if (connectedEntity.getConnectedEntityType() == EntityType.AVAILABILITY_ZONE_VALUE) {
                    regionByZoneId.put(connectedEntity.getConnectedEntityId(), region);
                }
            });
        });

        // create mapping from volume id to storage tier and region
        final Map<Long, TopologyEntityDTO> storageTierByVolumeId = Maps.newHashMap();
        final Map<Long, TopologyEntityDTO> regionByVolumeId = Maps.newHashMap();
        volumes.forEach(volume -> {
            Long volumeId = volume.getOid();
            volume.getConnectedEntityListList().forEach(connectedEntity -> {
                int connectedEntityType = connectedEntity.getConnectedEntityType();
                Long connectedEntityId = connectedEntity.getConnectedEntityId();
                if (connectedEntityType == EntityType.STORAGE_TIER_VALUE) {
                    storageTierByVolumeId.put(volumeId, storageTierById.get(connectedEntityId));
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
        Map<Long, StatApiDTO> volumeCostStatById = getVolumeCostStats(volumes.stream()
                .map(TopologyEntityDTO::getOid).collect(Collectors.toSet()));

        // get all VMs consuming given storage tiers
        List<TopologyEntityDTO> vms = storageTierIds.stream()
                .flatMap(id -> traverseAndGetEntities(id, TraversalDirection.PRODUCES,
                        UIEntityType.VIRTUAL_MACHINE.getValue()).stream())
                .collect(Collectors.toList());

        final Map<Long, TopologyEntityDTO> vmByVolumeId = Maps.newHashMap();
        vms.forEach(vm ->
                vm.getConnectedEntityListList().forEach(connectedEntity -> {
                    if (connectedEntity.getConnectedEntityType() == EntityType.VIRTUAL_VOLUME_VALUE) {
                        vmByVolumeId.put(connectedEntity.getConnectedEntityId(), vm);
                    }
                })
        );

        List<VirtualDiskApiDTO> virtualDisks = volumes.stream()
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
    private EntityAspect mapVirtualMachines(@Nonnull List<TopologyEntityDTO> vms) {
        final Map<Long, TopologyEntityDTO> vmByVolumeId = Maps.newHashMap();

        // create mapping from volume id to storage tier and region
        final Map<Long, TopologyEntityDTO> storageTierByVolumeId = Maps.newHashMap();
        final Map<Long, Set<Long>> volumeIdsByRegionId = Maps.newHashMap();
        final Map<Long, Long> zoneIdByVolumeId = Maps.newHashMap();

        vms.forEach(vm -> {
            Long regionId = null;
            Long zoneId = null;
            Set<Long> volumeIds = Sets.newHashSet();
            for (ConnectedEntity connectedEntity : vm.getConnectedEntityListList()) {
                int connectedEntityType = connectedEntity.getConnectedEntityType();
                Long connectedEntityId = connectedEntity.getConnectedEntityId();
                if (connectedEntityType == EntityType.VIRTUAL_VOLUME_VALUE) {
                    volumeIds.add(connectedEntityId);
                    vmByVolumeId.put(connectedEntityId, vm);
                } else if (connectedEntityType == EntityType.REGION_VALUE) {
                    regionId = connectedEntityId;
                } else if (connectedEntityType == EntityType.AVAILABILITY_ZONE_VALUE) {
                    zoneId = connectedEntityId;
                }
            }

            for (Long volumeId : volumeIds) {
                if (regionId != null) {
                    // vm connected to region (azure)
                    volumeIdsByRegionId.computeIfAbsent(regionId, k -> new HashSet<>()).add(volumeId);
                } else if (zoneId != null) {
                    // vm connected to zone (aws)
                    zoneIdByVolumeId.put(volumeId, zoneId);
                }

                // find StorageTier connected to this volume
                List<TopologyEntityDTO> storageTier = traverseAndGetEntities(String.valueOf(volumeId),
                        TraversalDirection.CONNECTED_TO, UIEntityType.STORAGE_TIER.getValue());
                if (storageTier.size() == 0) {
                    logger.warn("Volume {} is not connected to any StorageTier", volumeId);
                } else if (storageTier.size() > 1) {
                    logger.warn("Volume {} is connected to {} StorageTiers", volumeId,
                            storageTier.size());
                } else {
                    storageTierByVolumeId.put(volumeId, storageTier.get(0));
                }
            }
        });

        // mapping from volume id to region entity
        final Map<Long, TopologyEntityDTO> regionByVolumeId = Maps.newHashMap();
        zoneIdByVolumeId.forEach((volumeId, zoneId) -> {
            List<TopologyEntityDTO> region = traverseAndGetEntities(String.valueOf(zoneId),
                    TraversalDirection.CONNECTED_FROM, UIEntityType.REGION.getValue());
            if (region.size() == 1) {
                regionByVolumeId.put(volumeId, region.get(0));
            }
        });
        if (volumeIdsByRegionId.keySet().size() > 0) {
            searchTopologyEntityDTOs(volumeIdsByRegionId.keySet()).forEach(region -> {
                Set<Long> volumeIds = volumeIdsByRegionId.get(region.getOid());
                if (volumeIds != null) {
                    volumeIds.forEach(volumeId -> regionByVolumeId.put(volumeId, region));
                }
            });
        }

        // get cost stats for all volumes
        final Map<Long, StatApiDTO> volumeCostStatById = getVolumeCostStats(vmByVolumeId.keySet());

        // convert to VirtualDiskApiDTO
        List<VirtualDiskApiDTO> virtualDisks = searchTopologyEntityDTOs(vmByVolumeId.keySet()).stream()
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
            virtualDisks.addAll(
                traverseAndGetEntities(String.valueOf(storage.getOid()),
                    TraversalDirection.CONNECTED_FROM,
                    UIEntityType.VIRTUAL_VOLUME.getValue()).stream()
                    .filter(topoEntity ->
                        traverseAndGetEntityCount(String.valueOf(topoEntity.getOid()),
                            TraversalDirection.CONNECTED_FROM,
                            UIEntityType.VIRTUAL_MACHINE.getValue()) == 0)
                    .filter(TopologyEntityDTO::hasTypeSpecificInfo)
                    .map(TopologyEntityDTO::getTypeSpecificInfo)
                    .filter(TypeSpecificInfo::hasVirtualVolume)
                    .map(TypeSpecificInfo::getVirtualVolume)
                    .flatMap(virtualVolInfo -> virtualVolInfo.getFilesList().stream())
                    .map(fileDescriptor -> fileToDiskApiDto(storage, fileDescriptor))
                    .collect(Collectors.toList())));
        final VirtualDisksAspectApiDTO aspect = new VirtualDisksAspectApiDTO();
        aspect.setVirtualDisks(virtualDisks);
        return aspect;
    }

    /**
     * Retrieve cost for volumes and create cost StatApiDTO for each volume.
     *
     * @param volumeIds list of volume ids to get cost for
     * @return map of cost StatApiDTO for each volume id
     */
    private Map<Long, StatApiDTO> getVolumeCostStats(@Nonnull Set<Long> volumeIds) {
        final GetCloudCostStatsRequest request = GetCloudCostStatsRequest.newBuilder()
                .setEntityFilter(EntityFilter.newBuilder()
                        .addAllEntityId(volumeIds)
                        .build())
                .build();
        try {
            final List<CloudCostStatRecord> cloudStatRecords = costServiceRpc.getCloudCostStats(request)
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
        } catch (Exception e) {
            logger.error("Error when getting cost for volumes: ", e);
            return Collections.emptyMap();
        }
    }

    /**
     * Return the {@link SearchParameters} for a search starting at the given oid in the
     * {@link TraversalDirection} given for one hop ending in an entity of the given entityType.
     *
     * @param startOid the oid of the object to start the search from.
     * @param traversalDirection the traversal direction to search in.
     * @param endEntityType a String giving the name of the EntityType to search for.
     * @return {@link SearchParameters} for the given search.
     */
    private SearchParameters createSearchParameters(
        @Nonnull String startOid,
        @Nonnull TraversalDirection traversalDirection,
        @Nonnull String endEntityType) {
        return SearchParameters.newBuilder()
            // start from storage tier oid
            .setStartingFilter(SearchMapper.stringPropertyFilter(GroupMapper.OID, startOid))
            // traverse CONNECTED_FROM
            .addSearchFilter(SearchFilter.newBuilder()
                .setTraversalFilter(TraversalFilter.newBuilder()
                    .setTraversalDirection(traversalDirection)
                    .setStoppingCondition(StoppingCondition.newBuilder()
                        .setNumberHops(1).build()))
                .build())
            // find all volumes
            .addSearchFilter(SearchMapper.searchFilterProperty(
                SearchMapper.entityTypeFilter(endEntityType)))
            .build();
    }


    /**
     * Get the number of instances of a given EntityType by traversing one hop from the starting oid.
     *
     * @param startOid           Oid of entity to start the traversal from.
     * @param traversalDirection the traversal direction.
     * @param endEntityType      the type of entity to count.
     * @return int giving the number of entities found.
     */
    public int traverseAndGetEntityCount(
        @Nonnull String startOid,
        @Nonnull TraversalDirection traversalDirection,
        @Nonnull String endEntityType) {
        try {
            EntityCountResponse response = searchServiceRpc.countEntities(
                CountEntitiesRequest.newBuilder()
                    .addSearchParameters(createSearchParameters(startOid, traversalDirection,
                        endEntityType)).build());
            return response.getEntityCount();
        } catch (Exception e) {
            logger.error("Error when getting entity count for search parameters: {}",
                createSearchParameters(startOid, traversalDirection, endEntityType), e);
            return 0;
        }

    }

    /**
     * Traverse the topology from a starting oid using a specified {@link TraversalDirection} for
     * one hop and return entities of the specified type.
     *
     * @param startOid           the oid to start from.
     * @param traversalDirection the traversal direction.
     * @param endEntityType      the type of entity to search for
     * @return list of {@link TopologyEntityDTO} of the specified entity type within one hop of the
     * starting oid in the given traversalDirection.
     */
    public List<TopologyEntityDTO> traverseAndGetEntities(
        @Nonnull String startOid,
        @Nonnull TraversalDirection traversalDirection,
        @Nonnull String endEntityType) {
        return searchTopologyEntityDTOs(SearchTopologyEntityDTOsRequest.newBuilder()
            .addSearchParameters(createSearchParameters(startOid, traversalDirection,
                endEntityType))
            .build());
    }

    /**
     * Search for TopologyEntityDTOs for a given request.
     */
    private List<TopologyEntityDTO> searchTopologyEntityDTOs(@Nonnull SearchTopologyEntityDTOsRequest request) {
        try {
            return searchServiceRpc.searchTopologyEntityDTOs(request).getTopologyEntityDtosList();
        } catch (Exception e) {
            logger.error("Error when getting TopologyEntityDTOs for request: {}", request, e);
            return Collections.emptyList();
        }
    }

    /**
     * Search for TopologyEntityDTOs for a set of oids.
     */
    public List<TopologyEntityDTO> searchTopologyEntityDTOs(@Nonnull Set<Long> oids) {
        return searchTopologyEntityDTOs(SearchTopologyEntityDTOsRequest.newBuilder()
                        .addAllEntityOid(oids)
                        .build());
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
        retVal.setProvider(createBaseApiDTO(storage));
        retVal.setLastModified(file.getModificationTimeMs());
        retVal.setTier(UNKNOWN);
        // storage amount stats

        retVal.setStats(Collections.singletonList(createStatApiDTO(
            CommodityTypeUnits.STORAGE_AMOUNT.getMixedCase(),
            CommodityTypeUnits.STORAGE_AMOUNT.getUnits(), file.getSizeKb() / 1024F,
            file.getSizeKb() / 1024F, storage, file.getPath())));
        return retVal;
    }

    /**
     * Create the BaseApiDTO based on a TopologyEntityDTO.
     *
     * @param entity The storage enclosing the virtual disk.
     * @return BaseApiDTO representing the storage.
     */
    private BaseApiDTO createBaseApiDTO(TopologyEntityDTO entity) {
        BaseApiDTO baseApiDTO = new BaseApiDTO();
        baseApiDTO.setUuid(String.valueOf(entity.getOid()));
        baseApiDTO.setDisplayName(entity.getDisplayName());

        return baseApiDTO;
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
            @Nonnull Map<Long, TopologyEntityDTO> storageTierByVolumeId,
            @Nonnull Map<Long, TopologyEntityDTO> regionByVolumeId,
            @Nonnull Map<Long, StatApiDTO> volumeCostStatById) {
        Long volumeId = volume.getOid();
        // the VirtualDiskApiDTO to return
        VirtualDiskApiDTO virtualDiskApiDTO = new VirtualDiskApiDTO();
        virtualDiskApiDTO.setUuid(String.valueOf(volumeId));
        virtualDiskApiDTO.setDisplayName(volume.getDisplayName());
        virtualDiskApiDTO.setEnvironmentType(EnvironmentType.CLOUD);

        // find region for the volume and set it in VirtualDiskApiDTO
        final TopologyEntityDTO region = regionByVolumeId.get(volume.getOid());
        if (region != null) {
            BaseApiDTO regionDTO = new BaseApiDTO();
            regionDTO.setUuid(String.valueOf(region.getOid()));
            regionDTO.setDisplayName(region.getDisplayName());
            virtualDiskApiDTO.setDataCenter(regionDTO);
        }
        // set storage tier
        final TopologyEntityDTO storageTier = storageTierByVolumeId.get(volumeId);
        if (storageTier != null) {
            // set tier
            virtualDiskApiDTO.setTier(storageTier.getDisplayName());
            // set storage tier as provider
            BaseApiDTO provider = new BaseApiDTO();
            provider.setUuid(String.valueOf(storageTier.getOid()));
            provider.setDisplayName(storageTier.getDisplayName());
            virtualDiskApiDTO.setProvider(provider);
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
            virtualDiskApiDTO.setAttachedVirtualMachine(vm);

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
    private StatApiDTO createStatApiDTO(@Nonnull String statName, @Nonnull String statUnit,
            float used, float capacity, @Nullable TopologyEntityDTO relatedEntity,
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
            BaseApiDTO relatedEntityApiDTO = new BaseApiDTO();
            relatedEntityApiDTO.setUuid(String.valueOf(relatedEntity.getOid()));
            relatedEntityApiDTO.setDisplayName(relatedEntity.getDisplayName());
            relatedEntityApiDTO.setClassName(ServiceEntityMapper.toUIEntityType(
                    relatedEntity.getEntityType()));
            statApiDTO.setRelatedEntity(relatedEntityApiDTO);

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
