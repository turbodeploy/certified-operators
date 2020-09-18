package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import io.grpc.StatusRuntimeException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.common.api.mappers.EnvironmentTypeMapper;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery;
import com.vmturbo.common.protobuf.cost.Cost.EntityFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsResponse;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.stats.Stats.GetMostRecentStatRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetMostRecentStatResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatHistoricalEpoch;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.Units;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.VirtualVolumeFileDescriptor;

/**
 * Mapper for getting virtual disks aspect.
 */
public class VirtualVolumeAspectMapper extends AbstractAspectMapper {

    private static final Logger logger = LogManager.getLogger();
    // use for storage tier value for non cloud entities
    private static final String UNKNOWN = "unknown";

    /**
     * Unit used for Cloud Storage.
     */
    protected static final String CLOUD_STORAGE_AMOUNT_UNIT = "GiB";

    /**
     * Unit value used for Cloud Storage.
     */
    private static final float CLOUD_STORAGE_AMOUNT_UNIT_IN_BYTE = Units.GBYTE;

    private final String COSTCOMPONENT = "costComponent";

    private final CostServiceBlockingStub costServiceRpc;

    private final RepositoryApi repositoryApi;

    private final StatsHistoryServiceBlockingStub historyRpcService;

    private final long getMostRecentStatRpcDeadlineDurationSeconds;

    private final long getMostRecentStatRpcFutureTimeoutSeconds;

    private static final String ENCRYPTION_STATE_ENABLED = "Enabled";

    private static final String ENCRYPTION_STATE_DISABLED = "Disabled";

    public VirtualVolumeAspectMapper(@Nonnull final CostServiceBlockingStub costServiceRpc,
                                     @Nonnull final RepositoryApi repositoryApi,
                                     @Nonnull final StatsHistoryServiceBlockingStub
                                             historyRpcService,
                                     final long getMostRecentStatRpcDeadlineDurationSeconds,
                                     final long getMostRecentStatRpcFutureTimeoutSeconds) {
        this.costServiceRpc = costServiceRpc;
        this.repositoryApi = repositoryApi;
        this.historyRpcService = historyRpcService;
        this.getMostRecentStatRpcDeadlineDurationSeconds =
            getMostRecentStatRpcDeadlineDurationSeconds;
        this.getMostRecentStatRpcFutureTimeoutSeconds = getMostRecentStatRpcFutureTimeoutSeconds;
    }

    @Override
    public boolean supportsGroup() {
        return true;
    }

    @Override
    public boolean supportsGroupAspectExpansion() {
        return true;
    }

    @Override
    public @Nonnull AspectName getAspectName() {
        return AspectName.VIRTUAL_VOLUME;
    }

    @Override
    public EntityAspect mapEntityToAspect(@Nonnull final TopologyEntityDTO entity)
            throws InterruptedException, ConversionException {
        STEntityAspectApiDTO aspect = new STEntityAspectApiDTO();
        aspect.setDisplayName(entity.getDisplayName());
        aspect.setName(String.valueOf(entity.getOid()));
        return mapEntitiesToAspect(Lists.newArrayList(entity));
    }

    @Override
    @Nonnull
    public Optional<Map<Long, EntityAspect>> mapEntityToAspectBatch(@Nonnull final List<TopologyEntityDTO> entities)
            throws InterruptedException, ConversionException {
        EntityAspect aspect = mapEntitiesToAspect(entities);

        if (aspect != null) {
            Map<String, EntityAspect> aspectMap = mapOneToManyAspects(entities, aspect);
            if (aspectMap != null) {
                return Optional.of(aspectMap.entrySet().stream()
                        .collect(Collectors.toMap(e -> Long.valueOf(e.getKey()), e -> e.getValue())));
            }
        }

        return Optional.empty();
    }

    /**
     * Only homogeneous {@param entities} collections are supported, so checking the type of the first is sufficient
     * to determine the appropriate method of aspect composition.
     *
     * @param entities list of entities to get aspect for, which are members of a group
     * @return an {@link EntityAspect} of type {@link VirtualDisksAspectApiDTO} representing the details of {@param entities}
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    @Nullable
    @Override
    public EntityAspect mapEntitiesToAspect(@Nonnull List<TopologyEntityDTO> entities)
            throws InterruptedException, ConversionException {
        if (CollectionUtils.isEmpty(entities)) {
            return null;
        }
        final int entityType = entities.get(0).getEntityType();
        switch (entityType) {
            case EntityType.VIRTUAL_VOLUME_VALUE:
                return mapVirtualVolumes(entities);
            case EntityType.STORAGE_TIER_VALUE:
                return mapStorageTiers(entities);
            case EntityType.VIRTUAL_MACHINE_VALUE:
                return mapVirtualMachines(entities);
            case EntityType.STORAGE_VALUE:
                return mapStorages(entities);
            default:
                return null;
        }
    }

    /**
     * Only homogeneous {@param entities} collections are supported, so checking the type of the first is sufficient
     * to determine the appropriate method of aspect composition. The type of {@param entities} determines how the key
     * of the returned value is computed. For instance, if {@param entities} is a collection of VirtualMachines,
     * the UUIDs of the returned map are those of the {@link VirtualDiskApiDTO} attachedVirtualMachine.
     *
     * @param entities list of entities for which to compute {@link EntityAspect}s
     * @param entityAspect a single {@link EntityAspect} representing multiple {@link EntityAspect} instances
     * @return a map of UUID to {@link EntityAspect}, representing the details of {@param entities}
     */
    @Nullable
    @Override
    public Map<String, EntityAspect> mapOneToManyAspects(@Nullable List<TopologyEntityDTO> entities, @Nullable EntityAspect entityAspect) {
        if (Objects.isNull(entityAspect)
            || !(entityAspect instanceof VirtualDisksAspectApiDTO)) {
            return null;
        }
        final VirtualDisksAspectApiDTO virtualDisksAspectApiDTO = (VirtualDisksAspectApiDTO)entityAspect;
        if (virtualDisksAspectApiDTO.getVirtualDisks() == null) {
            return null;
        }
        if (CollectionUtils.isEmpty(entities)) {
            return null;
        }
        Function<VirtualDiskApiDTO, String> getIdentifier;
        final int entityType = entities.get(0).getEntityType();
        switch (entityType) {
            case EntityType.VIRTUAL_VOLUME_VALUE:
                getIdentifier = (entity) -> entity.getUuid() != null
                    ? entity.getUuid() : "";
                break;
            case EntityType.VIRTUAL_MACHINE_VALUE:
                getIdentifier = (entity) -> entity.getAttachedVirtualMachine() != null
                    ? entity.getAttachedVirtualMachine().getUuid() : "";
                break;
            case EntityType.STORAGE_TIER_VALUE:
            case EntityType.STORAGE_VALUE:
                getIdentifier = (entity) -> entity.getProvider() != null
                    ? entity.getProvider().getUuid() : "";
                break;
            default:
                return null;
        }

        Map<String, EntityAspect> uuidToMergedAspect = new HashMap<>();
        virtualDisksAspectApiDTO.getVirtualDisks().stream()
                .collect(Collectors.groupingBy(getIdentifier))
                .forEach((identifier, virtualDiskApiDTOList) -> {
                    if (!identifier.isEmpty()) {
                        final VirtualDisksAspectApiDTO aspect = new VirtualDisksAspectApiDTO();
                        aspect.setVirtualDisks(virtualDiskApiDTOList);
                        uuidToMergedAspect.put(identifier, aspect);
                    }
                });
        return uuidToMergedAspect;
    }

    /**
     * Create VirtualVolumeAspect for volumes related to a list of storage tiers.
     *
     * @param entities entities to map
     * @return entity aspect
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    private EntityAspect mapStorageTiers(@Nonnull List<TopologyEntityDTO> entities)
            throws ConversionException, InterruptedException {
        final Map<Long, TopologyEntityDTO> storageTierById = Maps.newHashMap();
        final Set<Long> regionIds = Sets.newHashSet();
        final Map<Long, ApiPartialEntity> regionByZoneId = Maps.newHashMap();
        final Map<Long, ApiPartialEntity> regionById = Maps.newHashMap();

        // find ids of all regions associated with the storage tiers
        for (TopologyEntityDTO storageTier : entities) {
            storageTierById.put(storageTier.getOid(), storageTier);
            storageTier.getConnectedEntityListList().stream()
                    .filter(c -> c.getConnectionType() == ConnectionType.AGGREGATED_BY_CONNECTION
                            && c.getConnectedEntityType() == EntityType.REGION_VALUE)
                    .map(ConnectedEntity::getConnectedEntityId)
                    .forEach(regionIds::add);
        }

        // find all regions for the given storage tiers
            // and populate regionByZoneId map
        repositoryApi.entitiesRequest(regionIds).getEntities().forEach(region -> {
            regionById.put(region.getOid(), region);
            region.getConnectedToList().forEach(connectedEntity -> {
                if (connectedEntity.getEntityType() == EntityType.AVAILABILITY_ZONE_VALUE) {
                    regionByZoneId.put(connectedEntity.getOid(), region);
                }
            });
        });

        // find all volumes connected to these storage tiers
        List<TopologyEntityDTO> volumes = storageTierById.values().stream()
                .map(TopologyEntityDTO::getOid)
                .flatMap(id ->
                        repositoryApi.newSearchRequest(SearchProtoUtil.neighborsOfType(
                                id, TraversalDirection.PRODUCES, ApiEntityType.VIRTUAL_VOLUME))
                                .getFullEntities())
                .collect(Collectors.toList());

        // create mapping from volume id to storage tier and region
        final Map<Long, ServiceEntityApiDTO> storageTierByVolumeId = Maps.newHashMap();
        final Map<Long, ApiPartialEntity> regionByVolumeId = Maps.newHashMap();
        volumes.forEach(volume -> {
            Long volumeId = volume.getOid();
            volume.getConnectedEntityListList().forEach(connectedEntity -> {
                int connectedEntityType = connectedEntity.getConnectedEntityType();
                Long connectedEntityId = connectedEntity.getConnectedEntityId();
                if (connectedEntityType == EntityType.AVAILABILITY_ZONE_VALUE) {
                    // if zone exists, find region based on zone id (for aws, volume is connected to az)
                    regionByVolumeId.put(volumeId, regionByZoneId.get(connectedEntityId));
                } else if (connectedEntityType == EntityType.REGION_VALUE) {
                    // if no zone, get region directly (for azure, volume is connected to region)
                    regionByVolumeId.put(volumeId, regionById.get(connectedEntityId));
                }
            });
            volume.getCommoditiesBoughtFromProvidersList().stream()
                    .filter(commBought -> commBought.getProviderEntityType()
                            == EntityType.STORAGE_TIER.getNumber())
                    .map(CommoditiesBoughtFromProvider::getProviderId)
                    .forEach(storageTierId -> storageTierByVolumeId.put(volumeId,
                            ServiceEntityMapper.toBaseServiceEntityApiDTO(
                                    storageTierById.get(storageTierId))));
        });

        // get cost stats for all volumes
        Multimap<Long, StatApiDTO> volumeCostStatById = getVolumeCostStats(volumes, null);

        // get all VMs consuming given storage tiers
        List<TopologyEntityDTO> vms = storageTierById.keySet().stream()
            .flatMap(id -> repositoryApi.newSearchRequest(
                SearchProtoUtil.neighborsOfType(id, TraversalDirection.PRODUCES, ApiEntityType.VIRTUAL_MACHINE)).getFullEntities())
            .collect(Collectors.toList());

        final Map<Long, TopologyEntityDTO> vmByVolumeId = Maps.newHashMap();
        vms.forEach(vm ->
            vm.getCommoditiesBoughtFromProvidersList().forEach(commBought -> {
                if (commBought.getProviderEntityType() == EntityType.VIRTUAL_VOLUME_VALUE) {
                    vmByVolumeId.put(commBought.getProviderId(), vm);
                }
            })
        );
        final List<VirtualDiskApiDTO> virtualDisks = new ArrayList<>(volumes.size());
        for (TopologyEntityDTO volume: volumes) {
            virtualDisks.add(convert(volume, vmByVolumeId, storageTierByVolumeId,
                    regionByVolumeId, volumeCostStatById, volumes.size() == 1));
        }
        if (virtualDisks.isEmpty()) {
            return null;
        }

        final VirtualDisksAspectApiDTO aspect = new VirtualDisksAspectApiDTO();
        aspect.setVirtualDisks(virtualDisks);
        return aspect;
    }

    /**
     * Create VirtualVolumeAspect for volumes related to a list of virtual machines.
     *
     * @param vmDTOs VMs to create aspects from
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     */
    @Nullable
    private EntityAspect mapVirtualMachines(@Nonnull List<TopologyEntityDTO> vmDTOs)
            throws ConversionException, InterruptedException {
        Set<Long> vmIds = vmDTOs.stream()
                .map(TopologyEntityDTO::getOid)
                .collect(Collectors.toSet());
        Map<Long, List<VirtualDiskApiDTO>> volumeAspectsByVMId = mapVirtualMachines(vmIds, null);
        if (volumeAspectsByVMId.isEmpty()) {
            return null;
        }
        final VirtualDisksAspectApiDTO aspect = new VirtualDisksAspectApiDTO();
        aspect.setVirtualDisks(volumeAspectsByVMId.values().stream()
            .flatMap(List::stream).collect(Collectors.toList()));
        return aspect;
    }

    /**
     * Get a map of VM ID to a list of VirtualDiskApiDTO.
     *
     * @param vmIds Set of VM IDs
     * @param topologyContextId topology context ID. Null if real-time topology
     * @return Map of VM ID to list of VirtualDiskpiDTO
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     */
    public Map<Long, List<VirtualDiskApiDTO>> mapVirtualMachines(@Nonnull Set<Long> vmIds,
                                                                  @Nullable final Long topologyContextId)
            throws InterruptedException, ConversionException {
        // VM entities from source topology
        final List<TopologyEntityDTO> sourceVms = repositoryApi.entitiesRequest(vmIds)
                .contextId(topologyContextId)
                .getFullEntities()
                .collect(Collectors.toList());

        // VM entities from projected topology
        final List<TopologyEntityDTO> projectedVms = repositoryApi.entitiesRequest(vmIds)
                .contextId(topologyContextId)
                .projectedTopology()
                .getFullEntities()
                .collect(Collectors.toList());

        // mapping from zone id to region
        final Map<Long, ApiPartialEntity> regionByZoneId = Maps.newHashMap();

        // mapping from volume id to storage tier
        final Map<Long, ServiceEntityApiDTO> storageTierByVolumeId = Maps.newHashMap();

        // mapping from volume id to region entity
        final Map<Long, ApiPartialEntity> regionByVolumeId = Maps.newHashMap();

        // Set of volume IDs of volumes attached to the VMs.
        final Set<Long> volumeIds = new HashSet<>();
        sourceVms.forEach(vm -> {
            // In old model (On Prem) volumes are attached to VMs using ConnectedTo relationship
            vm.getConnectedEntityListList().forEach(connectedEntity -> {
                if (connectedEntity.getConnectedEntityType() == EntityType.VIRTUAL_VOLUME_VALUE) {
                    volumeIds.add(connectedEntity.getConnectedEntityId());
                }
            });

            // In new model (Cloud) volumes are attached to VMs using bought commodities
            vm.getCommoditiesBoughtFromProvidersList().forEach(commBought -> {
                if (commBought.getProviderEntityType() == EntityType.VIRTUAL_VOLUME_VALUE) {
                    volumeIds.add(commBought.getProviderId());
                }
            });
        });

        // Map of volume ID to volume entities on the projected topology
        final Map<Long, TopologyEntityDTO> projectedVolumeMap = repositoryApi.entitiesRequest(volumeIds)
                .contextId(topologyContextId)
                .projectedTopology()
                .getFullEntities()
                .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));

        // Map of volume ID to a list of cost stats
        final Multimap<Long, StatApiDTO> volIdToCostStatsMap =
                getVolumeCostStats(new ArrayList<>(projectedVolumeMap.values()), topologyContextId);

        // fetch all the regions and create mapping from region id to region
        final Map<Long, ApiPartialEntity> regionById = fetchRegions();
        regionById.values().forEach(region ->
                region.getConnectedToList().stream()
                        .filter(connectedEntity -> connectedEntity.getEntityType() == EntityType.AVAILABILITY_ZONE_VALUE)
                        .forEach(connectedEntity -> regionByZoneId.put(connectedEntity.getOid(), region))
        );

        // fetch all the storage tiers and create mapping from tier id to tier
        final Map<Long, ServiceEntityApiDTO> storageTierById = fetchStorageTiers();
        projectedVolumeMap.values().forEach(volume -> {
            volume.getConnectedEntityListList().forEach(connectedEntity -> {
                int connectedEntityType = connectedEntity.getConnectedEntityType();
                Long connectedEntityId = connectedEntity.getConnectedEntityId();
                if (connectedEntityType == EntityType.REGION_VALUE) {
                    // volume connected to region (azure)
                    regionByVolumeId.put(volume.getOid(), regionById.get(connectedEntityId));
                } else if (connectedEntityType == EntityType.AVAILABILITY_ZONE_VALUE) {
                    // volume connected to zone (aws)
                    regionByVolumeId.put(volume.getOid(), regionByZoneId.get(connectedEntityId));
                }
            });
            volume.getCommoditiesBoughtFromProvidersList().stream()
                    .filter(commBought -> commBought.getProviderEntityType()
                            == EntityType.STORAGE_TIER.getNumber())
                    .map(CommoditiesBoughtFromProvider::getProviderId)
                    .map(storageTierById::get)
                    .forEach(storageTier -> storageTierByVolumeId.put(volume.getOid(), storageTier));
        });

        // Map volume ID to before-plan commList
        // VM ID -> Volume ID -> List<commodityBoughtDTO>
        Map<Long, Map<Long, List<CommodityBoughtDTO>>> beforePlanVolIdToCommListMap = new HashMap<>();
        for (TopologyEntityDTO vm : sourceVms) {
            final List<CommoditiesBoughtFromProvider> virtualDiskCommBoughtLists =
                    vm.getCommoditiesBoughtFromProvidersList().stream()
                            .filter(commList -> commList.getProviderEntityType() == EntityType.STORAGE_VALUE
                                    || commList.getProviderEntityType() == EntityType.VIRTUAL_VOLUME_VALUE)
                            .collect(Collectors.toList());
            Function<CommoditiesBoughtFromProvider, Long> getVolumeId = commoditiesBoughtFromProvider ->
                commoditiesBoughtFromProvider.hasVolumeId()
                         ? commoditiesBoughtFromProvider.getVolumeId() : commoditiesBoughtFromProvider.getProviderId();
            beforePlanVolIdToCommListMap.put(vm.getOid(), virtualDiskCommBoughtLists.stream()
                    .collect(Collectors.toMap(getVolumeId, CommoditiesBoughtFromProvider::getCommodityBoughtList)));
        }

        final Map<Long, List<VirtualDiskApiDTO>> virtualDisksByVmId = new HashMap<>();
        for (TopologyEntityDTO vm : projectedVms) {
            List<CommoditiesBoughtFromProvider> virtualDiskCommBoughtLists =
                    vm.getCommoditiesBoughtFromProvidersList().stream()
                            .filter(commList -> commList.getProviderEntityType() == EntityType.STORAGE_VALUE
                                    || commList.getProviderEntityType() == EntityType.STORAGE_TIER_VALUE)
                            .collect(Collectors.toList());


            List<VirtualDiskApiDTO> virtualDiskList = new ArrayList<>();
            for (CommoditiesBoughtFromProvider commList : virtualDiskCommBoughtLists) {
                long volId = commList.getVolumeId();
                Collection<StatApiDTO> costStats = volIdToCostStatsMap.get(volId);
                Map<Long, List<CommodityBoughtDTO>> volIdToCommListMap = beforePlanVolIdToCommListMap.get(vm.getOid());
                List<CommodityBoughtDTO> sourceCommList = volIdToCommListMap != null ? volIdToCommListMap.get(volId) : null;
                if (sourceCommList == null) {
                    continue;
                }
                VirtualDiskApiDTO virtualDiskApiDTO = createVirtualDiskApiDTO(vm, projectedVolumeMap.get(volId),
                        beforePlanVolIdToCommListMap.get(vm.getOid()).get(volId),
                        commList.getCommodityBoughtList(), costStats, regionByVolumeId, storageTierByVolumeId,
                        projectedVolumeMap.values().size() == 1);
                virtualDiskList.add(virtualDiskApiDTO);
            }
            virtualDisksByVmId.put(vm.getOid(), virtualDiskList);
        }
        return virtualDisksByVmId;
    }

    /**
     * Create a VirtualDiskApiDTO object from data collected about volume and VM.
     *
     * @param vm VM entity
     * @param volume Volume entity
     * @param beforeActionCommList the commodity bought list of the VM before the action
     * @param afterActionCommList the commodity bought list of the VM after the action
     * @param costStats cost stats
     * @param regionByVolumeId map of volume ID to region
     * @param storageTierByVolumeId map of volume ID to storage tier
     * @param fetchAttachmentHistory true if attachment history should be retrieved for the volume
     * @return VirtualDiskApiDTO
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     */
    private VirtualDiskApiDTO createVirtualDiskApiDTO(TopologyEntityDTO vm,
                                                      TopologyEntityDTO volume,
                                                      List<CommodityBoughtDTO> beforeActionCommList,
                                                      List<CommodityBoughtDTO> afterActionCommList,
                                                      Collection<StatApiDTO> costStats,
                                                      final Map<Long, ApiPartialEntity> regionByVolumeId,
                                                      final Map<Long, ServiceEntityApiDTO> storageTierByVolumeId,
                                                      boolean fetchAttachmentHistory)
            throws InterruptedException, ConversionException {
        VirtualDiskApiDTO virtualDiskApiDTO = new VirtualDiskApiDTO();
        virtualDiskApiDTO.setUuid(String.valueOf(volume.getOid()));
        virtualDiskApiDTO.setDisplayName(volume.getDisplayName());
        virtualDiskApiDTO.setAttachedVirtualMachine(ServiceEntityMapper.toBaseServiceEntityApiDTO(vm));
        virtualDiskApiDTO.setEnvironmentType(EnvironmentType.CLOUD);

        List<StatApiDTO> statDTOs = Lists.newArrayList();
        // Add projected stats
        double afterActionStorageAmountUsed = 0d;
        double afterActionStorageAccessUsed = 0d;
        double afterActionIOThroughputUsed = 0d;
        for (CommodityBoughtDTO commodity : afterActionCommList) {
            switch (commodity.getCommodityType().getType()) {
                case CommodityType.STORAGE_AMOUNT_VALUE:
                    afterActionStorageAmountUsed = commodity.getUsed();
                    break;
                case CommodityType.STORAGE_ACCESS_VALUE:
                    afterActionStorageAccessUsed = commodity.getUsed();
                    break;
                case CommodityType.IO_THROUGHPUT_VALUE:
                    afterActionIOThroughputUsed = commodity.getUsed();
                    break;
            }
        }
        statDTOs.add(createStatApiDTO(CommodityTypeUnits.STORAGE_AMOUNT.getMixedCase(),
                CLOUD_STORAGE_AMOUNT_UNIT, (float)afterActionStorageAmountUsed,
                (float)(getCommodityCapacity(volume, CommodityType.STORAGE_AMOUNT) / Units.KIBI),
                null, volume.getDisplayName(), null, false));
        statDTOs.add(createStatApiDTO(CommodityTypeUnits.STORAGE_ACCESS.getMixedCase(),
                CommodityTypeUnits.STORAGE_ACCESS.getUnits(), (float)afterActionStorageAccessUsed,
                getCommodityCapacity(volume, CommodityType.STORAGE_ACCESS),
                null, volume.getDisplayName(), null, false));
        statDTOs.add(createStatApiDTO(CommodityTypeUnits.IO_THROUGHPUT.getMixedCase(),
                CommodityTypeUnits.IO_THROUGHPUT.getUnits(), (float)afterActionIOThroughputUsed,
                getCommodityCapacity(volume, CommodityType.IO_THROUGHPUT),
                null, volume.getDisplayName(), null, false));

        // Add stats for before action stats.
        for (CommodityBoughtDTO commodity : beforeActionCommList) {
            switch (commodity.getCommodityType().getType()) {
                case CommodityType.STORAGE_AMOUNT_VALUE:
                    // Unit of storage amount in source topology is in MB.
                    statDTOs.add(createStatApiDTO(CommodityTypeUnits.STORAGE_AMOUNT.getMixedCase(),
                            CLOUD_STORAGE_AMOUNT_UNIT, (float)(commodity.getUsed() / Units.KIBI),
                            (float)(commodity.getUsed() / Units.KIBI),
                            null, volume.getDisplayName(), null, true));
                    break;
                case CommodityType.STORAGE_ACCESS_VALUE:
                    statDTOs.add(createStatApiDTO(CommodityTypeUnits.STORAGE_ACCESS.getMixedCase(),
                            CommodityTypeUnits.STORAGE_ACCESS.getUnits(), (float)commodity.getUsed(),
                            (float)commodity.getUsed(),
                            null, volume.getDisplayName(), null, true));
                    break;
                case CommodityType.IO_THROUGHPUT_VALUE:
                    statDTOs.add(createStatApiDTO(CommodityTypeUnits.IO_THROUGHPUT.getMixedCase(),
                            CommodityTypeUnits.IO_THROUGHPUT.getUnits(), (float)commodity.getUsed(),
                            (float)commodity.getUsed(),
                            null, volume.getDisplayName(), null, true));
                    break;
            }
        }

        // Get cost stats
        if (costStats != null) {
            statDTOs.addAll(costStats);
        }

        // find region for the volume and set it in VirtualDiskApiDTO
        final ApiPartialEntity region = regionByVolumeId.get(volume.getOid());
        if (region != null) {
            virtualDiskApiDTO.setDataCenter(ServiceEntityMapper.toBaseServiceEntityApiDTO(region));
        }
        // set storage tier
        final ServiceEntityApiDTO storageTier = storageTierByVolumeId.get(volume.getOid());
        if (storageTier != null) {
            // set tier
            virtualDiskApiDTO.setTier(storageTier.getDisplayName());
            // set storage tier as provider
            virtualDiskApiDTO.setProvider(storageTier);
        }
        if (virtualDiskApiDTO.getEnvironmentType() == EnvironmentType.CLOUD
                && AttachmentState.UNATTACHED.name()
                .equals(virtualDiskApiDTO.getAttachmentState()) && fetchAttachmentHistory) {
            populateUnattachedHistoryInfo(volume, virtualDiskApiDTO);
        }
        repositoryApi.newSearchRequest(
                SearchProtoUtil.neighborsOfType(volume.getOid(),
                        TraversalDirection.OWNED_BY,
                        ApiEntityType.BUSINESS_ACCOUNT))
                .getSEList()
                .forEach(businessAccount -> virtualDiskApiDTO.setBusinessAccount(businessAccount));

        if (volume.hasTypeSpecificInfo() && volume.getTypeSpecificInfo().hasVirtualVolume()) {
            VirtualVolumeInfo volumeInfo = volume.getTypeSpecificInfo().getVirtualVolume();
            if (volumeInfo.hasSnapshotId()) {
                virtualDiskApiDTO.setSnapshotId(volumeInfo.getSnapshotId());
            }
            if (volumeInfo.hasAttachmentState()) {
                virtualDiskApiDTO.setAttachmentState(volumeInfo.getAttachmentState().name());
            }
            if (volumeInfo.hasEncryption()) {
                String encrpytionState = ENCRYPTION_STATE_DISABLED;
                if (volumeInfo.getEncryption()) {
                    encrpytionState = ENCRYPTION_STATE_ENABLED;
                }
                virtualDiskApiDTO.setEncryption(encrpytionState);
            }
            if (volumeInfo.hasIsEphemeral()) {
                virtualDiskApiDTO.setEphemeral(Boolean.toString(volumeInfo.getIsEphemeral()));
            }
        }

        virtualDiskApiDTO.setStats(statDTOs);
        return virtualDiskApiDTO;
    }

    /**
     * Map Virtual Volumes.
     *
     * @param volumeIds - uuids of unattached volumes
     * @param topologyContextId - context ID of topology
     * @return virtual volume aspect by volume uuid
     * @throws ConversionException if errors faced during converting data to API DTOs
     * @throws InterruptedException if thread has been interrupted
     */
    public Optional<Map<Long, EntityAspect>> mapVirtualVolumes(@Nonnull Set<Long> volumeIds,
               final long topologyContextId) throws ConversionException, InterruptedException {
        final List<TopologyEntityDTO> volumes = repositoryApi.entitiesRequest(volumeIds)
                .contextId(topologyContextId)
                .getFullEntities()
                .collect(Collectors.toList());
        return mapEntityToAspectBatch(volumes);
    }

    /**
     * Create VirtualVolumeAspect for a list of virtual volumes.
     *
     * @param volumeDTOs a list of virtual volumes.
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    @Nullable
    private EntityAspect mapVirtualVolumes(@Nonnull List<TopologyEntityDTO> volumeDTOs)
            throws InterruptedException, ConversionException {
        return mapVirtualVolumes(volumeDTOs, null);
    }

    @Nullable
    private EntityAspect mapVirtualVolumes(@Nonnull List<TopologyEntityDTO> vols,
            @Nullable Long topologyContextId) throws InterruptedException, ConversionException {
        final Map<Long, TopologyEntityDTO> vmByVolumeId = Maps.newHashMap();

        // create mapping from volume id to storage tier and region
        final Map<Long, ServiceEntityApiDTO> storageTierByVolumeId = Maps.newHashMap();
        final Map<Long, ApiPartialEntity> regionByVolumeId = Maps.newHashMap();
        final Map<Long, Long> storageTierIdByVolumeId = Maps.newHashMap();
        final Set<Long> storageTierIds = Sets.newHashSet();

        final Map<Long, ApiPartialEntity> regionByZoneId = Maps.newHashMap();
        final Map<Long, ApiPartialEntity> regionById = fetchRegions();
        regionById.values().forEach(region ->
                region.getConnectedToList().stream()
                        .filter(connectedEntity -> connectedEntity.getEntityType() == EntityType.AVAILABILITY_ZONE_VALUE)
                        .forEach(connectedEntity -> regionByZoneId.put(connectedEntity.getOid(), region))
        );

        vols.forEach(vol -> {
            for (ConnectedEntity connectedEntity : vol.getConnectedEntityListList()) {
                switch (connectedEntity.getConnectedEntityType()) {
                    case EntityType.AVAILABILITY_ZONE_VALUE:
                        // get region from zone
                        regionByVolumeId.put(vol.getOid(), regionByZoneId.get(connectedEntity.getConnectedEntityId()));
                        break;
                    case EntityType.REGION_VALUE:
                        // in case of Azure, volume connected from Region directly.
                        regionByVolumeId.put(vol.getOid(), regionById.get(connectedEntity.getConnectedEntityId()));
                        break;
                    default:
                        break;
                }
            }

            // Retrieve Storage Tier
            final Long storageTierId = vol.getCommoditiesBoughtFromProvidersList().stream()
                    .filter(commoditiesBoughtFromProvider -> commoditiesBoughtFromProvider
                            .getProviderEntityType() == EntityType.STORAGE_TIER_VALUE)
                    .map(CommoditiesBoughtFromProvider::getProviderId)
                    .findAny().orElse(null);
            if (storageTierId != null) {
                storageTierIdByVolumeId.put(vol.getOid(), storageTierId);
                storageTierIds.add(storageTierId);
            }

            // Find connected VMs:
            // - for Cloud case we use Produces relationship
            // - for On Prem we use ConnectedTo until On Prem probes are switched to the new model
            final TraversalDirection traversalDirection = storageTierId != null
                    ? TraversalDirection.PRODUCES
                    : TraversalDirection.CONNECTED_FROM;
            repositoryApi.newSearchRequest(
                    SearchProtoUtil.neighborsOfType(vol.getOid(),
                            traversalDirection,
                            ApiEntityType.VIRTUAL_MACHINE))
                    .getFullEntities().forEach(vm ->
                        vmByVolumeId.put(vol.getOid(), vm));
        });

        final Map<Long, ServiceEntityApiDTO> stTierBasicEntityById = Maps.newHashMap();
        repositoryApi.entitiesRequest(storageTierIds).getMinimalEntities()
                .forEach(storageTierEntity -> {
                    final ServiceEntityApiDTO stTierBasicEntity =
                            ServiceEntityMapper.toBaseServiceEntityApiDTO(storageTierEntity);
                    stTierBasicEntityById.put(storageTierEntity.getOid(), stTierBasicEntity);
                });
        storageTierIdByVolumeId.forEach((volId, stId) -> {
                    storageTierByVolumeId.put(volId, stTierBasicEntityById.get(stId));
                }
        );

        // get cost stats for all volumes
        final Multimap<Long, StatApiDTO> volumeCostStatById = getVolumeCostStats(vols, topologyContextId);

        // convert to VirtualDiskApiDTO
        final List<VirtualDiskApiDTO> virtualDisks = new ArrayList<>(vols.size());
        for (TopologyEntityDTO volume: vols) {
            final VirtualDiskApiDTO disk = convert(volume, vmByVolumeId, storageTierByVolumeId,
                    regionByVolumeId, volumeCostStatById, vols.size() == 1);
            if (disk != null) {
                virtualDisks.add(disk);
            }
        }
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
            repositoryApi.newSearchRequest(SearchProtoUtil.neighborsOfType(
                storage.getOid(), TraversalDirection.CONNECTED_FROM, ApiEntityType.VIRTUAL_VOLUME)).getFullEntities()
                .filter(topoEntity ->
                    repositoryApi.newSearchRequest(SearchProtoUtil.neighborsOfType(
                        topoEntity.getOid(),
                        TraversalDirection.CONNECTED_FROM,
                        ApiEntityType.VIRTUAL_MACHINE)).count() == 0)
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
    private Multimap<Long, StatApiDTO> getVolumeCostStats(@Nonnull List<TopologyEntityDTO> volumes,
                                                           @Nullable Long topologyContextId) {
        final Multimap<Long, StatApiDTO> result = ArrayListMultimap.create();

        final Set<Long> cloudVolumeIds = volumes.stream()
            .filter(AbstractAspectMapper::isCloudEntity)
            .map(TopologyEntityDTO::getOid)
            .collect(Collectors.toSet());

        if (cloudVolumeIds.isEmpty()) {
            return result;
        }
        final GetCloudCostStatsRequest.Builder request = GetCloudCostStatsRequest.newBuilder();
        final CloudCostStatsQuery.Builder cloudCostStatsQuery = CloudCostStatsQuery.newBuilder();
                cloudCostStatsQuery.setEntityFilter(EntityFilter.newBuilder()
                                .addAllEntityId(cloudVolumeIds)
                                .build());
        if (topologyContextId != null) {
            // get projected cost
            cloudCostStatsQuery.setTopologyContextId(topologyContextId);
            cloudCostStatsQuery.setRequestProjected(true);
        }
        request.addCloudCostStatsQuery(cloudCostStatsQuery.build());
        try {
            final Iterator<GetCloudCostStatsResponse> response =
                costServiceRpc.getCloudCostStats(request.build());
            final List<CloudCostStatRecord> cloudStatRecords = new ArrayList<>();
            while (response.hasNext()) {
                cloudStatRecords.addAll(response.next().getCloudStatRecordList());
            }
            // Update projected stats record values in result.
            cloudStatRecords.stream()
                    .filter(costStatRecord -> costStatRecord.hasIsProjected()
                            && costStatRecord.getIsProjected())
                    .forEach(costStatRecord -> {
                        List<StatRecord> statRecordList = costStatRecord.getStatRecordsList();
                        for (StatRecord record : statRecordList) {
                            Long volumeId = record.getAssociatedEntityId();
                            StatApiDTO statApiDTO = createStatApiDTO(StringConstants.COST_PRICE,
                                    record.getUnits(), record.getValues().getTotal(),
                                    null, null, null,
                                    record.getCategory().toString(), false);
                            result.put(volumeId, statApiDTO);
                        }
                    });
        } catch (StatusRuntimeException e) {
            logger.error("Error when getting cost for volumes: ", e);
        }
        return result;
    }

    /**
     * Fetch all the regions and create mapping from region id to region. It only fetch from real
     * time topology since it should be same in plan topology.
     */
    private Map<Long, ApiPartialEntity> fetchRegions() {
        return repositoryApi.newSearchRequest(SearchParameters.newBuilder()
                .setStartingFilter(SearchProtoUtil.entityTypeFilter(ApiEntityType.REGION.apiStr()))
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
                .setStartingFilter(SearchProtoUtil.entityTypeFilter(ApiEntityType.STORAGE_TIER.apiStr()))
                .build())
            .getMinimalEntities().collect(Collectors.toMap(MinimalEntity::getOid,
                        ServiceEntityMapper::toBaseServiceEntityApiDTO));
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
        retVal.setUuid(storage.getOid() + file.getPath());
        retVal.setDisplayName(file.getPath());
        retVal.setEnvironmentType(EnvironmentType.ONPREM);
        retVal.setProvider(ServiceEntityMapper.toBaseServiceEntityApiDTO(storage));
        retVal.setLastModified(file.getModificationTimeMs());
        retVal.setTier(UNKNOWN);
        // storage amount stats

        retVal.setStats(Collections.singletonList(createStatApiDTO(
            CommodityTypeUnits.STORAGE_AMOUNT.getMixedCase(),
            CommodityTypeUnits.STORAGE_AMOUNT.getUnits(), file.getSizeKb() / 1024F,
            file.getSizeKb() / 1024F, ServiceEntityMapper.toBaseServiceEntityApiDTO(storage), file.getPath(),
                null, false)));
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
     * @param fetchAttachmentHistory true if attachment history should be retrieved for the volume
     * @return VirtualDiskApiDTO representing the volume
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    private VirtualDiskApiDTO convert(@Nonnull TopologyEntityDTO volume,
            @Nonnull Map<Long, TopologyEntityDTO> vmByVolumeId,
            @Nonnull Map<Long, ServiceEntityApiDTO> storageTierByVolumeId,
            @Nonnull Map<Long, ApiPartialEntity> regionByVolumeId,
            @Nonnull Multimap<Long, StatApiDTO> volumeCostStatById,
            boolean fetchAttachmentHistory)
            throws InterruptedException, ConversionException {
        long volumeId = volume.getOid();
        // the VirtualDiskApiDTO to return
        VirtualDiskApiDTO virtualDiskApiDTO = new VirtualDiskApiDTO();
        virtualDiskApiDTO.setUuid(String.valueOf(volumeId));
        virtualDiskApiDTO.setDisplayName(volume.getDisplayName());
        if (volume.hasEnvironmentType()) {
            virtualDiskApiDTO.setEnvironmentType(EnvironmentTypeMapper.fromXLToApi(
                                                    volume.getEnvironmentType()));
        }

        // find region for the volume and set it in VirtualDiskApiDTO
        final ApiPartialEntity region = regionByVolumeId.get(volume.getOid());
        if (region != null) {
            virtualDiskApiDTO.setDataCenter(ServiceEntityMapper.toBaseServiceEntityApiDTO(region));
        }
        // set storage tier
        final ServiceEntityApiDTO storageTier = storageTierByVolumeId.get(volumeId);
        if (storageTier != null) {
            // set tier
            virtualDiskApiDTO.setTier(storageTier.getDisplayName());
            // set storage tier as provider
            virtualDiskApiDTO.setProvider(storageTier);
        }

        repositoryApi.newSearchRequest(
                SearchProtoUtil.neighborsOfType(volumeId,
                        TraversalDirection.OWNED_BY,
                        ApiEntityType.BUSINESS_ACCOUNT))
                .getSEList()
                .forEach(businessAccount -> virtualDiskApiDTO.setBusinessAccount(businessAccount));

        // set attached VM (uuid + displayName)
        TopologyEntityDTO vmDTO = vmByVolumeId.get(volume.getOid());

        // commodity used
        float storageAmountUsed = 0.0f;
        float storageAccessUsed = 0.0f;
        float ioThroughputUsed = 0.0f;
        // if vmDTO is not null, it means attached volume; if null, then it is unattached volume, used is 0
        if (vmDTO != null) {
            // set attached vm
            BaseApiDTO vm = new BaseApiDTO();
            vm.setUuid(String.valueOf(vmDTO.getOid()));
            vm.setDisplayName(vmDTO.getDisplayName());
            virtualDiskApiDTO.setAttachedVirtualMachine(
                    ServiceEntityMapper.toBaseServiceEntityApiDTO(vmDTO));

            for (CommoditiesBoughtFromProvider cbfp : vmDTO.getCommoditiesBoughtFromProvidersList()) {
                final long volumeOid = volume.getOid();
                if (cbfp.getProviderId() == volumeOid || cbfp.getVolumeId() == volumeOid) {
                    for (CommodityBoughtDTO cb : cbfp.getCommodityBoughtList()) {
                        if (cb.getCommodityType().getType() == CommodityType.STORAGE_AMOUNT_VALUE) {
                            storageAmountUsed = (float)cb.getUsed();
                        } else if (cb.getCommodityType().getType() == CommodityType.STORAGE_ACCESS_VALUE) {
                            storageAccessUsed = (float)cb.getUsed();
                        } else if (cb.getCommodityType().getType() == CommodityType.IO_THROUGHPUT_VALUE) {
                            ioThroughputUsed = (float)cb.getUsed();
                        }
                    }
                }
            }
        }

        if (volume.hasTypeSpecificInfo() && volume.getTypeSpecificInfo().hasVirtualVolume()) {
            VirtualVolumeInfo volumeInfo = volume.getTypeSpecificInfo().getVirtualVolume();
            if (volumeInfo.hasSnapshotId()) {
                virtualDiskApiDTO.setSnapshotId(volumeInfo.getSnapshotId());
            }
            if (volumeInfo.hasAttachmentState()) {
                virtualDiskApiDTO.setAttachmentState(volumeInfo.getAttachmentState().name());
            }
            if (volumeInfo.hasEncryption()) {
                String ENCRYPTION_STATE = "Disabled";
                if (volumeInfo.getEncryption()) {
                    ENCRYPTION_STATE = "Enabled";
                }
                 virtualDiskApiDTO.setEncryption(ENCRYPTION_STATE);
            }
            if (volumeInfo.hasIsEphemeral()) {
                virtualDiskApiDTO.setEphemeral(Boolean.toString(volumeInfo.getIsEphemeral()));
            }
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
            statDTOs.addAll(volumeCostStatById.get(volume.getOid()));
        }
        float storageAmountCapacity = getCommodityCapacity(volume, CommodityType.STORAGE_AMOUNT);
        float storageAccessCapacity = getCommodityCapacity(volume, CommodityType.STORAGE_ACCESS);
        float ioThroughputCapacity = getCommodityCapacity(volume, CommodityType.IO_THROUGHPUT);
        // storage amount stats
        // Note: Different units are used for ON-PERM and CLOUD.  But for api requires
        //       the same commodity type.
        if (isCloudEntity(volume)) {
            statDTOs.add(createStatApiDTO(CommodityTypeUnits.STORAGE_AMOUNT.getMixedCase(),
                CLOUD_STORAGE_AMOUNT_UNIT, convertStorageAmountToCloudStorageAmount(storageAmountUsed),
                convertStorageAmountToCloudStorageAmount(storageAmountCapacity), storageTier, volume.getDisplayName(), null, false));
        } else {
            statDTOs.add(createStatApiDTO(CommodityTypeUnits.STORAGE_AMOUNT.getMixedCase(),
                CommodityTypeUnits.STORAGE_AMOUNT.getUnits(), storageAmountUsed,
                storageAmountCapacity, storageTier, volume.getDisplayName(), null, false));
        }
        // storage access stats
        statDTOs.add(createStatApiDTO(CommodityTypeUnits.STORAGE_ACCESS.getMixedCase(),
            CommodityTypeUnits.STORAGE_ACCESS.getUnits(), storageAccessUsed,
            storageAccessCapacity, storageTier, volume.getDisplayName(), null, false));

        // storage throughput stats
        statDTOs.add(createStatApiDTO(CommodityTypeUnits.IO_THROUGHPUT.getMixedCase(),
            CommodityTypeUnits.IO_THROUGHPUT.getUnits(), ioThroughputUsed,
            ioThroughputCapacity, storageTier, volume.getDisplayName(), null, false));

        virtualDiskApiDTO.setStats(statDTOs);

        if (virtualDiskApiDTO.getEnvironmentType() == EnvironmentType.CLOUD
                && AttachmentState.UNATTACHED.name()
                .equals(virtualDiskApiDTO.getAttachmentState()) && fetchAttachmentHistory) {
            populateUnattachedHistoryInfo(volume, virtualDiskApiDTO);
        }
        return virtualDiskApiDTO;
    }

    private static float getCommodityCapacity(
            @Nonnull final TopologyEntityDTO volume,
            @Nonnull final CommodityType commodityType) {
        return volume.getCommoditySoldListList().stream()
                .filter(commodity -> commodity.getCommodityType().getType()
                        == commodityType.getNumber())
                .map(CommoditySoldDTO::getCapacity)
                .findAny().orElse(0D).floatValue();
    }

    private void populateUnattachedHistoryInfo(final TopologyEntityDTO volume,
                                               final VirtualDiskApiDTO virtualDiskApiDTO)
        throws InterruptedException {
        final GetMostRecentStatResponse response = retrieveUnattachedHistoryStat(volume);
        if (response.hasSnapshotDate() && response.hasEpoch()) {
            final long currentTime = System.currentTimeMillis();
            final long lastAttachedTime = response.getSnapshotDate();
            if (currentTime > lastAttachedTime) {
                final long numDaysUnattached =
                        TimeUnit.MILLISECONDS.toDays(currentTime - lastAttachedTime);
                String numDaysUnattachedDisplayValue = Long.toString(numDaysUnattached);
                if (response.getEpoch() == StatHistoricalEpoch.MONTH) {
                    numDaysUnattachedDisplayValue += "+";
                }
                numDaysUnattachedDisplayValue += numDaysUnattached == 1 ? " day" : " days";
                virtualDiskApiDTO.setNumDaysUnattached(numDaysUnattachedDisplayValue);
                if (response.hasEntityDisplayName()) {
                    virtualDiskApiDTO.setLastAttachedVm(response.getEntityDisplayName());
                }
            }
            logger.trace("Last attached history for volume: {}, days: {}, vm name: {}",
                    volume.getOid(), virtualDiskApiDTO.getNumDaysUnattached(),
                    virtualDiskApiDTO.getLastAttachedVm());
        }
    }

    private GetMostRecentStatResponse retrieveUnattachedHistoryStat(
            final TopologyEntityDTO volume) throws InterruptedException {
        final GetMostRecentStatRequest request = GetMostRecentStatRequest.newBuilder()
                .setCommodityName(StringConstants.STORAGE_AMOUNT)
                .setEntityType(StringConstants.VIRTUAL_MACHINE)
                .setProviderId(Long.toString(volume.getOid()))
                .build();
        final GetMostRecentStatResponse response = executeGetMostRecentStatQuery(request, volume);
        logger.debug("Unattached volume history for volume: {}, request: {}, response: {}",
                volume::getOid, () -> request, () -> response);
        return response;
    }

    private GetMostRecentStatResponse executeGetMostRecentStatQuery(
        final GetMostRecentStatRequest request, final TopologyEntityDTO volume)
        throws InterruptedException {
        final Future<GetMostRecentStatResponse> responseFuture =
            CompletableFuture.supplyAsync(() -> historyRpcService
                .withDeadlineAfter(getMostRecentStatRpcDeadlineDurationSeconds,
                    TimeUnit.SECONDS).getMostRecentStat(request));
        GetMostRecentStatResponse response = GetMostRecentStatResponse.newBuilder().build();
        try {
            response = responseFuture.get(getMostRecentStatRpcFutureTimeoutSeconds,
                TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            logger.debug("Error encountered while retrieving volume attachment history for: "
                + volume.getOid(), e.getCause());
        } catch (TimeoutException e) {
            logger.error("Timed out while retrieving volume attachment history for volume: {}",
                volume::getOid);
            responseFuture.cancel(true);
        }
        return response;
    }

    /**
     * Helper method to convert Storage Amount to the preferred unit value for Cloud.
     *
     * @param storageAmount Original storage Amount in the unit of {@link CommodityTypeUnits}.STORAGE_AMOUNT
     * @return the converted amount in unit of Cloud Storage
     */
    private static float convertStorageAmountToCloudStorageAmount(float storageAmount) {
        switch (CommodityTypeUnits.STORAGE_AMOUNT.getUnits()) {
            case "MB":
                return storageAmount * (Units.MBYTE / CLOUD_STORAGE_AMOUNT_UNIT_IN_BYTE);
            case "KB":
                return storageAmount * (Units.KBYTE / CLOUD_STORAGE_AMOUNT_UNIT_IN_BYTE);
            case "GB":
                return storageAmount * (Units.GBYTE / CLOUD_STORAGE_AMOUNT_UNIT_IN_BYTE);
            case "TB":
                return storageAmount * (Units.TBYTE / CLOUD_STORAGE_AMOUNT_UNIT_IN_BYTE);
            default:
                logger.error("Undefined Storage Amount Units {}.", CommodityTypeUnits.STORAGE_AMOUNT.getUnits());
                return storageAmount;
        }
    }

    /*
     * Helper method to create StatApiDTO for given stats.
     *
     * @param statName Stat name
     * @param statUnit Stat unit
     * @param used "used" value
     * @param capacity capacity value
     * @param relatedEntity related entity
     * @param volumeName volume name
     * @param costComponent Name of the cost component category if not null.
     * @param isBeforePlan true is the stats is the before action value, false if it is the projected value.
     * @return
     */
    private StatApiDTO createStatApiDTO(@Nonnull String statName,
            @Nonnull String statUnit,
            float used,
            @Nullable Float capacity,
            @Nullable ServiceEntityApiDTO relatedEntity,
            @Nullable String volumeName,
            @Nullable String costComponent,
            boolean isBeforePlan) {
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
        if (capacity != null) {
            StatValueApiDTO capacityDTO = new StatValueApiDTO();
            capacityDTO.setAvg(capacity);
            capacityDTO.setMin(capacity);
            capacityDTO.setMax(capacity);
            capacityDTO.setTotal(capacity);
            statApiDTO.setCapacity(capacityDTO);
        }

        // related entity
        if (relatedEntity != null) {
            statApiDTO.setRelatedEntity(relatedEntity);

        }

        // filters
        List<StatFilterApiDTO> filters = Lists.newArrayList();
        if (volumeName != null) {
            StatFilterApiDTO filter1 = new StatFilterApiDTO();
            filter1.setType("key");
            filter1.setValue(volumeName);
            filters.add(filter1);
        }

        StatFilterApiDTO filter2 = new StatFilterApiDTO();
        filter2.setType("relation");
        filter2.setValue("bought");
        filters.add(filter2);

        if (costComponent != null) {
            StatFilterApiDTO costComponentFilter = new StatFilterApiDTO();
            costComponentFilter.setType(COSTCOMPONENT);
            costComponentFilter.setValue(costComponent);
            filters.add(costComponentFilter);
        }

        if (isBeforePlan) {
            StatFilterApiDTO resultTypeFilter = new StatFilterApiDTO();
            resultTypeFilter.setType(StringConstants.RESULTS_TYPE);
            resultTypeFilter.setValue(StringConstants.BEFORE_PLAN);
            filters.add(resultTypeFilter);
        }

        statApiDTO.setFilters(filters);
        return statApiDTO;
    }
}
