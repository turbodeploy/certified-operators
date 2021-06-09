package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import io.grpc.StatusRuntimeException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.conversion.entity.CommodityTypeMapping;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.VirtualDiskApiDTO;
import com.vmturbo.api.dto.entityaspect.VirtualDisksAspectApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.enums.StorageCompatibility;
import com.vmturbo.api.enums.StorageUsageType;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.common.api.mappers.EnvironmentTypeMapper;
import com.vmturbo.common.protobuf.cloud.CloudCommon.EntityFilter;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsResponse;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.stats.Stats.GetVolumeAttachmentHistoryRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetVolumeAttachmentHistoryResponse;
import com.vmturbo.common.protobuf.stats.Stats.GetVolumeAttachmentHistoryResponse.VolumeAttachmentHistory;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
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
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.StorageCompatibilityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.UsageType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.VirtualVolumeFileDescriptor;

/**
 * Mapper for getting virtual disks aspect.
 */
public class VirtualVolumeAspectMapper extends AbstractAspectMapper {
    private static final Collection<Integer> STORAGE_PROVIDER_TYPES =
                    ImmutableSet.of(
                                    // Migrate to Cloud Plan
                                    EntityType.STORAGE_VALUE,
                                    // Cloud to Cloud Plan
                                    EntityType.VIRTUAL_VOLUME_VALUE);

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

    private final long getVolumeAttachmentHistoryRpcFutureTimeoutSeconds;

    // Function to get the "correct" used value which was used in the analysis.
    private static final Function<CommoditySoldDTO, Float> getUsedFromCommodity = commoditySoldDTO -> {
        if (commoditySoldDTO.hasHistoricalUsed()) {
            final HistoricalValues historicalUsed = commoditySoldDTO.getHistoricalUsed();
            if (historicalUsed.hasPercentile() && commoditySoldDTO.hasCapacity()) {
                return (float)(historicalUsed.getPercentile() * commoditySoldDTO.getCapacity());
            } else {
                return (float)historicalUsed.getHistUtilization();
            }
        }
        return (float)commoditySoldDTO.getUsed();
    };

    private static final String ENCRYPTION_STATE_ENABLED = "Enabled";

    private static final String ENCRYPTION_STATE_DISABLED = "Disabled";

    public VirtualVolumeAspectMapper(@Nonnull final CostServiceBlockingStub costServiceRpc,
                                     @Nonnull final RepositoryApi repositoryApi,
                                     @Nonnull final StatsHistoryServiceBlockingStub
                                         historyRpcService,
                                     final long getVolumeAttachmentHistoryRpcFutureTimeoutSeconds) {
        this.costServiceRpc = costServiceRpc;
        this.repositoryApi = repositoryApi;
        this.historyRpcService = historyRpcService;
        this.getVolumeAttachmentHistoryRpcFutureTimeoutSeconds =
            getVolumeAttachmentHistoryRpcFutureTimeoutSeconds;
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

    @Nonnull
    @Override
    public Optional<Map<Long, EntityAspect>> mapPlanEntityToAspectBatch(
        @Nonnull List<TopologyEntityDTO> entities, final long planTopologyContextId)
        throws InterruptedException, ConversionException, InvalidOperationException {
        throw new InvalidOperationException(
            String.format("Plan entity aspects not supported by {}", getClass().getSimpleName()));
    }

    @Nonnull
    @Override
    public Optional<Map<Long, EntityAspect>> mapPlanEntityToAspectBatchPartial(
        @Nonnull List<ApiPartialEntity> entities, final long planTopologyContextId)
        throws InterruptedException, ConversionException, InvalidOperationException {
        throw new InvalidOperationException(
            String.format("Plan entity aspects not supported by {}", getClass().getSimpleName()));
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
            virtualDisks.add(convert(volume, new HashMap<>(), vmByVolumeId, storageTierByVolumeId,
                    regionByVolumeId, volumeCostStatById, Collections.emptyMap()));
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
        // This will be used for debug output
        final Multimap<String, Long> missingProjectedVolumes = HashMultimap.create();
        // VM entities from source topology
        final Map<Long, TopologyEntityDTO> sourceVms =
                        requestEntities(vmIds, topologyContextId, false);

        // VM entities from projected topology
        final Map<Long, TopologyEntityDTO> projectedVms =
                        requestEntities(vmIds, topologyContextId, true);

        // Mapping from Volume identifier to consuming or connected VM
        final Map<Long, Long> volumeIdToVmId = collectVolumesOfVms(sourceVms);

        // Map of volume ID to volume entities on the source topology
        final Set<Long> volumeIds = volumeIdToVmId.keySet();
        final Map<Long, TopologyEntityDTO> sourceVolumes =
                        requestEntities(volumeIds, topologyContextId, false);

        // Map of volume ID to volume entities on the projected topology
        // there may be less projected volumes than source ones
        final Map<Long, Long> projVolumeIdToVmId = collectVolumesOfVms(projectedVms);
        // Inverse map so that projected volumes can be accessed by vm ID later on
        final Map<Long, Set<Long>> projVmIdToVolumeId = projVolumeIdToVmId.entrySet().stream()
            .collect(Collectors.groupingBy(Map.Entry::getValue,
                Collectors.mapping(Map.Entry::getKey, Collectors.toSet())));
        final Map<Long, TopologyEntityDTO> projectedVolumes = requestEntities(projVolumeIdToVmId.keySet(),
                        topologyContextId, true);

        // Map of volume ID to a list of cost stats
        final Multimap<Long, StatApiDTO> volIdToCostStatsMap =
                        getVolumeCostStats(projectedVolumes.values(), topologyContextId);

        // mapping from zone id to region
        final Map<Long, ApiPartialEntity> regionByZoneId = new HashMap<>();
        // fetch all the regions and create mapping from region id to region
        final Map<Long, ApiPartialEntity> regionById = fetchRegions();
        regionById.values().forEach(region ->
                region.getConnectedToList().stream()
                        .filter(connectedEntity -> connectedEntity.getEntityType() == EntityType.AVAILABILITY_ZONE_VALUE)
                        .forEach(connectedEntity -> regionByZoneId.put(connectedEntity.getOid(), region))
        );

        // mapping from volume id to storage tier
        final Map<Long, ServiceEntityApiDTO> storageTierByVolumeId = new HashMap<>();
        // mapping from volume id to region entity
        final Map<Long, ApiPartialEntity> regionByVolumeId = new HashMap<>();
        // fetch all the storage tiers and create mapping from tier id to tier
        final Map<Long, ServiceEntityApiDTO> storageTierById = fetchStorageTiers();
        for (Entry<Long, TopologyEntityDTO> vvIdToVv : projectedVolumes.entrySet()) {
            final long volumeId = vvIdToVv.getKey();
            final TopologyEntityDTO volume = vvIdToVv.getValue();
            for (ConnectedEntity connectedEntity : volume.getConnectedEntityListList()) {
                final int connectedEntityType = connectedEntity.getConnectedEntityType();
                final long connectedEntityId = connectedEntity.getConnectedEntityId();
                if (connectedEntityType == EntityType.REGION_VALUE) {
                    // volume connected to region (azure)
                    regionByVolumeId.put(volumeId, regionById.get(connectedEntityId));
                } else if (connectedEntityType == EntityType.AVAILABILITY_ZONE_VALUE) {
                    // volume connected to zone (aws)
                    regionByVolumeId.put(volumeId, regionByZoneId.get(connectedEntityId));
                }
            }
            volume.getCommoditiesBoughtFromProvidersList().stream()
                            .filter(commBought -> commBought.getProviderEntityType()
                                            == EntityType.STORAGE_TIER.getNumber())
                            .map(CommoditiesBoughtFromProvider::getProviderId)
                            .map(storageTierById::get).forEach(storageTier -> storageTierByVolumeId
                            .put(volumeId, storageTier));
        }

        final Map<Long, Collection<CommodityBoughtDTO>> volumeIdToSourceBoughtCommodities =
                        new HashMap<>();
        final Map<Long, List<CommoditySoldDTO>> volumeIdToSourceSoldCommodities = new HashMap<>();
        for (Entry<Long, TopologyEntityDTO> vvIdToVv : sourceVolumes.entrySet()) {
            final Long vvId = vvIdToVv.getKey();
            final TopologyEntityDTO vv = vvIdToVv.getValue();
            final Long vmId = volumeIdToVmId.get(vvId);
            final Set<Long> connectedProviders = vv.getConnectedEntityListList().stream()
                            .filter(ce -> STORAGE_PROVIDER_TYPES
                                            .contains(ce.getConnectedEntityType()))
                            .map(ConnectedEntity::getConnectedEntityId).collect(Collectors.toSet());
            for (CommoditiesBoughtFromProvider cbfp : sourceVms.get(vmId)
                            .getCommoditiesBoughtFromProvidersList()) {
                if (!STORAGE_PROVIDER_TYPES.contains(cbfp.getProviderEntityType())) {
                    continue;
                }
                if (connectedProviders.contains(cbfp.getProviderId())) {
                    volumeIdToSourceBoughtCommodities.put(vvId, cbfp.getCommodityBoughtList());
                }
            }
            // For Cloud Volume, use volume's commoditySold to create VirtualDiskApiDTO later
            volumeIdToSourceSoldCommodities.put(vvId, vv.getCommoditySoldListList());
        }
        final Map<Long, List<VirtualDiskApiDTO>> virtualDisksByVmId = new HashMap<>();
        for (final Entry<Long, TopologyEntityDTO> vmIdToVm : projectedVms.entrySet()) {
            final Long vmId = vmIdToVm.getKey();
            final TopologyEntityDTO vm = vmIdToVm.getValue();
            // VM migrated to cloud should buy storage commodities from VV only
            final Collection<CommoditiesBoughtFromProvider> commsBoughtFromStorageOrVV =
                            vm.getCommoditiesBoughtFromProvidersList().stream()
                                            .filter(commList -> STORAGE_PROVIDER_TYPES
                                                            .contains(commList
                                                                            .getProviderEntityType()))
                                            .collect(Collectors.toList());
            final Set<Long> projectedVolIds = projVmIdToVolumeId.get(vmId);
            if (CollectionUtils.isNotEmpty(projectedVolIds)) {
                final List<VirtualDiskApiDTO> virtualDiskApiDTOs = new ArrayList<>(projectedVolIds.size());
                for (final long volId : projectedVolIds) {
                    final TopologyEntityDTO projectedVolume = projectedVolumes.get(volId);
                    if (projectedVolume == null) {
                        // sometimes we may not get projected volume entities from repository
                        // one example of those would be inaccessible VMs
                        if (logger.isTraceEnabled()) {
                            if (projectedVms.get(vmId) != null) {
                                missingProjectedVolumes.put(projectedVms.get(vmId).getDisplayName(),
                                    volId);
                            }
                        }
                        continue;
                    }
                    Collection<CommodityBoughtDTO> projectedBoughtCommodities = new HashSet<>();
                    // Note that on-prem VM will not buy commodities from virtual volume if virtual
                    // volume analysis is turned off.
                    // In that case we pass commodities bought from storage.
                    final CommoditiesBoughtFromProvider commBoughtFromVol = commsBoughtFromStorageOrVV
                        .stream()
                        .filter(commBought -> volId == commBought.getProviderId()).findFirst()
                        .orElse(null);
                    if (commBoughtFromVol != null) {
                        projectedBoughtCommodities = commBoughtFromVol.getCommodityBoughtList();
                    } else {
                        for (final CommoditiesBoughtFromProvider cbfp : getStorageCommoditiesBought(
                            commsBoughtFromStorageOrVV, projectedVolume)) {
                            projectedBoughtCommodities.addAll(cbfp.getCommodityBoughtList());
                        }
                    }
                    final Collection<StatApiDTO> costStats = volIdToCostStatsMap.get(volId);
                    final Collection<CommodityBoughtDTO> sourceBoughtCommodities = volumeIdToSourceBoughtCommodities
                        .getOrDefault(volId, Collections.emptySet());
                    final VirtualDiskApiDTO virtualDiskApiDTO = createVirtualDiskApiDTO(vm,
                        projectedVolume,
                        sourceBoughtCommodities,
                        projectedBoughtCommodities,
                        volumeIdToSourceSoldCommodities.getOrDefault(volId, Collections.emptyList()),
                        costStats, regionByVolumeId, storageTierByVolumeId,
                        projectedVolumes.size() == 1);
                    virtualDiskApiDTOs.add(virtualDiskApiDTO);
                }
                virtualDisksByVmId.put(vmId, virtualDiskApiDTOs);
            }
        }
        if (logger.isTraceEnabled() && !missingProjectedVolumes.isEmpty()) {
            missingProjectedVolumes.asMap().forEach((vmName, colIds) ->
                logger.trace("No projected volumes found for vm [{}], volume ids [{}]",
                    vmName, colIds.stream().map(String::valueOf).collect(
                        Collectors.joining(", "))));
        }
        return virtualDisksByVmId;
    }

    /**
     * Get storages connected to the provided on-prem virtual volume.
     *
     * @param virtualVolume on-prem virtual volume
     * @return connected on-prem storage, or null if none found
     */
    @Nonnull
    private static Set<ConnectedEntity> getConnectedStorageProviders(
        @Nonnull final TopologyEntityDTO virtualVolume) {
        return virtualVolume.getConnectedEntityListList().stream()
            .filter(ce -> STORAGE_PROVIDER_TYPES
                .contains(ce.getConnectedEntityType()))
                .collect(Collectors.toSet());
    }

    /**
     * Get storage commodities bought for the specified volume.
     *
     * @param storageCommBought commodities bought from storage providers
     * @param volume volume
     * @return storage commodities bought by the volume or empty collection if none found
     */
    @Nonnull
    private static Collection<CommoditiesBoughtFromProvider> getStorageCommoditiesBought(
        @Nonnull Collection<CommoditiesBoughtFromProvider> storageCommBought,
        @Nonnull final TopologyEntityDTO volume) {
        final Collection<CommoditiesBoughtFromProvider> retSet = new HashSet<>();
        final Set<ConnectedEntity> connectedStorages = getConnectedStorageProviders(volume);
        if (CollectionUtils.isNotEmpty(connectedStorages)) {
            final Set<Long> connectedStorageIds = connectedStorages.stream()
                .map(ConnectedEntity::getConnectedEntityId)
                .collect(Collectors.toSet());
            storageCommBought.forEach(cbfp -> {
                if (connectedStorageIds.contains(cbfp.getProviderId())) {
                    retSet.add(cbfp);
                }
            });
        }
        return retSet;
    }

    @Nonnull
    private static Map<Long, Long> collectVolumesOfVms(@Nonnull Map<Long, TopologyEntityDTO> vms) {
        Map<Long, Long> volumeIdToVmId = new HashMap<>();
        vms.forEach((vmId, vm) -> {
            // In old model (On Prem) volumes are attached to VMs using ConnectedTo relationship
            collectVolumeIds(TopologyEntityDTO::getConnectedEntityListList,
                            ConnectedEntity::getConnectedEntityType,
                            ConnectedEntity::getConnectedEntityId, vm, volumeIdToVmId);
            // In new model (Cloud) volumes are attached to VMs using bought commodities
            collectVolumeIds(TopologyEntityDTO::getCommoditiesBoughtFromProvidersList,
                            CommoditiesBoughtFromProvider::getProviderEntityType,
                            CommoditiesBoughtFromProvider::getProviderId, vm, volumeIdToVmId);
        });
        return volumeIdToVmId;
    }

    @Nonnull
    private Map<Long, TopologyEntityDTO> requestEntities(@Nonnull Set<Long> vmIds,
                    @Nullable Long topologyContextId, boolean projected) {
        final MultiEntityRequest request =
                        repositoryApi.entitiesRequest(vmIds).contextId(topologyContextId);
        if (projected) {
            request.projectedTopology();
        }
        return request.getFullEntities()
                        .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));
    }

    private static <T> void collectVolumeIds(
                    Function<TopologyEntityDTO, Collection<T>> relatedEntitiesGetter,
                    Function<T, Integer> entityTypeExtractor,
                    Function<T, Long> relatedEntityIdExtractor, TopologyEntityDTO vm,
                    Map<Long, Long> volumeIdToVmId) {
        relatedEntitiesGetter.apply(vm).stream().filter(e -> entityTypeExtractor.apply(e)
                        == EntityType.VIRTUAL_VOLUME_VALUE).map(relatedEntityIdExtractor)
                        .forEach(volumeId -> volumeIdToVmId.put(volumeId, vm.getOid()));
    }

    /**
     * Create a VirtualDiskApiDTO object from data collected about volume and VM.
     *
     * @param vm VM entity
     * @param volume Volume entity
     * @param beforeActionComms the commodity bought list of the VM before the action
     * @param afterActionComms the commodity bought list of the VM after the action
     * @param beforeActionCommSoldForCloudVV commodity sold list of Cloud VV before the action. For On-perm, this will be empty list.
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
                                                      Collection<CommodityBoughtDTO> beforeActionComms,
                                                      Collection<CommodityBoughtDTO> afterActionComms,
                                                      @Nonnull Collection<CommoditySoldDTO> beforeActionCommSoldForCloudVV,
                                                      Collection<StatApiDTO> costStats,
                                                      final Map<Long, ApiPartialEntity> regionByVolumeId,
                                                      final Map<Long, ServiceEntityApiDTO> storageTierByVolumeId,
                                                      boolean fetchAttachmentHistory)
            throws InterruptedException, ConversionException {
        final VirtualDiskApiDTO virtualDiskApiDTO = convertToApiDto(volume, regionByVolumeId,
                storageTierByVolumeId);
        virtualDiskApiDTO.setAttachedVirtualMachine(ServiceEntityMapper.toBaseServiceEntityApiDTO(vm));

        final boolean isCloudVolume = isCloudEntity(volume);
        final String storageAmountUnit = isCloudVolume ? CLOUD_STORAGE_AMOUNT_UNIT :
            CommodityTypeMapping.getUnitForCommodityType(CommonDTO.CommodityDTO.CommodityType.STORAGE_AMOUNT);

        List<StatApiDTO> statDTOs = Lists.newArrayList();
        // Add projected stats
        double afterActionStorageAmountUsed = 0d;
        double afterActionStorageAccessUsed = 0d;
        double afterActionIOThroughputUsed = 0d;
        for (CommodityBoughtDTO commodity : afterActionComms) {
            switch (commodity.getCommodityType().getType()) {
                case CommodityType.STORAGE_AMOUNT_VALUE:
                    afterActionStorageAmountUsed =  isCloudVolume ? commodity.getUsed() / Units.KIBI : commodity.getUsed();
                    break;
                case CommodityType.STORAGE_ACCESS_VALUE:
                    afterActionStorageAccessUsed = commodity.getUsed();
                    break;
                case CommodityType.IO_THROUGHPUT_VALUE:
                    afterActionIOThroughputUsed = commodity.getUsed();
                    break;
            }
        }

        statDTOs.add(createStatApiDTO(CommodityTypeMapping.getMixedCaseFromCommodityType(CommonDTO.CommodityDTO.CommodityType.STORAGE_AMOUNT),
                storageAmountUnit, (float)afterActionStorageAmountUsed,
            (float)(getCommodityCapacity(volume, CommodityType.STORAGE_AMOUNT) / Units.KIBI),
                null, volume.getDisplayName(), null, false));
        statDTOs.add(createStatApiDTO(CommodityTypeMapping.getMixedCaseFromCommodityType(CommonDTO.CommodityDTO.CommodityType.STORAGE_ACCESS),
                CommodityTypeMapping.getUnitForCommodityType(CommonDTO.CommodityDTO.CommodityType.STORAGE_ACCESS), (float)afterActionStorageAccessUsed,
                getCommodityCapacity(volume, CommodityType.STORAGE_ACCESS),
                null, volume.getDisplayName(), null, false));
        statDTOs.add(createStatApiDTO(CommodityTypeMapping.getMixedCaseFromCommodityType(CommonDTO.CommodityDTO.CommodityType.IO_THROUGHPUT),
                CommodityTypeMapping.getUnitForCommodityType(CommonDTO.CommodityDTO.CommodityType.IO_THROUGHPUT), (float)afterActionIOThroughputUsed,
                getCommodityCapacity(volume, CommodityType.IO_THROUGHPUT),
                null, volume.getDisplayName(), null, false));

        // Add stats for before action stats.
        if (beforeActionCommSoldForCloudVV.isEmpty()) {
            for (CommodityBoughtDTO commodity : beforeActionComms) {
                switch (commodity.getCommodityType().getType()) {
                    case CommodityType.STORAGE_AMOUNT_VALUE:
                        // Unit of storage amount in source topology is in MB.
                        final float storageAmountUsed = isCloudVolume ? (float)(commodity.getUsed() / Units.KIBI) : (float)commodity.getUsed();
                        statDTOs.add(createStatApiDTO(CommodityTypeMapping.getMixedCaseFromCommodityType(CommonDTO.CommodityDTO.CommodityType.STORAGE_AMOUNT),
                            storageAmountUnit, storageAmountUsed, storageAmountUsed,
                            null, volume.getDisplayName(), null, true));
                        break;
                    case CommodityType.STORAGE_ACCESS_VALUE:
                        statDTOs.add(createStatApiDTO(CommodityTypeMapping.getMixedCaseFromCommodityType(CommonDTO.CommodityDTO.CommodityType.STORAGE_ACCESS),
                            CommodityTypeMapping.getUnitForCommodityType(CommonDTO.CommodityDTO.CommodityType.STORAGE_ACCESS), (float)commodity.getUsed(),
                            (float)commodity.getUsed(),
                            null, volume.getDisplayName(), null, true));
                        break;
                    case CommodityType.IO_THROUGHPUT_VALUE:
                        statDTOs.add(createStatApiDTO(CommodityTypeMapping.getMixedCaseFromCommodityType(CommonDTO.CommodityDTO.CommodityType.IO_THROUGHPUT),
                            CommodityTypeMapping.getUnitForCommodityType(CommonDTO.CommodityDTO.CommodityType.IO_THROUGHPUT), (float)commodity.getUsed(),
                            (float)commodity.getUsed(),
                            null, volume.getDisplayName(), null, true));
                        break;
                }
            }
        } else {
            for (CommoditySoldDTO commoditySoldDTO : beforeActionCommSoldForCloudVV) {
                final float used = getUsedFromCommodity.apply(commoditySoldDTO);
                switch (commoditySoldDTO.getCommodityType().getType()) {
                    case CommodityType.STORAGE_AMOUNT_VALUE:
                        // Unit of storage amount in source topology is in MB.
                        final float storageAmountUsed = (float)(used / Units.KIBI);
                        statDTOs.add(createStatApiDTO(CommodityTypeMapping.getMixedCaseFromCommodityType(CommonDTO.CommodityDTO.CommodityType.STORAGE_AMOUNT),
                            storageAmountUnit, storageAmountUsed, storageAmountUsed,
                            null, volume.getDisplayName(), null, true));
                        break;
                    case CommodityType.STORAGE_ACCESS_VALUE:
                        statDTOs.add(createStatApiDTO(CommodityTypeMapping.getMixedCaseFromCommodityType(CommonDTO.CommodityDTO.CommodityType.STORAGE_ACCESS),
                            CommodityTypeMapping.getUnitForCommodityType(CommonDTO.CommodityDTO.CommodityType.STORAGE_ACCESS), used, used,
                            null, volume.getDisplayName(), null, true));
                        break;
                    case CommodityType.IO_THROUGHPUT_VALUE:
                        statDTOs.add(createStatApiDTO(CommodityTypeMapping.getMixedCaseFromCommodityType(CommonDTO.CommodityDTO.CommodityType.IO_THROUGHPUT),
                            CommodityTypeMapping.getUnitForCommodityType(CommonDTO.CommodityDTO.CommodityType.IO_THROUGHPUT), used, used,
                            null, volume.getDisplayName(), null, true));
                        break;
                }
            }
        }
        // Get cost stats
        if (costStats != null) {
            statDTOs.addAll(costStats);
        }
        virtualDiskApiDTO.setStats(statDTOs);
        return virtualDiskApiDTO;
    }

    /**
     * Map Virtual Volumes.
     *
     * @param volumeIds - uuids of unattached volumes
     * @param topologyContextId - context ID of topology
     * @param requestPlanCommStats - request plan commodity statistics or not
     * @return virtual volume aspect by volume uuid
     * @throws ConversionException if errors faced during converting data to API DTOs
     * @throws InterruptedException if thread has been interrupted
     */
    public Map<Long, List<VirtualDiskApiDTO>> mapVirtualVolumes(@Nonnull Set<Long> volumeIds,
                                                               final long topologyContextId,
                                                               final boolean requestPlanCommStats)
            throws ConversionException, InterruptedException {
        final List<TopologyEntityDTO> volumes = repositoryApi.entitiesRequest(volumeIds)
                .contextId(topologyContextId)
                .getFullEntities()
                .collect(Collectors.toList());
        VirtualDisksAspectApiDTO aspect = mapVirtualVolumes(volumes, topologyContextId, requestPlanCommStats);
        Map<Long, List<VirtualDiskApiDTO>> volIds2DTOs = new HashMap<>();
        if (aspect != null) {
            aspect.getVirtualDisks().stream()
                    .collect(Collectors.groupingBy(VirtualDiskApiDTO::getUuid))
                    .forEach((identifier, virtualDiskApiDTOList) -> {
                        if (!identifier.isEmpty()) {
                            volIds2DTOs.put(Long.valueOf(identifier), virtualDiskApiDTOList);
                        }
                    });
        }
        return volIds2DTOs;
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
        return mapVirtualVolumes(volumeDTOs, null, false);
    }

    @Nullable
    private VirtualDisksAspectApiDTO mapVirtualVolumes(@Nonnull List<TopologyEntityDTO> vols,
                                           @Nullable Long topologyContextId,
                                           final boolean requestPlanCommStats)
            throws InterruptedException, ConversionException {
        final Set<Long> volumeIds = new HashSet<>();
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
        final List<Long> unattachedVolumes = new ArrayList<>();
        vols.forEach(vol -> {
            volumeIds.add(vol.getOid());

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
            if (vol.getEnvironmentType() == EnvironmentTypeEnum.EnvironmentType.CLOUD
                && vol.hasTypeSpecificInfo()
                && vol.getTypeSpecificInfo().hasVirtualVolume()
                && vol.getTypeSpecificInfo().getVirtualVolume().getAttachmentState()
                == AttachmentState.UNATTACHED) {
                unattachedVolumes.add(vol.getOid());
            }
        });

        final Map<Long, VolumeAttachmentHistory> volumeAttachmentHistory =
            retrieveVolumeAttachmentHistory(unattachedVolumes);

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
        // Map of volume ID to volume entity on the plan projected topology.
        final Map<Long, TopologyEntityDTO> planProjectedVolumeMap = requestPlanCommStats
                ? repositoryApi.entitiesRequest(volumeIds)
                        .contextId(topologyContextId)
                        .projectedTopology()
                        .getFullEntities()
                        .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()))
                : new HashMap<>();
        for (TopologyEntityDTO volume : vols) {
            final VirtualDiskApiDTO disk = convert(volume, planProjectedVolumeMap, vmByVolumeId, storageTierByVolumeId,
                    regionByVolumeId, volumeCostStatById, volumeAttachmentHistory);
            virtualDisks.add(disk);
        }
        if (virtualDisks.isEmpty()) {
            return null;
        }

        final VirtualDisksAspectApiDTO aspect = new VirtualDisksAspectApiDTO();
        aspect.setVirtualDisks(virtualDisks);
        return aspect;
    }

    private Map<Long, VolumeAttachmentHistory> retrieveVolumeAttachmentHistory(
        final List<Long> unattachedVolumeOids) throws InterruptedException {
        if (unattachedVolumeOids.isEmpty()) {
            return Collections.emptyMap();
        }
        final GetVolumeAttachmentHistoryRequest request = GetVolumeAttachmentHistoryRequest
            .newBuilder()
            .addAllVolumeOid(unattachedVolumeOids)
            // single unattached volume passed to this method implies that this request is being
            // made for displaying Delete Volume Action details. For this case, last attached VM
            // names must be retrieved. This is a workaround as there is no mechanism currently
            // for the UI to include whether it needs last attached VM name information or not.
            // For all other cases such as Action Summary and Storage Breakdown widgets, there is no
            // need to retrieve last attached VM name.
            .setRetrieveVmNames(unattachedVolumeOids.size() == 1)
            .build();
        final Future<Iterator<GetVolumeAttachmentHistoryResponse>> responseFuture =
            CompletableFuture.supplyAsync(
                () -> historyRpcService.getVolumeAttachmentHistory(request));
        final List<GetVolumeAttachmentHistoryResponse> responses = new ArrayList<>();
        Iterator<GetVolumeAttachmentHistoryResponse> responseIterator;
        try {
            responseIterator =
                responseFuture.get(getVolumeAttachmentHistoryRpcFutureTimeoutSeconds,
                    TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            logger.error("Error encountered while retrieving volume attachment history for: {}",
                unattachedVolumeOids, e.getCause());
            responseIterator = Collections.emptyIterator();
        } catch (TimeoutException e) {
            logger.error("Timed out while retrieving volume attachment history for volume: {}",
                unattachedVolumeOids);
            responseIterator = Collections.emptyIterator();
        }
        responseIterator.forEachRemaining(responses::add);
        return responses.stream()
            .map(GetVolumeAttachmentHistoryResponse::getHistoryList)
            .flatMap(Collection::stream)
            .collect(Collectors.toMap(VolumeAttachmentHistory::getVolumeOid, Function.identity()));
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
    private Multimap<Long, StatApiDTO> getVolumeCostStats(@Nonnull Collection<TopologyEntityDTO> volumes,
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
            CommodityTypeMapping.getMixedCaseFromCommodityType(CommonDTO.CommodityDTO.CommodityType.STORAGE_AMOUNT),
            CommodityTypeMapping.getUnitForCommodityType(CommonDTO.CommodityDTO.CommodityType.STORAGE_AMOUNT), file.getSizeKb() / 1024F,
            file.getSizeKb() / 1024F, ServiceEntityMapper.toBaseServiceEntityApiDTO(storage), file.getPath(),
                null, false)));
        return retVal;
    }

    /**
     * Convert the given volume into VirtualDiskApiDTO.
     *
     * @param volume the volume entity to convert
     * @param planProjectedVolumeMap plan projected volume map to get plan related statistics
     * @param vmByVolumeId mapping from volume id to vm
     * @param storageTierByVolumeId mapping from volume id to storage tier
     * @param regionByVolumeId mapping from volume id to region
     * @param volumeCostStatById mapping from volume id to its cost stat
     * @param volumeAttachmentHistoryMap map from volume oid to Volume Attachment History
     * @return VirtualDiskApiDTO representing the volume
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    @Nonnull
    private VirtualDiskApiDTO convert(@Nonnull TopologyEntityDTO volume,
            @Nonnull Map<Long, TopologyEntityDTO> planProjectedVolumeMap,
            @Nonnull Map<Long, TopologyEntityDTO> vmByVolumeId,
            @Nonnull Map<Long, ServiceEntityApiDTO> storageTierByVolumeId,
            @Nonnull Map<Long, ApiPartialEntity> regionByVolumeId,
            @Nonnull Multimap<Long, StatApiDTO> volumeCostStatById,
            @Nonnull Map<Long, VolumeAttachmentHistory> volumeAttachmentHistoryMap)
            throws InterruptedException, ConversionException {
        final VirtualDiskApiDTO virtualDiskApiDTO = convertToApiDto(volume, regionByVolumeId,
                storageTierByVolumeId);

        // set attached VM (uuid + displayName)
        final long volumeId = volume.getOid();
        TopologyEntityDTO vmDTO = vmByVolumeId.get(volumeId);

        // commodity used
        float storageAmountUsed = 0.0f;
        float storageAccessUsed = 0.0f;
        float ioThroughputUsed = 0.0f;

        // If volumes have commodities and provide their own stats,
        // then use the get the stats directly from the commodities sold by the volumes
        for (CommoditySoldDTO cs : volume.getCommoditySoldListList()) {
            final int commodityType = cs.getCommodityType().getType();
            if (commodityType == CommodityType.STORAGE_AMOUNT_VALUE) {
                storageAmountUsed = (float)cs.getUsed();
            } else if (commodityType == CommodityType.STORAGE_ACCESS_VALUE) {
                storageAccessUsed = (float)cs.getUsed();
            } else if (commodityType == CommodityType.IO_THROUGHPUT_VALUE) {
                ioThroughputUsed = (float)cs.getUsed();
            }
        }

        // if vmDTO is not null, it means attached volume; if null, then it is unattached volume, used is 0
        if (vmDTO != null) {
            // set attached vm
            BaseApiDTO vm = new BaseApiDTO();
            vm.setUuid(String.valueOf(vmDTO.getOid()));
            vm.setDisplayName(vmDTO.getDisplayName());
            virtualDiskApiDTO.setAttachedVirtualMachine(
                    ServiceEntityMapper.toBaseServiceEntityApiDTO(vmDTO));

            if (volume.getCommoditySoldListList().size() == 0) {
                final Collection<Long> vvCommoditiesProviderIds = Stream.concat(Stream.of(volumeId),
                        volume.getConnectedEntityListList().stream()
                                .filter(e -> e.getConnectedEntityType()
                                        == EntityType.STORAGE_VALUE)
                                .map(ConnectedEntity::getConnectedEntityId))
                        .collect(Collectors.toSet());
                for (CommoditiesBoughtFromProvider cbfp : vmDTO.getCommoditiesBoughtFromProvidersList()) {
                    if (vvCommoditiesProviderIds.contains(cbfp.getProviderId())) {
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
        }

        // set following stats:
        //     cost: the cost of the volume, retrieved from cost component
        //     storage amount: set the used (comes from VM bought) and capacity (from volume info)
        //     storage access: set the used (comes from VM bought) and capacity (from volume info)
        // todo: currently we don't need to go through stats API since we have everything. If in the
        // future we need to support "stats/{volumeId}", we may need to move this logic to stats API
        List<StatApiDTO> statDTOs = Lists.newArrayList();
        // cost stats
        if (volumeCostStatById.containsKey(volumeId)) {
            statDTOs.addAll(volumeCostStatById.get(volumeId));
        }
        // commodity stats
        final TopologyEntityDTO planProjectedVolume = planProjectedVolumeMap.get(volumeId);
        final ServiceEntityApiDTO storageTier = storageTierByVolumeId.get(volumeId);
        if (planProjectedVolume == null) {
            getVolumeCommStats(volume, storageTier, statDTOs,
                    storageAmountUsed, storageAccessUsed, ioThroughputUsed, false);
        } else {
            // get before plan commodity stats from volume
            getVolumeCommStats(volume, storageTier, statDTOs,
                    storageAmountUsed, storageAccessUsed, ioThroughputUsed, true);
            // get after plan commodity stats from planProjectedVolume
            getVolumeCommStats(planProjectedVolume, null, statDTOs,
                    storageAmountUsed, storageAccessUsed, ioThroughputUsed, false);
        }
        final VolumeAttachmentHistory history = volumeAttachmentHistoryMap.get(volumeId);
        if (history != null && history.hasLastAttachedDateMs()) {
            final long currentTime = System.currentTimeMillis();
            final long lastAttachedDate = history.getLastAttachedDateMs();
            if (currentTime > lastAttachedDate) {
                virtualDiskApiDTO.setNumDaysUnattached(
                    Long.toString(TimeUnit.MILLISECONDS.toDays(currentTime - lastAttachedDate)));
            }
            final List<String> lastAttachedVms = history.getVmNameList();
            if (!lastAttachedVms.isEmpty()) {
                // Currently assume that a Volume can have exactly one VM name in the history.
                // This may not be the case once multi-attach Volumes are supported.
                virtualDiskApiDTO.setLastAttachedVm(lastAttachedVms.iterator().next());
            }
        }
        virtualDiskApiDTO.setStats(statDTOs);
        return virtualDiskApiDTO;
    }

    @Nonnull
    private VirtualDiskApiDTO convertToApiDto(
            @Nonnull final TopologyEntityDTO volume,
            @Nonnull Map<Long, ApiPartialEntity> regionByVolumeId,
            @Nonnull Map<Long, ServiceEntityApiDTO> storageTierByVolumeId)
            throws ConversionException, InterruptedException {
        final VirtualDiskApiDTO apiDto = new VirtualDiskApiDTO();
        final long volumeId = volume.getOid();
        apiDto.setUuid(String.valueOf(volumeId));
        apiDto.setDisplayName(volume.getDisplayName());

        final EnvironmentType volumeEnvironmentType = volume.hasEnvironmentType()
                ? EnvironmentTypeMapper.fromXLToApi(volume.getEnvironmentType())
                : EnvironmentType.UNKNOWN;
        apiDto.setEnvironmentType(volumeEnvironmentType);

        final ApiPartialEntity region = regionByVolumeId.get(volumeId);
        if (region != null) {
            apiDto.setDataCenter(ServiceEntityMapper.toBaseServiceEntityApiDTO(region));
        }

        final ServiceEntityApiDTO storageTier = storageTierByVolumeId.get(volumeId);
        if (storageTier != null) {
            apiDto.setTier(storageTier.getDisplayName());
            apiDto.setProvider(storageTier);
        }

        repositoryApi.newSearchRequest(
                SearchProtoUtil.neighborsOfType(volumeId,
                        TraversalDirection.OWNED_BY,
                        ApiEntityType.BUSINESS_ACCOUNT))
                .getSEList()
                .forEach(apiDto::setBusinessAccount);

        if (volume.hasTypeSpecificInfo() && volume.getTypeSpecificInfo().hasVirtualVolume()) {
            final VirtualVolumeInfo volumeInfo = volume.getTypeSpecificInfo().getVirtualVolume();
            if (volumeInfo.hasSnapshotId()) {
                apiDto.setSnapshotId(volumeInfo.getSnapshotId());
            }
            if (volumeInfo.hasAttachmentState()) {
                apiDto.setAttachmentState(volumeInfo.getAttachmentState().name());
            }
            if (volumeInfo.hasEncryption()) {
                final String encryptionState = volumeInfo.getEncryption()
                        ? ENCRYPTION_STATE_ENABLED
                        : ENCRYPTION_STATE_DISABLED;
                apiDto.setEncryption(encryptionState);
            }
            if (volumeInfo.hasIsEphemeral()) {
                apiDto.setEphemeral(Boolean.toString(volumeInfo.getIsEphemeral()));
            }
            if (volumeInfo.hasHourlyBilledOps()) {
                apiDto.setHourlyBilledOps(volumeInfo.getHourlyBilledOps());
            }
            // Get the most recent date from associated files
            apiDto.setLastModified(volumeInfo.getFilesList().stream()
                    .mapToLong(VirtualVolumeFileDescriptor::getModificationTimeMs)
                    .max().orElse(0));
            if (volumeInfo.hasStorageCompatibilityForConsumer()) {
                final StorageCompatibilityType storageCompatibility = volumeInfo.getStorageCompatibilityForConsumer();
                if (storageCompatibility == StorageCompatibilityType.PREMIUM) {
                    apiDto.setAttachedVMStorageCompatibility(StorageCompatibility.PREMIUM);
                } else if (storageCompatibility == StorageCompatibilityType.STANDARD) {
                    apiDto.setAttachedVMStorageCompatibility(StorageCompatibility.STANDARD);
                }
            }
            if (volumeInfo.hasUsageType()) {
                if (volumeInfo.getUsageType() == UsageType.SITE_RECOVERY) {
                    apiDto.setStorageUsageType(StorageUsageType.SITE_RECOVERY);
                } else if (volumeInfo.getUsageType() == UsageType.BACKUP) {
                    apiDto.setStorageUsageType(StorageUsageType.BACKUP);
                }
            }
        }

        return apiDto;
    }

    private void getVolumeCommStats(@Nonnull final TopologyEntityDTO volume,
                                    @Nullable final ServiceEntityApiDTO storageTier,
                                    @Nonnull final List<StatApiDTO> statDTOs,
                                    final float storageAmountUsed,
                                    final float storageAccessUsed,
                                    final float ioThroughputUsed,
                                    final boolean isBeforePlan) {
        float storageAmountCapacity = getCommodityCapacity(volume, CommodityType.STORAGE_AMOUNT);
        float storageAccessCapacity = getCommodityCapacity(volume, CommodityType.STORAGE_ACCESS);
        float ioThroughputCapacity = getCommodityCapacity(volume, CommodityType.IO_THROUGHPUT);
        // storage amount stats
        // Note: Different units are used for ON-PERM and CLOUD.  But for api requires
        //       the same commodity type.
        if (isCloudEntity(volume)) {
            statDTOs.add(createStatApiDTO(CommodityTypeMapping.getMixedCaseFromCommodityType(CommonDTO.CommodityDTO.CommodityType.STORAGE_AMOUNT),
                    CLOUD_STORAGE_AMOUNT_UNIT, convertStorageAmountToCloudStorageAmount(storageAmountUsed),
                    convertStorageAmountToCloudStorageAmount(storageAmountCapacity), storageTier, volume.getDisplayName(), null, isBeforePlan));
        } else {
            statDTOs.add(createStatApiDTO(CommodityTypeMapping.getMixedCaseFromCommodityType(CommonDTO.CommodityDTO.CommodityType.STORAGE_AMOUNT),
                    CommodityTypeMapping.getUnitForCommodityType(CommonDTO.CommodityDTO.CommodityType.STORAGE_AMOUNT), storageAmountUsed,
                    storageAmountCapacity, storageTier, volume.getDisplayName(), null, isBeforePlan));
        }
        // storage access stats
        statDTOs.add(createStatApiDTO(CommodityTypeMapping.getMixedCaseFromCommodityType(CommonDTO.CommodityDTO.CommodityType.STORAGE_ACCESS),
                CommodityTypeMapping.getUnitForCommodityType(CommonDTO.CommodityDTO.CommodityType.STORAGE_ACCESS), storageAccessUsed,
                storageAccessCapacity, storageTier, volume.getDisplayName(), null, isBeforePlan));

        // storage throughput stats
        statDTOs.add(createStatApiDTO(CommodityTypeMapping.getMixedCaseFromCommodityType(CommonDTO.CommodityDTO.CommodityType.IO_THROUGHPUT),
                CommodityTypeMapping.getUnitForCommodityType(CommonDTO.CommodityDTO.CommodityType.IO_THROUGHPUT), ioThroughputUsed,
                ioThroughputCapacity, storageTier, volume.getDisplayName(), null, isBeforePlan));
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

    /**
     * Helper method to convert Storage Amount to the preferred unit value for Cloud.
     *
     * @param storageAmount Original storage Amount in the unit of {@link CommonDTO.CommodityDTO.CommodityType}
     *                      .STORAGE_AMOUNT
     * @return the converted amount in unit of Cloud Storage
     */
    private static float convertStorageAmountToCloudStorageAmount(float storageAmount) {
        final String storageAmountCommodityType =
            CommodityTypeMapping.getUnitForCommodityType(CommonDTO.CommodityDTO.CommodityType.STORAGE_AMOUNT);
        switch (storageAmountCommodityType) {
            case "MB":
                return storageAmount * (Units.MBYTE / CLOUD_STORAGE_AMOUNT_UNIT_IN_BYTE);
            case "KB":
                return storageAmount * (Units.KBYTE / CLOUD_STORAGE_AMOUNT_UNIT_IN_BYTE);
            case "GB":
                return storageAmount * (Units.GBYTE / CLOUD_STORAGE_AMOUNT_UNIT_IN_BYTE);
            case "TB":
                return storageAmount * (Units.TBYTE / CLOUD_STORAGE_AMOUNT_UNIT_IN_BYTE);
            default:
                logger.error("Undefined Storage Amount Units {}.", storageAmountCommodityType);
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
