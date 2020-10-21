package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.CollectionUtils;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entityaspect.CloudAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.common.protobuf.VirtualMachineProtoUtil;
import com.vmturbo.common.protobuf.cost.Cost.EntityFilter;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.GetEntityReservedInstanceCoverageRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetEntityReservedInstanceCoverageResponse;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.search.Search.GraphRequest;
import com.vmturbo.common.protobuf.search.Search.GraphResponse;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo.DriverInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.VirtualMachineData.VMBillingType;

/**
 * Mapper for getting {@link CloudAspectApiDTO}.
 */
public class CloudAspectMapper extends AbstractAspectMapper {
    private static final Logger logger = LogManager.getLogger();
    private static final Map<Integer, Integer> WORKLOAD_ENTITY_TYPE_TO_TIER_TYPE =
            ImmutableMap.of(EntityType.VIRTUAL_MACHINE_VALUE, EntityType.COMPUTE_TIER_VALUE,
                    EntityType.DATABASE_SERVER_VALUE, EntityType.DATABASE_SERVER_TIER_VALUE,
                    EntityType.DATABASE_VALUE, EntityType.DATABASE_TIER_VALUE);

    private static final Set<Integer> AVAILABILITY_ZONE_AND_REGION =
            ImmutableSet.of(EntityType.AVAILABILITY_ZONE_VALUE, EntityType.REGION_VALUE);

    private final RepositoryApi repositoryApi;
    private final ReservedInstanceUtilizationCoverageServiceBlockingStub riCoverageService;
    private final GroupServiceGrpc.GroupServiceBlockingStub groupServiceBlockingStub;
    private final ExecutorService executorService;

    /**
     * Constructor.
     *
     * @param repositoryApi the {@link RepositoryApi}
     * @param riCoverageService service to retrieve entity RI coverage information.
     * @param executorService service for executing.
     * @param groupServiceBlockingStub do resource group reverse lookups (member to containing group)
     */
    public CloudAspectMapper(@Nonnull final RepositoryApi repositoryApi,
                             @Nonnull final ReservedInstanceUtilizationCoverageServiceBlockingStub riCoverageService,
                             @Nonnull final GroupServiceGrpc.GroupServiceBlockingStub groupServiceBlockingStub,
                             @Nonnull final ExecutorService executorService) {
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.riCoverageService = Objects.requireNonNull(riCoverageService);
        this.groupServiceBlockingStub = Objects.requireNonNull(groupServiceBlockingStub);
        this.executorService = Objects.requireNonNull(executorService);
    }

    /**
     * Maps the {@link ApiPartialEntity} to {@link CloudAspectApiDTO}.
     * This method sets only the necessary fields to describe actions.
     * Since getting an extended list of properties that are not necessary to describe an action
     * affects performance.
     *
     * @param entities the {@link ApiPartialEntity}
     * @return the {@link CloudAspectApiDTO}
     */
    @Nullable
    @Override
    public Optional<Map<Long, EntityAspect>> mapEntityToAspectBatchPartial(@Nonnull final List<ApiPartialEntity> entities) {
        List<Long> cloudEntities = entities.stream()
                .filter(e -> isCloudEntity(e))
                .map(e -> e.getOid())
                .collect(Collectors.toList());

        if (cloudEntities.isEmpty()) {
            return Optional.empty();
        }

        GraphRequest request = GraphRequest.newBuilder().addAllOids(cloudEntities)
                        .putNodes("account", SearchProtoUtil.node(Type.MINIMAL, TraversalDirection.OWNED_BY, EntityType.BUSINESS_ACCOUNT).build())
                        .build();
        GraphResponse response = repositoryApi.graphSearch(request);

        Map<Long, EntityAspect> retMap = new HashMap<>();
        SearchProtoUtil.getMapMinimal(response.getNodesOrThrow("account")).forEach((oid, accounts) -> {
            final CloudAspectApiDTO aspect = new CloudAspectApiDTO();
            if (accounts != null && !accounts.isEmpty()) {
                aspect.setBusinessAccount(ServiceEntityMapper.toBaseApiDTO(accounts.get(0)));
            }
            retMap.put(oid, aspect);
        });

        return Optional.of(retMap);
    }

    @Override
    @Nullable
    public EntityAspect mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
        Optional<Map<Long, EntityAspect>> result = mapEntityToAspectBatch(Collections.singletonList(entity));
        if (result.isPresent()) {
            return result.get().get(entity.getOid());
        }
        return null;
    }

    /**
     * Create aspects for a list of entities.
     * @param entities the list of {@link TopologyEntityDTO} to get aspects for
     * @return map containing aspect
     */
    @Override
    @Nullable
    public Optional<Map<Long, EntityAspect>> mapEntityToAspectBatch(@Nonnull List<TopologyEntityDTO> entities) {
        // this aspect only applies to cloud service entities
        List<TopologyEntityDTO> cloudEntities = entities.stream().filter(CloudAspectMapper::isCloudEntity).collect(Collectors.toList());
        if (cloudEntities.isEmpty()) {
            return Optional.ofNullable(null);
        }
        Set<Long> entityIds = cloudEntities.stream().map(TopologyEntityDTO::getOid).collect(Collectors.toSet());
        Set<Long> entityIdsForRIQuery = cloudEntities.stream().filter(entity -> entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                .filter(entity -> entity.getTypeSpecificInfo().getVirtualMachine().getBillingType() != VirtualMachineData.VMBillingType.BIDDING)
                .map(entity -> entity.getOid())
                .collect(Collectors.toSet());

        GraphRequest request = GraphRequest.newBuilder().addAllOids(entityIds)
                .putNodes("account", SearchProtoUtil.node(Type.MINIMAL, TraversalDirection.OWNED_BY, EntityType.BUSINESS_ACCOUNT).build())
                .putNodes("region", SearchProtoUtil.node(Type.MINIMAL, TraversalDirection.AGGREGATED_BY, EntityType.REGION).build())
                .putNodes("zone", SearchProtoUtil.node(Type.MINIMAL, TraversalDirection.AGGREGATED_BY, EntityType.AVAILABILITY_ZONE).build())
                .putNodes("regionByZone", SearchProtoUtil.node(Type.MINIMAL, TraversalDirection.AGGREGATED_BY, EntityType.AVAILABILITY_ZONE, TraversalDirection.AGGREGATED_BY, EntityType.REGION).build())
                .build();

        Set<Long> templateIds = cloudEntities.stream().map(CloudAspectMapper::getTemplateOid)
                .filter(Optional::isPresent).map(Optional::get)
                .collect(Collectors.toSet());

        Future<GraphResponse> graphResponseF = executorService.submit(() -> repositoryApi.graphSearch(request));
        Future<Map<Long, MinimalEntity>> templatesF = executorService.submit(() -> repositoryApi.entitiesRequest(templateIds).getMinimalEntities().collect(Collectors.toMap(MinimalEntity::getOid, e -> e)));
        Future<Map<Long, Grouping>> resourceGroupMapF = executorService.submit(() -> retrieveResourceGroupsForEntities(entityIds));
        Future<Map<Long, EntityReservedInstanceCoverage>> riMapF = executorService.submit(() -> getRiCoverageRelatedInformation(entityIdsForRIQuery));

        Map<Long, EntityAspect> resp = new HashMap<>();
        try {
            // calls - begin
            GraphResponse graphResponse = graphResponseF.get();
            Map<Long, Grouping> resourceGroupMap = resourceGroupMapF.get();
            Map<Long, MinimalEntity> templateMap = templatesF.get();
            Map<Long, EntityReservedInstanceCoverage> riMap = riMapF.get();
            // calls - end

            Map<Long, MinimalEntity> regions = SearchProtoUtil.getMapMinimalWithFirst(graphResponse.getNodesOrThrow("region"));
            Map<Long, MinimalEntity> zones = SearchProtoUtil.getMapMinimalWithFirst(graphResponse.getNodesOrThrow("zone"));
            Map<Long, MinimalEntity> regionByZone = SearchProtoUtil.getMapMinimalWithFirst(graphResponse.getNodesOrThrow("regionByZone"));
            Map<Long, MinimalEntity> accounts = SearchProtoUtil.getMapMinimalWithFirst(graphResponse.getNodesOrThrow("account"));

            cloudEntities.forEach(entity -> {
                CloudAspectApiDTO aspect = new CloudAspectApiDTO();
                resp.put(entity.getOid(), aspect);

                final Grouping resourceGroup = resourceGroupMap.get(entity.getOid());
                if (resourceGroup != null) {
                    aspect.setResourceGroup(GroupMapper.toBaseApiDTO(resourceGroup));
                }

                getTemplateOid(entity).ifPresent(templateOid -> {
                    final MinimalEntity template = templateMap.get(templateOid);
                    if (template != null) {
                        aspect.setTemplate(ServiceEntityMapper.toBaseApiDTO(template));
                    } else {
                        logger.error(
                                "Failed to get template by oid {} from repository for entity with oid {}",
                                templateOid, entity.getOid());
                    }
                });

                MinimalEntity zone = zones.get(entity.getOid());
                if (zone != null) {
                    aspect.setZone(ServiceEntityMapper.toBaseApiDTO(zone));
                }

                MinimalEntity region = regions.get(entity.getOid());
                if (region == null) {
                    region = regionByZone.get(entity.getOid());
                }
                if (region != null) {
                    aspect.setRegion(ServiceEntityMapper.toBaseApiDTO(region));
                }

                MinimalEntity account = accounts.get(entity.getOid());
                if (account != null) {
                    aspect.setBusinessAccount(ServiceEntityMapper.toBaseApiDTO(account));
                }

                if (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                    setVirtualMachineSpecificInfo(entity, aspect);
                    EntityReservedInstanceCoverage entityRiCoverage = riMap.get(entity.getOid());
                    if (entityRiCoverage != null) {
                        setRiCoverage(entity.getOid(), entityRiCoverage, aspect);
                    }
                }
            });
        } catch (Exception e) {
            logger.error("mapEntityToAspectBatch failed with exception", e);
            return Optional.empty();
        }

        return Optional.of(resp);
    }

    /**
     * Retrieve resource groups for a list of entities.
     * @param entities list of oids
     * @return map containing resourcegroups, indexed by oid
     */
    @Nonnull
    private Map<Long, Grouping> retrieveResourceGroupsForEntities(@Nonnull Set<Long> entities) {
        final GroupDTO.GetGroupsForEntitiesResponse response = groupServiceBlockingStub.getGroupsForEntities(
            GroupDTO.GetGroupsForEntitiesRequest.newBuilder()
                .addAllEntityId(entities)
                .addGroupType(CommonDTO.GroupDTO.GroupType.RESOURCE)
                .setLoadGroupObjects(true)
                .build());
        final Map<Long, Grouping> containedEntityToContainingGrouping = new HashMap<>();
        if (Objects.isNull(response) || response.getGroupsCount() == 0) {
            return containedEntityToContainingGrouping;
        }
        final Map<Long, Grouping> groupIdToDefinition = Maps.uniqueIndex(
                response.getGroupsList(),
                Grouping::getId);
        for (Map.Entry<Long, GroupDTO.Groupings> groupingsEntry : response.getEntityGroupMap().entrySet()) {
            final long entityId = groupingsEntry.getKey();
            for (Long groupId : groupingsEntry.getValue().getGroupIdList()) {
                if (groupIdToDefinition.containsKey(groupId)) {
                    Grouping thisGrouping = groupIdToDefinition.get(groupId);
                    Grouping oldGrouping = containedEntityToContainingGrouping.get(groupId);
                    if (oldGrouping != null) {
                        logger.info("Found multiple resource groups for entity {}: {} ({}) and {} ({})", entityId,
                                oldGrouping.getId(), oldGrouping.getDefinition().getDisplayName(),
                                thisGrouping.getId(), thisGrouping.getDefinition().getDisplayName());
                    }
                    containedEntityToContainingGrouping.put(entityId, thisGrouping);
                }
            }
        }
        return Collections.unmodifiableMap(containedEntityToContainingGrouping);
    }

    /**
     * Get RI coverage information for a list of oids.
     * @param oids list of oids
     * @return map containing RI information indexed by oid
     */
    private Map<Long, EntityReservedInstanceCoverage> getRiCoverageRelatedInformation(final Set<Long> oids) {
        if (oids.isEmpty()) {
            return Collections.emptyMap();
        }
        final GetEntityReservedInstanceCoverageRequest entityRiCoverageRequest =
                GetEntityReservedInstanceCoverageRequest.newBuilder()
                        .setEntityFilter(EntityFilter.newBuilder()
                                .addAllEntityId(oids)
                                .build())
                        .build();

        final GetEntityReservedInstanceCoverageResponse entityRiCoverageResponse = riCoverageService.getEntityReservedInstanceCoverage(entityRiCoverageRequest);

        return entityRiCoverageResponse.getCoverageByEntityIdMap();
    }

    private void setRiCoverage(Long oid, EntityReservedInstanceCoverage entityRiCoverage, CloudAspectApiDTO aspect) {
        logger.trace("Setting RI Coverage info for  with VM oid: {}, entityRiCoverage: {}",
                () -> oid, () -> entityRiCoverage);
        final int couponCapacity = entityRiCoverage.getEntityCouponCapacity();
        final double couponsUsed = entityRiCoverage.getCouponsCoveredByRiMap().values()
                .stream()
                .reduce(Double::sum)
                .orElse(0D);

        float percentageCovered;
        if (couponsUsed == 0) {
            percentageCovered = 0f;
        } else {
            percentageCovered = (float)couponsUsed * 100 / couponCapacity;
        }

        aspect.setRiCoveragePercentage(percentageCovered);
        final StatApiDTO riCoverageStatsDto = createRiCoverageStatsDto((float)couponsUsed,
                (float)couponCapacity);
        aspect.setRiCoverage(riCoverageStatsDto);

        // Set Billing Type based on percentageCovered
        if (Math.abs(100 - percentageCovered) < 0.1) {
            aspect.setBillingType(VMBillingType.RESERVED.name());
        } else if (percentageCovered > 0) {
            aspect.setBillingType(VMBillingType.HYBRID.name());
        } else {
            aspect.setBillingType(VMBillingType.ONDEMAND.name());
        }
    }

    private static StatApiDTO createRiCoverageStatsDto(float value, float capacity) {
        final StatValueApiDTO statsValueDto = new StatValueApiDTO();
        statsValueDto.setMin(value);
        statsValueDto.setMax(value);
        statsValueDto.setAvg(value);
        statsValueDto.setTotal(value);
        final StatApiDTO statsDto = new StatApiDTO();
        statsDto.setValues(statsValueDto);
        statsDto.setValue(value);
        final StatValueApiDTO capacityDto = new StatValueApiDTO();
        capacityDto.setMin(capacity);
        capacityDto.setMax(capacity);
        capacityDto.setAvg(capacity);
        statsDto.setCapacity(capacityDto);
        statsDto.setUnits(StringConstants.RI_COUPON_UNITS);
        return statsDto;
    }

    private static void setVirtualMachineSpecificInfo(@Nonnull TopologyEntityDTO entity,
            @Nonnull CloudAspectApiDTO aspect) {
        if (entity.hasTypeSpecificInfo()) {
            final TypeSpecificInfo typeSpecificInfo = entity.getTypeSpecificInfo();
            if (typeSpecificInfo.hasVirtualMachine()) {
                final VirtualMachineInfo virtualMachine = typeSpecificInfo.getVirtualMachine();
                if (virtualMachine.hasArchitecture()) {
                    aspect.setArchitecture(VirtualMachineProtoUtil.ARCHITECTURE.inverse()
                            .get(virtualMachine.getArchitecture()));
                }
                if (virtualMachine.hasVirtualizationType()) {
                    aspect.setVirtualizationType(
                            VirtualMachineProtoUtil.VIRTUALIZATION_TYPE.inverse()
                                    .get(virtualMachine.getVirtualizationType()));
                }
                if (virtualMachine.hasDriverInfo()) {
                    final DriverInfo driverInfo = virtualMachine.getDriverInfo();
                    if (driverInfo.hasHasEnaDriver()) {
                        aspect.setEnaActive(driverInfo.getHasEnaDriver() ?
                                VirtualMachineProtoUtil.ENA_IS_ACTIVE :
                                VirtualMachineProtoUtil.ENA_IS_NOT_ACTIVE);
                    }
                    if (driverInfo.hasHasNvmeDriver()) {
                        aspect.setNvme(String.valueOf(driverInfo.getHasNvmeDriver()));
                    }
                }
                if (virtualMachine.hasBillingType()) {
                    aspect.setBillingType(virtualMachine.getBillingType().name());
                }
            }
        }
    }

    @Nonnull
    private static Optional<Long> getTemplateOid(@Nonnull TopologyEntityDTO entity) {
        final Integer tierTypeValue =
                WORKLOAD_ENTITY_TYPE_TO_TIER_TYPE.get(entity.getEntityType());
        if (tierTypeValue != null) {
            final List<Long> tiers = entity.getCommoditiesBoughtFromProvidersList()
                    .stream()
                    .filter(c -> c.getProviderEntityType() == tierTypeValue)
                    .map(CommoditiesBoughtFromProvider::getProviderId)
                    .collect(Collectors.toList());
            if (!tiers.isEmpty()) {
                if (tiers.size() > 1) {
                    logger.warn("Found {} tiers with type {} for entity with oid {}, return first",
                            tiers::size, () -> EntityType.forNumber(tierTypeValue), entity::getOid);
                }
                return Optional.of(tiers.iterator().next());
            }
        }
        return Optional.empty();
    }

    @Override
    @Nonnull
    public AspectName getAspectName() {
        return AspectName.CLOUD;
    }
}
