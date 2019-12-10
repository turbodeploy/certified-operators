package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entityaspect.CloudAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.common.protobuf.VirtualMachineProtoUtil;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo.DriverInfo;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Mapper for getting {@link CloudAspectApiDTO}.
 */
public class CloudAspectMapper extends AbstractAspectMapper {
    private static final Logger logger = LogManager.getLogger();
    private static final Map<Integer, Integer> ENTITY_TYPE_VALUE_TO_TIER_TYPE_VALUE =
            ImmutableMap.of(EntityType.VIRTUAL_MACHINE_VALUE, EntityType.COMPUTE_TIER_VALUE,
                    EntityType.STORAGE_VALUE, EntityType.STORAGE_TIER_VALUE,
                    EntityType.DATABASE_SERVER_VALUE, EntityType.DATABASE_SERVER_TIER_VALUE,
                    EntityType.DATABASE_VALUE, EntityType.DATABASE_TIER_VALUE);

    private static final Set<Integer> AVAILABILITY_ZONE_AND_REGION =
            ImmutableSet.of(EntityType.AVAILABILITY_ZONE_VALUE, EntityType.REGION_VALUE);

    private final RepositoryApi repositoryApi;

    /**
     * Constructor.
     *
     * @param repositoryApi the {@link RepositoryApi}
     */
    public CloudAspectMapper(@Nonnull final RepositoryApi repositoryApi) {
        this.repositoryApi = repositoryApi;
    }

    /**
     * Maps the {@link ApiPartialEntity} to {@link CloudAspectApiDTO}.
     * This method sets only the necessary fields to describe actions.
     * Since getting an extended list of properties that are not necessary to describe an action
     * affects performance.
     *
     * @param entity the {@link TopologyEntityDTO}
     * @return the {@link CloudAspectApiDTO}
     */
    @Nullable
    @Override
    public EntityAspect mapEntityToAspect(@Nonnull final ApiPartialEntity entity) {
        // this aspect only applies to cloud service entities
        if (!isCloudEntity(entity)) {
            return null;
        }
        final CloudAspectApiDTO aspect = new CloudAspectApiDTO();
        searchConnectedFromEntity(entity.getOid(), UIEntityType.BUSINESS_ACCOUNT).ifPresent(
                e -> aspect.setBusinessAccount(createBaseApiDTO(e)));
        return aspect;
    }

    @Override
    @Nullable
    public EntityAspect mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
        // this aspect only applies to cloud service entities
        if (!isCloudEntity(entity)) {
            return null;
        }
        final CloudAspectApiDTO aspect = new CloudAspectApiDTO();
        final Set<Long> oids = new HashSet<>();
        final Optional<Long> templateOid = getTemplateOid(entity);
        templateOid.ifPresent(oids::add);
        final Optional<ConnectedEntity> connectedAvailabilityZoneOrRegion =
                getConnectedAvailabilityZoneOrRegion(entity);
        connectedAvailabilityZoneOrRegion.ifPresent(e -> oids.add(e.getConnectedEntityId()));

        final Map<Long, MinimalEntity> oidToMinimalEntity = repositoryApi.entitiesRequest(oids)
                .getMinimalEntities()
                .collect(Collectors.toMap(MinimalEntity::getOid, e -> e));

        templateOid.ifPresent(oid -> {
            final MinimalEntity template = oidToMinimalEntity.get(oid);
            if (template != null) {
                aspect.setTemplate(createBaseApiDTO(template));
            } else {
                logger.error(
                        "Failed to get template by oid {} from repository for entity with oid {}",
                        oid, entity.getOid());
            }
        });

        connectedAvailabilityZoneOrRegion.ifPresent(connectedEntity -> {
            final MinimalEntity availabilityZoneOrRegion =
                    oidToMinimalEntity.get(connectedEntity.getConnectedEntityId());
            if (availabilityZoneOrRegion != null) {
                final BaseApiDTO baseApiDTO = createBaseApiDTO(availabilityZoneOrRegion);
                if (availabilityZoneOrRegion.getEntityType() ==
                        EntityType.AVAILABILITY_ZONE_VALUE) {
                    // AWS case
                    searchConnectedFromEntity(availabilityZoneOrRegion.getOid(),
                            UIEntityType.REGION).ifPresent(
                            e -> aspect.setRegion(createBaseApiDTO(e)));
                    aspect.setZone(baseApiDTO);
                } else if (availabilityZoneOrRegion.getEntityType() == EntityType.REGION_VALUE) {
                    // Azure case
                    aspect.setRegion(baseApiDTO);
                }
            } else {
                logger.error("Failed to get {} by oid {} from repository for entity with oid {}",
                        EntityType.forNumber(connectedEntity.getConnectedEntityType()),
                        connectedEntity.getConnectedEntityId(), entity.getOid());
            }
        });

        searchConnectedFromEntity(entity.getOid(), UIEntityType.BUSINESS_ACCOUNT).ifPresent(
                e -> aspect.setBusinessAccount(createBaseApiDTO(e)));

        if (entity.getEntityType() != EntityType.VIRTUAL_MACHINE_VALUE) {
            return aspect;
        }
        setVirtualMachineSpecificInfo(entity, aspect);
        return aspect;
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
                ENTITY_TYPE_VALUE_TO_TIER_TYPE_VALUE.get(entity.getEntityType());
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
        } else {
            logger.warn("Could not find corresponding tier type for entity with oid {} and type {}",
                    entity::getOid, () -> EntityType.forNumber(entity.getEntityType()));
        }
        return Optional.empty();
    }

    @Nonnull
    private static Optional<ConnectedEntity> getConnectedAvailabilityZoneOrRegion(
            @Nonnull TopologyEntityDTO entity) {
        final List<ConnectedEntity> connectedAvailabilityZoneOrRegionEntities =
                entity.getConnectedEntityListList()
                        .stream()
                        .filter(e -> AVAILABILITY_ZONE_AND_REGION.contains(
                                e.getConnectedEntityType()))
                        .collect(Collectors.toList());
        if (connectedAvailabilityZoneOrRegionEntities.isEmpty()) {
            return Optional.empty();
        } else {
            if (connectedAvailabilityZoneOrRegionEntities.size() > 1) {
                logger.warn(
                        "Found availability zone or region entities {} connected to entity with oid {}, return first",
                        () -> connectedAvailabilityZoneOrRegionEntities, entity::getOid);
            }
            return Optional.of(connectedAvailabilityZoneOrRegionEntities.iterator().next());
        }
    }

    /**
     * Search connected from {@code entityType} for a entity.
     *
     * @param entityOid oid of the entity
     * @param entityType type of the entity
     * @return {@link MinimalEntity} describing a entity with {@code entityType}
     */
    @Nonnull
    private Optional<MinimalEntity> searchConnectedFromEntity(final long entityOid,
            @Nonnull UIEntityType entityType) {
        final List<MinimalEntity> entities = repositoryApi.newSearchRequest(
                SearchProtoUtil.neighborsOfType(entityOid, TraversalDirection.CONNECTED_FROM,
                        entityType)).getMinimalEntities().collect(Collectors.toList());
        if (entities.isEmpty()) {
            return Optional.empty();
        } else {
            if (entities.size() > 1) {
                logger.warn(
                        "Found {} connected from entities with type {} for entity with oid {}, return first",
                        entities::size, entityType::displayName, () -> entityOid);
            }
            return Optional.of(entities.iterator().next());
        }
    }

    @Override
    @Nonnull
    public AspectName getAspectName() {
        return AspectName.CLOUD;
    }
}
