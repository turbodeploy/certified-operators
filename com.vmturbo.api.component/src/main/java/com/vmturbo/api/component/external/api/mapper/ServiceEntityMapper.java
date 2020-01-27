package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.dto.template.TemplateApiDTO;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostCategoryFilter;
import com.vmturbo.common.protobuf.cost.Cost.EntityFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsResponse;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainScope;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainSeed;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

public class ServiceEntityMapper {
    private static final Collection<CostCategory> TEMPLATE_PRICE_CATEGORIES =
                    ImmutableSet.of(CostCategory.ON_DEMAND_COMPUTE, CostCategory.ON_DEMAND_LICENSE);

    private final Logger logger = LogManager.getLogger();

    private final ThinTargetCache thinTargetCache;
    private final CostServiceBlockingStub costServiceBlockingStub;
    private final SupplyChainServiceBlockingStub supplyChainBlockingStub;

    /**
     * Creates {@link ServiceEntityMapper} instance.
     *
     * @param thinTargetCache a cache for simple target information
     * @param costServiceBlockingStub service which will provide cost information by
     *                 entities.
     * @param supplyChainBlockingStub service used to get the supply chain information
     *                 for one or more entities.
     */
    public ServiceEntityMapper(@Nonnull final ThinTargetCache thinTargetCache,
                               @Nonnull CostServiceBlockingStub costServiceBlockingStub,
                               @Nonnull SupplyChainServiceBlockingStub supplyChainBlockingStub) {
        this.thinTargetCache = thinTargetCache;
        this.costServiceBlockingStub = Objects.requireNonNull(costServiceBlockingStub);
        this.supplyChainBlockingStub = Objects.requireNonNull(supplyChainBlockingStub);
    }


    /**
     * Copies the the basic fields of a {@link BaseApiDTO} object from a {@link TopologyEntityDTO}
     * object.  Basic fields are: display name, class name, and uuid.
     *
     * @param topologyEntityDTO the object whose basic fields are to be copied.
     */
    public static ServiceEntityApiDTO toBasicEntity(@Nonnull final ApiPartialEntity topologyEntityDTO) {
        final ServiceEntityApiDTO baseApiDTO = new ServiceEntityApiDTO();
        baseApiDTO.setDisplayName(Objects.requireNonNull(topologyEntityDTO).getDisplayName());
        baseApiDTO.setClassName(UIEntityType.fromType(topologyEntityDTO.getEntityType()).apiStr());
        baseApiDTO.setUuid(String.valueOf(topologyEntityDTO.getOid()));
        setNonEmptyDisplayName(baseApiDTO);
        return baseApiDTO;
    }

    @Nonnull
    public static ServiceEntityApiDTO toBasicEntity(@Nonnull final TopologyEntityDTO topologyEntityDTO) {
        final ServiceEntityApiDTO baseApiDTO = new ServiceEntityApiDTO();
        baseApiDTO.setDisplayName(Objects.requireNonNull(topologyEntityDTO).getDisplayName());
        baseApiDTO.setClassName(UIEntityType.fromType(topologyEntityDTO.getEntityType()).apiStr());
        baseApiDTO.setUuid(String.valueOf(topologyEntityDTO.getOid()));
        setNonEmptyDisplayName(baseApiDTO);
        return baseApiDTO;
    }

    @Nonnull
    public static ServiceEntityApiDTO toBasicEntity(@Nonnull final MinimalEntity topologyEntityDTO) {
        final ServiceEntityApiDTO baseApiDTO = new ServiceEntityApiDTO();
        baseApiDTO.setDisplayName(Objects.requireNonNull(topologyEntityDTO).getDisplayName());
        baseApiDTO.setClassName(UIEntityType.fromType(topologyEntityDTO.getEntityType()).apiStr());
        baseApiDTO.setUuid(String.valueOf(topologyEntityDTO.getOid()));
        setNonEmptyDisplayName(baseApiDTO);
        return baseApiDTO;
    }

    /**
     * Set displayName to UUID if the former is null or empty
     * @param apiDTO    DTO to update
     */
    private static void setNonEmptyDisplayName(final ServiceEntityApiDTO apiDTO) {
        if (StringUtils.isEmpty(apiDTO.getDisplayName())) {
            apiDTO.setDisplayName(apiDTO.getUuid());
        }
    }

    /**
     * Converts a {@link ApiPartialEntity} instance to a {@link ServiceEntityApiDTO} instance
     * to be returned by the REST API.
     *
     * Note: because of the structure of {@link ServiceEntityApiDTO},
     * only one of the discovering targets can be included in the result.
     *
     * @param apiEntity the internal {@link ApiPartialEntity} to convert
     * @return an {@link ServiceEntityApiDTO} populated from the given topologyEntity
     */
    @Nonnull
    public ServiceEntityApiDTO toServiceEntityApiDTO(@Nonnull ApiPartialEntity apiEntity) {
        return toServiceEntityApiDTO(Collections.singletonList(apiEntity)).get(0);
    }

    /**
     * Converts the input collection of {@link ApiPartialEntity} instances into a list
     * {@link ServiceEntityApiDTO} values.
     *
     * @param apiEntities The input collection of {@link ApiPartialEntity} that will be converted into DTOs.
     * @return A list of {@link ServiceEntityApiDTO}, in the order of the input API entities.
     */
    public List<ServiceEntityApiDTO> toServiceEntityApiDTO(@Nonnull final Collection<ApiPartialEntity> apiEntities) {
        final List<ServiceEntityApiDTO> retList = new ArrayList<>(apiEntities.size());
        bulkMapToServiceEntityApiDTO(apiEntities,
            (oid, convertedEntity) -> retList.add(convertedEntity));
        return retList;
    }

    /**
     * Converts the input collection of {@link ApiPartialEntity} instances into a map containing
     * {@link ServiceEntityApiDTO} values. The map keys are the corresponding OIDs of the entity DTOs.
     *
     * @param apiEntities The input collection of {@link ApiPartialEntity} that will be converted into DTOs.
     * @return A map of {@link ServiceEntityApiDTO}s arranged by OID.
     */
    public Map<Long, ServiceEntityApiDTO> toServiceEntityApiDTOMap(@Nonnull final Collection<ApiPartialEntity> apiEntities) {
        final Map<Long, ServiceEntityApiDTO> retMap = new HashMap<>(apiEntities.size());
        bulkMapToServiceEntityApiDTO(apiEntities, retMap::put);
        return retMap;
    }

    /**
     * Bulk convesion logic for {@link ApiPartialEntity} objects. Converting in bulk helps us
     * optimize necessary outgoing RPC calls.
     *
     * @param apiEntities The collection of entities to convert.
     * @param convertedEntityConsumer Consumer for converted entities and their OIDs. The consumer
     *                                will be called in the order of the input entities.
     */
    private void bulkMapToServiceEntityApiDTO(@Nonnull final Collection<ApiPartialEntity> apiEntities,
                                              @Nonnull final BiConsumer<Long, ServiceEntityApiDTO> convertedEntityConsumer) {
        final Set<Long> costPriceEntities = new HashSet<>();
        final Set<Long> numberOfVmEntities = new HashSet<>();
        for (ApiPartialEntity entity : apiEntities) {
            if (shouldGetNumberOfVms(entity)) {
                numberOfVmEntities.add(entity.getOid());
            }

            if (shouldGetCostPrice(entity)) {
                costPriceEntities.add(entity.getOid());
            }
        }

        final Map<Long, Integer> numVmsPerEntity = computeNrOfVmsPerEntity(numberOfVmEntities);
        final Map<Long, Double> costPriceByEntity = computeCostPriceByEntity(costPriceEntities);

        apiEntities.forEach(apiEntity -> {
            // basic information
            final ServiceEntityApiDTO result = ServiceEntityMapper.toBasicEntity(apiEntity);
            if (apiEntity.hasEntityState()) {
                result.setState(UIEntityState.fromEntityState(apiEntity.getEntityState()).apiStr());
            }
            if (apiEntity.hasEnvironmentType()) {
                EnvironmentTypeMapper
                    .fromXLToApi(apiEntity.getEnvironmentType())
                    .ifPresent(result::setEnvironmentType);
            }

            setDiscoveredBy(apiEntity::getDiscoveredTargetDataMap, result);

            //tags
            result.setTags(
                apiEntity.getTags().getTagsMap().entrySet().stream()
                    .collect(
                        Collectors.toMap(Entry::getKey, entry -> entry.getValue().getValuesList())));

            Optional.ofNullable(numVmsPerEntity.get(apiEntity.getOid()))
                .ifPresent(result::setNumRelatedVMs);

            // template name
            final Optional<RelatedEntity> primaryProvider = apiEntity.getProvidersList().stream()
                .filter(provider -> provider.hasDisplayName() &&
                    TopologyDTOUtil.isPrimaryTierEntityType(apiEntity.getEntityType(),
                        provider.getEntityType()))
                .findAny();
            final Optional<RelatedEntity> backupPrimary = apiEntity.getConnectedToList().stream()
                .filter(connected -> connected.hasDisplayName() &&
                    TopologyDTOUtil.isPrimaryTierEntityType(apiEntity.getEntityType(),
                        connected.getEntityType()))
                .findAny();
            backupPrimary.map(primaryProvider::orElse).ifPresent(provider -> {
                final TemplateApiDTO template = new TemplateApiDTO();
                template.setUuid(Long.toString(provider.getOid()));
                template.setPrice(Optional.ofNullable(costPriceByEntity.get(apiEntity.getOid()))
                    .map(Double::floatValue)
                    .orElse(0.0f));
                template.setDisplayName(provider.getDisplayName());
                result.setTemplate(template);
            });

            convertedEntityConsumer.accept(apiEntity.getOid(), result);
        });
    }

    /**
     * Converts a {@link TopologyEntityDTO} instance to a {@link ServiceEntityApiDTO} instance
     * to be returned by the REST API.
     *
     * Note: because of the structure of {@link ServiceEntityApiDTO},
     * only one of the discovering targets can be included in the result.
     *
     * @param topologyEntityDTO the internal {@link TopologyEntityDTO} to convert
     * @return an {@link ServiceEntityApiDTO} populated from the given topologyEntity
     */
    @Nonnull
    public ServiceEntityApiDTO toServiceEntityApiDTO(@Nonnull final TopologyEntityDTO topologyEntityDTO) {
        // basic information
        final ServiceEntityApiDTO seDTO = ServiceEntityMapper.toBasicEntity(topologyEntityDTO);
        if (topologyEntityDTO.hasEntityState()) {
            seDTO.setState(UIEntityState.fromEntityState(topologyEntityDTO.getEntityState()).apiStr());
        }
        if (topologyEntityDTO.hasEnvironmentType()) {
            EnvironmentTypeMapper
                .fromXLToApi(topologyEntityDTO.getEnvironmentType())
                .ifPresent(seDTO::setEnvironmentType);
        }

        setDiscoveredBy(() -> topologyEntityDTO.getOrigin().getDiscoveryOrigin()
                        .getDiscoveredTargetDataMap(), seDTO);

        //tags
        seDTO.setTags(
            topologyEntityDTO.getTags().getTagsMap().entrySet().stream()
                .collect(
                    Collectors.toMap(Entry::getKey, entry -> entry.getValue().getValuesList())));

        return seDTO;
    }

    /**
     * Creates a shallow clone of a {@link ServiceEntityApiDTO} object.
     *
     * @param serviceEntityApiDTO the object to clone.
     * @return the new object.
     */
    @Nonnull
    public static ServiceEntityApiDTO copyServiceEntityAPIDTO(
            @Nonnull ServiceEntityApiDTO serviceEntityApiDTO) {
        // basic information
        final ServiceEntityApiDTO result = new ServiceEntityApiDTO();
        result.setDisplayName(Objects.requireNonNull(serviceEntityApiDTO).getDisplayName());
        result.setClassName(serviceEntityApiDTO.getClassName());
        result.setUuid(serviceEntityApiDTO.getUuid());
        result.setState(serviceEntityApiDTO.getState());
        result.setEnvironmentType(serviceEntityApiDTO.getEnvironmentType());

        // aspects, if required
        result.setAspects(serviceEntityApiDTO.getAspects());

        // target, if exists
        if (serviceEntityApiDTO.getDiscoveredBy() != null) {
            final TargetApiDTO targetApiDTO = new TargetApiDTO();
            targetApiDTO.setUuid(serviceEntityApiDTO.getDiscoveredBy().getUuid());
            targetApiDTO.setDisplayName(serviceEntityApiDTO.getDiscoveredBy().getDisplayName());
            targetApiDTO.setType(serviceEntityApiDTO.getDiscoveredBy().getType());
            result.setDiscoveredBy(targetApiDTO);
        }

        //tags
        result.setTags(serviceEntityApiDTO.getTags());

        result.setVendorIds(serviceEntityApiDTO.getVendorIds());

        return result;
    }

    private void setDiscoveredBy(Supplier<Map<Long, PerTargetEntityInformation>> idMapGetter, ServiceEntityApiDTO result) {
        Map<Long, PerTargetEntityInformation> target2data = idMapGetter.get();
        if (!target2data.isEmpty()) {
            // only one target will be returned - preferably any non-hidden, if none then any at all
            ThinTargetInfo preferredInfo = null;
            for (Map.Entry<Long, PerTargetEntityInformation> info : target2data.entrySet()) {
                Optional<ThinTargetInfo> thinInfo = thinTargetCache.getTargetInfo(info.getKey());
                if (thinInfo.isPresent() && (!thinInfo.get().isHidden() || preferredInfo == null)) {
                    preferredInfo = thinInfo.get();
                }
            }
            if (preferredInfo != null) {
                result.setDiscoveredBy(createTargetApiDto(preferredInfo));
            } else {
                logger.debug("Failed to locate discoveredBy target information for " + result.getUuid());
            }
            // convert target ids to strings for api
            // skip targets where vendor ids are unset
            result.setVendorIds(target2data.entrySet().stream()
                .filter(target2id -> target2id.getValue().hasVendorId() && !StringUtils
                                .isEmpty(target2id.getValue().getVendorId()))
                .collect(Collectors
                                .toMap(target2id -> thinTargetCache
                                                .getTargetInfo(target2id.getKey())
                                                .map(ThinTargetInfo::displayName)
                                                .orElse(String.valueOf(target2id.getKey())),
                                       target2id -> target2id.getValue().getVendorId(),
                                       (d1, d2) -> d1)));
        }
    }

    @Nonnull
    private TargetApiDTO createTargetApiDto(@Nonnull final ThinTargetInfo thinTargetInfo) {
        final TargetApiDTO apiDTO = new TargetApiDTO();
        apiDTO.setType(thinTargetInfo.probeInfo().type());
        apiDTO.setUuid(Long.toString(thinTargetInfo.oid()));
        apiDTO.setDisplayName(thinTargetInfo.displayName());
        apiDTO.setCategory(thinTargetInfo.probeInfo().category());
        return apiDTO;
    }

    private Map<Long, Double> computeCostPriceByEntity(Set<Long> costPriceEntities) {
        if (costPriceEntities.isEmpty()) {
            return Collections.emptyMap();
        }

        // Do a multi-get to the cost service.
        final EntityFilter entityFilter = EntityFilter.newBuilder()
            .addAllEntityId(costPriceEntities)
            .build();
        final GetCloudCostStatsRequest request = GetCloudCostStatsRequest.newBuilder()
            .addCloudCostStatsQuery(CloudCostStatsQuery.newBuilder()
                .setCostCategoryFilter(CostCategoryFilter.newBuilder()
                    .addAllCostCategory(TEMPLATE_PRICE_CATEGORIES))
                .setEntityFilter(entityFilter))
            .build();
        final GetCloudCostStatsResponse response =
            costServiceBlockingStub.getCloudCostStats(request);
        return response.getCloudStatRecordList().stream()
            .flatMap(record -> record.getStatRecordsList().stream())
            .collect(Collectors.groupingBy(StatRecord::getAssociatedEntityId,
                    Collectors.summingDouble(record -> record.getValues().getAvg())));
    }

    private Map<Long, Integer> computeNrOfVmsPerEntity(Set<Long> numberOfVmEntities) {
        if (numberOfVmEntities.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<Long, Integer> result = new HashMap<>(numberOfVmEntities.size());

        GetMultiSupplyChainsRequest.Builder builder = GetMultiSupplyChainsRequest.newBuilder();
        numberOfVmEntities.forEach(oid -> {
            builder.addSeeds(SupplyChainSeed.newBuilder()
                .setSeedOid(oid)
                .setScope(SupplyChainScope.newBuilder()
                    .addStartingEntityOid(oid)
                    .addEntityTypesToInclude(UIEntityType.VIRTUAL_MACHINE.apiStr())));
        });
        supplyChainBlockingStub.getMultiSupplyChains(builder.build()).forEachRemaining(response -> {
            result.put(response.getSeedOid(),
                response.getSupplyChain().getSupplyChainNodesList().stream()
                    .filter(node -> node.getEntityType().equals(UIEntityType.VIRTUAL_MACHINE.apiStr()))
                    .map(RepositoryDTOUtil::getMemberCount)
                    .findFirst().orElse(0));
        });

        return result;
    }

    private boolean shouldGetNumberOfVms(@Nonnull final ApiPartialEntity entity) {
        return entity.getEntityType() == UIEntityType.REGION.typeNumber() ||
            entity.getEntityType() == UIEntityType.AVAILABILITY_ZONE.typeNumber();
    }

    private boolean shouldGetCostPrice(@Nonnull final ApiPartialEntity entity) {
        return getTemplateProvider(entity).isPresent();
    }

    private static Optional<RelatedEntity> getTemplateProvider(final ApiPartialEntity entity) {
        return Stream.concat(entity.getProvidersList().stream(), entity.getConnectedToList().stream())
            .filter(connected -> connected.hasDisplayName() &&
                TopologyDTOUtil.isPrimaryTierEntityType(entity.getEntityType(),
                    connected.getEntityType()))
            .findFirst();

    }

}
