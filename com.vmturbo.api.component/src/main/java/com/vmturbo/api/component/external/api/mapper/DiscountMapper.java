package com.vmturbo.api.component.external.api.mapper;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitPriceAdjustmentApiDTO;
import com.vmturbo.api.dto.businessunit.CloudServicePriceAdjustmentApiDTO;
import com.vmturbo.api.dto.businessunit.EntityDiscountDTO;
import com.vmturbo.api.dto.businessunit.EntityPriceDTO;
import com.vmturbo.api.dto.businessunit.TemplatePriceAdjustmentDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.BusinessUnitType;
import com.vmturbo.api.enums.CloudType;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.enums.ServicePricingModel;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo.ServiceLevelDiscount;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo.ServiceLevelDiscount.Builder;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo.TierLevelDiscount;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Mapping between Cost domain DTO {@link Discount} and API DTOs
 * {@link BusinessUnitApiDTO} and {@link BusinessUnitPriceAdjustmentApiDTO}. To get the topology information,
 * this class will rely on the Repository API calls.
 * Note: XL currently only support discount type, and seems account abstraction is not yet needed.
 *
 * @Todo: consider adding business unit abstraction to handle CRUD operations, when supporting other business unit types.
 */
public class DiscountMapper {
    private static final Logger logger = LogManager.getLogger();

    private static final Set<Integer> TIER_TYPES = ImmutableSet.of(
            EntityType.COMPUTE_TIER_VALUE,
            EntityType.DATABASE_TIER_VALUE,
            EntityType.STORAGE_TIER_VALUE);

    private final RepositoryApi repositoryApi;

    private final Set<String> SUPPORTED_CLOUD_TYPE = ImmutableSet.of("AWS", "AZURE");

    // EntityDiscountDTO predicate to filter those only have price adjustment
    private static final Predicate<EntityDiscountDTO> ENTITY_DISCOUNT_DTO_PREDICATE = dto ->
            dto.getPriceAdjustment() != null
            && dto.getPriceAdjustment().getValue() != null;

    /**
     * Constructor for the {@link DiscountMapper}.
     *
     * @param repositoryApi {@link RepositoryApi} for calls to the repository to get entity information.
     */
    public DiscountMapper(@Nonnull final RepositoryApi repositoryApi) {
        this.repositoryApi = repositoryApi;
    }

    /**
     * Convert domain object {@link Discount} to API BusinessUnitApiDTO
     * TODO: (September 17, 2019) handle other discount type
     *
     * @param discount         discount from Cost component
     * @return BusinessUnitApiDTO
     */
    @Nonnull
    public BusinessUnitApiDTO toBusinessUnitApiDTO(@Nonnull Discount discount) {
        final BusinessUnitApiDTO businessUnitApiDTO = new BusinessUnitApiDTO();
        businessUnitApiDTO.setUuid(String.valueOf(discount.getAssociatedAccountId()));
        businessUnitApiDTO.setDisplayName(discount.getDiscountInfo().getDisplayName());
        businessUnitApiDTO.setClassName(UIEntityType.BUSINESS_ACCOUNT.apiStr());
        businessUnitApiDTO.setEnvironmentType(EnvironmentType.CLOUD);
        businessUnitApiDTO.setDiscount((float) discount.getDiscountInfo()
                .getAccountLevelDiscount()
                .getDiscountPercentage());
        // It seems it's always NOT master in legacy for new business discount unit.
        businessUnitApiDTO.setMaster(false);
        // It seems it always doesn't have related target in legacy for new business discount unit.
        businessUnitApiDTO.setAssociatedTargetId(null);

        businessUnitApiDTO.setBusinessUnitType(BusinessUnitType.DISCOUNT);
        final String targetType = getTargetType(discount.getAssociatedAccountId())
                .orElse(new TargetApiDTO())
                .getType();
        businessUnitApiDTO.setCloudType(SUPPORTED_CLOUD_TYPE.contains(targetType) ? CloudType.valueOf(targetType) : CloudType.UNKNOWN);
        businessUnitApiDTO.setChildrenBusinessUnits(ImmutableSet.of(String.valueOf(discount.getAssociatedAccountId())));

        // TODO provide Cost for this business account
        businessUnitApiDTO.setCostPrice(0.0f);
        // TODO provide severity for this business account
        businessUnitApiDTO.setSeverity(Severity.NORMAL.name());
        // It's not clear if business account can have many members.
        // It seems it's always 0 in legacy for business discount unit.
        businessUnitApiDTO.setMembersCount(0);
        // It seems it's always "workload" type in legacy for business discount unit.
        businessUnitApiDTO.setMemberType(StringConstants.WORKLOAD);
        return businessUnitApiDTO;
    }

    /**
     * Convert {@link BusinessUnitPriceAdjustmentApiDTO} to Cost domain {@link TierLevelDiscount}
     *
     * @param businessUnitDiscountApiDTO Api DTO for business unit discount
     * @return TierLevelDiscount
     */
    @Nonnull
    public TierLevelDiscount toTierDiscountProto(@Nonnull final BusinessUnitPriceAdjustmentApiDTO businessUnitDiscountApiDTO) {
        final TierLevelDiscount.Builder builder = TierLevelDiscount.newBuilder();
        businessUnitDiscountApiDTO.getServicePriceAdjustments().stream()
                .filter(serviceDiscountApiDTO -> serviceDiscountApiDTO.getTemplatePriceAdjustments() != null)
                .flatMap(serviceDiscountApiDTO -> serviceDiscountApiDTO.getTemplatePriceAdjustments().stream())
                .filter(ENTITY_DISCOUNT_DTO_PREDICATE)
                .forEach(templateDiscountDTO -> builder.
                        putDiscountPercentageByTierId(Long.parseLong(templateDiscountDTO.getUuid()),
                                templateDiscountDTO.getPriceAdjustment().getValue()));
        return builder.build();
    }

    /**
     * Convert {@link BusinessUnitPriceAdjustmentApiDTO} to domain object {@link ServiceLevelDiscount}
     *
     * @param businessUnitDiscountApiDTO Api DTO for business unit discount
     * @return ServiceLevelDiscount
     */
    @Nonnull
    public ServiceLevelDiscount toServiceDiscountProto(@Nonnull final BusinessUnitPriceAdjustmentApiDTO businessUnitDiscountApiDTO) {
        final Builder builder = DiscountInfo.ServiceLevelDiscount.newBuilder();
        businessUnitDiscountApiDTO.getServicePriceAdjustments().stream()
                .filter(ENTITY_DISCOUNT_DTO_PREDICATE)
                .forEach(serviceWithDiscount -> builder.putDiscountPercentageByServiceId(Long.parseLong(serviceWithDiscount.getUuid()),
                        serviceWithDiscount.getPriceAdjustment().getValue()));
        return builder.build();
    }


    /**
     * Convert Cost domain objects {@link Discount} to business units with discount type
     *
     * @param discounts        discounts from Cost component
     * @return list for business units with discount type
     */
    @Nonnull
    public List<BusinessUnitApiDTO> toDiscountBusinessUnitApiDTO(@Nonnull final Iterator<Discount> discounts) {
        Objects.requireNonNull(discounts);
        final Iterable<Discount> iterable = () -> discounts;
        return StreamSupport.stream(iterable.spliterator(), false)
                .map(this::toBusinessUnitApiDTO)
                .collect(Collectors.toList());
    }

    /**
     * Get target type by {@link Discount}.
     * Steps:
     * 1. Use repository to resolve topology entity by account id
     * 2. retrieve origin from topology entity
     * 3. Use target service to retrieve TargetApiDTO by origin id
     */
    private Optional<TargetApiDTO> getTargetType(final long associatedAccountId) {
        return repositoryApi.entityRequest(associatedAccountId).getSE()
            .flatMap(entity -> Optional.ofNullable(entity.getDiscoveredBy()));
    }

    /**
     * Convert from Cost domain object {@link Discount} to business unit discount API DTO
     *
     * @param discount         discount from Cost component
     * @return BusinessUnitPriceAdjustmentApiDTO
     * @throws InvalidOperationException
     */
    public BusinessUnitPriceAdjustmentApiDTO toDiscountApiDTO(@Nonnull final Discount discount)
            throws Exception {
        final BusinessUnitPriceAdjustmentApiDTO businessUnitDiscountApiDTO = new BusinessUnitPriceAdjustmentApiDTO();
        businessUnitDiscountApiDTO.setServiceDiscounts(toCloudServicePriceAdjustmentApiDTOs(discount));
        return businessUnitDiscountApiDTO;
    }

    /**
     * Convert discount to CloudServicePriceAdjustmentApiDTOs.
     * Steps:
     * 1. get all the Cloud services and tiers from search service
     * 2. match Cloud services with service discounts from {@link Discount} DTO, and assign discount
     * 3. match Cloud tiers with tier discounts from {@link Discount} DTO, and assign discount
     *
     * @param discount         discount from Cost component
     * @return CloudServicePriceAdjustmentApiDTOs
     * @throws InvalidOperationException if search operation failed
     */
    private List<CloudServicePriceAdjustmentApiDTO> toCloudServicePriceAdjustmentApiDTOs(@Nonnull final Discount discount)
            throws Exception {
        final List<CloudServicePriceAdjustmentApiDTO> cloudServiceDiscountApiDTOs;
        if (discount.hasAssociatedAccountId()) {
            cloudServiceDiscountApiDTOs = getCloudServicePriceAdjustmentApiDTOsForAccount(discount.getAssociatedAccountId());
        } else {
            cloudServiceDiscountApiDTOs = getCloudServicePriceAdjustmentApiDTOs();
        }
        final DiscountInfo discountInfo = discount.getDiscountInfo();
        if (discountInfo.hasServiceLevelDiscount()) {
            discount.getDiscountInfo().getServiceLevelDiscount().getDiscountPercentageByServiceIdMap().forEach((serviceId, rate) -> {
                cloudServiceDiscountApiDTOs.stream()
                        .filter(cloudServiceDiscountApiDTO -> cloudServiceDiscountApiDTO.getUuid().equals(String.valueOf(serviceId)))
                        .forEach(dto -> {
                            dto.setDiscount(rate.floatValue());
                        });
            });
        }

        if (discountInfo.hasTierLevelDiscount()) {
            discount.getDiscountInfo().getTierLevelDiscount().getDiscountPercentageByTierIdMap().forEach((tierId, rate) -> {
                cloudServiceDiscountApiDTOs.forEach(
                        cloudServiceDiscountApiDTO -> {
                            if (cloudServiceDiscountApiDTO.getTemplatePriceAdjustments() != null)
                                cloudServiceDiscountApiDTO.getTemplatePriceAdjustments().stream().
                                        filter(templateDiscountDTO -> templateDiscountDTO.getUuid().equals(String.valueOf(tierId)))
                                        .forEach(dto -> {
                                            dto.setDiscount(rate.floatValue());
                                        });
                        }

                );
            });
        }
        return cloudServiceDiscountApiDTOs;
    }

    /**
     * Get all discovered Cloud services from search service
     *
     * @return CloudServicePriceAdjustmentApiDTO
     * @throws InvalidOperationException if search operation failed
     */
    private List<CloudServicePriceAdjustmentApiDTO> getCloudServicePriceAdjustmentApiDTOs() {
        return repositoryApi.newSearchRequest(SearchProtoUtil.makeSearchParameters(
            SearchProtoUtil.entityTypeFilter(UIEntityType.CLOUD_SERVICE)).build())
                .getEntities()
                .map(this::buildCloudServicePriceAdjustmentApiDTO)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());
    }

    /**
     * Get all discovered Cloud services for an account from search service.
     *
     * @param accountId oid of account
     * @return CloudServicePriceAdjustmentApiDTO
     * @throws InvalidOperationException if search operation failed
     */
    private List<CloudServicePriceAdjustmentApiDTO> getCloudServicePriceAdjustmentApiDTOsForAccount(long accountId) {

        SearchParameters params = SearchProtoUtil.makeSearchParameters(SearchProtoUtil.idFilter(accountId))
                .addSearchFilter(
                        SearchFilter.newBuilder().setTraversalFilter(
                                SearchProtoUtil.traverseToType(
                                        TraversalFilter.TraversalDirection.AGGREGATED_BY,
                                        "ServiceProvider")))
                .addSearchFilter(
                        SearchFilter.newBuilder().setTraversalFilter(
                                SearchProtoUtil.traverseToType(
                                        TraversalFilter.TraversalDirection.OWNS,
                                        "CloudService")))
                .build();

        return
                repositoryApi.newSearchRequest(params)
                .getEntities()
                .map(this::buildCloudServicePriceAdjustmentApiDTO)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    /**
     * Build {@link CloudServicePriceAdjustmentApiDTO)
     *
     * @param baseApiDTO        base Api DTO from search service
     * @param topologyEntityDTO topology DTO for repository
     * @param repositoryClient  repository client to find "owned" tiers
     * @return CloudServicePriceAdjustmentApiDTOs which includes their TemplatePriceAdjustmentDTOs
     */
    private Optional<CloudServicePriceAdjustmentApiDTO> buildCloudServicePriceAdjustmentApiDTO(@Nonnull final ApiPartialEntity entity) {
        final CloudServicePriceAdjustmentApiDTO cloudServiceDiscountApiDTO = new CloudServicePriceAdjustmentApiDTO();
        cloudServiceDiscountApiDTO.setUuid(Long.toString(entity.getOid()));
        cloudServiceDiscountApiDTO.setDisplayName(entity.getDisplayName());
        cloudServiceDiscountApiDTO.setPricingModel(ServicePricingModel.ON_DEMAND);
        final List<TemplatePriceAdjustmentDTO> templateDiscountDTOS =
                generateTemplatePriceAdjustmentDTO(entity.getConnectedToList());
        cloudServiceDiscountApiDTO.setTemplateDiscounts(templateDiscountDTOS);
        return Optional.of(cloudServiceDiscountApiDTO);
    }

    /**
     * Build {@link TemplatePriceAdjustmentDTO}
     *
     * @param connectedEntityListList entity list has all the tiers owned by the service
     * @return TemplatePriceAdjustmentDTOs
     */
    private List<TemplatePriceAdjustmentDTO> generateTemplatePriceAdjustmentDTO(@Nonnull final List<RelatedEntity> connectedEntityListList) {
        return getTopologyEntityDTOS(connectedEntityListList)
                .filter(dto -> TIER_TYPES.contains(dto.getEntityType()))
                .map(topologyEntityDTO -> {
                    TemplatePriceAdjustmentDTO templateDiscountDTO = new TemplatePriceAdjustmentDTO();
                    templateDiscountDTO.setFamily(getFamilyName(topologyEntityDTO.getDisplayName()));
                    templateDiscountDTO.setUuid(String.valueOf(topologyEntityDTO.getOid()));
                    templateDiscountDTO.setDisplayName(topologyEntityDTO.getDisplayName());
                    templateDiscountDTO.setPricesPerDatacenter(generateEntityDiscountDTO(topologyEntityDTO.getConnectedToList()));
                    return templateDiscountDTO;
                }).collect(Collectors.toList());
    }

    /**
     * Build {@link EntityPriceDTO}
     *
     * @param connectedEntityList entity list has all the regions owned by the tiers
     * @return EntityPriceDTOs
     */
    private List<EntityPriceDTO> generateEntityDiscountDTO(@Nonnull final List<RelatedEntity> connectedEntityList) {
        return getTopologyEntityDTOS(connectedEntityList)
                .map(tpDTO -> {
                    EntityPriceDTO entityPriceDTO = new EntityPriceDTO();
                    // TODO add entity price
                    //  entityPriceDTO.setPrice(30f);
                    entityPriceDTO.setUuid(String.valueOf(tpDTO.getOid()));
                    entityPriceDTO.setDisplayName(tpDTO.getDisplayName());
                    return entityPriceDTO;
                }).collect(Collectors.toList());
    }

    /**
     * Utility function to retrieve {@link TopologyEntityDTO}
     *
     * @param connectedEntityList connected entity list
     * @return {@link TopologyEntityDTO}s
     */
    private Stream<ApiPartialEntity> getTopologyEntityDTOS(@Nonnull final List<RelatedEntity> connectedEntityList) {
        final Set<Long> oids = connectedEntityList.stream()
                .map(RelatedEntity::getOid)
                .collect(Collectors.toSet());
        return repositoryApi.entitiesRequest(oids).getEntities();
    }

    /**
     * Helper to extract family name from display name
     *
     * @param displayName display name, e.g. db.x1.32xlarge
     * @return family name, e.g. db
     */
    private String getFamilyName(@Nonnull final String displayName) {
        return displayName.split("\\.")[0];
    }
}
