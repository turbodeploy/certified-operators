package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
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
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.group.BillingFamilyApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.BusinessUnitType;
import com.vmturbo.api.enums.CloudType;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.enums.ServicePricingModel;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.ITargetsService;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo.ServiceLevelDiscount;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo.ServiceLevelDiscount.Builder;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo.TierLevelDiscount;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessAccountInfo;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier;

/**
 * Mapping between Cost domain DTO {@link Discount} and API DTOs
 * {@link BusinessUnitApiDTO} and {@link BusinessUnitPriceAdjustmentApiDTO}. To get the topology information,
 * this class will rely on the Repository API calls.
 * Note: XL currently only support discount type, and seems account abstraction is not yet needed.
 *
 * @Todo: consider adding business unit abstraction to handle CRUD operations, when supporting other business unit types.
 */
public class BusinessUnitMapper {

    private static final String TARGET_ADDRESS = "address";

    private static final String FAILED_TO_GET_TARGET_NAME_FROM_TARGET = "Failed to get target name from target: ";

    private static final String REPOSITORY_CANNOT_RESOLVE_OIDS = "Repository cannot resolve oids: ";

    private static final String FAILED_TO_GET_TARGET_INFORMATION_BY_TARGET_ORIGIN_ID = "Failed to get target information by target originId: ";

    private static final String TARGET_TYPE_UNKNOWN = "UNKNOWN";

    private static final double ZERO = 0.0;

    private static final Logger logger = LogManager.getLogger();

    private static final Set<Integer> TIER_TYPES = ImmutableSet.of(
            EntityType.COMPUTE_TIER_VALUE,
            EntityType.DATABASE_TIER_VALUE,
            EntityType.STORAGE_TIER_VALUE);

    private static final Set<String> WORKLOAD_ENTITY_TYPES = ImmutableSet.of(
            StringConstants.VIRTUAL_MACHINE,
            StringConstants.DATABASE,
            StringConstants.DATABASE_SERVER);

    private final long realtimeTopologyContextId;

    private final RepositoryApi repositoryApi;

    private final Set<String> SUPPORTED_CLOUD_TYPE = ImmutableSet.of("AWS", "AZURE");

    // EntityDiscountDTO predicate to filter those only have price adjustment
    private static final Predicate<EntityDiscountDTO> ENTITY_DISCOUNT_DTO_PREDICATE = dto ->
            dto.getPriceAdjustment() != null
            && dto.getPriceAdjustment().getValue() != null;

    public BusinessUnitMapper(final long realtimeTopologyContextId, @Nonnull final RepositoryApi repositoryApi) {
        this.realtimeTopologyContextId = realtimeTopologyContextId;
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
        final List<CloudServicePriceAdjustmentApiDTO> cloudServiceDiscountApiDTOs = getCloudServicePriceAdjustmentApiDTOs();
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
                .filter(entity -> entity.getEntityType() == EntityType.REGION_VALUE)
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

    /**
     * Find all the discovered business unit with discount type
     *
     * @param targetsService   target service to find the Cloud type (AWS or Azure)
     * @return discovered business unit with discount type
     * @throws InvalidOperationException if search operation failed
     */
    public List<BusinessUnitApiDTO> getAndConvertDiscoveredBusinessUnits(
        @Nonnull final ITargetsService targetsService)
        throws Exception {
        return this.getAndConvertDiscoveredBusinessUnits(targetsService, Collections.emptyList());
    }

    /**
     * Find all discovered business units discovered by targets in the scope.  If scope is null
     * or empty, return all discovered business units.
     *
     * @param targetsService target service needed to enforce scope and populate target information
     *                       in the {@link BusinessUnitApiDTO}.
     * @param scopes a list of target ids.
     * @return Set of discovered business units.
     */
    public List<BusinessUnitApiDTO> getAndConvertDiscoveredBusinessUnits(
        @Nonnull final ITargetsService targetsService,
        List<String> scopes) {
        final Set<Long> scopeTargets = new HashSet<>();
        final Set<String> validTargetIds = new HashSet<>();
        if (scopes != null && !scopes.isEmpty()) {
            try {
                targetsService.getTargets(EnvironmentType.CLOUD)
                    .forEach(targetApiDTO -> validTargetIds.add(targetApiDTO.getUuid()));
            } catch (UnknownObjectException e) {
                logger.warn("Could not get target list due to exception {}.  Cannot enforce scope.", e);
            }
        }
        if (!validTargetIds.isEmpty()) {
            scopes.forEach(targetId -> {
                try {
                    if (validTargetIds.contains(targetId)) {
                        scopeTargets.add(Long.parseLong(targetId));
                    } else {
                        logger.warn("No target matching target ID {} found."
                            + "  {} will not be added to scope.", targetId, targetId);
                    }
                } catch (NumberFormatException nfe) {
                    // should never happen
                    logger.warn("Could not parse target id: {}", targetId);
                }
            });
        }
        final List<TopologyEntityDTO> businessAccounts = repositoryApi.newSearchRequest(
            SearchProtoUtil.makeSearchParameters(SearchProtoUtil.entityTypeFilter(UIEntityType.BUSINESS_ACCOUNT))
                .build())
            .getFullEntities()
            .collect(Collectors.toList());

        final List<TargetApiDTO> cloudTargetList = getCloudTargets(targetsService);

        return businessAccounts.stream()
            .filter(entity -> scopeTargets.isEmpty()
                || entity.getOrigin().getDiscoveryOrigin().getDiscoveringTargetIdsList()
                .stream()
                .anyMatch(scopeTargets::contains))
            .map(tpDto -> buildDiscoveredBusinessUnitApiDTO(tpDto, cloudTargetList))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());
    }

    /**
     * Get the Business Unit for the input OID.
     *
     * @param targetsService The target service to find the cloud type(AWS or Azure).
     * @param oid The input OID value.
     *
     * @return The Business Unit DTO for the input OID.
     * @throws UnsupportedOperationException if the Business Unit cannot be found or is invalid.
     */
    public BusinessUnitApiDTO getBusinessUnitByOID(ITargetsService targetsService, String oid) {
        TopologyEntityDTO outputDTO = repositoryApi.newSearchRequest(
                        SearchProtoUtil.makeSearchParameters(SearchProtoUtil.stringPropertyFilterExact(SearchableProperties.OID, ImmutableList.of(oid) ))
                        .build())
                        .getFullEntities()
                        .collect(Collectors.toList())
                        .stream()
                        .findAny()
                        .orElseThrow(() -> new UnsupportedOperationException("Cannot find Business Unit with OID: " + oid));

        List<TargetApiDTO> cloudTargetList = getCloudTargets(targetsService);;
        return buildDiscoveredBusinessUnitApiDTO(outputDTO, cloudTargetList)
                        .orElseThrow(() -> new UnsupportedOperationException("Invalid Business Unit for OID: " + oid));
    }

    /**
     * Build discovered business unit API DTO.
     *
     * @param topologyEntityDTO topology entity DTOP for the business account
     * @param cloudTargetList    cloud targets as returned by the target service
     * @return BusinessUnitApiDTO
     */
    private Optional<BusinessUnitApiDTO> buildDiscoveredBusinessUnitApiDTO(@Nonnull final TopologyEntityDTO topologyEntityDTO,
                                                                           @Nonnull final List<TargetApiDTO> cloudTargetList) {
        final CloudType cloudType = CloudType.fromProbeType(getTargetType(topologyEntityDTO.getOid())
            .map(TargetApiDTO::getType)
            .orElse(TARGET_TYPE_UNKNOWN));
        final BusinessUnitApiDTO businessUnitApiDTO = new BusinessUnitApiDTO();
        businessUnitApiDTO.setBusinessUnitType(BusinessUnitType.DISCOVERED);
        businessUnitApiDTO.setUuid(Long.toString(topologyEntityDTO.getOid()));
        businessUnitApiDTO.setEnvironmentType(EnvironmentType.CLOUD);
        businessUnitApiDTO.setClassName(UIEntityType.BUSINESS_ACCOUNT.apiStr());
        businessUnitApiDTO.setBudget(new StatApiDTO());

        // TODO set cost
        businessUnitApiDTO.setCostPrice(0.0f);
        // discovered account doesn't have discount (yet)
        businessUnitApiDTO.setDiscount(0.0f);

        businessUnitApiDTO.setMemberType(StringConstants.WORKLOAD);
        final List<ConnectedEntity> accounts = topologyEntityDTO.getConnectedEntityListList().stream()
                .filter(entity -> entity.getConnectedEntityType() == EntityType.BUSINESS_ACCOUNT_VALUE)
                .collect(Collectors.toList());

        // Get the OIDs of the entities owned by the BusinessAccount
        Set<Long> entityOids = topologyEntityDTO.getConnectedEntityListList()
                        .stream()
                        .map(entity -> entity.getConnectedEntityId())
                        .collect(Collectors.toSet());

        List<ServiceEntityApiDTO> results = repositoryApi.entitiesRequest(entityOids)
                .getSEMap()
                .values()
                .stream()
                .filter(se -> WORKLOAD_ENTITY_TYPES.contains(se.getClassName()))
                .collect(Collectors.toList());
        businessUnitApiDTO.setMembersCount(results.size());

        businessUnitApiDTO.setChildrenBusinessUnits(accounts.stream()
                .map(connectedEntity -> String.valueOf(connectedEntity.getConnectedEntityId()))
                .collect(Collectors.toList()));
        businessUnitApiDTO.setCloudType(cloudType);
        businessUnitApiDTO.setDisplayName(topologyEntityDTO.getDisplayName());
        if (topologyEntityDTO.hasTypeSpecificInfo()
            && topologyEntityDTO.getTypeSpecificInfo().hasBusinessAccount()) {
            BusinessAccountInfo bizInfo =
                topologyEntityDTO.getTypeSpecificInfo().getBusinessAccount();
            if (bizInfo.hasAccountId()) {
                businessUnitApiDTO.setAccountId(bizInfo.getAccountId());
            }
            if (bizInfo.hasAssociatedTargetId()) {
                businessUnitApiDTO.setAssociatedTargetId(
                    bizInfo.getAssociatedTargetId());
            }
            businessUnitApiDTO.setPricingIdentifiers(bizInfo.getPricingIdentifiersList()
                .stream()
                .collect(Collectors.toMap(pricingId -> pricingId.getIdentifierName().name(),
                    PricingIdentifier::getIdentifierValue)));
        }
        businessUnitApiDTO.setMaster(accounts.size() > 0);
        final Map<String, TargetApiDTO> targetApiDtoByTargetId = new HashMap<>();
        cloudTargetList.stream()
            .forEach(targetApiDTO -> {
                targetApiDTO.setType(cloudType.name());
                targetApiDtoByTargetId.put(targetApiDTO.getUuid(), targetApiDTO);
            });
        final List<TargetApiDTO> targetApiDTOS = topologyEntityDTO
                .getOrigin()
                .getDiscoveryOrigin()
                .getDiscoveringTargetIdsList()
                .stream()
                .map(String::valueOf)
                .map(targetApiDtoByTargetId::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        businessUnitApiDTO.setTargets(targetApiDTOS);
        return Optional.of(businessUnitApiDTO);
    }

    /**
     * Convert a {@link BusinessUnitApiDTO} to {@link BillingFamilyApiDTO}.
     *
     * @param masterAccount the master BusinessAccount to convert
     * @param accountIdToDisplayName map from account id to its displayName
     * @return the converted BillingFamilyApiDTO for the given BusinessUnitApiDTO
     */
    public BillingFamilyApiDTO businessUnitToBillingFamily(@Nonnull BusinessUnitApiDTO masterAccount,
                                                           @Nonnull Map<String, String> accountIdToDisplayName) {
        BillingFamilyApiDTO billingFamilyApiDTO = new BillingFamilyApiDTO();
        billingFamilyApiDTO.setMasterAccountUuid(masterAccount.getUuid());
        Map<String, String> uuidToName = new HashMap<>();
        uuidToName.put(masterAccount.getUuid(), masterAccount.getDisplayName());
        masterAccount.getChildrenBusinessUnits().forEach(subAccountId ->
            uuidToName.put(subAccountId, accountIdToDisplayName.get(subAccountId)));
        billingFamilyApiDTO.setUuidToNameMap(uuidToName);
        billingFamilyApiDTO.setMembersCount(masterAccount.getChildrenBusinessUnits().size());
        billingFamilyApiDTO.setClassName(StringConstants.BILLING_FAMILY);
        billingFamilyApiDTO.setDisplayName(masterAccount.getDisplayName());
        billingFamilyApiDTO.setEnvironmentType(EnvironmentType.CLOUD);
        return billingFamilyApiDTO;
    }

    /**
     * Throws when target name is NOT in the target.
     */
    private class MissingTargetNameException extends RuntimeException {

        private static final long serialVersionUID = -6058086629099834840L;

        public MissingTargetNameException(final String msg) {
            super(msg);
        }
    }

    /**
     * Throws when the repository cannot find the oid
     */
    class MissingTopologyEntityException extends RuntimeException {

        private static final long serialVersionUID = 7240718389533990427L;

        public MissingTopologyEntityException(final String s) {
            super(s);
        }
    }

    /**
     * Get the cloud Targets from the provided Targets Service.
     * @param targetsService object of type ITargetsService.
     * @return List of targets with EnvironmentType.CLOUD.
     */
    private List<TargetApiDTO> getCloudTargets(ITargetsService targetsService) {
        List<TargetApiDTO> cloudTargetList = new ArrayList<>();

        try {
            cloudTargetList.addAll(targetsService.getTargets(EnvironmentType.CLOUD));
        } catch (UnknownObjectException e) {
            logger.warn("Could not get target list due to exception {}.  "
                            + "Cannot populate targets in BusinessUnitApiDTO.", e);
        }

        return cloudTargetList;
    }

}
