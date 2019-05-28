package com.vmturbo.api.component.external.api.mapper;

import java.util.Collections;
import java.util.HashMap;
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitPriceAdjustmentApiDTO;
import com.vmturbo.api.dto.businessunit.CloudServicePriceAdjustmentApiDTO;
import com.vmturbo.api.dto.businessunit.EntityDiscountDTO;
import com.vmturbo.api.dto.businessunit.EntityPriceDTO;
import com.vmturbo.api.dto.businessunit.TemplatePriceAdjustmentDTO;
import com.vmturbo.api.dto.group.BillingFamilyApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.BusinessUnitType;
import com.vmturbo.api.enums.CloudType;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.enums.ServicePricingModel;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.pagination.SearchPaginationRequest;
import com.vmturbo.api.pagination.SearchPaginationRequest.SearchPaginationResponse;
import com.vmturbo.api.serviceinterfaces.ISearchService;
import com.vmturbo.api.serviceinterfaces.ITargetsService;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo.ServiceLevelDiscount;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo.ServiceLevelDiscount.Builder;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo.TierLevelDiscount;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.api.RepositoryClient;

/**
 * Mapping between Cost domain DTO {@link Discount} and API DTOs
 * {@link BusinessUnitApiDTO} and {@link BusinessUnitPriceAdjustmentApiDTO}.
 * Note: XL currently only support discount type, and seems account abstraction is not yet needed.
 *
 * @Todo: consider adding business unit abstraction to handle CRUD operations, when supporting other business unit types.
 */
public class BusinessUnitMapper {

    private static final String TARGET_ADDRESS = "address";

    private static final String FAILED_TO_GET_TARGET_NAME_FROM_TARGET = "Failed to get target name from target: ";

    private static final String REPOSITORY_CANNOT_RESOLVE_OIDS = "Repository cannot resolve oids: ";

    private static final String FAILED_TO_GET_TARGET_INFORMATION_BY_TARGET_ORIGIN_ID = "Failed to get target information by target originId: ";

    private static final double ZERO = 0.0;

    private static final Logger logger = LogManager.getLogger();

    private static final Set<String> AWS_PROBE = ImmutableSet.of("AWS", "AWS Billing", "AWS Cost");

    private static final Set<Integer> TIER_TYPES = ImmutableSet.of(
            EntityType.COMPUTE_TIER_VALUE,
            EntityType.DATABASE_TIER_VALUE,
            EntityType.STORAGE_TIER_VALUE);

    // business unit workload member type
    private static final String WORKLOAD_MEMBER_TYPE = "Workload";

    private final long realtimeTopologyContextId;

    private final Set SUPPORTED_CLOUD_TYPE = ImmutableSet.of("AWS", "AZURE");

    // EntityDiscountDTO predicate to filter those only have price adjustment
    private static final Predicate<EntityDiscountDTO> ENTITY_DISCOUNT_DTO_PREDICATE = dto ->
            dto.getPriceAdjustment() != null
            && dto.getPriceAdjustment().getValue() != null;

    public BusinessUnitMapper(final long realtimeTopologyContextId) {
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    /**
     * Convert domain object {@link Discount} to API BusinessUnitApiDTO
     * TODO: (September 17, 2019) handle other discount type
     *
     * @param discount         discount from Cost component
     * @param repositoryClient repository client to resolve target type which could be AWS or AZURE
     * @param targetsService   target service to resolve target type which could be AWS or AZURE
     * @return BusinessUnitApiDTO
     */
    @Nonnull
    public BusinessUnitApiDTO toBusinessUnitApiDTO(@Nonnull Discount discount,
                                                   @Nonnull RepositoryClient repositoryClient,
                                                   @Nonnull ITargetsService targetsService) {
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
        businessUnitApiDTO.setHasRelatedTarget(false);

        businessUnitApiDTO.setBusinessUnitType(BusinessUnitType.DISCOUNT);
        final String targetTye = getTargetType(repositoryClient, targetsService, discount.getAssociatedAccountId())
                .orElse(new TargetApiDTO())
                .getType();
        businessUnitApiDTO.setCloudType(SUPPORTED_CLOUD_TYPE.contains(targetTye) ? CloudType.valueOf(targetTye) : CloudType.UNKNOWN);
        businessUnitApiDTO.setChildrenBusinessUnits(ImmutableSet.of(String.valueOf(discount.getAssociatedAccountId())));

        // TODO provide Cost for this business account
        businessUnitApiDTO.setCostPrice(0.0f);
        // TODO provide severity for this business account
        businessUnitApiDTO.setSeverity(Severity.NORMAL.name());
        // It's not clear if business account can have many members.
        // It seems it's always 0 in legacy for business discount unit.
        businessUnitApiDTO.setMembersCount(0);
        // It seems it's always "workload" type in legacy for business discount unit.
        businessUnitApiDTO.setMemberType(WORKLOAD_MEMBER_TYPE);
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
     * @param repositoryClient repository client
     * @param targetsService   target service
     * @return list for business units with discount type
     */
    @Nonnull
    public List<BusinessUnitApiDTO> toDiscountBusinessUnitApiDTO(@Nonnull final Iterator<Discount> discounts,
                                                                 @Nonnull final RepositoryClient repositoryClient,
                                                                 @Nonnull final ITargetsService targetsService) {
        Objects.requireNonNull(discounts);
        final Iterable<Discount> iterable = () -> discounts;
        final List<BusinessUnitApiDTO> businessUnitApiDTOs = StreamSupport.stream(iterable.spliterator(), false)
                .map(discount -> toBusinessUnitApiDTO(discount, repositoryClient, targetsService))
                .collect(Collectors.toList());
        return businessUnitApiDTOs;
    }

    /**
     * Get target type by {@link Discount}.
     * Steps:
     * 1. Use repository to resolve topology entity by account id
     * 2. retrieve origin from topology entity
     * 3. Use target service to retrieve TargetApiDTO by origin id
     */
    private Optional<TargetApiDTO> getTargetType(@Nonnull final RepositoryClient repositoryClient,
                                                 @Nonnull final ITargetsService targetsService,
                                                 final long associatedAccountId) {
        final Stream<TopologyEntityDTO> response = repositoryClient
                .retrieveTopologyEntities(ImmutableList.of(associatedAccountId), realtimeTopologyContextId);

        return response
                .findFirst()
                .flatMap(topologyEntityDTO
                        -> topologyEntityDTO.getOrigin().getDiscoveryOrigin().getDiscoveringTargetIdsList().stream()
                        .findFirst()
                        .flatMap(originId -> {
                            try {
                                return Optional.ofNullable(targetsService.getTarget(String.valueOf(originId)));
                            } catch (Exception e) {
                                logger.error(FAILED_TO_GET_TARGET_INFORMATION_BY_TARGET_ORIGIN_ID + originId);
                                return Optional.of(new TargetApiDTO());
                            }
                        }));
    }

    /**
     * Convert from Cost domain object {@link Discount} to business unit discount API DTO
     *
     * @param discount         discount from Cost component
     * @param repositoryClient repository client
     * @param searchService    search service
     * @return BusinessUnitPriceAdjustmentApiDTO
     * @throws InvalidOperationException
     */
    public BusinessUnitPriceAdjustmentApiDTO toDiscountApiDTO(@Nonnull final Discount discount,
                                                       @Nonnull final RepositoryClient repositoryClient,
                                                       @Nonnull final ISearchService searchService)
            throws Exception {
        final BusinessUnitPriceAdjustmentApiDTO businessUnitDiscountApiDTO = new BusinessUnitPriceAdjustmentApiDTO();
        businessUnitDiscountApiDTO.setServiceDiscounts(toCloudServicePriceAdjustmentApiDTOs(discount, repositoryClient, searchService));
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
     * @param repositoryClient repository client
     * @param searchService    search service
     * @return CloudServicePriceAdjustmentApiDTOs
     * @throws InvalidOperationException if search operation failed
     */
    private List<CloudServicePriceAdjustmentApiDTO> toCloudServicePriceAdjustmentApiDTOs(@Nonnull final Discount discount,
                                                                           @Nonnull final RepositoryClient repositoryClient,
                                                                           @Nonnull final ISearchService searchService)
            throws Exception {
        final List<CloudServicePriceAdjustmentApiDTO> cloudServiceDiscountApiDTOs = getCloudServicePriceAdjustmentApiDTOs(repositoryClient, searchService);
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
     * @param repositoryClient repository client
     * @param searchService    search service
     * @return CloudServicePriceAdjustmentApiDTO
     * @throws InvalidOperationException if search operation failed
     */
    private List<CloudServicePriceAdjustmentApiDTO> getCloudServicePriceAdjustmentApiDTOs(@Nonnull final RepositoryClient repositoryClient,
                                                                            @Nonnull final ISearchService searchService)
            throws Exception {
        // TODO optimize following search, using search service to get all the Cloud services seems overkill
        final GroupApiDTO groupApiDTO = new GroupApiDTO();
        groupApiDTO.setClassName(UIEntityType.CLOUD_SERVICE.apiStr());
        final SearchPaginationRequest searchPaginationRequest =
                new SearchPaginationRequest(null, null, false, null);
        final SearchPaginationResponse searchResponse =
                searchService.getMembersBasedOnFilter("", groupApiDTO, searchPaginationRequest);
        final List<Long> cloudServiceOids = searchResponse.getRawResults().stream()
                .map(baseApiDTO -> Long.parseLong(baseApiDTO.getUuid()))
                .collect(Collectors.toList());
        final List<TopologyEntityDTO> topologyEntityDTOS = repositoryClient
                .retrieveTopologyEntities(cloudServiceOids, realtimeTopologyContextId)
                .collect(Collectors.toList());

        return searchResponse.getRawResults().stream()
                .flatMap(apiDTO -> topologyEntityDTOS
                        .stream()
                        .filter(dto -> dto.getOid() == Long.parseLong(apiDTO.getUuid()))
                        .map(tpDto -> buildCloudServicePriceAdjustmentApiDTO(apiDTO, tpDto, repositoryClient))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                ).collect(Collectors.toList());
    }

    /**
     * Build {@link CloudServicePriceAdjustmentApiDTO)
     *
     * @param baseApiDTO        base Api DTO from search service
     * @param topologyEntityDTO topology DTO for repository
     * @param repositoryClient  repository client to find "owned" tiers
     * @return CloudServicePriceAdjustmentApiDTOs which includes their TemplatePriceAdjustmentDTOs
     */
    private Optional<CloudServicePriceAdjustmentApiDTO> buildCloudServicePriceAdjustmentApiDTO(@Nonnull final BaseApiDTO baseApiDTO,
                                                                                 @Nonnull final TopologyEntityDTO topologyEntityDTO,
                                                                                 @Nonnull final RepositoryClient repositoryClient) {
        final CloudServicePriceAdjustmentApiDTO cloudServiceDiscountApiDTO = new CloudServicePriceAdjustmentApiDTO();
        cloudServiceDiscountApiDTO.setUuid(baseApiDTO.getUuid());
        cloudServiceDiscountApiDTO.setDisplayName(baseApiDTO.getDisplayName());
        cloudServiceDiscountApiDTO.setPricingModel(ServicePricingModel.ON_DEMAND);
        final List<TemplatePriceAdjustmentDTO> templateDiscountDTOS =
                generateTemplatePriceAdjustmentDTO(topologyEntityDTO.getConnectedEntityListList(), repositoryClient);
        cloudServiceDiscountApiDTO.setTemplateDiscounts(templateDiscountDTOS);
        return Optional.of(cloudServiceDiscountApiDTO);
    }

    /**
     * Build {@link TemplatePriceAdjustmentDTO}
     *
     * @param connectedEntityListList entity list has all the tiers owned by the service
     * @param repositoryClient        repository client
     * @return TemplatePriceAdjustmentDTOs
     */
    private List<TemplatePriceAdjustmentDTO> generateTemplatePriceAdjustmentDTO(@Nonnull final List<ConnectedEntity> connectedEntityListList,
                                                                  @Nonnull final RepositoryClient repositoryClient) {
        return getTopologyEntityDTOS(connectedEntityListList, repositoryClient)
                .stream()
                .filter(dto -> TIER_TYPES.contains(dto.getEntityType()))
                .map(topologyEntityDTO -> {
                    TemplatePriceAdjustmentDTO templateDiscountDTO = new TemplatePriceAdjustmentDTO();
                    templateDiscountDTO.setFamily(getFamilyName(topologyEntityDTO.getDisplayName()));
                    templateDiscountDTO.setUuid(String.valueOf(topologyEntityDTO.getOid()));
                    templateDiscountDTO.setDisplayName(topologyEntityDTO.getDisplayName());
                    templateDiscountDTO.setPricesPerDatacenter(generateEntityDiscountDTO(topologyEntityDTO.getConnectedEntityListList(), repositoryClient));
                    return templateDiscountDTO;
                }).collect(Collectors.toList());
    }

    /**
     * Build {@link EntityPriceDTO}
     *
     * @param connectedEntityList entity list has all the regions owned by the tiers
     * @param repositoryClient    repository client
     * @return EntityPriceDTOs
     */
    private List<EntityPriceDTO> generateEntityDiscountDTO(@Nonnull final List<ConnectedEntity> connectedEntityList,
                                                           @Nonnull final RepositoryClient repositoryClient) {
        return getTopologyEntityDTOS(connectedEntityList, repositoryClient).stream()
                .filter(topologyEntityDTO -> topologyEntityDTO.getEntityType() == EntityType.REGION_VALUE)
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
     * @param repositoryClient    repository client
     * @return {@link TopologyEntityDTO}s
     */
    private List<TopologyEntityDTO> getTopologyEntityDTOS(@Nonnull final List<ConnectedEntity> connectedEntityList,
                                                          @Nonnull final RepositoryClient repositoryClient) {
        final List<Long> oids = connectedEntityList.stream()
                .map(connectedEntity -> connectedEntity.getConnectedEntityId())
                .collect(Collectors.toList());
        return oids.isEmpty() ? Collections.emptyList() :
                repositoryClient.retrieveTopologyEntities(oids, realtimeTopologyContextId)
                .collect(Collectors.toList());
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
     * @param searchService    search service to find all the oids
     * @param targetsService   target service to find the Cloud type (AWS or Azure)
     * @param repositoryClient repository client
     * @return discovered business unit with discount type
     * @throws InvalidOperationException if search operation failed
     */
    public List<BusinessUnitApiDTO> getAndConvertDiscoveredBusinessUnits(@Nonnull final ISearchService searchService,
                                                                         @Nonnull final ITargetsService targetsService,
                                                                         @Nonnull final RepositoryClient repositoryClient)
            throws Exception {
        // TODO optimize following search, using search service to get all the discovered business accounts seems overkill
        final GroupApiDTO groupApiDTO = new GroupApiDTO();
        groupApiDTO.setClassName(UIEntityType.BUSINESS_ACCOUNT.apiStr());
        final SearchPaginationRequest searchPaginationRequest = new SearchPaginationRequest(null, null, false, null);
        final List<BaseApiDTO> baseApiDTOS = searchService.getMembersBasedOnFilter("", groupApiDTO, searchPaginationRequest).getRawResults();

        final List<Long> oids = baseApiDTOS.stream()
                .map(baseApiDTO -> Long.parseLong(baseApiDTO.getUuid()))
                .collect(Collectors.toList());
        Stream<TopologyEntityDTO> entityStream = repositoryClient.retrieveTopologyEntities(oids, realtimeTopologyContextId);
        final List<TopologyEntityDTO> entities = entityStream
                .collect(Collectors.toList());

        if (entities.size() > 0) {
            final long firstTopologyEntityOid = entities.get(0).getOid();
            final String type = getTargetType(repositoryClient, targetsService, firstTopologyEntityOid)
                    .map(targetApiDTO -> targetApiDTO.getType())
                    .orElse("UNKNOWN");

            return baseApiDTOS.stream()
                    .flatMap(apiDTO -> entities
                            .stream()
                            .filter(dto -> dto.getOid() == Long.parseLong(apiDTO.getUuid()))
                            .map(tpDto -> buildDiscoveredBusinessUnitApiDTO(apiDTO, tpDto, normalize(type), targetsService))
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                    ).collect(Collectors.toList());
        }
        throw new MissingTopologyEntityException(REPOSITORY_CANNOT_RESOLVE_OIDS + oids);
    }

    // From UI perspective, AWS is one Cloud type, but in implementation, we have AWS, AWS billing
    // and AWS Cost probe, and business unit from these are all from AWS Cloud type.
    private CloudType normalize(@Nonnull final String type) {
        if (AWS_PROBE.contains(type)) {
            return CloudType.AWS;
        }
        try {
            return CloudType.valueOf(type);
        } catch (IllegalArgumentException exception) {
            return CloudType.UNKNOWN;
        }
    }

    /**
     * Build discovered business unit API DTO.
     *
     * @param baseApiDTO        API dto
     * @param topologyEntityDTO topology entity DTOP for the business account
     * @param cloudType         account Cloud type
     * @param targetsService    target service to get the account's target
     * @return BusinessUnitApiDTO
     */
    private Optional<BusinessUnitApiDTO> buildDiscoveredBusinessUnitApiDTO(@Nonnull final BaseApiDTO baseApiDTO,
                                                                           @Nonnull final TopologyEntityDTO topologyEntityDTO,
                                                                           @Nonnull final CloudType cloudType,
                                                                           @Nonnull final ITargetsService targetsService) {
        final BusinessUnitApiDTO businessUnitApiDTO = new BusinessUnitApiDTO();
        businessUnitApiDTO.setBusinessUnitType(BusinessUnitType.DISCOVERED);
        businessUnitApiDTO.setUuid(baseApiDTO.getUuid());
        businessUnitApiDTO.setEnvironmentType(EnvironmentType.CLOUD);
        businessUnitApiDTO.setClassName(UIEntityType.BUSINESS_ACCOUNT.apiStr());
        businessUnitApiDTO.setBudget(new StatApiDTO());

        // TODO set cost
        businessUnitApiDTO.setCostPrice(0.0f);
        // discovered account doesn't have discount (yet)
        businessUnitApiDTO.setDiscount(0.0f);

        businessUnitApiDTO.setMemberType(WORKLOAD_MEMBER_TYPE);
        final List<ConnectedEntity> accounts = topologyEntityDTO.getConnectedEntityListList().stream()
                .filter(entity -> entity.getConnectedEntityType() == EntityType.BUSINESS_ACCOUNT_VALUE)
                .collect(Collectors.toList());
        businessUnitApiDTO.setMembersCount(accounts.size());

        businessUnitApiDTO.setChildrenBusinessUnits(accounts.stream()
                .map(connectedEntity -> String.valueOf(connectedEntity.getConnectedEntityId()))
                .collect(Collectors.toList()));
        businessUnitApiDTO.setCloudType(cloudType);
        businessUnitApiDTO.setDisplayName(topologyEntityDTO.getDisplayName());
        businessUnitApiDTO.setHasRelatedTarget(
            topologyEntityDTO.getTypeSpecificInfo().getBusinessAccount().getHasAssociatedTarget());
        businessUnitApiDTO.setMaster(accounts.size() > 0);
        if (accounts.size() > 0) {
            final List<TargetApiDTO> targetApiDTOS = topologyEntityDTO
                    .getOrigin()
                    .getDiscoveryOrigin()
                    .getDiscoveringTargetIdsList()
                    .stream()
                    .map(oid -> getTargetAPIDTO(oid, targetsService, cloudType))
                    .collect(Collectors.toList());
            businessUnitApiDTO.setTargets(targetApiDTOS);
        }
        return Optional.of(businessUnitApiDTO);
    }

    /**
     * Set business account's targets.
     *
     * @param targetId       target id
     * @param targetsService target service to find target information
     * @param cloudType      type of the target
     * @return TargetApiDTO
     */
    private TargetApiDTO getTargetAPIDTO(@Nonnull final Long targetId,
                                         @Nonnull final ITargetsService targetsService,
                                         @Nonnull final CloudType cloudType) {
        try {
            final TargetApiDTO targetApiDTO = targetsService.getTarget(String.valueOf(targetId));
            targetApiDTO.setType(cloudType.name());
            targetApiDTO.setDisplayName(targetApiDTO.getInputFields().stream()
                    .filter(apiDTO -> apiDTO.getName().equalsIgnoreCase(TARGET_ADDRESS))
                    .findFirst()
                    .orElseThrow(() -> new MissingTargetNameException(FAILED_TO_GET_TARGET_NAME_FROM_TARGET + targetApiDTO)).getValue());
            return targetApiDTO;
        } catch (Exception e) {
            logger.error(e.getMessage());
            return new TargetApiDTO();
        }
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
        public MissingTargetNameException(final String msg) {
            super(msg);
        }
    }

    /**
     * Throws when the repository cannot find the oid
     */
    class MissingTopologyEntityException extends RuntimeException {
        public MissingTopologyEntityException(final String s) {
            super(s);
        }
    }
}
