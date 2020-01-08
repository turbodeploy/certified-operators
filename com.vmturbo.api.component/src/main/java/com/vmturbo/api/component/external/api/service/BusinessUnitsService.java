package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.validation.Errors;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.SingleEntityRequest;
import com.vmturbo.api.component.external.api.mapper.CloudTypeMapper;
import com.vmturbo.api.component.external.api.mapper.DiscountMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.util.BusinessAccountRetriever;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiInputDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitPriceAdjustmentApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.BusinessUnitType;
import com.vmturbo.api.enums.CloudType;
import com.vmturbo.api.enums.EntityDetailType;
import com.vmturbo.api.enums.EntityState;
import com.vmturbo.api.enums.HierarchicalRelationship;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.ActionPaginationRequest;
import com.vmturbo.api.pagination.ActionPaginationRequest.ActionPaginationResponse;
import com.vmturbo.api.pagination.EntityPaginationRequest;
import com.vmturbo.api.pagination.EntityPaginationRequest.EntityPaginationResponse;
import com.vmturbo.api.serviceinterfaces.IBusinessUnitsService;
import com.vmturbo.common.protobuf.cost.Cost.CreateDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.CreateDiscountResponse;
import com.vmturbo.common.protobuf.cost.Cost.DeleteDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo.AccountLevelDiscount;
import com.vmturbo.common.protobuf.cost.Cost.DiscountQueryFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.UpdateDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.UpdateDiscountResponse;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * {@inheritDoc}
 */
public class BusinessUnitsService implements IBusinessUnitsService {

    // Error message for missing business unit DTO.
    public static final String INVALID_BUSINESS_UNIT_DTO = "Business unit input DTO is missing associated account id: ";

    private static final Logger logger = LogManager.getLogger();

    private final long realtimeTopologyContextId;

    private final CostServiceBlockingStub costService;

    private final DiscountMapper mapper;

    private final ThinTargetCache thinTargetCache;

    private final UuidMapper uuidMapper;

    private final EntitiesService entitiesService;

    private final RepositoryApi repositoryApi;

    private final SupplyChainFetcherFactory supplyChainFetcherFactory;

    private final BusinessAccountRetriever businessAccountRetriever;

    private final CloudTypeMapper cloudTypeMapper;

    public BusinessUnitsService(@Nonnull final CostServiceBlockingStub costServiceBlockingStub,
                                @Nonnull final DiscountMapper mapper,
                                @Nonnull final ThinTargetCache thinTargetCache,
                                final long realtimeTopologyContextId,
                                @Nonnull final UuidMapper uuidMapper,
                                @Nonnull final EntitiesService entitiesService,
                                @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                                @Nonnull final RepositoryApi repositoryApi,
                                @Nonnull final BusinessAccountRetriever businessAccountRetriever,
                                @Nonnull final CloudTypeMapper cloudTypeMapper) {
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.costService = Objects.requireNonNull(costServiceBlockingStub);
        this.mapper = Objects.requireNonNull(mapper);
        this.thinTargetCache = Objects.requireNonNull(thinTargetCache);
        this.uuidMapper = Objects.requireNonNull(uuidMapper);
        this.entitiesService = entitiesService;
        this.supplyChainFetcherFactory = supplyChainFetcherFactory;
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.businessAccountRetriever = Objects.requireNonNull(businessAccountRetriever);
        this.cloudTypeMapper = Objects.requireNonNull(cloudTypeMapper);
    }

    /**
     * Note: the UI currently always passes the  @param cloudType as null (September 19, 2019).
     * So, the codes will not use cloudType.
     * {@inheritDoc}
     */
    @Override
    public List<BusinessUnitApiDTO> getBusinessUnits(@Nullable BusinessUnitType type,
                                                     @Nullable String cloudType,
                                                     @Nullable Boolean hasParent,
                                                     @Nullable String scopeUuid) throws Exception {
        // TODO OM-35804 implement required behavior for other types other than discount
        if (BusinessUnitType.DISCOUNT.equals(type)) {
            final Iterator<Discount> discounts = costService.getDiscounts(GetDiscountRequest.newBuilder()
                .build());
            // We also create discounts for sub-accounts when creating a discount for a master
            // account. We should filter them when sending the discounts to UI.
            List<Discount> discountsList = Lists.newArrayList(discounts);
            if (discountsList.size() > 0) {
                Set<Long> accountIds = discountsList.stream()
                    .map(Discount::getAssociatedAccountId)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());

                Collection<BusinessUnitApiDTO> accounts = businessAccountRetriever.getBusinessAccounts(accountIds);
                Set<Long> masterAccounts = accounts.stream().filter(BusinessUnitApiDTO::isMaster)
                    .map(BusinessUnitApiDTO::getUuid)
                    .map(Long::valueOf)
                    .collect(Collectors.toSet());

                discountsList = discountsList.stream()
                    .filter(discount -> masterAccounts.contains(discount.getAssociatedAccountId()))
                    .collect(Collectors.toList());
            }

            return mapper.toDiscountBusinessUnitApiDTO(discountsList.iterator());
        } else if (BusinessUnitType.DISCOVERED.equals(type)) {
            final List<BusinessUnitApiDTO> cloudTypeScopedBusinessUnits =
                businessAccountRetriever.getBusinessAccountsInScope(Collections.emptyList(), null)
                    .stream()
                    .filter(businessUnit -> cloudType == null ||
                        matchesCloudType(cloudType, businessUnit))
                    .collect(Collectors.toList());
            if (hasParent == null) {
                return cloudTypeScopedBusinessUnits;
            }
            final Set<String> childrenBusinessUnits = cloudTypeScopedBusinessUnits.stream()
                .flatMap(businessUnitApiDTO ->
                    businessUnitApiDTO.getChildrenBusinessUnits().stream())
                .collect(Collectors.toSet());
            if (!hasParent) {
                return cloudTypeScopedBusinessUnits.stream()
                    .filter(businessUnit -> !childrenBusinessUnits.contains(businessUnit.getUuid()))
                    .collect(Collectors.toList());
            }
            // hasParent == true
            return cloudTypeScopedBusinessUnits.stream()
                .filter(businessUnit -> childrenBusinessUnits.contains(businessUnit.getUuid()))
                .collect(Collectors.toList());
            }
        return ImmutableList.of(new BusinessUnitApiDTO());
    }

    /**
     * Check if a given {@link BusinessUnitApiDTO} is of the given cloudType.
     *
     * @param cloudType String representing the cloud type to check for.
     * @param businessUnit {@link BusinessUnitApiDTO} whose cloud type we're checking.
     * @return true if the {@link BusinessUnitApiDTO} is discovered by a target of the given cloud
     * type.
     */
    private boolean matchesCloudType(@Nonnull String cloudType,
                                     @Nonnull BusinessUnitApiDTO businessUnit) {
        Optional<CloudType> optCloudTypeEnum = CloudType.getByName(cloudType);
        if (!optCloudTypeEnum.isPresent()) {
            logger.warn("No matching CloudType found for string {}", cloudType);
            return false;
        }
        return businessUnit.getTargets().stream()
            .map(TargetApiDTO::getUuid)
            .filter(StringUtils::isNumeric)
            .map(Long::parseLong)
            .map(thinTargetCache::getTargetInfo)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .filter(thinTargetInfo -> !thinTargetInfo.isHidden())
            .anyMatch(thinTargetInfo -> optCloudTypeEnum.get().equals(
                cloudTypeMapper.fromTargetType(thinTargetInfo.probeInfo().type())));
    }

    @Override
    public BusinessUnitApiDTO getBusinessUnitByUuid(final String uuid) throws Exception {
        return businessAccountRetriever.getBusinessAccount(uuid);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BusinessUnitApiDTO createBusinessUnit(
            final BusinessUnitApiInputDTO businessUnitApiInputDTO) throws Exception {
        final String associatedAccountId = businessUnitApiInputDTO.getChildrenBusinessUnits()
                .stream()
                .findFirst().orElseThrow(() -> new IllegalArgumentException(INVALID_BUSINESS_UNIT_DTO
                        + businessUnitApiInputDTO));
        // TODO validate input DTO with validateInput method. It must have displayName
        return createPriceAdjustment(Long.valueOf(associatedAccountId),
            businessUnitApiInputDTO);
    }

    /**
     * Use case #1: Create new discount with account level discount (only)
     * Use case #2: Update the existing discount with new account level discount (only).
     * <p>
     * {@inheritDoc}
     */
    @Override
    public BusinessUnitApiDTO editBusinessUnit(final String uuid,
                                               final BusinessUnitApiInputDTO businessUnitApiInputDTO) throws Exception {
        if (uuid != null && !uuid.isEmpty()) {
            // update account level discount only
            final DiscountInfo discountInfo = DiscountInfo.newBuilder()
                .setAccountLevelDiscount(AccountLevelDiscount.newBuilder()
                    .setDiscountPercentage(businessUnitApiInputDTO.getPriceAdjustment().getValue()).build())
                .setDisplayName(businessUnitApiInputDTO.getName())
                .build();
            return  mapper.toBusinessUnitApiDTO(editPriceAdjustment(Long.valueOf(uuid),
                discountInfo));
        } else {
            // create a new discount with provided account level discount
            final String associatedAccountId = businessUnitApiInputDTO.getTargets().stream()
                    .findFirst().orElseThrow(() -> new IllegalArgumentException(INVALID_BUSINESS_UNIT_DTO
                            + businessUnitApiInputDTO));
            return createPriceAdjustment(Long.valueOf(associatedAccountId), businessUnitApiInputDTO);
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public BusinessUnitPriceAdjustmentApiDTO
    editBusinessUnitPriceAdjustments(@Nonnull String uuid,
                              @Nonnull BusinessUnitPriceAdjustmentApiDTO businessUnitDiscountApiDTO)
            throws Exception {
        Objects.requireNonNull(uuid);
        Objects.requireNonNull(businessUnitDiscountApiDTO);
        final DiscountInfo discountInfo = DiscountInfo.newBuilder()
                .setServiceLevelDiscount(mapper.toServiceDiscountProto(businessUnitDiscountApiDTO))
                .setTierLevelDiscount(mapper.toTierDiscountProto(businessUnitDiscountApiDTO))
                .build();
        return  mapper.toDiscountApiDTO(editPriceAdjustment(Long.valueOf(uuid), discountInfo));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Boolean deleteBusinessUnit(final String uuid) throws Exception {
        Objects.requireNonNull(uuid);
        Long businessAccountId = Long.valueOf(uuid);
        Iterator<Discount> discountIterator = costService.getDiscounts(GetDiscountRequest.newBuilder()
            .setFilter(DiscountQueryFilter.newBuilder().addAssociatedAccountId(businessAccountId).build())
            .build());

        if (!discountIterator.hasNext()) {
            logger.error("Cannot delete a price adjustment that doesn't " +
                "exist. Business Unit: {}", businessAccountId);
            throw new OperationFailedException("Cannot delete a price adjustment that doesn't " +
                "exist.");
        }

        Discount currentDiscount = discountIterator.next();

        // We also need to delete entries for children accounts of the account
        // as well.
        List<Long> childAccounts = getChildAccounts(businessAccountId);

        Set<Long> accountsWithDeletedDiscount = new HashSet<>();
        boolean isDeleted = costService.deleteDiscount(DeleteDiscountRequest.newBuilder()
                .setAssociatedAccountId(businessAccountId)
                .build())
                .getDeleted();
        accountsWithDeletedDiscount.add(businessAccountId);

        for (Long accOid : childAccounts) {
            try {
                costService.deleteDiscount(DeleteDiscountRequest.newBuilder()
                    .setAssociatedAccountId(accOid)
                    .build());
                accountsWithDeletedDiscount.add(accOid);
            } catch (StatusRuntimeException exception) {
                if (exception.getStatus().getCode() == Status.Code.NOT_FOUND) {
                    // If the account does not exist just ignore it
                    logger.warn("Account {} discount does not exist.", accOid);
                } else {
                    // if we had an exception half way through creating the discounts
                    // try adding back all deleted discounts
                    accountsWithDeletedDiscount.forEach(accUuid ->
                        costService.createDiscount(CreateDiscountRequest.newBuilder()
                            .setDiscountInfo(currentDiscount.getDiscountInfo())
                            .setId(accUuid)
                            .build()));
                    throw exception;
                }
            }
        }

        return isDeleted;
    }

    @Override
    public void validateInput(final Object o, final Errors errors) {
        //TODO implement validator for business unit
    }

    @Nonnull
    @Override
    public Collection<BusinessUnitApiDTO> getRelatedBusinessUnits(@Nonnull String uuid,
                                                                  HierarchicalRelationship relationship) throws Exception {
        // TODO (roman, Nov 14 2019): Handle other relationship types (parent, siblings).
        return businessAccountRetriever.getChildAccounts(uuid);
    }

    @Override
    public EntityPaginationResponse getEntities(final String uuid,
                                                final EntityPaginationRequest paginationRequest) throws Exception {
        // expand the scope and get the oids for all individual entities

        SingleEntityRequest singleRequest = repositoryApi.entityRequest(uuidMapper.fromUuid(uuid).oid());
        TopologyEntityDTO topologyEntityDTO = singleRequest
                        .getFullEntity()
                        .orElseThrow(() -> new UnknownObjectException(uuid));;

        // get the entities owned by BusinessAccount: BA.getConsistsOf
        Set<Long> entitiesOid = topologyEntityDTO.getConnectedEntityListList()
                        .stream()
                        .map(connEnt -> connEnt.getConnectedEntityId())
                        .collect(Collectors.toSet());;

        List<ServiceEntityApiDTO> results = new ArrayList<>(repositoryApi.entitiesRequest(entitiesOid).getSEMap().values());

        return paginationRequest.allResultsResponse(results);
    }

    @Override
    public ActionPaginationResponse getActions(final String uuid,
                                               final ActionApiInputDTO inputDto,
                                               final ActionPaginationRequest paginationRequest) throws Exception {
        return entitiesService.getActionsByEntityUuid(uuid, inputDto, paginationRequest);
    }

    @Override
    public List<StatSnapshotApiDTO> getActionCountStatsByUuid(final String uuid,
                                                              final ActionApiInputDTO inputDto) throws Exception {
        SingleEntityRequest singleRequest = repositoryApi.entityRequest(uuidMapper.fromUuid(uuid).oid());
        TopologyEntityDTO topologyEntityDTO = singleRequest
                        .getFullEntity()
                        .orElseThrow(() -> new UnknownObjectException(uuid));;

        List<String> relatedEntityTypes = topologyEntityDTO.getConnectedEntityListList()
                        .stream()
                        .map(connEnt -> connEnt.getConnectedEntityType())
                        .distinct()
                        .map(type ->  UIEntityType.fromType(type).apiStr())
                        .collect(Collectors.toList());

        inputDto.setRelatedEntityTypes(relatedEntityTypes);
        return entitiesService.getActionCountStatsByUuid(uuid, inputDto);
    }

    @Override
    public List<StatSnapshotApiDTO> getStatsByQuery(final String uuid,
                                                    @Nullable final StatPeriodApiInputDTO statPeriodApiInputDTO)
            throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public SupplychainApiDTO getSupplychainByUuid(@Nonnull final String uuid,
                                                  @Nullable final List<String> types,
                                                  @Nullable final List<EntityState> entityStates,
                                                  @Nullable final EntityDetailType detailTypes,
                                                  @Nullable final List<String> aspectNames,
                                                  @Nullable final Boolean healthSummary) throws Exception {
        return supplyChainFetcherFactory.newApiDtoFetcher()
                        .topologyContextId(realtimeTopologyContextId)
                        .addSeedUuids(Lists.newArrayList(uuid))
                        .includeHealthSummary(false)
                        .entityDetailType(EntityDetailType.entity)
                        .aspectsToInclude(aspectNames)
                        .fetch();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public BusinessUnitPriceAdjustmentApiDTO getBusinessUnitPriceAdjustments(@Nonnull String uuid)
            throws Exception {
        // TODO valid the uuid with validInput method
        final Iterator<Discount> discounts = costService.getDiscounts(GetDiscountRequest.newBuilder()
                .setFilter(DiscountQueryFilter.newBuilder().addAssociatedAccountId(Long.parseLong(uuid)))
                .build());
        if (discounts.hasNext()) {
            return mapper.toDiscountApiDTO(discounts.next());
        }
        throw new MissingDiscountException("Failed to find discount by associated account id: " + uuid);
    }


    private BusinessUnitApiDTO createPriceAdjustment(long businessAccountId,
                                                     BusinessUnitApiInputDTO businessUnitApiInputDTO)
        throws OperationFailedException, InvalidOperationException, UnknownObjectException {
        // We also need to create separate discount entries for children accounts of the account
        // as well.
        List<Long> childAccounts = getChildAccounts(businessAccountId);

        DiscountInfo.Builder discountBuilder = DiscountInfo.newBuilder();
        if (businessUnitApiInputDTO.getName() != null) {
            discountBuilder.setDisplayName(businessUnitApiInputDTO.getName());
        }
        if (businessUnitApiInputDTO.getPriceAdjustment() != null) {
            discountBuilder.setAccountLevelDiscount(DiscountInfo
                .AccountLevelDiscount
                .newBuilder()
                .setDiscountPercentage(businessUnitApiInputDTO.getPriceAdjustment().getValue()));
        }

        Set<Long> accountsWithCreatedDiscount = new HashSet<>();
        final CreateDiscountResponse response =
            costService.createDiscount(CreateDiscountRequest.newBuilder()
                .setDiscountInfo(discountBuilder)
                .setId(businessAccountId)
                .build());
        accountsWithCreatedDiscount.add(businessAccountId);

        try {
            for (Long accOid : childAccounts) {
                costService.createDiscount(CreateDiscountRequest.newBuilder()
                    .setDiscountInfo(discountBuilder)
                    .setId(accOid)
                    .build());
                accountsWithCreatedDiscount.add(businessAccountId);
            }
        } catch (RuntimeException exception) {
            // if we had an exception half way through creating the discounts
            // try deleting back all created discounts
            accountsWithCreatedDiscount.forEach(uuid ->
                costService.deleteDiscount(DeleteDiscountRequest.newBuilder()
                    .setAssociatedAccountId(uuid)
                    .build()));
            throw exception;
        }

        return mapper.toBusinessUnitApiDTO(response.getDiscount());
    }

    private Discount editPriceAdjustment(long businessAccountId,
                                         @Nonnull DiscountInfo discountInfo) throws Exception {
        Iterator<Discount> discountIterator = costService.getDiscounts(GetDiscountRequest.newBuilder()
            .setFilter(DiscountQueryFilter.newBuilder().addAssociatedAccountId(businessAccountId).build())
            .build());

        if (!discountIterator.hasNext()) {
            logger.error("Cannot edit a price adjustment that doesn't " +
                "exist. Business Unit: {}", businessAccountId);
            throw new InvalidOperationException("Cannot edit a price adjustment that doesn't " +
                "exist.");
        }

        Discount currentDiscount = discountIterator.next();

        // We also need to update discount entries for children accounts of the account
        // as well.
        List<Long> childAccounts = getChildAccounts(businessAccountId);

        Set<Long> accountsWithUpdatedDiscount = new HashSet<>();
        final UpdateDiscountResponse response = costService.updateDiscount(UpdateDiscountRequest.newBuilder()
            .setAssociatedAccountId(businessAccountId)
            .setNewDiscountInfo(discountInfo)
            .build());
        accountsWithUpdatedDiscount.add(businessAccountId);

        for (Long accOid : childAccounts) {
            try {
                costService.updateDiscount(UpdateDiscountRequest.newBuilder()
                    .setAssociatedAccountId(accOid)
                    .setNewDiscountInfo(discountInfo)
                    .build());
                accountsWithUpdatedDiscount.add(accOid);
            } catch (StatusRuntimeException exception) {
                if (exception.getStatus().getCode() == Status.Code.NOT_FOUND) {
                    // The account does not exist, this can happen when the account was added
                    // after we defined the price adjustment simply add the account
                    logger.warn("Corresponding discount did not exist for account {} which is" +
                        " a child of account {}. Creating a discount for it.", accOid, businessAccountId);
                    costService.createDiscount(CreateDiscountRequest.newBuilder()
                        .setDiscountInfo(discountInfo)
                        .setId(accOid)
                        .build());
                    accountsWithUpdatedDiscount.add(accOid);
                } else {
                    // If something goes wrong we update the values that we updated
                    // till now to the old values
                    accountsWithUpdatedDiscount.forEach(uuid -> {
                        costService.updateDiscount(UpdateDiscountRequest.newBuilder()
                            .setAssociatedAccountId(uuid)
                            .setNewDiscountInfo(currentDiscount.getDiscountInfo())
                            .build());
                    });
                    throw exception;
                }
            }
        }

        return response.getUpdatedDiscount();
    }

    private List<Long> getChildAccounts(long businessAccountId) throws OperationFailedException, UnknownObjectException, InvalidOperationException {
        BusinessUnitApiDTO businessAccount =
            businessAccountRetriever.getBusinessAccount(String.valueOf(businessAccountId));
        if (!businessAccount.isMaster()) {
            logger.error("Price adjustment only can be defined for a " +
                "master account but was requested for {}", businessAccount.toString());
            throw new OperationFailedException("Price adjustment only can be defined for a " +
                "master account.");
        }

        return businessAccount.getChildrenBusinessUnits().stream()
            .map(Long::valueOf)
            .collect(Collectors.toList());
    }

    class MissingDiscountException extends Exception {
        public MissingDiscountException(final String s) {
            super(s);
        }
    }
}
