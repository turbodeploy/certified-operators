package com.vmturbo.api.component.external.api.service;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.validation.Errors;

import com.google.common.collect.ImmutableList;

import com.vmturbo.api.component.external.api.mapper.BusinessUnitMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiInputDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitPriceAdjustmentApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.enums.BusinessUnitType;
import com.vmturbo.api.enums.EntityDetailType;
import com.vmturbo.api.enums.EntityState;
import com.vmturbo.api.enums.HierarchicalRelationship;
import com.vmturbo.api.pagination.ActionPaginationRequest;
import com.vmturbo.api.pagination.ActionPaginationRequest.ActionPaginationResponse;
import com.vmturbo.api.pagination.EntityPaginationRequest;
import com.vmturbo.api.pagination.EntityPaginationRequest.EntityPaginationResponse;
import com.vmturbo.api.serviceinterfaces.IBusinessUnitsService;
import com.vmturbo.api.serviceinterfaces.ISearchService;
import com.vmturbo.api.serviceinterfaces.ITargetsService;
import com.vmturbo.common.protobuf.cost.Cost.CreateDiscountResponse;
import com.vmturbo.common.protobuf.cost.Cost.DeleteDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo.AccountLevelDiscount;
import com.vmturbo.common.protobuf.cost.Cost.DiscountQueryFilter;
import com.vmturbo.common.protobuf.cost.Cost.CreateDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.UpdateDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.UpdateDiscountResponse;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.repository.api.RepositoryClient;

/**
 * {@inheritDoc}
 */
public class BusinessUnitsService implements IBusinessUnitsService {
    public static final String INVALID_BUSINESS_UNIT_DTO = "Business unit input DTO is missing associated account id: ";
    private static final Logger logger = LogManager.getLogger();
    private final CostServiceBlockingStub costService;
    private final BusinessUnitMapper mapper;
    private final RepositoryClient repositoryClient;
    private final ISearchService searchService;
    private final ITargetsService targetsService;

    public BusinessUnitsService(@Nonnull final RepositoryClient repositoryClient,
                                @Nonnull final CostServiceBlockingStub costServiceBlockingStub,
                                @Nonnull final BusinessUnitMapper mapper,
                                @Nonnull final ISearchService searchService,
                                @Nonnull final ITargetsService targetsService) {
        this.repositoryClient = Objects.requireNonNull(repositoryClient);
        this.costService = Objects.requireNonNull(costServiceBlockingStub);
        this.mapper = Objects.requireNonNull(mapper);
        this.searchService = Objects.requireNonNull(searchService);
        this.targetsService = Objects.requireNonNull(targetsService);
    }

    /**
     * Note: the UI currently always passes the  @param cloudType as null (September 19, 2019).
     * So, the codes will not use cloudType.
     * {@inheritDoc}
     */
    @Override
    public List<BusinessUnitApiDTO> getBusinessUnits(@Nullable BusinessUnitType type,
                                                     @Nullable String cloudType,
                                                     @Nullable Boolean hasParent) throws Exception {
        // TODO OM-35804 implement required behavior for other types other than discount
        if (BusinessUnitType.DISCOUNT.equals(type)) {
            final Iterator<Discount> discounts = costService.getDiscounts(GetDiscountRequest.newBuilder()
                    .build());
            return mapper.toDiscountBusinessUnitApiDTO(discounts, repositoryClient, targetsService);
        } else if (BusinessUnitType.DISCOVERED.equals(type)) {
            return mapper.toDiscoveredBusinessUnitDTO(searchService, targetsService, repositoryClient);
        }
        return ImmutableList.of(new BusinessUnitApiDTO());
    }

    @Override
    public BusinessUnitApiDTO getBusinessUnitByUuid(final String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
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
        final DiscountInfo discountInfo = (businessUnitApiInputDTO.getDiscount() != null) ? DiscountInfo.newBuilder()
                .setDisplayName(businessUnitApiInputDTO.getName())
                .setAccountLevelDiscount(DiscountInfo
                        .AccountLevelDiscount
                        .newBuilder()
                        .setDiscountPercentage(businessUnitApiInputDTO.getDiscount()))
                .build()
                : DiscountInfo.newBuilder()
                .setDisplayName(businessUnitApiInputDTO.getName())
                .build();
        final CreateDiscountResponse response = costService.createDiscount(CreateDiscountRequest.newBuilder()
                .setDiscountInfo(discountInfo)
                .setId(Long.parseLong(associatedAccountId))
                .build());
        return mapper.toBusinessUnitApiDTO(response.getDiscount(), repositoryClient, targetsService);
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
            final UpdateDiscountResponse response = costService.updateDiscount(UpdateDiscountRequest.newBuilder()
                    .setAssociatedAccountId(Long.parseLong(uuid))
                    .setNewDiscountInfo(DiscountInfo.newBuilder()
                            .setAccountLevelDiscount(AccountLevelDiscount.newBuilder()
                                    .setDiscountPercentage(businessUnitApiInputDTO.getDiscount()).build())
                            .setDisplayName(businessUnitApiInputDTO.getName()))
                    .build());
            return mapper.toBusinessUnitApiDTO(response.getUpdatedDiscount(), repositoryClient, targetsService);
        } else {
            // create a new discount with provided account level discount
            final String associatedAccountId = businessUnitApiInputDTO.getTargets().stream()
                    .findFirst().orElseThrow(() -> new IllegalArgumentException(INVALID_BUSINESS_UNIT_DTO
                            + businessUnitApiInputDTO));
            final DiscountInfo discountInfo = DiscountInfo.newBuilder().
                    setAccountLevelDiscount(DiscountInfo
                            .AccountLevelDiscount
                            .newBuilder()
                            .setDiscountPercentage(businessUnitApiInputDTO.getDiscount()))
                    .build();
            final CreateDiscountResponse response = costService.createDiscount(CreateDiscountRequest.newBuilder()
                    .setId(Long.parseLong(associatedAccountId))
                    .setDiscountInfo(discountInfo)
                    .build());
            return mapper.toBusinessUnitApiDTO(response.getDiscount(), repositoryClient, targetsService);
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public BusinessUnitPriceAdjustmentApiDTO
    editBusinessUnitDiscounts(@Nonnull String uuid,
                              @Nonnull BusinessUnitPriceAdjustmentApiDTO businessUnitDiscountApiDTO)
            throws Exception {
        Objects.requireNonNull(uuid);
        Objects.requireNonNull(businessUnitDiscountApiDTO);
        final DiscountInfo discountInfo = DiscountInfo.newBuilder()
                .setServiceLevelDiscount(mapper.toServiceDiscountProto(businessUnitDiscountApiDTO))
                .setTierLevelDiscount(mapper.toTierDiscountProto(businessUnitDiscountApiDTO))
                .build();
        final UpdateDiscountResponse response = costService.updateDiscount(UpdateDiscountRequest.newBuilder()
                .setAssociatedAccountId(Long.parseLong(uuid))
                .setNewDiscountInfo(discountInfo)
                .build());
        return mapper.toDiscountApiDTO(response.getUpdatedDiscount(), repositoryClient, searchService);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Boolean deleteBusinessUnit(final String uuid) throws Exception {
        Objects.requireNonNull(uuid);
        return costService.deleteDiscount(DeleteDiscountRequest.newBuilder()
                .setAssociatedAccountId(Long.parseLong(uuid))
                .build())
                .getDeleted();
    }

    @Override
    public void validateInput(final Object o, final Errors errors) {
        //TODO implement validator for business unit
    }

    @Nonnull
    @Override
    public Collection<BusinessUnitApiDTO> getRelatedBusinessUnits(@Nonnull String uuid,
                                                                  HierarchicalRelationship relationship) throws Exception {
        // TODO OM-35804 implement required behavior
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public EntityPaginationResponse getEntities(final String uuid,
                                                final EntityPaginationRequest paginationRequest) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ActionPaginationResponse getActions(final String uuid,
                                               final ActionApiInputDTO inputDto,
                                               final ActionPaginationRequest paginationRequest) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<StatSnapshotApiDTO> getActionCountStatsByUuid(final String uuid,
                                                              final ActionApiInputDTO paginationRequest) throws Exception {
        throw ApiUtils.notImplementedInXL();
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
                                                  @Nullable final Boolean healthSummary) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public BusinessUnitPriceAdjustmentApiDTO getBusinessUnitDiscounts(@Nonnull String uuid) throws Exception {
        // TODO valid the uuid with validInput method
        final Iterator<Discount> discounts = costService.getDiscounts(GetDiscountRequest.newBuilder()
                .setFilter(DiscountQueryFilter.newBuilder().addAssociatedAccountId(Long.parseLong(uuid)))
                .build());
        if (discounts.hasNext()) {
            return mapper.toDiscountApiDTO(discounts.next(), repositoryClient, searchService);
        }
        throw new MissingDiscountException("Failed to find discount by associated account id: " + uuid);
    }

    class MissingDiscountException extends Exception {
        public MissingDiscountException(final String s) {
            super(s);
        }
    }
}
