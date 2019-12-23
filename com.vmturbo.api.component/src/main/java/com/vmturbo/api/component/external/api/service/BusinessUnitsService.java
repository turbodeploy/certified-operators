package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.validation.Errors;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.SingleEntityRequest;
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
import com.vmturbo.api.enums.BusinessUnitType;
import com.vmturbo.api.enums.EntityDetailType;
import com.vmturbo.api.enums.EntityState;
import com.vmturbo.api.enums.HierarchicalRelationship;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.ActionPaginationRequest;
import com.vmturbo.api.pagination.ActionPaginationRequest.ActionPaginationResponse;
import com.vmturbo.api.pagination.EntityPaginationRequest;
import com.vmturbo.api.pagination.EntityPaginationRequest.EntityPaginationResponse;
import com.vmturbo.api.serviceinterfaces.IBusinessUnitsService;
import com.vmturbo.api.serviceinterfaces.ITargetsService;
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

    private final ITargetsService targetsService;

    private final UuidMapper uuidMapper;

    private final EntitiesService entitiesService;

    private final RepositoryApi repositoryApi;

    private final SupplyChainFetcherFactory supplyChainFetcherFactory;

    private final BusinessAccountRetriever businessAccountRetriever;

    public BusinessUnitsService(@Nonnull final CostServiceBlockingStub costServiceBlockingStub,
                                @Nonnull final DiscountMapper mapper,
                                @Nonnull final ITargetsService targetsService,
                                final long realtimeTopologyContextId,
                                @Nonnull final UuidMapper uuidMapper,
                                @Nonnull final EntitiesService entitiesService,
                                @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                                @Nonnull final RepositoryApi repositoryApi,
                                @Nonnull final BusinessAccountRetriever businessAccountRetriever) {
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.costService = Objects.requireNonNull(costServiceBlockingStub);
        this.mapper = Objects.requireNonNull(mapper);
        this.targetsService = Objects.requireNonNull(targetsService);
        this.uuidMapper = Objects.requireNonNull(uuidMapper);
        this.entitiesService = entitiesService;
        this.supplyChainFetcherFactory = supplyChainFetcherFactory;
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.businessAccountRetriever = Objects.requireNonNull(businessAccountRetriever);
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
            return mapper.toDiscountBusinessUnitApiDTO(discounts);
        } else if (BusinessUnitType.DISCOVERED.equals(type)) {
            return businessAccountRetriever.getBusinessAccountsInScope(Collections.emptyList(), null);
        }
        return ImmutableList.of(new BusinessUnitApiDTO());
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
        final DiscountInfo discountInfo = (businessUnitApiInputDTO.getPriceAdjustment() != null) ? DiscountInfo.newBuilder()
                .setDisplayName(businessUnitApiInputDTO.getName())
                .setAccountLevelDiscount(DiscountInfo
                        .AccountLevelDiscount
                        .newBuilder()
                        .setDiscountPercentage(businessUnitApiInputDTO.getPriceAdjustment().getValue()))
                .build()
                : DiscountInfo.newBuilder()
                .setDisplayName(businessUnitApiInputDTO.getName())
                .build();
        final CreateDiscountResponse response = costService.createDiscount(CreateDiscountRequest.newBuilder()
                .setDiscountInfo(discountInfo)
                .setId(Long.parseLong(associatedAccountId))
                .build());
        return mapper.toBusinessUnitApiDTO(response.getDiscount());
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
                                    .setDiscountPercentage(businessUnitApiInputDTO.getPriceAdjustment().getValue()).build())
                            .setDisplayName(businessUnitApiInputDTO.getName()))
                    .build());
            return mapper.toBusinessUnitApiDTO(response.getUpdatedDiscount());
        } else {
            // create a new discount with provided account level discount
            final String associatedAccountId = businessUnitApiInputDTO.getTargets().stream()
                    .findFirst().orElseThrow(() -> new IllegalArgumentException(INVALID_BUSINESS_UNIT_DTO
                            + businessUnitApiInputDTO));
            final DiscountInfo discountInfo = DiscountInfo.newBuilder().
                    setAccountLevelDiscount(DiscountInfo
                            .AccountLevelDiscount
                            .newBuilder()
                            .setDiscountPercentage(businessUnitApiInputDTO.getPriceAdjustment().getValue()))
                    .build();
            final CreateDiscountResponse response = costService.createDiscount(CreateDiscountRequest.newBuilder()
                    .setId(Long.parseLong(associatedAccountId))
                    .setDiscountInfo(discountInfo)
                    .build());
            return mapper.toBusinessUnitApiDTO(response.getDiscount());
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
        final UpdateDiscountResponse response = costService.updateDiscount(UpdateDiscountRequest.newBuilder()
                .setAssociatedAccountId(Long.parseLong(uuid))
                .setNewDiscountInfo(discountInfo)
                .build());
        return mapper.toDiscountApiDTO(response.getUpdatedDiscount());
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

    class MissingDiscountException extends Exception {
        public MissingDiscountException(final String s) {
            super(s);
        }
    }
}
