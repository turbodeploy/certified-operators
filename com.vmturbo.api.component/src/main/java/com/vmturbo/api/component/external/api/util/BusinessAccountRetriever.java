package com.vmturbo.api.component.external.api.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableFloat;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.group.BillingFamilyApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.BusinessUnitType;
import com.vmturbo.api.enums.CloudType;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo.ServiceExpenses;
import com.vmturbo.common.protobuf.cost.Cost.GetCurrentAccountExpensesRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCurrentAccountExpensesRequest.AccountExpenseQueryScope;
import com.vmturbo.common.protobuf.cost.Cost.GetCurrentAccountExpensesRequest.AccountExpenseQueryScope.IdList;
import com.vmturbo.common.protobuf.cost.Cost.GetCurrentAccountExpensesResponse;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessAccountInfo;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * Responsible for retrieving business accounts from the repository, and decorating them
 * with all the interesting bits of information the API and UI needs. This may involve call-outs
 * to multiple other gRPC services/components.
 */
public class BusinessAccountRetriever {

    private static final Logger logger = LogManager.getLogger();

    private final RepositoryApi repositoryApi;

    private final ThinTargetCache thinTargetCache;

    private final BusinessAccountMapper businessAccountMapper;

    /**
     * Public constructor for the retriever.
     *
     * @param repositoryApi Utility class to access the repository for searches.
     * @param costService Stub to get cost/expense information.
     * @param thinTargetCache Utility that cashes target-related information we need on each
     *                        business account.
     */
    public BusinessAccountRetriever(@Nonnull final RepositoryApi repositoryApi,
                                    @Nonnull final CostServiceBlockingStub costService,
                                    @Nonnull final ThinTargetCache thinTargetCache) {
        this(repositoryApi, thinTargetCache, new BusinessAccountMapper(thinTargetCache, new SupplementaryDataFactory(costService)));
    }

    @VisibleForTesting
    BusinessAccountRetriever(@Nonnull final RepositoryApi repositoryApi,
                             @Nonnull final ThinTargetCache thinTargetCache,
                             @Nonnull final BusinessAccountMapper businessAccountMapper) {
        this.repositoryApi = repositoryApi;
        this.thinTargetCache = thinTargetCache;
        this.businessAccountMapper = businessAccountMapper;
    }



    /**
     * Find master business accounts and convert them to BillingFamilyApiDTO.
     *
     * @return list of BillingFamilyApiDTOs
     */
    public List<BillingFamilyApiDTO> getBillingFamilies() {
        final List<BusinessUnitApiDTO> businessAccounts = getBusinessAccountsInScope(null);
        final Map<String, BusinessUnitApiDTO> accountsByUuid = businessAccounts.stream()
            .collect(Collectors.toMap(BusinessUnitApiDTO::getUuid, Function.identity()));
        return businessAccounts.stream()
            // Select accounts that own at least one business account.
            .filter(BusinessUnitApiDTO::isMaster)
            .map(masterAccount -> businessUnitToBillingFamily(masterAccount, accountsByUuid))
            .collect(Collectors.toList());
    }

    /**
     * Find all discovered business units discovered by targets in the scope.  If scope is null
     * or empty, return all discovered business units.
     *
     * @param scopeUuids a list of target ids.
     * @return Set of discovered business units.
     */
    public List<BusinessUnitApiDTO> getBusinessAccountsInScope(@Nullable List<String> scopeUuids) {
        // TODO - handle more scopes than just targets.
        final Set<Long> scopeTargets = CollectionUtils.emptyIfNull(scopeUuids).stream()
            .filter(StringUtils::isNumeric)
            .map(Long::parseLong)
            .filter(oid -> thinTargetCache.getTargetInfo(oid).isPresent())
            .collect(Collectors.toSet());

        final SearchParameters.Builder builder = SearchProtoUtil.makeSearchParameters(
            SearchProtoUtil.entityTypeFilter(UIEntityType.BUSINESS_ACCOUNT));
        if (!scopeTargets.isEmpty()) {
            // The search will only return business accounts discovered by the specific targets.
            builder.addSearchFilter(SearchProtoUtil.searchFilterProperty(
                SearchProtoUtil.discoveredBy(scopeTargets)));
        }

        final List<TopologyEntityDTO> businessAccounts = repositoryApi.newSearchRequest(builder.build())
            .getFullEntities()
            .collect(Collectors.toList());
        // We need to distinguish between the "get all business accounts" case and the
        // "get some" business accounts case. For now, the presence of scope targets is
        // sufficient. When we support additional scopes this will need to change.
        final boolean allAccounts = scopeTargets.isEmpty();
        return businessAccountMapper.convert(businessAccounts, allAccounts);
    }

    /**
     * Get the Business Unit for the input OID.
     *
     * @param uuid The input UUID value.
     *
     * @return The Business Unit DTO for the input OID.
     * @throws UnknownObjectException if the Business Unit cannot be found or is invalid.
     * @throws InvalidOperationException If the UUID is not numeric.
     */
    public BusinessUnitApiDTO getBusinessAccount(@Nonnull final String uuid)
            throws InvalidOperationException, UnknownObjectException {
        if (!StringUtils.isNumeric(uuid)) {
            throw new InvalidOperationException("Business account ID must be numeric. Got: " + uuid);
        }
        final long oid = Long.parseLong(uuid);

        return getBusinessAccounts(Collections.singleton(oid)).stream()
            .findFirst()
            .orElseThrow(() -> new UnknownObjectException("Cannot find Business Unit with OID: " + oid));
    }

    /**
     * Retrieve the child accounts associated with a particular account.
     *
     * @param uuid The UUID of the master account to look for.
     * @return The {@link BusinessUnitApiDTO}s describing this account's children.
     * @throws InvalidOperationException If the input UUID is invalid.
     * @throws UnknownObjectException If the input UUID does not refer to an existing business account.
     */
    @Nonnull
    public List<BusinessUnitApiDTO> getChildAccounts(@Nonnull final String uuid)
            throws InvalidOperationException, UnknownObjectException {
        if (!StringUtils.isNumeric(uuid)) {
            throw new InvalidOperationException("Business account ID must be numeric. Got: " + uuid);
        }
        final long oid = Long.parseLong(uuid);
        final EntityWithConnections accountWithConnections = repositoryApi.entityRequest(oid)
            .getEntityWithConnections()
            .filter(entity -> entity.getEntityType() == UIEntityType.BUSINESS_ACCOUNT.typeNumber())
            .orElseThrow(() -> new UnknownObjectException("Cannot find Business Unit with OID: " + oid));

        return getBusinessAccounts(accountWithConnections.getConnectedEntitiesList().stream()
            .filter(connection -> connection.getConnectedEntityType() == UIEntityType.BUSINESS_ACCOUNT.typeNumber())
            .map(ConnectedEntity::getConnectedEntityId)
            .collect(Collectors.toSet()));
    }

    /**
     * Returns the BusinessUnitApiDTOs for the business units that have the provided oids.
     *
     * @param ids the oids of the business units to retrieve.
     * @return the business units with the oids provided.
     */
    public List<BusinessUnitApiDTO> getBusinessAccounts(@Nonnull final Set<Long> ids) {
        if (ids.isEmpty()) {
            return Collections.emptyList();
        }

        final List<TopologyEntityDTO> accounts = repositoryApi.newSearchRequest(
                SearchProtoUtil.makeSearchParameters(SearchProtoUtil.idFilter(ids))
                    .addSearchFilter(SearchProtoUtil.searchFilterProperty(
                        // We want to handle the case where some of the input IDs don't refer to
                        // business accounts.
                        SearchProtoUtil.entityTypeFilter(UIEntityType.BUSINESS_ACCOUNT)))
                    .build())
            .getFullEntities()
            .collect(Collectors.toList());

        return businessAccountMapper.convert(accounts, false);
    }

    /**
     * Container class for all data from other components required to decorate the output
     * {@link BusinessUnitApiDTO}s.
     */
    @VisibleForTesting
    static class SupplementaryData {

        private final Map<Long, Float> costsByAccountId;

        private SupplementaryData(final Map<Long, Float> costsByAccountId) {
            this.costsByAccountId = costsByAccountId;
        }

        @Nonnull
        Float getCostPrice(@Nonnull final Long accountId) {
            return costsByAccountId.getOrDefault(accountId, 0.0f);
        }
    }

    /**
     * Factory class for {@link SupplementaryData}, responsible for the heavy lifting of
     * actually doing the bulk fetches of data from other components (e.g. costs from the cost
     * component, severities from the action orchestrator, maybe resource groups from the
     * group component).
     *
     * <p>Also helps unit test the {@link BusinessAccountRetriever} more manageably.
     */
    @VisibleForTesting
    static class SupplementaryDataFactory {
        private final CostServiceBlockingStub costService;

        @VisibleForTesting
        SupplementaryDataFactory(final CostServiceBlockingStub costServiceBlockingStub) {
            this.costService = costServiceBlockingStub;
        }

        /**
         * Create a new {@link SupplementaryData} instance.
         *
         * @param specificAccountIds If empty, request supplementary data for all accounts in
         *                           the system. If set, request supplementary data for only
         *                           the specified accounts. Using an optional around a set to
         *                           prevent accidental uses of "empty" to indicate "all".
         * @return The {@link SupplementaryData}.
         */
        @VisibleForTesting
        SupplementaryData newSupplementaryData(Optional<Set<Long>> specificAccountIds) {
            final Map<Long, Float> costsByAccount = getCostsByAccount(specificAccountIds);
            return new SupplementaryData(costsByAccount);
        }

        @Nonnull
        private Map<Long, Float> getCostsByAccount(@Nonnull final Optional<Set<Long>> specificAccountIds) {
            final AccountExpenseQueryScope.Builder scopeBldr = AccountExpenseQueryScope.newBuilder();
            if (specificAccountIds.isPresent()) {
                scopeBldr.setSpecificAccounts(IdList.newBuilder()
                    .addAllAccountIds(specificAccountIds.get()));
            } else {
                scopeBldr.setAll(true);
            }

            final GetCurrentAccountExpensesResponse response = costService.getCurrentAccountExpenses(
                GetCurrentAccountExpensesRequest.newBuilder()
                    .setScope(scopeBldr)
                    .build());

            // Sum the expenses across services for each account.
            //
            // It's not clear whether we should also be adding the per-tier expenses, or if
            // the per-tier expenses are part of the per-service expenses.
            //
            // As this logic gets more complex we should move it out to a separate calculator class.
            final Map<Long, Float> costsByAccount = new HashMap<>();
            response.getAccountExpenseList().forEach(expense -> {
                final long accountId = expense.getAssociatedAccountId();
                float totalExpense = 0.0f;
                for (ServiceExpenses svcExpense : expense.getAccountExpensesInfo().getServiceExpensesList()) {
                    totalExpense += svcExpense.getExpenses().getAmount();
                }
                costsByAccount.put(accountId, totalExpense);
            });
            return costsByAccount;
        }

    }

    /**
     * Isolates the logic for mapping {@link TopologyEntityDTO}s representing a business account
     * to the {@link BusinessUnitApiDTO} that can be returned to the API.
     */
    @VisibleForTesting
    static class BusinessAccountMapper {

        private final ThinTargetCache thinTargetCache;

        private final SupplementaryDataFactory supplementaryDataFactory;

        @VisibleForTesting
        BusinessAccountMapper(@Nonnull final ThinTargetCache thinTargetCache,
                              @Nonnull final SupplementaryDataFactory supplementaryDataFactory) {
            this.thinTargetCache = thinTargetCache;
            this.supplementaryDataFactory = supplementaryDataFactory;
        }

        /**
         * Convert a list of {@link TopologyEntityDTO}s to the appropriate {@link BusinessUnitApiDTO}s.
         *
         * @param entities The {@link TopologyEntityDTO} representations of the accounts.
         * @param allAccounts A hint to say whether these accounts represent ALL accounts. This
         *                    allows us to optimize queries for related data, if necessary.
         * @return The {@link BusinessUnitApiDTO} representations of the input accounts.
         */
        @Nonnull
        @VisibleForTesting
        List<BusinessUnitApiDTO> convert(@Nonnull final List<TopologyEntityDTO> entities,
                                         final boolean allAccounts) {
            if (entities.isEmpty()) {
                return Collections.emptyList();
            }

            final SupplementaryData supplementaryData =
                supplementaryDataFactory.newSupplementaryData(
                    allAccounts ? Optional.empty() :
                        Optional.of(entities.stream()
                        .map(TopologyEntityDTO::getOid)
                        .collect(Collectors.toSet())));

            return entities.stream()
                .map(entity -> buildDiscoveredBusinessUnitApiDTO(entity, supplementaryData))
                .collect(Collectors.toList());
        }

        /**
         * Build discovered business unit API DTO.
         *
         * @param businessAccount topology entity DTOP for the business account
         * @param supplementaryData Supplementary data required to produce the final API DTO.
         * @return The API DTO that can be returned to the UI.
         */
        private BusinessUnitApiDTO buildDiscoveredBusinessUnitApiDTO(@Nonnull final TopologyEntityDTO businessAccount,
                                                             @Nonnull final SupplementaryData supplementaryData) {
            final BusinessUnitApiDTO businessUnitApiDTO = new BusinessUnitApiDTO();
            businessUnitApiDTO.setBusinessUnitType(BusinessUnitType.DISCOVERED);
            businessUnitApiDTO.setUuid(Long.toString(businessAccount.getOid()));
            businessUnitApiDTO.setEnvironmentType(EnvironmentType.CLOUD);
            businessUnitApiDTO.setClassName(UIEntityType.BUSINESS_ACCOUNT.apiStr());
            businessUnitApiDTO.setBudget(new StatApiDTO());

            businessUnitApiDTO.setCostPrice(
                supplementaryData.getCostPrice(businessAccount.getOid()));
            // discovered account doesn't have discount (yet)
            businessUnitApiDTO.setDiscount(0.0f);

            businessUnitApiDTO.setMemberType(StringConstants.WORKLOAD);

            final MutableInt workloadMemberCount = new MutableInt(0);
            final Set<String> childAccountIds = new HashSet<>();
            businessAccount.getConnectedEntityListList().forEach(connectedEntity -> {
                UIEntityType type = UIEntityType.fromType(connectedEntity.getConnectedEntityType());
                if (UIEntityType.WORKLOAD_ENTITY_TYPES.contains(type)) {
                    workloadMemberCount.increment();
                }
                if (type == UIEntityType.BUSINESS_ACCOUNT) {
                    childAccountIds.add(Long.toString(connectedEntity.getConnectedEntityId()));
                }
            });

            businessUnitApiDTO.setMembersCount(workloadMemberCount.intValue());
            businessUnitApiDTO.setChildrenBusinessUnits(childAccountIds);

            businessUnitApiDTO.setDisplayName(businessAccount.getDisplayName());
            if (businessAccount.getTypeSpecificInfo().hasBusinessAccount()) {
                final BusinessAccountInfo bizInfo = businessAccount.getTypeSpecificInfo().getBusinessAccount();
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

            businessUnitApiDTO.setMaster(childAccountIds.size() > 0);

            final List<ThinTargetInfo> discoveringTargets = businessAccount
                .getOrigin()
                .getDiscoveryOrigin()
                .getDiscoveringTargetIdsList()
                .stream()
                .map(thinTargetCache::getTargetInfo)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

            final CloudType cloudType = discoveringTargets.stream()
                .findFirst()
                .map(ThinTargetInfo::probeInfo)
                .map(ThinProbeInfo::type)
                .map(CloudType::fromProbeType)
                .orElse(CloudType.UNKNOWN);

            businessUnitApiDTO.setCloudType(cloudType);

            final List<TargetApiDTO> targetApiDTOS = discoveringTargets.stream()
                .map(thinTargetInfo -> {
                    final TargetApiDTO apiDTO = new TargetApiDTO();
                    apiDTO.setType(thinTargetInfo.probeInfo().type());
                    apiDTO.setUuid(Long.toString(thinTargetInfo.oid()));
                    apiDTO.setDisplayName(thinTargetInfo.displayName());
                    apiDTO.setCategory(thinTargetInfo.probeInfo().category());
                    return apiDTO;
                })
                .collect(Collectors.toList());
            businessUnitApiDTO.setTargets(targetApiDTOS);
            return businessUnitApiDTO;
        }
    }

    /**
     * Convert a {@link BusinessUnitApiDTO} to {@link BillingFamilyApiDTO}.
     *
     * @param masterAccount the master BusinessAccount to convert
     * @param accountIdToDisplayName map from account id to its {@link BusinessUnitApiDTO}.
     * @return the converted BillingFamilyApiDTO for the given BusinessUnitApiDTO
     */
    private BillingFamilyApiDTO businessUnitToBillingFamily(
            @Nonnull final BusinessUnitApiDTO masterAccount,
            @Nonnull final Map<String, BusinessUnitApiDTO> accountIdToDisplayName) {
        BillingFamilyApiDTO billingFamilyApiDTO = new BillingFamilyApiDTO();
        billingFamilyApiDTO.setMasterAccountUuid(masterAccount.getUuid());
        final Map<String, String> uuidToName = new HashMap<>();
        uuidToName.put(masterAccount.getUuid(), masterAccount.getDisplayName());
        final MutableFloat costPrice = new MutableFloat(masterAccount.getCostPrice());
        masterAccount.getChildrenBusinessUnits().stream()
            .map(accountIdToDisplayName::get)
            .filter(Objects::nonNull)
            .forEach(subAccount -> {
                uuidToName.put(subAccount.getUuid(), subAccount.getDisplayName());
                costPrice.add(subAccount.getCostPrice());
            });
        billingFamilyApiDTO.setCostPrice(costPrice.toFloat());
        billingFamilyApiDTO.setUuidToNameMap(uuidToName);
        billingFamilyApiDTO.setMembersCount(masterAccount.getChildrenBusinessUnits().size());
        billingFamilyApiDTO.setClassName(StringConstants.BILLING_FAMILY);
        billingFamilyApiDTO.setDisplayName(masterAccount.getDisplayName());
        billingFamilyApiDTO.setUuid(masterAccount.getUuid());
        billingFamilyApiDTO.setEnvironmentType(EnvironmentType.CLOUD);
        return billingFamilyApiDTO;
    }
}
