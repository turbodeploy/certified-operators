package com.vmturbo.api.component.external.api.util;

import java.util.ArrayList;
import java.util.Collection;
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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableFloat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.EntityFilterMapper;
import com.vmturbo.api.component.external.api.util.businessaccount.BusinessAccountMapper;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.group.BillingFamilyApiDTO;
import com.vmturbo.api.dto.group.FilterApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchFilterResolver;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * Responsible for retrieving business accounts from the repository, and decorating them
 * with all the interesting bits of information the API and UI needs. This may involve call-outs
 * to multiple other gRPC services/components.
 */
public class BusinessAccountRetriever {

    private static final Logger logger = LogManager.getLogger();

    private final RepositoryApi repositoryApi;

    private final GroupExpander groupExpander;

    private final ThinTargetCache thinTargetCache;

    private final BusinessAccountMapper businessAccountMapper;

    private final EntityFilterMapper entityFilterMapper;

    private final SearchFilterResolver filterResolver;

    /**
     * Public constructor for the retriever.
     *
     * @param repositoryApi Utility class to access the repository for searches.
     * @param groupExpander The group expander used to get the group members details.
     * @param thinTargetCache Utility that cashes target-related information we need on each
     *                        business account.
     * @param entityFilterMapper entity filter mapping to perform search using FilterApiDTO
     * @param filterResolver filter resolver to search for criteria using services other then
     *          search service
     * @param businessAccountMapper mapper to convert to REST API format
     */
    public BusinessAccountRetriever(@Nonnull final RepositoryApi repositoryApi,
                             @Nonnull final GroupExpander groupExpander,
                             @Nonnull final ThinTargetCache thinTargetCache,
                             @Nonnull final EntityFilterMapper entityFilterMapper,
                             @Nonnull final BusinessAccountMapper businessAccountMapper,
                             @Nonnull final SearchFilterResolver filterResolver) {
        this.repositoryApi = repositoryApi;
        this.groupExpander = groupExpander;
        this.thinTargetCache = thinTargetCache;
        this.businessAccountMapper = businessAccountMapper;
        this.entityFilterMapper = Objects.requireNonNull(entityFilterMapper);
        this.filterResolver = Objects.requireNonNull(filterResolver);
    }

    /**
     * Find master business accounts and convert them to BillingFamilyApiDTO.
     *
     * @return list of BillingFamilyApiDTOs
     * @throws OperationFailedException If there is an issue mapping input scopes.
     */
    public List<BillingFamilyApiDTO> getBillingFamilies() throws OperationFailedException {
        final List<BusinessUnitApiDTO> businessAccounts = getBusinessAccountsInScope(null, null);
        final Map<String, BusinessUnitApiDTO> accountsByUuid = businessAccounts.stream()
            .collect(Collectors.toMap(BusinessUnitApiDTO::getUuid, Function.identity()));
        return businessAccounts.stream()
            // Select accounts that own at least one business account.
            .filter(BusinessUnitApiDTO::isMaster)
            .map(masterAccount -> businessUnitToBillingFamily(masterAccount, accountsByUuid))
            .collect(Collectors.toList());
    }

    /**
     * Find all discovered business units based on the input scope. If scope is null
     * or empty, return all discovered business units.
     *
     * @param scopeUuids The list of input IDs.
     * @param criterias criteria list the query is requested for
     * @return The set of discovered business units.
     * @throws OperationFailedException If there is an error expanding an OID in the scope.
     */
    public List<BusinessUnitApiDTO> getBusinessAccountsInScope(@Nullable List<String> scopeUuids,
            @Nullable List<FilterApiDTO> criterias) throws OperationFailedException {
        boolean allAccounts = true;
        final List<SearchParameters> searchParameters = new ArrayList<>();

        final Set<Long> numericIds = CollectionUtils.emptyIfNull(scopeUuids)
                .stream()
                .filter(StringUtils::isNumeric)
                .map(Long::parseLong)
                .collect(Collectors.toSet());

        Set<Long> targetIds = new HashSet<>();
        if (!numericIds.isEmpty()) {
            // We need to distinguish between the "get all business accounts" case and the
            // "get some" business accounts case. For now, the presence of scope targets is
            // sufficient. When we support additional scopes this will need to change.
            allAccounts = false;
           targetIds = numericIds.stream()
                .filter(oid -> thinTargetCache.getTargetInfo(oid).isPresent())
                .collect(Collectors.toSet());

            final SearchParameters.Builder builder = SearchParameters.newBuilder();
            builder.setStartingFilter(
                    SearchProtoUtil.entityTypeFilter(ApiEntityType.BUSINESS_ACCOUNT));
            if (!targetIds.isEmpty()) {
                // The search will only return business accounts discovered by the specific targets.
                builder.addSearchFilter(SearchProtoUtil.searchFilterProperty(
                    SearchProtoUtil.discoveredBy(targetIds)));
            } else {
                String firstIdAsStr = numericIds.iterator().next().toString();

                // Check if the first ID is a valid group ID.
                Optional<Grouping> groupOptional = groupExpander.getGroup(firstIdAsStr);
                if (groupOptional.isPresent()) {
                    // Valid Group ID found. Expand its members to get the list of Account OIDs.
                    final Set<Long> expandedOidsList = groupExpander.expandUuid(firstIdAsStr);
                    builder.addSearchFilter(SearchProtoUtil
                        .searchFilterProperty(SearchProtoUtil.idFilter(expandedOidsList)));
                } else {
                    builder.addSearchFilter(SearchProtoUtil
                        .searchFilterProperty(SearchProtoUtil.idFilter(numericIds)));
                }
            }
            searchParameters.add(builder.build());
        } else {
            logger.debug("The input scope list doesn't contain numeric IDs. Returning all Business Units.");
        }
        // We add the usual entity search filter if there is something to filter on or if
        // there is nothing in search parameters yet.
        if ((!CollectionUtils.isEmpty(criterias)) || searchParameters.isEmpty()) {
            searchParameters.addAll(
                    entityFilterMapper.convertToSearchParameters(ListUtils.emptyIfNull(criterias),
                            ApiEntityType.BUSINESS_ACCOUNT.apiStr(), null));
        }
        // for target scope we need to display all accounts (even not discovered)
        if (targetIds.isEmpty()) {
            //this parameter used for searching only monitored accounts which have associated target
            searchParameters.add(SearchProtoUtil.makeSearchParameters(
                    SearchProtoUtil.entityTypeFilter(ApiEntityType.BUSINESS_ACCOUNT))
                    .addSearchFilter(SearchFilter.newBuilder()
                            .setPropertyFilter(SearchProtoUtil.associatedTargetFilter())
                            .build())
                    .build());
        }
        final List<SearchParameters> effectiveParameters = searchParameters.stream()
                .map(filterResolver::resolveExternalFilters)
                .collect(Collectors.toList());
        final List<TopologyEntityDTO> businessAccounts =
                repositoryApi.newSearchRequestMulti(effectiveParameters)
                        .getFullEntities()
                        .collect(Collectors.toList());

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
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     */
    public BusinessUnitApiDTO getBusinessAccount(@Nonnull final String uuid)
            throws InvalidOperationException, UnknownObjectException, ConversionException,
            InterruptedException {
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
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     */
    @Nonnull
    public Collection<BusinessUnitApiDTO> getChildAccounts(@Nonnull final String uuid)
            throws InvalidOperationException, UnknownObjectException, ConversionException,
            InterruptedException {
        if (!StringUtils.isNumeric(uuid)) {
            throw new InvalidOperationException("Business account ID must be numeric. Got: " + uuid);
        }
        final long oid = Long.parseLong(uuid);
        final EntityWithConnections accountWithConnections = repositoryApi.entityRequest(oid)
            .getEntityWithConnections()
            .filter(entity -> entity.getEntityType() == ApiEntityType.BUSINESS_ACCOUNT.typeNumber())
            .orElseThrow(() -> new UnknownObjectException("Cannot find Business Unit with OID: " + oid));

        return getBusinessAccounts(accountWithConnections.getConnectedEntitiesList().stream()
            .filter(connection -> connection.getConnectedEntityType() == ApiEntityType.BUSINESS_ACCOUNT.typeNumber())
            .map(ConnectedEntity::getConnectedEntityId)
            .collect(Collectors.toSet()));
    }

    /**
     * Returns the BusinessUnitApiDTOs for the business units that have the provided oids.
     *
     * @param ids the oids of the business units to retrieve.
     * @return the business units with the oids provided.
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     */
    @Nonnull
    public Collection<BusinessUnitApiDTO> getBusinessAccounts(@Nonnull final Set<Long> ids) throws
            ConversionException, InterruptedException {
        final Collection<BusinessUnitApiDTO> result =
                repositoryApi.getByIds(ids, Collections.singleton(EntityType.BUSINESS_ACCOUNT),
                        false).getBusinessAccounts();
        return result;
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
        MutableBoolean hasCost = new MutableBoolean(masterAccount.getCostPrice() != null);
        final MutableFloat costPrice = new MutableFloat(0.0F);
        if (masterAccount.getCostPrice() != null) {
            costPrice.add(masterAccount.getCostPrice());
        }
        masterAccount.getChildrenBusinessUnits().stream()
            .map(accountIdToDisplayName::get)
            .filter(Objects::nonNull)
            .forEach(subAccount -> {
                uuidToName.put(subAccount.getUuid(), subAccount.getDisplayName());
                hasCost.setValue(subAccount.getCostPrice() != null || hasCost.getValue());
                if (subAccount.getCostPrice() != null) {
                    costPrice.add(subAccount.getCostPrice());
                }
            });
        if (hasCost.booleanValue()) {
            billingFamilyApiDTO.setCostPrice(costPrice.toFloat());
        }
        billingFamilyApiDTO.setUuidToNameMap(uuidToName);
        billingFamilyApiDTO.setMembersCount(masterAccount.getChildrenBusinessUnits().size());
        billingFamilyApiDTO.setClassName(StringConstants.BILLING_FAMILY);
        billingFamilyApiDTO.setDisplayName(masterAccount.getDisplayName());
        billingFamilyApiDTO.setUuid(masterAccount.getUuid());
        billingFamilyApiDTO.setEnvironmentType(EnvironmentType.CLOUD);
        return billingFamilyApiDTO;
    }
}
