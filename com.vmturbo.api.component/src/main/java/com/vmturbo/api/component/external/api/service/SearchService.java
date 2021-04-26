package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.mapper.EntityFilterMapper.ACCOUNT_OID;
import static com.vmturbo.api.component.external.api.mapper.EntityFilterMapper.CONNECTED_STORAGE_TIER_FILTER_PATH;
import static com.vmturbo.api.component.external.api.mapper.EntityFilterMapper.REGION_FILTER_PATH;
import static com.vmturbo.api.component.external.api.mapper.EntityFilterMapper.STATE;
import static com.vmturbo.api.component.external.api.mapper.EntityFilterMapper.VOLUME_ATTACHMENT_STATE_FILTER_PATH;
import static com.vmturbo.common.protobuf.utils.StringConstants.BUSINESS_ACCOUNT;
import static com.vmturbo.common.protobuf.utils.StringConstants.GROUP;
import static com.vmturbo.common.protobuf.utils.StringConstants.WORKLOAD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.PaginatedSearchRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SingleEntityRequest;
import com.vmturbo.api.component.external.api.mapper.EntityFilterMapper;
import com.vmturbo.api.component.external.api.mapper.GroupFilterMapper;
import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.component.external.api.mapper.GroupUseCaseParser;
import com.vmturbo.api.component.external.api.mapper.MarketMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.PriceIndexPopulator;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.SeverityPopulator;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.util.BusinessAccountRetriever;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entity.TagApiDTO;
import com.vmturbo.api.dto.group.FilterApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.market.MarketApiDTO;
import com.vmturbo.api.dto.search.CriteriaOptionApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.EntityDetailType;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.enums.Origin;
import com.vmturbo.api.enums.QueryType;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.SearchOrderBy;
import com.vmturbo.api.pagination.SearchPaginationRequest;
import com.vmturbo.api.pagination.SearchPaginationRequest.SearchPaginationResponse;
import com.vmturbo.api.serviceinterfaces.ISearchService;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.api.mappers.EnvironmentTypeMapper;
import com.vmturbo.common.protobuf.PaginationProtoUtil;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceBlockingStub;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetTagsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.search.CloudType;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.LogicalOperator;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchQuery;
import com.vmturbo.common.protobuf.search.SearchFilterResolver;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.search.UIBooleanFilter;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityList;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.WorkloadControllerInfo.ControllerTypeCase;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.OperationStatus;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * Service entry points to search the Repository.
 **/
public class SearchService implements ISearchService {

    private static final Logger logger = LogManager.getLogger();

    private final RepositoryApi repositoryApi;

    private final MarketsService marketsService;

    private final GroupsService groupsService;
    private final GroupExpander groupExpander;

    //Mapper for getting aspects for entity or group
    private final EntityAspectMapper entityAspectMapper;

    private final TargetsService targetsService;

    private final SearchServiceBlockingStub searchServiceRpc;

    private final PriceIndexPopulator priceIndexPopulator;

    private final EntitySeverityServiceBlockingStub entitySeverityRpc;

    private final SeverityPopulator severityPopulator;

    private final StatsHistoryServiceBlockingStub statsHistoryServiceRpc;

    private final PaginationMapper paginationMapper;

    private final GroupUseCaseParser groupUseCaseParser;

    private final long realtimeContextId;

    private final TagsService tagsService;

    private BusinessAccountRetriever businessAccountRetriever;

    private final UserSessionContext userSessionContext;

    private final GroupServiceBlockingStub groupServiceRpc;

    private final ServiceEntityMapper serviceEntityMapper;

    private final EntityFilterMapper entityFilterMapper;
    private final SearchFilterResolver filterResolver;

    private final ThinTargetCache thinTargetCache;

    private static Map<String, CriteriaOptionProvider> criteriaOptionProviders;

    SearchService(@Nonnull final RepositoryApi repositoryApi,
                  @Nonnull final MarketsService marketsService,
                  @Nonnull final GroupsService groupsService,
                  @Nonnull final TargetsService targetsService,
                  @Nonnull final SearchServiceBlockingStub searchServiceRpc,
                  @Nonnull final EntitySeverityServiceBlockingStub entitySeverityRpc,
                  @Nonnull final SeverityPopulator severityPopulator,
                  @Nonnull final StatsHistoryServiceBlockingStub statsHistoryServiceRpc,
                  @Nonnull GroupExpander groupExpander,
                  @Nonnull final PaginationMapper paginationMapper,
                  @Nonnull final GroupUseCaseParser groupUseCaseParser,
                  @Nonnull TagsService tagsService,
                  @Nonnull BusinessAccountRetriever businessAccountRetriever,
                  final long realtimeTopologyContextId,
                  @Nonnull final UserSessionContext userSessionContext,
                  @Nonnull final GroupServiceBlockingStub groupServiceRpc,
                  @Nonnull final ServiceEntityMapper serviceEntityMapper,
                  @Nonnull final EntityFilterMapper entityFilterMapper,
                  @Nonnull final EntityAspectMapper entityAspectMapper,
                  @Nonnull final SearchFilterResolver searchFilterResolver,
                  @Nonnull final PriceIndexPopulator priceIndexPopulator,
                  @Nonnull final ThinTargetCache thinTargetCache) {
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.marketsService = Objects.requireNonNull(marketsService);
        this.groupsService = Objects.requireNonNull(groupsService);
        this.targetsService = Objects.requireNonNull(targetsService);
        this.searchServiceRpc = Objects.requireNonNull(searchServiceRpc);
        this.entitySeverityRpc = Objects.requireNonNull(entitySeverityRpc);
        this.severityPopulator = Objects.requireNonNull(severityPopulator);
        this.statsHistoryServiceRpc = Objects.requireNonNull(statsHistoryServiceRpc);
        this.groupExpander = Objects.requireNonNull(groupExpander);
        this.paginationMapper = Objects.requireNonNull(paginationMapper);
        this.groupUseCaseParser = groupUseCaseParser;
        this.tagsService = tagsService;
        this.businessAccountRetriever = businessAccountRetriever;
        this.realtimeContextId = realtimeTopologyContextId;
        this.userSessionContext = userSessionContext;
        this.groupServiceRpc = Objects.requireNonNull(groupServiceRpc);
        this.serviceEntityMapper = Objects.requireNonNull(serviceEntityMapper);
        this.entityFilterMapper = Objects.requireNonNull(entityFilterMapper);
        this.entityAspectMapper = Objects.requireNonNull(entityAspectMapper);
        this.priceIndexPopulator = Objects.requireNonNull(priceIndexPopulator);
        this.thinTargetCache = Objects.requireNonNull(thinTargetCache);

        this.filterResolver = Objects.requireNonNull(searchFilterResolver);
        criteriaOptionProviders = ImmutableMap.<String, CriteriaOptionProvider>builder().put(STATE,
                (a, b, c) -> getStateOptions())
                .put(StringConstants.TAGS_ATTR, this::getSTagAttributeOptions)
                .put("ConsistsOf:PhysicalMachine:MemberOf:Cluster:tags",
                        this::getSTagAttributeOptions)
                .put(ACCOUNT_OID, (a, b, c) -> getAccountOptions())
                .put("BusinessAccount:oid",  (a, b, c) -> getAccountOptions())
                .put(VOLUME_ATTACHMENT_STATE_FILTER_PATH,
                        (a, b, c) -> getVolumeAttachmentStateOptions())
                .put(CONNECTED_STORAGE_TIER_FILTER_PATH,
                        (a, b, c) -> getConnectionStorageTierOptions())
                .put(REGION_FILTER_PATH, (a, b, c) -> getRegionFilterOptions())
                .put("discoveredBy:cloudProvider", (a, b, c) -> getCloudProviderOptions())
                .put(EntityFilterMapper.MEMBER_OF_RESOURCE_GROUP_OID, (a, b, c) -> getResourceGroupsUUIDOptions())
                .put(EntityFilterMapper.MEMBER_OF_RESOURCE_GROUP_NAME, (a, b, c) -> getResourceGroupsNameOptions())
                .put(EntityFilterMapper.MEMBER_OF_BILLING_FAMILY_OID, (a, b, c) -> getBillingFamiliesOptions())
                .put(EntityFilterMapper.OWNER_OF_RESOURCE_GROUP_OID, (a, b, c) -> getResourceGroupsUUIDOptions())
                .put("discoveredBy:validationStatus", (a, b, c) -> getValidationStatusOptions())
                .put(EntityFilterMapper.WORKLOAD_CONTROLLER_TYPE, (a, b, c) -> getWorkloadControllerTypeOptions())
                .put(EntityFilterMapper.CONTAINER_WORKLOAD_CONTROLLER_TYPE, (a, b, c) -> getWorkloadControllerTypeOptions())
                .put(EntityFilterMapper.CONTAINER_POD_WORKLOAD_CONTROLLER_TYPE, (a, b, c) -> getWorkloadControllerTypeOptions())
                .put(EntityFilterMapper.CONTAINER_SPEC_WORKLOAD_CONTROLLER_TYPE, (a, b, c) -> getWorkloadControllerTypeOptions())
                .put(EntityFilterMapper.USER_DEFINED_ENTITY, (a, b, c) -> getBooleanFilterOptions())
                .build();
    }

    @Override
    public BaseApiDTO getObjectByUuid(@Nonnull String uuidString) throws Exception {

        if (uuidString.equals(UuidMapper.UI_REAL_TIME_MARKET_STR)) {
            // Forward requests regarding market to the market-service.
            return marketsService.getMarketByUuid(uuidString);
        }
        // search for a group or cluster by that ID;  if found, return it
        try {
            // getGroupByUuid searches _both_ groups and clusters
            return groupsService.getGroupByUuid(uuidString, true);
        } catch (UnknownObjectException e) {
            // not a group or cluster...fall through
        }

        if (!StringUtils.isNumeric(uuidString)) {
            throw new InvalidOperationException("Object UUID must be numeric. Got: " + uuidString);
        }
        final long oid = Long.parseLong(uuidString);

        // Search for business units next. We cannot use Repository API entity request call
        // because BusinessUnitApiDTO doesn't inherit from ServiceEntityApiDTO class.
        final Iterable<BaseApiDTO> dtos =
                repositoryApi.getByIds(Collections.singleton(oid), Collections.emptySet(), false)
                        .getAllResults();
        final Iterator<BaseApiDTO> iterator = dtos.iterator();
        if (iterator.hasNext()) {
            return iterator.next();
        } else {
            throw new UnknownObjectException("No entity found: " + uuidString);
        }
    }

    /**
     * Create CONTAINS pattern matching regex.
     * @param nameRegex String input name
     * @return String name regex corresponding to CONTAINS matching pattern.
      */
    private String escapeAndEncloseInDotAsterisk(String nameRegex) {
        // escape nameRegex and then enclose in '.*'
        // to account for special char in nameRegex
        final StringBuilder nameRegexBuilder = new StringBuilder();
        nameRegexBuilder.append(nameRegex);
        // we are removing `.*` to allow quote of the pattern
        // in the middle required for CONTAINS pattern matching
        if ( nameRegexBuilder.toString().startsWith(".*") ) {
            nameRegexBuilder.delete(0,  2);
        }
        if ( nameRegexBuilder.toString().endsWith(".*") ) {
            nameRegexBuilder.delete(nameRegexBuilder.length() - 2, nameRegexBuilder.length());
        }

        nameRegexBuilder.replace(0,
                nameRegexBuilder.length(),
                escapeSpecialCharactersInSearchQueryPattern( nameRegexBuilder.toString()));

        nameRegexBuilder.insert(0, ".*");
        nameRegexBuilder.append(".*");
        return nameRegexBuilder.toString();
    }

    private String updatedNameQueryByQueryType(String nameQuery, @Nonnull QueryType queryType) {
        switch ( queryType ) {
            case CONTAINS:
               return escapeAndEncloseInDotAsterisk(nameQuery);
            case EXACT:
                return escapeSpecialCharactersInSearchQueryPattern(nameQuery);
            case REGEX:
            default:
                return nameQuery;
        }
    }

    private Stream<ServiceEntityApiDTO> queryByTypeAndStateAndName(
                    @Nullable String nameQueryString, @Nullable List<String> types,
                    @Nullable String state, @Nullable EnvironmentTypeEnum.EnvironmentType envType,
                    @Nullable EntityDetailType entityDetailType,
                    @Nullable QueryType queryType)
                    throws ConversionException, InterruptedException{
        // TODO Now, we only support one type of entities in the search
        if (types == null || types.isEmpty()) {
            throw new IllegalArgumentException("Type must be set for search result.");
        }

        final SearchParameters.Builder searchParamsBuilder =
                        SearchProtoUtil.makeSearchParameters(SearchProtoUtil.entityTypeFilter(types));

        if (!StringUtils.isEmpty(nameQueryString)) {
            if (queryType != null) {
                searchParamsBuilder.addSearchFilter(SearchProtoUtil.searchFilterProperty(
                                SearchProtoUtil.nameFilterRegex(updatedNameQueryByQueryType(nameQueryString, queryType))));
            } else {
                // Without a queryType parameter, we fallback to existing logic for backward
                // compatibility.
                searchParamsBuilder.addSearchFilter(SearchProtoUtil.searchFilterProperty(
                                SearchProtoUtil.nameFilterRegex(nameQueryString)));
            }
        }

        if (envType != null) {
            searchParamsBuilder.addSearchFilter(
                            SearchProtoUtil.searchFilterProperty(SearchProtoUtil.environmentTypeFilter(envType)));
        }

        if (!StringUtils.isEmpty(state)) {
            searchParamsBuilder.addSearchFilter(SearchProtoUtil.searchFilterProperty(
                            SearchProtoUtil.stateFilter(state)));
        }

        SearchRequest searchRequest = repositoryApi.newSearchRequest(searchParamsBuilder.build());

        if (EntityDetailType.aspects == entityDetailType) {
            searchRequest.useAspectMapper(entityAspectMapper);
        }

        //TODO: OM-52167 Differentiate between EntityDetailType.compact and EntityDetailType.entity

        return searchRequest
                        .getSEList()
                        .stream();
    }

    /**
     * Handles paginated search request on service entities with options.
     * @param nameQueryString matching name pattern
     * @param types Object types
     * @param state service entity state
     * @param envType service entity environment type
     * @param entityDetailType level of details of the response
     * @param searchPaginationRequest the {@link SearchPaginationRequest}
     * @param scopes scope to focus search
     * @param probeTypes The probe types used to filter results
     * @param queryType Pattern matching strategy to be used for stringToMatch.
     * @return SearchPaginationResponse service entity results
     * @throws ConversionException If there is an issue with entity conversion.
     * @throws InterruptedException If there is an issue waiting for a dependent operation.
     * @throws InvalidOperationException if pagination request is invalid
     */
    @VisibleForTesting
    SearchPaginationResponse queryServiceEntitiesPaginated(
                    @Nullable String nameQueryString,
                    @Nonnull List<String> types,
                    @Nullable String state,
                    @Nullable EnvironmentTypeEnum.EnvironmentType envType,
                    @Nullable EntityDetailType entityDetailType,
                    @Nullable SearchPaginationRequest searchPaginationRequest,
                    @Nullable List<String> scopes,
                    @Nullable List<String> probeTypes,
                    @Nullable QueryType queryType)
                    throws ConversionException, InterruptedException, InvalidOperationException {
        // TODO Now, we only support one type of entities in the search
        if (types == null || types.isEmpty()) {
            throw new IllegalArgumentException("Type must be set for search result.");
        }

        final SearchParameters.Builder searchParamsBuilder =
                        SearchProtoUtil.makeSearchParameters(SearchProtoUtil.entityTypeFilter(types));

        if (!StringUtils.isEmpty(nameQueryString)) {
            if (queryType != null) {
                searchParamsBuilder.addSearchFilter(SearchProtoUtil.searchFilterProperty(
                        SearchProtoUtil.nameFilterRegex(updatedNameQueryByQueryType(nameQueryString, queryType))));
            } else {
                // Without a queryType parameter, we fallback to existing logic for backward
                // compatibility.
                searchParamsBuilder.addSearchFilter(SearchProtoUtil.searchFilterProperty(
                        SearchProtoUtil.nameFilterRegex(nameQueryString)));
            }
        }

        if (envType != null) {
            searchParamsBuilder.addSearchFilter(
                            SearchProtoUtil.searchFilterProperty(SearchProtoUtil.environmentTypeFilter(envType)));
        }

        if (!StringUtils.isEmpty(state)) {
            searchParamsBuilder.addSearchFilter(SearchProtoUtil.searchFilterProperty(
                            SearchProtoUtil.stateFilter(state)));
        }

        if (probeTypes != null && !probeTypes.isEmpty()) {
            //Map probeTypes to targetIds
            Set<String> probes = new HashSet<>(probeTypes);
            Set<Long> targetsIds = thinTargetCache.getAllTargets()
                            .stream()
                            .filter(target -> probes.contains(target.probeInfo().type()))
                            .map(target -> target.oid())
                            .collect(Collectors.toSet());
            searchParamsBuilder.addSearchFilter(SearchProtoUtil.searchFilterProperty(SearchProtoUtil.discoveredBy(targetsIds)));
        }

        final SearchQuery searchQuery = SearchQuery.newBuilder().addSearchParameters(searchParamsBuilder).build();

        final Set<Long> scopeOids = scopes == null
                                    || (scopes.size() == 1
                                        && scopes.get(0).equals(UuidMapper.UI_REAL_TIME_MARKET_STR))
                                    ? Collections.EMPTY_SET : scopes.stream()
                        .map(Long::valueOf).collect(Collectors.toSet());

        searchPaginationRequest = searchPaginationRequest != null ? searchPaginationRequest
                        : new SearchPaginationRequest(null, null, true, null);

        PaginatedSearchRequest paginatedSearchRequest = repositoryApi.newPaginatedSearch(searchQuery,
            scopeOids, searchPaginationRequest);

        if (EntityDetailType.aspects == entityDetailType) {
            //TODO: OM-52167 Differentiate between EntityDetailType.compact and EntityDetailType.entity
            paginatedSearchRequest.requestAspects(entityAspectMapper, null);
        }

        return paginatedSearchRequest.getResponse();
    }

    /**
     * Note: Recent ISearchService changes:
     * - String groupType -> List <String> groupTypes
     * - add a new parameter: List <String> entityTypes
     * The new logic will be implemented in XL by JIRA items mentioned on OM-38355.
     * {@inheritDoc}
     */
    @Override
    public SearchPaginationResponse getSearchResults(String query,
                                                     List<String> types,
                                                     List<String> scopes,
                                                     String state,
                                                     List<String> groupTypes,
                                                     @Nullable EnvironmentType environmentType,
                                                     // Ignored for now.
                                                     @Nullable EntityDetailType entityDetailType,
                                                     SearchPaginationRequest paginationRequest,
                                                     List<String> entityTypes,
                                                     List<String> probeTypes,
                                                     boolean isRegex,
                                                     @Nullable Origin groupOrigin,
                                                     @Nullable QueryType queryType)
            throws Exception {
        if (types == null && CollectionUtils.isEmpty(groupTypes)) {
            throw new IllegalArgumentException("Type or groupType must be set for search result.");
        }
        // Determine which of many (many) types of searches is requested. G
        // NB: this method is heavily overloaded.  The REST endpoint to be redefined
        // TODO most of the cases below only handle one type of scope.  We need to generalize scope
        // handling to handle target, market, entity, or group for all use cases.
        if (!CollectionUtils.isEmpty(groupTypes)) {
            // Get all groups containing elements of type 'groupType' including any type of group
            // (regular, cluster, storage_cluster, etc.)
            return groupsService.getPaginatedGroupApiDTOs(
                addNameMatcher(query, Collections.emptyList(), GroupFilterMapper.GROUPS_FILTER_TYPE, queryType),
                paginationRequest, null, new HashSet(groupTypes), environmentType, null, scopes, true, groupOrigin);
        } else if (types != null) {
            final Set<String> typesHashSet = new HashSet(types);
            // Check for a type that requires a query to a specific service, vs. Repository search.
            if (typesHashSet.contains(GROUP)) {
                // IN Classic, this returns all Groups + Clusters. So we pass in true for
                // the "includeAllGroupClasses" flag of the groupService.getPaginatedGroupApiDTOs call.
                return groupsService.getPaginatedGroupApiDTOs(
                    addNameMatcher(query, Collections.emptyList(), GroupFilterMapper.GROUPS_FILTER_TYPE, queryType),
                    paginationRequest, null, null, environmentType, null, scopes, true, groupOrigin);
            } else if (Sets.intersection(typesHashSet,
                    GroupMapper.API_GROUP_TYPE_TO_GROUP_TYPE.keySet()).size() > 0) {
                for (Map.Entry<String, GroupType> entry : GroupMapper.API_GROUP_TYPE_TO_GROUP_TYPE.entrySet()) {
                    if (types.contains(entry.getKey())) {
                        String filter = GroupMapper.API_GROUP_TYPE_TO_FILTER_GROUP_TYPE.get(entry.getKey() );
                        return groupsService.getPaginatedGroupApiDTOs(
                                addNameMatcher(query, Collections.emptyList(), filter, queryType),
                                paginationRequest, entry.getValue(), null,
                                environmentType, null, scopes, false, groupOrigin);
                    }
                }
                throw new IllegalStateException("This can never happen because intersect(types, groupTypes) > 0 if and only if there is at least one groupType in types.");
            } else if (typesHashSet.contains(MarketMapper.MARKET)) {
                final Collection<MarketApiDTO> markets = marketsService.getMarkets(scopes);
                return paginationRequest.allResultsResponse(Lists.newArrayList(markets));
            } else if (typesHashSet.contains(TargetsService.TARGET)) {
                final Collection<TargetApiDTO> targets = targetsService.getTargets();
                return paginationRequest.allResultsResponse(Lists.newArrayList(targets));
            } else if (typesHashSet.contains(ApiEntityType.BUSINESS_ACCOUNT.apiStr())) {
                final Collection<BusinessUnitApiDTO> businessAccounts =
                        businessAccountRetriever.getBusinessAccountsInScope(scopes,
                            addNameMatcher(query, Collections.emptyList(),
                                EntityFilterMapper.ACCOUNT_NAME, queryType));
                return paginationRequest.allResultsResponse(Lists.newArrayList(businessAccounts));
            } else if (typesHashSet.contains(StringConstants.BILLING_FAMILY)) {
                return paginationRequest.allResultsResponse(
                    Lists.newArrayList(businessAccountRetriever.getBillingFamilies()));
            }
        }

        // Must be a search for a ServiceEntity
        return searchServiceEntities(query, types, scopes, state, environmentType, entityDetailType,
                                     paginationRequest, probeTypes, queryType);

    }

    /**
     * Searchs for service Entities.
     * @param query matching name pattern
     * @param types Object types
     * @param scopes scope to focus search
     * @param state service entity state
     * @param environmentType service entity environment type
     * @param entityDetailType level of details of the response
     * @param paginationRequest the {@link SearchPaginationRequest}
     * @param probeTypes The probe types used to filter results
     * @param queryType Pattern matching strategy to be used for stringToMatch
     * @return SearchPaginationResponse
     * @throws ConversionException If there is an issue with entity conversion
     * @throws InterruptedException If there is an issue waiting for a dependent operation
     * @throws InvalidOperationException if pagination request is invalid
     * @throws OperationFailedException Issue expanding group
     */
    private SearchPaginationResponse searchServiceEntities(String query, List<String> types,
                                                           List<String> scopes, String state,
                                                           @Nullable EnvironmentType environmentType,
                                                           @Nullable EntityDetailType entityDetailType,
                                                           SearchPaginationRequest paginationRequest,
                                                           List<String> probeTypes,
                                                           @Nullable QueryType queryType)
                    throws ConversionException, InterruptedException, InvalidOperationException,
                    OperationFailedException {
        //Check to see if pagination is requested,
        //else use legacy code path with no pagination for backwards compatibility.
        if (paginationRequest.hasLimit() || paginationRequest.getCursor().isPresent()) {
            return searchServiceEntitiesPaginated(query, types, scopes, state, environmentType,
                                                  entityDetailType, paginationRequest, probeTypes,
                                                  queryType);
        }
        return searchServiceEntitiesNonPaginated(query, types, scopes, state, environmentType,
                                                  entityDetailType, paginationRequest, probeTypes,
                                                  queryType);

    }

    /**
     * Search for all service entities.
     * @param query matching name pattern
     * @param types Object types
     * @param scopes scope to focus search
     * @param state service entity state
     * @param environmentType service entity environment type
     * @param entityDetailType level of details of the response
     * @param paginationRequest the {@link SearchPaginationRequest}
     * @param probeTypes The probe types used to filter results
     * @param queryType Pattern matching strategy to be used for stringToMatch
     * @return paginationedResponse
     * @throws ConversionException If there is an issue with entity conversion.
     * @throws InterruptedException If there is an issue waiting for a dependent operation.
     * @throws OperationFailedException Issue expanding groups
     */
    private SearchPaginationResponse searchServiceEntitiesNonPaginated(String query,
                                                                       List<String> types,
                                                                       List<String> scopes,
                                                                       String state,
                                                                       @Nullable EnvironmentType environmentType,
                                                                       @Nullable EntityDetailType entityDetailType,
                                                                       @NotNull SearchPaginationRequest paginationRequest,
                                                                       List<String> probeTypes,
                                                                       @Nullable QueryType queryType)
                    throws InterruptedException, ConversionException, OperationFailedException {
        final EnvironmentTypeEnum.EnvironmentType environmentTypeXl =
                        EnvironmentTypeMapper.fromApiToXL(environmentType);

        final Stream<ServiceEntityApiDTO> entitiesResult;
        // Patrick: I am scoping this clause of the request on the results side rather than on the
        // source side (in the search service), because this search is still using REST, and
        // converting this method to GRPC looks like it will take more time than I have right now.
        // TODO: Convert the entity search to use GRPC instead of REST.
        final Predicate<ServiceEntityApiDTO> scopeFilter = getScopePredicate();

        if (scopes == null || scopes.size() <= 0 ||
            (scopes.get(0).equals(UuidMapper.UI_REAL_TIME_MARKET_STR))) {
            // Search with no scope requested; or a single scope == "Market"; then search in live Market
            entitiesResult =
                            queryByTypeAndStateAndName(query, types, state, environmentTypeXl, entityDetailType, queryType)
                                            .filter(scopeFilter);
        } else {
            // expand to include the supplychain for the 'scopes', some of which may be groups or
            // clusters, and derive a list of ServiceEntities
            Set<String> scopeServiceEntityIds = groupsService.expandUuids(Sets.newHashSet(scopes),
                                                                          types, environmentType).stream().map(String::valueOf).collect(Collectors.toSet());

            final boolean isGlobalScope = ApiUtils.containsGlobalScope(
                            Sets.newHashSet(scopes), groupExpander);

            // Check if the scope is not global and the set of scope ids is empty.
            // This means there is an invalid scope.
            if (!isGlobalScope && scopeServiceEntityIds.isEmpty()) {
                // checking if the scope is valid
                final Set<Grouping> scopeGroups = groupExpander.getGroups(scopes);
                final Stream<ApiPartialEntity> scopeEntities =
                    repositoryApi.entitiesRequest(
                            scopes.stream().map(Long::parseLong).collect(Collectors.toSet()))
                        .getEntities();
                if (scopeGroups.isEmpty() && scopeEntities.count() == 0) {
                    throw new IllegalArgumentException("Invalid scope specified. There are no " +
                        "entities or groups related to the scope.");
                }
                // returning an empty response
                return paginationRequest.allResultsResponse(Collections.emptyList());
            }

            // TODO (roman, June 17 2019: We should probably include the IDs in the query, unless
            // there are a lot of IDs.

            // Fetch service entities matching the given specs
            // Restrict entities to those whose IDs are in the expanded 'scopeEntities'
            entitiesResult = queryByTypeAndStateAndName(query, types, state,
                                                        environmentTypeXl, entityDetailType, queryType)
                            .filter(scopeFilter)
                            .filter(se -> scopeServiceEntityIds.contains(se.getUuid()));
        }

        // set discoveredBy, filter by probe types and environment type
        List<BaseApiDTO> result = entitiesResult
                        .filter(se -> CollectionUtils.isEmpty(probeTypes) ||
                                      probeTypes.contains(se.getDiscoveredBy().getType()))
                        .collect(Collectors.toList());

        return paginationRequest.allResultsResponse(result);
    }

    /**
     * Create a scope predicate taking into account whether the entity ID and entity type are included within the user's scope.
     * 
     * @return predicate evaluating as true if an entity falls within a user's scope, false otherwise
     */
    private Predicate<ServiceEntityApiDTO> getScopePredicate() {
        if (!userSessionContext.isUserScoped()) {
            return entity -> true;
        }
        return entity -> {
            final ApiEntityType entityType = ApiEntityType.fromString(entity.getClassName());
            final long oid = Long.valueOf(entity.getUuid());
            return userSessionContext.isEntityTypeAllowedForUser(entityType) &&
                userSessionContext.getUserAccessScope().contains(oid);
        };
    }

    /**
     * Apply paginated on searching for serviceEntities.
     * @param query matching name pattern
     * @param types object types
     * @param scopes scope to focus search
     * @param state service entity state
     * @param environmentType service entity environment type
     * @param entityDetailType level of details of the response
     * @param paginationRequest the {@link SearchPaginationRequest}
     * @param probeTypes The probe types used to filter results
     * @param queryType Pattern matching strategy to be used for stringToMatch
     * @return paginationedResponse
     * @throws ConversionException If there is an issue with entity conversion.
     * @throws InterruptedException If there is an issue waiting for a dependent operation.
     * @throws InvalidOperationException if pagination request is invalid
     * @throws OperationFailedException Issue expanding group
     */
    private SearchPaginationResponse searchServiceEntitiesPaginated(String query,
                                                                    List<String> types,
                                                                    List<String> scopes,
                                                                    String state,
                                                                    @Nullable EnvironmentType environmentType,
                                                                    @Nullable EntityDetailType entityDetailType,
                                                                    SearchPaginationRequest paginationRequest,
                                                                    List<String> probeTypes,
                                                                    @Nullable QueryType queryType)
                    throws ConversionException, InterruptedException, OperationFailedException,
                    InvalidOperationException {
        final EnvironmentTypeEnum.EnvironmentType environmentTypeXl = EnvironmentTypeMapper
                        .fromApiToXL(environmentType);

        if (scopes == null || scopes.size() <= 0
            || scopes.get(0).equals(UuidMapper.UI_REAL_TIME_MARKET_STR)) {
            // Search with no scope requested; or a single scope == "Market"; then search in live Market
            return  queryServiceEntitiesPaginated(query, types, state, environmentTypeXl,
                                                  entityDetailType,
                                                  paginationRequest, scopes, probeTypes, queryType);
        } else {
            // expand to include the supplychain for the 'scopes', some of which may be groups or
            // clusters, and derive a list of ServiceEntities
            Set<String> scopeServiceEntityIds = groupsService.expandUuids(Sets.newHashSet(scopes),
                                                                          types, environmentType).stream().map(String::valueOf).collect(Collectors.toSet());

            final boolean isGlobalScope = ApiUtils.containsGlobalScope(Sets.newHashSet(scopes), groupExpander);


            if (isGlobalScope || !scopeServiceEntityIds.isEmpty()) {
                return queryServiceEntitiesPaginated(query, types, state, environmentTypeXl,
                                                     entityDetailType,
                                                     paginationRequest, new ArrayList<>(scopeServiceEntityIds),
                                                     probeTypes, queryType);
            }

            // checking if the scope is valid
            final Set<Grouping> scopeGroups = groupExpander.getGroups(scopes);
            Stream<ApiPartialEntity> scopeEntities = repositoryApi.entitiesRequest(scopes.stream()
                .map(Long::parseLong).collect(Collectors.toSet()))
                .getEntities();
            if (scopeGroups.isEmpty() && scopeEntities.count() == 0) {
                throw new IllegalArgumentException("Invalid scope specified. There are no " +
                    "entities or groups related to the scope.");
            }
            // returning an empty response
            return paginationRequest.allResultsResponse(Collections.emptyList());
        }
    }

    /**
     * A general search given a filter - may be asked to search over ServiceEntities or Groups.
     *
     * @param nameQueryString The query to run against the display name of the results.
     * @param inputDTO the specification of what to search.
     * @param paginationRequest The pagination related parameter.
     * @param aspectNames The input list of aspect names for a particular entity type.
     * @param queryType Pattern matching strategy to be used for stringToMatch.
     * @return a list of DTOs based on the type of the search: ServiceEntityApiDTO or GroupApiDTO
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     */
    @Override
    public SearchPaginationResponse getMembersBasedOnFilter(String nameQueryString, GroupApiDTO inputDTO,
            SearchPaginationRequest paginationRequest, @Nullable List<String> aspectNames, @Nullable QueryType queryType)
            throws OperationFailedException, InvalidOperationException, ConversionException,
            InterruptedException, IllegalArgumentException {
        //since the criteriaList field of inputDTO which holds the filters does not contain enums about filters but string
        //we ave to check and validate the input filters.
        EntityFilterMapper.checkInputDTOParameters(inputDTO);

        // the query input is called a GroupApiDTO even though this search can apply to any type
        // if this is a group search, we need to know the right "name filter type" that can be used
        // to search for a group by name. These come from the groupBuilderUsecases.json file.
        final String className = StringUtils.defaultIfEmpty(inputDTO.getClassName(), "");
        final Set<String> groupTypes = inputDTO.getGroupType() == null
                ? null : Collections.singleton(inputDTO.getGroupType());
        if (GroupMapper.API_GROUP_TYPE_TO_GROUP_TYPE.containsKey(className)) {
            GroupType groupType = GroupMapper.API_GROUP_TYPE_TO_GROUP_TYPE.get(className);
            String filter = GroupMapper.API_GROUP_TYPE_TO_FILTER_GROUP_TYPE.get(className);
            return groupsService.getPaginatedGroupApiDTOs(
                    addNameMatcher(nameQueryString, inputDTO.getCriteriaList(), filter,
                            queryType), paginationRequest, groupType, groupTypes,
                    inputDTO.getEnvironmentType(), null, inputDTO.getScope(), false,
                    inputDTO.getGroupOrigin());
        } else if (BUSINESS_ACCOUNT.equals(className)) {
            return paginationRequest.allResultsResponse(Lists.newArrayList(
                    businessAccountRetriever.getBusinessAccountsInScope(inputDTO.getScope(),
                            addNameMatcher(nameQueryString, inputDTO.getCriteriaList(),
                                EntityFilterMapper.ACCOUNT_NAME, queryType))));
        } else if (WORKLOAD.equals(className)) {
            List<String> scope = inputDTO.getScope();

            if (scope == null || scope.size() == 0) {
                throw new UnsupportedOperationException("Invalid workload scope");
            }

            String scopeId = inputDTO.getScope().get(0);
            long businessAccountId;
            try {
                businessAccountId = Long.valueOf(scopeId);
            } catch (NumberFormatException ex) {
                throw new UnsupportedOperationException("Invalid workload scope ID: " + scopeId);
            }

            SingleEntityRequest entityRequest = repositoryApi.entityRequest(businessAccountId);
            Optional<TopologyEntityDTO> topologyEntityDTO = entityRequest
                .getFullEntity();
            if (!topologyEntityDTO.isPresent()) {
                // if this is not business account try regular search
                return searchEntitiesByParameters(inputDTO, nameQueryString, paginationRequest, aspectNames, queryType);
            }

            Set<Long> entitiesOid = topologyEntityDTO.get().getConnectedEntityListList()
                .stream()
                .map(entity -> entity.getConnectedEntityId())
                .collect(Collectors.toSet());

            List<ServiceEntityApiDTO> results = repositoryApi.entitiesRequest(entitiesOid)
                .getSEMap()
                .values()
                .stream()
                .filter(se -> ApiEntityType.WORKLOAD_ENTITY_TYPES.contains(ApiEntityType.fromString(se.getClassName())))
                .collect(Collectors.toList());

            priceIndexPopulator.populateRealTimeEntities(results);

            List<BaseApiDTO> apiResults = results.stream().collect(Collectors.toList());

            return paginationRequest.allResultsResponse(apiResults);
        } else {
            // this isn't a group search after all -- use a generic search method instead.
            return searchEntitiesByParameters(inputDTO, nameQueryString, paginationRequest, aspectNames, queryType);
        }
    }


    /**
     * This utility method will add a group/cluster/storage cluster display name filter based on the
     * input "string to match", if the "string to match" isn't empty. Otherwise, it does nothing.
     *
     * @param stringToMatch the potential string to match. Can be blank or null.
     * @param originalFilters the existing filters
     * @param filterTypeToUse the filter type to use when optionally creating the filter.
     * @param queryType the pattern matching strategy to be used for stringToMatch. Default:
     *         Contains
     * @return the list of filters + an additional one for the "string to match"
     */
    @VisibleForTesting
    protected List<FilterApiDTO> addNameMatcher(String stringToMatch,
                                              List<FilterApiDTO> originalFilters,
                                              String filterTypeToUse, QueryType queryType) {
        // if a queryType is not provided a simple string contains strategy is used
        QueryType qType = Objects.isNull(queryType) ? QueryType.CONTAINS : queryType;

        if (StringUtils.isEmpty(stringToMatch)) {
            return originalFilters;
        }
        // create a name filter for the 'query' and add it to the filters list.
        FilterApiDTO nameFilter = new FilterApiDTO();

        switch (qType) {
            case CONTAINS:
                nameFilter.setExpVal(escapeAndEncloseInDotAsterisk(stringToMatch));
                nameFilter.setExpType(EntityFilterMapper.REGEX_MATCH);
                break;
            case REGEX:
                nameFilter.setExpVal(stringToMatch);
                nameFilter.setExpType(EntityFilterMapper.REGEX_MATCH);
                break;
            case EXACT:
                nameFilter.setExpVal(escapeSpecialCharactersInSearchQueryPattern(stringToMatch));
                nameFilter.setExpType(EntityFilterMapper.EQUAL);
            default:
                break;
        }
        nameFilter.setCaseSensitive(false);
        nameFilter.setFilterType(filterTypeToUse);
        List<FilterApiDTO> returnFilters = new ArrayList<>();
        returnFilters.add(nameFilter);
        if (!CollectionUtils.isEmpty(originalFilters)) {
            returnFilters.addAll(originalFilters);
        }
        return returnFilters;
    }

    /**
     * Send a search request to search rpc service by passing a list of search
     * parameters based on the parameters in the inputDTO.
     *
     * @param inputDTO a Description of what search to conduct
     * @param nameQuery user specified search query for entity name, if no queryType is specified
     *                  then name query is performed with CONTAINS matching pattern.
     * @param paginationRequest The pagination related parameter.
     * @param aspectNames The input list of aspect names.
     * @param queryType Pattern matching strategy to be used for stringToMatch.
     * @return A list of {@link BaseApiDTO} will be sent back to client
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     * @throws OperationFailedException on error performing search
     */
    private SearchPaginationResponse searchEntitiesByParameters(@Nonnull GroupApiDTO inputDTO, @Nullable String nameQuery,
            @Nonnull SearchPaginationRequest paginationRequest, @Nullable List<String> aspectNames, @Nullable QueryType queryType)
                throws OperationFailedException, ConversionException, InterruptedException {
        String updatedQuery = null;
        if ( !Strings.isEmpty(nameQuery) && nameQuery != null ) {
            if ( queryType != null ) {
                  updatedQuery = updatedNameQueryByQueryType(nameQuery, queryType);
            } else {
                updatedQuery = updatedNameQueryByQueryType(nameQuery, QueryType.CONTAINS);
            }
        }

        final List<String> relatedTypes = getRelatedEntityTypes(inputDTO.getClassName());
        List<SearchParameters> searchParameters = entityFilterMapper.convertToSearchParameters(
                inputDTO.getCriteriaList(), relatedTypes, updatedQuery).stream()
                // Convert any cluster membership filters to property filters.
                .map(filterResolver::resolveExternalFilters)
                .collect(Collectors.toList());
        // operator is AND by default
        LogicalOperator logicaloperator = LogicalOperator.AND;
        if (inputDTO.getLogicalOperator().equalsIgnoreCase(LogicalOperator.OR.name())) {
            logicaloperator = LogicalOperator.OR;
        } else if (inputDTO.getLogicalOperator().equalsIgnoreCase(LogicalOperator.XOR.name())) {
            logicaloperator = LogicalOperator.XOR;
        }

        // match only the entity uuids which are part of the group or cluster
        // defined in the scope
        final Set<String> scopeSet = Optional.ofNullable(inputDTO.getScope())
                .map(ImmutableSet::copyOf)
                .orElse(ImmutableSet.of());
        // if no scope is provided, or it's a market scope, or it's a global temp group with the
        // same type as the related type, then consider this a global scope
        final boolean isGlobalScope = scopeSet.isEmpty()
                || scopeSet.stream().anyMatch(UuidMapper::isRealtimeMarket)
                || ApiUtils.isGlobalTempGroupWithSameEntityType(scopeSet, groupExpander, relatedTypes);
        // collect the superset of entities that should be used in search rpc service
        final Set<Long> allEntityOids;
        if (isGlobalScope) {
            // if this is a global scope, then use empty set to avoid passing a long list of oids
            // over grpc
            allEntityOids = Collections.emptySet();
            // add environment filter to all search parameters if requested
            if (inputDTO.getEnvironmentType() != null) {
                searchParameters = addEnvironmentTypeFilter(inputDTO.getEnvironmentType(), searchParameters);
            }
        } else {
            final Set<Long> expandedIds = groupsService.expandUuids(scopeSet, relatedTypes,
                    inputDTO.getEnvironmentType());
            if (expandedIds.isEmpty()) {
                // checking if the scope is valid
                Stream<ApiPartialEntity> scopeEntities = repositoryApi.entitiesRequest(scopeSet.stream()
                    .map(Long::parseLong).collect(Collectors.toSet()))
                    .getEntities();
                if (scopeEntities == null || scopeEntities.count() == 0) {
                    throw new IllegalArgumentException("Invalid scope specified. There are no entities"
                        + " related to the scope.");
                }
                // return empty response since there is no related entities in given scope
                return paginationRequest.allResultsResponse(Collections.emptyList());
            }
            // if scope is specified, result entities should be chosen from related entities in scope
            // note: environment type has already been filtered above in expandUuids
            allEntityOids = expandedIds;
        }

        SearchQuery searchQuery = SearchQuery.newBuilder()
                .addAllSearchParameters(searchParameters)
                .setLogicalOperator(logicaloperator)
                .build();

        try {
            //Repository does not support sorting on utilization
            if (paginationRequest.getOrderBy().equals(SearchOrderBy.UTILIZATION)) {
                final SearchEntityOidsRequest searchOidsRequest = SearchEntityOidsRequest.newBuilder()
                    .setSearch(searchQuery)
                    .addAllEntityOid(allEntityOids)
                    .build();
                return getServiceEntityPaginatedWithUtilization(inputDTO, updatedQuery, paginationRequest,
                        allEntityOids, searchOidsRequest, isGlobalScope, aspectNames);
            }
        } catch (RuntimeException e) {
            if (e instanceof StatusRuntimeException) {
                // This is a gRPC StatusRuntimeException
                Status status = ((StatusRuntimeException)e).getStatus();
                logger.warn("Unable to search entities ordered by {}: {} caused by {}.",
                        paginationRequest.getOrderBy(), status.getDescription(), status.getCause());
            } else {
                logger.error("Error when searching entities ordered by {}.",
                        paginationRequest.getOrderBy(), e);
            }
        }
        //Paginated calls to repository, supports order_by Name and Severity
        final RepositoryApi.PaginatedSearchRequest paginatedSearchRequest =
                        repositoryApi.newPaginatedSearch(searchQuery, allEntityOids, paginationRequest);
        if (aspectNames != null && !aspectNames.isEmpty()) {
            paginatedSearchRequest.requestAspects(entityAspectMapper, aspectNames);
        }

        return paginatedSearchRequest.getResponse();
    }

    /**
     * Add environment type property filter to the given list of search parameters. It's applied
     * to all the parameters in the list.
     *
     * @param environmentType the environment type to check
     * @param searchParameters the list of SearchParameters to add environment type filter to
     * @return list of SearchParameters with environment type filter
     */
    private List<SearchParameters> addEnvironmentTypeFilter(
            @Nonnull EnvironmentType environmentType,
            @Nonnull List<SearchParameters> searchParameters) {
        if (environmentType != EnvironmentType.HYBRID) {
            final SearchFilter envTypeFilter =
                SearchFilter.newBuilder()
                    .setPropertyFilter(SearchProtoUtil.environmentTypeFilter(
                                            EnvironmentTypeMapper.fromApiToXL(environmentType)))
                    .build();
            searchParameters = searchParameters.stream()
                    .map(searchParameter -> searchParameter.toBuilder()
                            .addSearchFilter(envTypeFilter)
                            .build())
                    .collect(Collectors.toList());
        }
        return searchParameters;
    }

    private List<String> getRelatedEntityTypes(String className) {
        if (className == null) {
            return Collections.emptyList();
        } else if (className.equals(WORKLOAD)) {
            return ApiEntityType.WORKLOAD_ENTITY_TYPES.stream()
                .map(ApiEntityType::apiStr)
                .collect(Collectors.toList());
        } else {
            return Collections.singletonList(className);
        }
    }

    /**
     * Search service entities with order by utilization. The query workflow will be: 1: query repository
     * to get all matched candidates. 2: send all candidates to history component to get paginated
     * entity oids with order by utilization. 3: query repository to get full entity information for
     * those top x entity oids.
     *
     * @param inputDTO a Description of what search to conduct.
     * @param nameQuery user specified search query for entity name.
     * @param paginationRequest {@link SearchPaginationRequest}.
     * @param expandedIds a list of entity oids after expanded.
     * @param searchRequest {@link Search.SearchEntityOidsRequest}.
     * @param isGlobalScope a boolean represents if search scope is global scope or not.
     * @param aspectNames aspect names
     * @return pagination response
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    private SearchPaginationResponse getServiceEntityPaginatedWithUtilization(
            @Nonnull final GroupApiDTO inputDTO,
            @Nullable final String nameQuery,
            @Nonnull final SearchPaginationRequest paginationRequest,
            @Nonnull final Set<Long> expandedIds,
            @Nonnull final Search.SearchEntityOidsRequest searchRequest,
            @Nonnull final boolean isGlobalScope,
            @Nullable Collection<String> aspectNames) throws ConversionException, InterruptedException {
        // if it is global scope and there is no other search criteria, it means all service entities
        // of same entity type are search candidates.
        final boolean isGlobalEntities =
                isGlobalScope && inputDTO.getCriteriaList().isEmpty() && StringUtils.isEmpty(nameQuery);
        // the search query with order by utilization workflow will be:
        // 1: (if necessary) query repository component to get all candidate entity oids.
        // 2: query history component to get top X entity oids from the candidates sorted by price index.
        // 3: query repository to get full entity information only for top X entity oids.
        final EntityStatsScope.Builder statsScope = EntityStatsScope.newBuilder();
        if (!isGlobalEntities) {
            statsScope.setEntityList(EntityList.newBuilder()
                .addAllEntities(getCandidateEntitiesForSearch(inputDTO, nameQuery, expandedIds, searchRequest)));
        } else {
            statsScope.setEntityType(ApiEntityType.fromString(inputDTO.getClassName()).typeNumber());
        }
        final GetEntityStatsResponse response = statsHistoryServiceRpc.getEntityStats(
            GetEntityStatsRequest.newBuilder()
                .setScope(statsScope)
                .setFilter(StatsFilter.newBuilder()
                    .addCommodityRequests(CommodityRequest.newBuilder()
                        .setCommodityName("priceIndex")))
                .setPaginationParams(paginationMapper.toProtoParams(paginationRequest))
                .build());
        final List<Long> nextPageIds = response.getEntityStatsList().stream()
                .map(EntityStats::getOid)
                .collect(Collectors.toList());
        final RepositoryApi.MultiEntityRequest entityRequest =
                repositoryApi.entitiesRequest(Sets.newHashSet(nextPageIds));

        if (CollectionUtils.isNotEmpty(aspectNames)) {
            entityRequest.useAspectMapper(entityAspectMapper, aspectNames);
        }

        final Map<Long, ServiceEntityApiDTO> serviceEntityMap = entityRequest.getSEMap();
        // It is important to keep the order of entityIdsList, because they have already sorted by
        // utilization.
        final List<ServiceEntityApiDTO> entities = nextPageIds.stream()
                .filter(serviceEntityMap::containsKey)
                .map(serviceEntityMap::get)
                .collect(Collectors.toList());
        return buildPaginationResponse(entities, response.getPaginationResponse(), paginationRequest);
    }

    @VisibleForTesting
    protected Set<Long> getCandidateEntitiesForSearch(
            @Nonnull final GroupApiDTO inputDTO,
            @Nullable final String nameQuery,
            @Nonnull final Set<Long> expandedIds,
            @Nonnull final Search.SearchEntityOidsRequest searchRequest) {
        final Set<Long> candidates;
        // if query request doesn't contains any filter criteria, it can directly use expanded ids
        // as results. Otherwise, it needs to query repository to get matched entity oids.
        if ( (inputDTO.getCriteriaList() == null || inputDTO.getCriteriaList().isEmpty())
                && StringUtils.isEmpty(nameQuery)
                && !expandedIds.isEmpty()) {
            candidates = expandedIds;
        } else {
            candidates = searchServiceRpc.searchEntityOids(searchRequest).getEntitiesList().stream()
                    .collect(Collectors.toSet());
        }
        return candidates;
    }

    private SearchPaginationResponse buildPaginationResponse(
            @Nonnull List<ServiceEntityApiDTO> entities,
            @Nonnull final PaginationResponse paginationResponse,
            @Nonnull final SearchPaginationRequest paginationRequest) {
        final List<? extends BaseApiDTO> results = entities;
        return PaginationProtoUtil.getNextCursor(paginationResponse)
                .map(nexCursor -> paginationRequest.nextPageResponse(
                        (List<BaseApiDTO>) results, nexCursor, paginationResponse.getTotalRecordCount()))
                .orElseGet(() -> paginationRequest.finalPageResponse(
                        (List<BaseApiDTO>) results, paginationResponse.getTotalRecordCount()));
    }

    @Override
    public Map<String, Object> getGroupBuilderUsecases() {
        return groupUseCaseParser.getUseCases().entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    @Nonnull
    private static List<CriteriaOptionApiDTO> getStateOptions() {
        final List<CriteriaOptionApiDTO> optionApiDTOs = new ArrayList<>();
        // options should be all possible states
        Arrays.stream(UIEntityState.values())
                .forEach(option -> {
                    final CriteriaOptionApiDTO optionApiDTO = new CriteriaOptionApiDTO();
                    optionApiDTO.setValue(option.apiStr());
                    optionApiDTOs.add(optionApiDTO);
                });
        return optionApiDTOs;
    }

    /**
     * Get "boolean" filter dropdown-menu options.
     *
     * @return List of {@CriteriaOptionApiDTO} to be presented in the UI.
     */
    @Nonnull
    private  List<CriteriaOptionApiDTO> getBooleanFilterOptions() {
        final List<CriteriaOptionApiDTO> optionApiDTOs = new ArrayList<>();

        Arrays.stream(UIBooleanFilter.values())
                .forEach(option -> {
                    final CriteriaOptionApiDTO optionApiDTO = new CriteriaOptionApiDTO();
                    optionApiDTO.setValue(option.apiStr());
                    optionApiDTOs.add(optionApiDTO);
                });
        return optionApiDTOs;
    }

    @Nonnull
    private List<CriteriaOptionApiDTO> getSTagAttributeOptions(final List<String> scopes,
            final String entityType,
            final EnvironmentType envType) throws OperationFailedException {
        final List<CriteriaOptionApiDTO> optionApiDTOs = new ArrayList<>();
        if (GroupMapper.GROUP_CLASSES.contains(entityType)) {
            // retrieve tags from the group service
            final Map<Long, Tags> tagsOfGroups =
                    groupServiceRpc.getTags(GetTagsRequest.newBuilder().build()).getTagsMap();
            final Map<String, TagValuesDTO> tags = convertReceivedTags(tagsOfGroups);

            // map to desired output
            tags.entrySet().forEach(tagEntry -> {
                final CriteriaOptionApiDTO criteriaOptionApiDTO = new CriteriaOptionApiDTO();
                criteriaOptionApiDTO.setValue(tagEntry.getKey());
                criteriaOptionApiDTO.setSubValues(tagEntry.getValue().getValuesList());
                optionApiDTOs.add(criteriaOptionApiDTO);
            });
        } else {
            // retrieve relevant tags from the tags service
            final List<TagApiDTO> tags = tagsService.getTags(scopes, entityType, envType, null).getList();

            // convert into a map
            final Map<String, List<TagApiDTO>> tagsAsMap =
                    tags.stream().collect(Collectors.groupingBy(TagApiDTO::getKey));

            // translate tags as criteria options in the result
            tagsAsMap.entrySet().forEach(e -> {
                final CriteriaOptionApiDTO criteriaOptionApiDTO = new CriteriaOptionApiDTO();
                criteriaOptionApiDTO.setValue(e.getKey());
                criteriaOptionApiDTO.setSubValues(
                        e.getValue()
                                .stream()
                                .flatMap(tagApiDTO -> tagApiDTO.getValues().stream())
                                .collect(Collectors.toList())
                );
                optionApiDTOs.add(criteriaOptionApiDTO);
            });
        }
        return optionApiDTOs;
    }

    /**
     * Convert received tags in order to have map contains entries of tag's name and related existed
     * values from all groups.
     *
     * @param tagsOfGroups tags of all groups
     * @return map of tag names to tag values
     */
    private Map<String, TagValuesDTO> convertReceivedTags(Map<Long, Tags> tagsOfGroups) {
        final Map<String, TagValuesDTO> tagsMap = new HashMap<>();
        for (Tags tags : tagsOfGroups.values()) {
            Map<String, TagValuesDTO> tagsOfGroup = tags.getTagsMap();
            for (Entry<String, TagValuesDTO> tag : tagsOfGroup.entrySet()) {
                tagsMap.merge(tag.getKey(), tag.getValue(),
                        (tagValuesDTO, tagValuesDTO2) -> TagValuesDTO.newBuilder()
                                .addAllValues(Stream.of(tagValuesDTO.getValuesList(),
                                        tagValuesDTO2.getValuesList())
                                        .flatMap(Collection::stream)
                                        .collect(Collectors.toSet()))
                                .build());
            }
        }
        return tagsMap;
    }

    @Nonnull
    private List<CriteriaOptionApiDTO> getAccountOptions() {
        final List<CriteriaOptionApiDTO> optionApiDTOs = new ArrayList<>();
        // get all business accounts which have associated target (monitored by probe)
        repositoryApi.newSearchRequest(SearchProtoUtil.makeSearchParameters(
                SearchProtoUtil.entityTypeFilter(ApiEntityType.BUSINESS_ACCOUNT.apiStr()))
                .addSearchFilter(SearchFilter.newBuilder()
                        .setPropertyFilter(SearchProtoUtil.associatedTargetFilter())
                        .build())
                .build())
                .getMinimalEntities()
                .forEach(ba -> {
                    // convert each business account to criteria option
                    CriteriaOptionApiDTO crOpt = new CriteriaOptionApiDTO();
                    crOpt.setDisplayName(ba.getDisplayName());
                    crOpt.setValue(String.valueOf(ba.getOid()));
                    optionApiDTOs.add(crOpt);
                });
        return optionApiDTOs;
    }

    @Nonnull
    private static List<CriteriaOptionApiDTO> getVolumeAttachmentStateOptions() {
        final List<CriteriaOptionApiDTO> optionApiDTOs = new ArrayList<>();
        Arrays.stream(AttachmentState.values())
                .forEach(option -> {
                    final CriteriaOptionApiDTO optionApiDTO = new CriteriaOptionApiDTO();
                    optionApiDTO.setValue(option.name());
                    optionApiDTOs.add(optionApiDTO);
                });
        return optionApiDTOs;
    }

    @Nonnull
    private List<CriteriaOptionApiDTO> getConnectionStorageTierOptions() {
        final List<CriteriaOptionApiDTO> optionApiDTOs = new ArrayList<>();
        repositoryApi.newSearchRequest(SearchProtoUtil.makeSearchParameters(
                SearchProtoUtil.entityTypeFilter(ApiEntityType.STORAGE_TIER.apiStr()))
                .build())
                .getMinimalEntities()
                .forEach(tier -> {
                    CriteriaOptionApiDTO crOpt = new CriteriaOptionApiDTO();
                    crOpt.setDisplayName(tier.getDisplayName());
                    crOpt.setValue(String.valueOf(tier.getOid()));
                    optionApiDTOs.add(crOpt);
                });
        return optionApiDTOs;
    }

    @Nonnull
    private List<CriteriaOptionApiDTO> getRegionFilterOptions() {
        final List<CriteriaOptionApiDTO> optionApiDTOs = new ArrayList<>();
        repositoryApi.newSearchRequest(SearchProtoUtil.makeSearchParameters(
                SearchProtoUtil.entityTypeFilter(ApiEntityType.REGION.apiStr()))
                .build())
                .getMinimalEntities()
                .forEach(region -> {
                    CriteriaOptionApiDTO crOpt = new CriteriaOptionApiDTO();
                    crOpt.setDisplayName(region.getDisplayName());
                    crOpt.setValue(String.valueOf(region.getOid()));
                    optionApiDTOs.add(crOpt);
                });
        return optionApiDTOs;
    }

    @Nonnull
    private List<CriteriaOptionApiDTO> getCloudProviderOptions() {
        return targetsService.getProbes()
                .stream()
                .map(probe -> CloudType.fromProbeType(probe.getType()))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(CloudType::name)
                .distinct()
                .map(providerType -> {
                    final CriteriaOptionApiDTO crOpt = new CriteriaOptionApiDTO();
                    crOpt.setValue(providerType);
                    return crOpt;
                })
                .collect(Collectors.toList());
    }

    @Nonnull
    private List<CriteriaOptionApiDTO> getResourceGroupsUUIDOptions()
            throws OperationFailedException, ConversionException, InterruptedException,
            InvalidOperationException {
        return getGroupsOptions(GroupType.RESOURCE);
    }

    @Nonnull
    private List<CriteriaOptionApiDTO> getResourceGroupsNameOptions()
            throws OperationFailedException, ConversionException, InterruptedException,
            InvalidOperationException {
        final List<GroupApiDTO> groups =
                groupsService.getGroupsByType(GroupType.RESOURCE, null, Collections.emptyList());
        // Collecting to set to filter duplicate group names.
        // For example, two resourceGroups from two accounts may have the same name.
        Set<String> groupNames = groups.stream().map(GroupApiDTO::getDisplayName).collect(Collectors.toSet());
        final List<CriteriaOptionApiDTO> result = new ArrayList<>(groups.size());
        for (String groupName : groupNames) {
            final CriteriaOptionApiDTO option = new CriteriaOptionApiDTO();
            option.setDisplayName(groupName);
            option.setValue(groupName);
            result.add(option);
        }
        return Collections.unmodifiableList(result);
    }

    @Nonnull
    private List<CriteriaOptionApiDTO> getBillingFamiliesOptions()
            throws OperationFailedException, ConversionException, InterruptedException,
            InvalidOperationException {
        return getGroupsOptions(GroupType.BILLING_FAMILY);
    }

    private List<CriteriaOptionApiDTO> getGroupsOptions(GroupType groupType)
            throws OperationFailedException, ConversionException, InterruptedException,
            InvalidOperationException {
        final List<GroupApiDTO> groups =
                groupsService.getGroupsByType(groupType, null, Collections.emptyList());
        final List<CriteriaOptionApiDTO> result = new ArrayList<>(groups.size());
        for (GroupApiDTO group : groups) {
            final CriteriaOptionApiDTO option = new CriteriaOptionApiDTO();
            option.setDisplayName(group.getDisplayName());
            option.setValue(group.getUuid());
            result.add(option);
        }
        return Collections.unmodifiableList(result);
    }

    @Nonnull
    private List<CriteriaOptionApiDTO> getValidationStatusOptions() {
        final List<CriteriaOptionApiDTO> result =
                new ArrayList<>(OperationStatus.Status.values().length - 1);
        for (OperationStatus.Status status : OperationStatus.Status.values()) {
            if (status != OperationStatus.Status.IN_PROGRESS) {
                final CriteriaOptionApiDTO option = new CriteriaOptionApiDTO();
                option.setValue(status.name());
                result.add(option);
            }
        }
        return Collections.unmodifiableList(result);
    }

    @Nonnull
    private List<CriteriaOptionApiDTO> getWorkloadControllerTypeOptions() {
        final List<CriteriaOptionApiDTO> optionApiDTOs = new ArrayList<>();
        // Options have all possible controller types except CUSTOM_CONTROLLER_INFO and
        // CONTROLLERTYPE_NOT_SET
        Arrays.stream(ControllerTypeCase.values())
            .forEach(controllerType -> {
                if (controllerType == ControllerTypeCase.CONTROLLERTYPE_NOT_SET
                    || controllerType == ControllerTypeCase.CUSTOM_CONTROLLER_INFO) {
                    return;
                }
                final CriteriaOptionApiDTO optionApiDTO = new CriteriaOptionApiDTO();
                optionApiDTO.setValue(controllerType.name());
                optionApiDTOs.add(optionApiDTO);
            });
        // Add an "Other" option
        CriteriaOptionApiDTO optionApiDTO = new CriteriaOptionApiDTO();
        optionApiDTO.setValue(SearchableProperties.OTHER_CONTROLLER_TYPE);
        optionApiDTOs.add(optionApiDTO);
        return optionApiDTOs;
    }

    /**
     * Get available option by criteria.
     * @param criteriaKey criteria key to query for
     * @param scopes scopes to apply
     * @param entityType entity type criteria is requested for
     * @param envType environment type
     * @return list of criteria options
     * @throws OperationFailedException if something failed executing
     * @throws UnknownObjectException if options loading is not supported for the criteria
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     * @throws InvalidOperationException if error faced processing the request
     */
    @Override
    public List<CriteriaOptionApiDTO> getCriteriaOptions(final String criteriaKey,
            final List<String> scopes, final String entityType, final EnvironmentType envType)
            throws OperationFailedException, UnknownObjectException, ConversionException,
            InterruptedException, InvalidOperationException {
        final CriteriaOptionProvider optionsProvider = criteriaOptionProviders.get(criteriaKey);
        if (optionsProvider == null) {
            throw new UnknownObjectException("Unknown criterion key: " + criteriaKey);
        } else {
            return optionsProvider.getOptions(scopes, entityType, envType);
        }
    }

    /**
     * Returns a String whose special characters have been escaped (if any)
     *
     * @param queryPattern the query string whose special characters will be escaped
     * @return a String whose special characters have been escaped.
     */
    @Nullable
    private String escapeSpecialCharactersInSearchQueryPattern(@Nullable String queryPattern) {
        if (StringUtils.isEmpty(queryPattern)) {
            return queryPattern;
        }
        // mark the pattern as a literal
        return Pattern.quote(queryPattern);
    }

    /**
     * Interface of an object providing options for the search criteria values to select from.
     */
    private interface CriteriaOptionProvider {
        @Nonnull
        List<CriteriaOptionApiDTO> getOptions(List<String> scopes, String entityType,
                EnvironmentType envType)
                throws OperationFailedException, ConversionException, InterruptedException,
                InvalidOperationException;
    }
}
