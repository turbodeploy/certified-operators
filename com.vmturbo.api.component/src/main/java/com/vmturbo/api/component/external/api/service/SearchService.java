package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.mapper.GroupMapper.ACCOUNT_OID;
import static com.vmturbo.api.component.external.api.mapper.GroupMapper.STATE;
import static com.vmturbo.components.common.utils.StringConstants.CLUSTER;
import static com.vmturbo.components.common.utils.StringConstants.GROUP;
import static com.vmturbo.components.common.utils.StringConstants.STORAGE_CLUSTER;
import static com.vmturbo.components.common.utils.StringConstants.VIRTUAL_MACHINE_CLUSTER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.CollectionUtils;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.BusinessUnitMapper;
import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.component.external.api.mapper.GroupUseCaseParser;
import com.vmturbo.api.component.external.api.mapper.MarketMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.SeverityPopulator;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
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
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.SearchOrderBy;
import com.vmturbo.api.pagination.SearchPaginationRequest;
import com.vmturbo.api.pagination.SearchPaginationRequest.SearchPaginationResponse;
import com.vmturbo.api.serviceinterfaces.ISearchService;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.PaginationProtoUtil;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.MultiEntityRequest;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceBlockingStub;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetTagsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupPropertyFilterList;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityList;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.repository.api.RepositoryClient;

/**
 * Service entry points to search the Repository.
 **/
public class SearchService implements ISearchService {

    private static final Logger logger = LogManager.getLogger();

    private final RepositoryApi repositoryApi;

    private final MarketsService marketsService;

    private final GroupsService groupsService;
    private final GroupExpander groupExpander;

    private final TargetsService targetsService;

    private final SearchServiceBlockingStub searchServiceRpc;

    private final EntitySeverityServiceBlockingStub entitySeverityRpc;

    private final SeverityPopulator severityPopulator;

    private final StatsHistoryServiceBlockingStub statsHistoryServiceRpc;

    private final GroupMapper groupMapper;

    private final PaginationMapper paginationMapper;

    private final SupplyChainFetcherFactory supplyChainFetcherFactory;

    private final GroupUseCaseParser groupUseCaseParser;

    private final UuidMapper uuidMapper;

    private final long realtimeContextId;

    private final TagsService tagsService;

    private final RepositoryClient repositoryClient;

    private BusinessUnitMapper businessUnitMapper;

    private final UserSessionContext userSessionContext;

    private final GroupServiceBlockingStub groupServiceRpc;

    private final ServiceEntityMapper serviceEntityMapper;

    SearchService(@Nonnull final RepositoryApi repositoryApi,
                  @Nonnull final MarketsService marketsService,
                  @Nonnull final GroupsService groupsService,
                  @Nonnull final TargetsService targetsService,
                  @Nonnull final SearchServiceBlockingStub searchServiceRpc,
                  @Nonnull final EntitySeverityServiceBlockingStub entitySeverityRpc,
                  @Nonnull final SeverityPopulator severityPopulator,
                  @Nonnull final StatsHistoryServiceBlockingStub statsHistoryServiceRpc,
                  @Nonnull GroupExpander groupExpander,
                  @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                  @Nonnull final GroupMapper groupMapper,
                  @Nonnull final PaginationMapper paginationMapper,
                  @Nonnull final GroupUseCaseParser groupUseCaseParser,
                  @Nonnull UuidMapper uuidMapper,
                  @Nonnull TagsService tagsService,
                  @Nonnull RepositoryClient repositoryClient,
                  @Nonnull BusinessUnitMapper businessUnitMapper,
                  final long realtimeTopologyContextId,
                  @Nonnull final UserSessionContext userSessionContext,
                  @Nonnull final GroupServiceBlockingStub groupServiceRpc,
                  @Nonnull final ServiceEntityMapper serviceEntityMapper) {
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.marketsService = Objects.requireNonNull(marketsService);
        this.groupsService = Objects.requireNonNull(groupsService);
        this.targetsService = Objects.requireNonNull(targetsService);
        this.searchServiceRpc = Objects.requireNonNull(searchServiceRpc);
        this.entitySeverityRpc = Objects.requireNonNull(entitySeverityRpc);
        this.severityPopulator = Objects.requireNonNull(severityPopulator);
        this.statsHistoryServiceRpc = Objects.requireNonNull(statsHistoryServiceRpc);
        this.groupExpander = Objects.requireNonNull(groupExpander);
        this.groupMapper = Objects.requireNonNull(groupMapper);
        this.paginationMapper = Objects.requireNonNull(paginationMapper);
        this.groupUseCaseParser = groupUseCaseParser;
        this.supplyChainFetcherFactory = supplyChainFetcherFactory;
        this.uuidMapper = uuidMapper;
        this.tagsService = tagsService;
        this.repositoryClient = repositoryClient;
        this.businessUnitMapper = businessUnitMapper;
        this.realtimeContextId = realtimeTopologyContextId;
        this.userSessionContext = userSessionContext;
        this.groupServiceRpc = Objects.requireNonNull(groupServiceRpc);
        this.serviceEntityMapper = Objects.requireNonNull(serviceEntityMapper);
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
        // The input is the uuid for a single entity.
        return repositoryApi.entityRequest(Long.valueOf(uuidString))
            .getSE()
            .orElseThrow(() -> new UnknownObjectException("No entity found: " + uuidString));
    }

    private Stream<ServiceEntityApiDTO> queryByTypeAndStateAndName(@Nullable String nameRegex,
                                                                   @Nullable List<String> types,
                                                                   @Nullable String state) {
        // TODO Now, we only support one type of entities in the search
        if (types == null || types.isEmpty()) {
            IllegalArgumentException e = new IllegalArgumentException("Type must be set for search result.");
            logger.error(e);
            throw e;
        }

        final SearchParameters.Builder searchParamsBuilder =
            SearchProtoUtil.makeSearchParameters(SearchProtoUtil.entityTypeFilter(types));

        if (!StringUtils.isEmpty(nameRegex)) {
            searchParamsBuilder.addSearchFilter(SearchProtoUtil.searchFilterProperty(
                SearchProtoUtil.nameFilterRegex(nameRegex)));
        }

        if (!StringUtils.isEmpty(state)) {
            searchParamsBuilder.addSearchFilter(SearchProtoUtil.searchFilterProperty(
                SearchProtoUtil.stateFilter(state)));
        }

        return repositoryApi.newSearchRequest(searchParamsBuilder.build())
            .getSEList()
            .stream();
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
                                                     EnvironmentType environmentType,
                                                     // Ignored for now.
                                                     @Nullable EntityDetailType entityDetailType,
                                                     SearchPaginationRequest paginationRequest,
                                                     List<String> entityTypes,
                                                     List<String> probeTypes,
                                                     boolean isRegex)
            throws Exception {
        // temporally hack to accommodate signature changes.
        final String groupType = (groupTypes != null && groupTypes.size() > 0) ? groupTypes.get(0) : null;
        // TODO: escape any special chars in query string if isregex is false.

        // Determine which of many (many) types of searches is requested.
        // NB: this method is heavily overloaded.  The REST endpoint to be redefined
        if (types == null && scopes == null && state == null && groupType == null) {
            return paginationRequest.allResultsResponse(searchAll());
        } else if (StringUtils.isNotEmpty(groupType)) {
            // if 'groupType' is specified, this MUST be a search over GROUPs

            final List<GroupApiDTO> groups = groupsService.getGroups();
            return paginationRequest.allResultsResponse(groups.stream()
                .filter(g -> groupType.equals(g.getGroupType()))
                .collect(Collectors.toList()));
        } else if (types != null) {
            // Check for a type that requires a query to a specific service, vs. Repository search.
            if (types.contains(GROUP)) {
                // IN Classic, this returns all Groups + Clusters. So we call getGroups which gets
                // all Groups(supertype).
                final Collection<GroupApiDTO> groups = groupsService.getGroups();
                return paginationRequest.allResultsResponse(Lists.newArrayList(groups));
            } else if (types.contains(CLUSTER)) {
                final Collection<GroupApiDTO> groups =
                    groupsService.getClusters(ClusterInfo.Type.COMPUTE, scopes, Collections.emptyList());
                return paginationRequest.allResultsResponse(Lists.newArrayList(groups));
            } else if (types.contains(STORAGE_CLUSTER)) {
                final Collection<GroupApiDTO> groups =
                    groupsService.getClusters(ClusterInfo.Type.STORAGE, scopes, Collections.emptyList());
                return paginationRequest.allResultsResponse(Lists.newArrayList(groups));
            } else if (types.contains(VIRTUAL_MACHINE_CLUSTER)) {
                final Collection<GroupApiDTO> groups =
                    groupsService.getClusters(ClusterInfo.Type.COMPUTE_VIRTUAL_MACHINE, scopes, Collections.emptyList());
                return paginationRequest.allResultsResponse(Lists.newArrayList(groups));
            } else if (types.contains(MarketMapper.MARKET)) {
                final Collection<MarketApiDTO> markets = marketsService.getMarkets(scopes);
                return paginationRequest.allResultsResponse(Lists.newArrayList(markets));
            } else if (types.contains(TargetsService.TARGET)) {
                final Collection<TargetApiDTO> targets = targetsService.getTargets(null);
                return paginationRequest.allResultsResponse(Lists.newArrayList(targets));
            } else if (types.contains(UIEntityType.BUSINESS_ACCOUNT.apiStr())) {
                final Collection<BusinessUnitApiDTO> businessAccounts =
                        businessUnitMapper.getAndConvertDiscoveredBusinessUnits(targetsService);
                return paginationRequest.allResultsResponse(Lists.newArrayList(businessAccounts));
            } else if (types.contains(StringConstants.BILLING_FAMILY)) {
                return paginationRequest.allResultsResponse(fetchBillingFamilyApiDTOs());
            }
        }

        // Must be a search for a ServiceEntity
        final Stream<ServiceEntityApiDTO> entitiesResult;
        // Patrick: I am scoping this clause of the request on the results side rather than on the
        // source side (in the search service), because this search is still using REST, and
        // converting this method to GRPC looks like it will take more time than I have right now.
        // TODO: Convert the entity search to use GRPC isntead of REST.
        Predicate<ServiceEntityApiDTO> scopeFilter = userSessionContext.isUserScoped()
                ? entity -> userSessionContext.getUserAccessScope().contains(Long.valueOf(entity.getUuid()))
                : entity -> true; // no-op if the user is not scoped
        if (scopes == null || scopes.size() <= 0 ||
                (scopes.get(0).equals(UuidMapper.UI_REAL_TIME_MARKET_STR))) {
            // Search with no scope requested; or a single scope == "Market"; then search in live Market
            entitiesResult = queryByTypeAndStateAndName(query, types, state)
                .filter(scopeFilter);
        } else {
            // expand to include the supplychain for the 'scopes', some of which may be groups or
            // clusters, and derive a list of ServiceEntities
            Set<String> scopeServiceEntityIds = groupsService.expandUuids(Sets.newHashSet(scopes),
                types, environmentType).stream().map(String::valueOf).collect(Collectors.toSet());

            // TODO (roman, June 17 2019: We should probably include the IDs in the query, unless
            // there are a lot of IDs.

            // Fetch service entities matching the given specs
            // Restrict entities to those whose IDs are in the expanded 'scopeEntities'
           entitiesResult = queryByTypeAndStateAndName(query, types, state)
                .filter(scopeFilter)
                .filter(se -> scopeServiceEntityIds.contains(se.getUuid()));
        }

        // set discoveredBy and filter by probe types
        List<BaseApiDTO> result = entitiesResult
            .filter(se -> CollectionUtils.isEmpty(probeTypes) ||
                probeTypes.contains(se.getDiscoveredBy().getType()))
            .collect(Collectors.toList());
        return paginationRequest.allResultsResponse(result);
    }

    /**
     * Find master business accounts and convert them to BillingFamilyApiDTO.
     *
     * @return list of BillingFamilyApiDTOs
     */
    private List<BaseApiDTO> fetchBillingFamilyApiDTOs() throws Exception {
        final List<BusinessUnitApiDTO> businessAccounts =
            businessUnitMapper.getAndConvertDiscoveredBusinessUnits(targetsService);
        final Map<String, String> accountIdToDisplayName = businessAccounts.stream()
            .collect(Collectors.toMap(BusinessUnitApiDTO::getUuid, BusinessUnitApiDTO::getDisplayName));
        return businessAccounts.stream()
            .filter(BusinessUnitApiDTO::isMaster)
            .map(masterAccount -> businessUnitMapper.businessUnitToBillingFamily(masterAccount,
                accountIdToDisplayName))
            .collect(Collectors.toList());
    }

    private static ExecutorService executor = Executors.newFixedThreadPool(3);

    private List<BaseApiDTO> searchAll() throws Exception {
        Future<Collection<ServiceEntityApiDTO>> entities = executor.submit(
            () -> repositoryApi.newSearchRequest(SearchProtoUtil.makeSearchParameters(
                SearchProtoUtil.entityTypeFilter(SearchProtoUtil.SEARCH_ALL_TYPES)).build())
                    .getSEList());
        Future<List<GroupApiDTO>> groups = executor.submit(groupsService::getGroups);
        Future<List<TargetApiDTO>> targets = executor.submit(() -> targetsService.getTargets(null));
        List<BaseApiDTO> result = Lists.newArrayList();
        result.addAll(entities.get());
        result.addAll(groups.get());
        result.addAll(targets.get());
        return result;
    }


    /**
     * A general search given a filter - may be asked to search over ServiceEntities or Groups.
     *
     * @param query The query to run against the display name of the results.
     * @param inputDTO the specification of what to search
     * @return a list of DTOs based on the type of the search: ServiceEntityApiDTO or GroupApiDTO
     */
    @Override
    public SearchPaginationResponse getMembersBasedOnFilter(String query,
                                                            GroupApiDTO inputDTO,
                                                            SearchPaginationRequest paginationRequest)
        throws OperationFailedException, InvalidOperationException {
        // the query input is called a GroupApiDTO even though this search can apply to any type
        // if this is a group search, we need to know the right "name filter type" that can be used
        // to search for a group by name. These come from the groupBuilderUsecases.json file.
        switch (StringUtils.defaultIfEmpty(inputDTO.getClassName(),"")) {
            case GROUP:
                return groupsService.getPaginatedGroupApiDTOS(
                        addNameMatcher(query, inputDTO.getCriteriaList(), GroupMapper.GROUPS_FILTER_TYPE),
                        paginationRequest);
            case CLUSTER:
                // this is a search for a compute Cluster of physical machines
                return paginationRequest.allResultsResponse(
                        Collections.unmodifiableList(
                            groupsService.getClusters(ClusterInfo.Type.COMPUTE,
                                inputDTO.getScope(),
                                addNameMatcher(query,
                                        inputDTO.getCriteriaList(),
                                        GroupMapper.CLUSTERS_FILTER_TYPE))));
            case STORAGE_CLUSTER:
                // this is a search for a storage Cluster
                return paginationRequest.allResultsResponse(
                        Collections.unmodifiableList(
                            groupsService.getClusters(Type.STORAGE,
                                inputDTO.getScope(),
                                addNameMatcher(query,
                                        inputDTO.getCriteriaList(),
                                        GroupMapper.STORAGE_CLUSTERS_FILTER_TYPE))));
            case VIRTUAL_MACHINE_CLUSTER:
                // this is a search for a compute cluster of virtual machines
                return paginationRequest.allResultsResponse(
                        Collections.unmodifiableList(
                                groupsService.getClusters(ClusterInfo.Type.COMPUTE_VIRTUAL_MACHINE,
                                        inputDTO.getScope(),
                                        addNameMatcher(query,
                                                inputDTO.getCriteriaList(),
                                                GroupMapper.VIRTUALMACHINE_CLUSTERS_FILTER_TYPE))));
            default:
                // this isn't a group search after all -- use a generic search method instead.
                return searchEntitiesByParameters(inputDTO, query, paginationRequest);
        }
    }

    /**
     * This utility method will add a group/cluster/storage cluster display name filter based on the
     * input "string to match", if the "string to match" isn't empty. Otherwise, it does nothing.
     *
     * @param stringToMatch the potential string to match. Can be blank or null.
     * @param originalFilters the existing filters
     * @param filterTypeToUse the filter type to use when optionally creating the filter.
     * @return the list of filters + an additional one for the "string to match"
     */
    @VisibleForTesting
    protected List<FilterApiDTO> addNameMatcher(String stringToMatch,
                                              List<FilterApiDTO> originalFilters,
                                              String filterTypeToUse) {
        if (StringUtils.isEmpty(stringToMatch)) {
            return originalFilters;
        }
        // create a name filter for the 'query' and add it to the filters list.
        FilterApiDTO nameFilter = new FilterApiDTO();
        nameFilter.setExpVal(".*" + stringToMatch + ".*"); // turn it into a wildcard regex
        nameFilter.setExpType(GroupMapper.REGEX_MATCH);
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
     * @param nameQuery user specified search query for entity name.
     * @return A list of {@link BaseApiDTO} will be sent back to client
     */
    private SearchPaginationResponse searchEntitiesByParameters(@Nonnull GroupApiDTO inputDTO,
                                                                @Nullable String nameQuery,
                                                                @Nonnull SearchPaginationRequest paginationRequest)
                throws OperationFailedException {
        final List<SearchParameters> searchParameters =
            groupMapper.convertToSearchParameters(inputDTO, inputDTO.getClassName(), nameQuery)
                .stream()
                // Convert any cluster membership filters to property filters.
                .map(this::resolveClusterFilters)
                .collect(Collectors.toList());

        // match only the entity uuids which are part of the group or cluster
        // defined in the scope
        final Set<String> scopeList = Optional.ofNullable(inputDTO.getScope())
                .map(ImmutableSet::copyOf)
                .orElse(ImmutableSet.of());
        final boolean isGlobalScope = containsGlobalScope(scopeList);
        final List<String> entityTypes = inputDTO.getClassName() == null
            ? Collections.emptyList() : Collections.singletonList(inputDTO.getClassName());
        final Set<Long> expandedIds = groupsService.expandUuids(scopeList, entityTypes, null);
        if (!isGlobalScope && expandedIds.isEmpty()) {
            // return empty response since there is no related entities in given scope
            return paginationRequest.allResultsResponse(Collections.emptyList());
        }
        // use empty set for global scope so it fetches all
        final Set<Long> allEntityOids = isGlobalScope ? Collections.emptySet() : expandedIds;
        if (paginationRequest.getOrderBy().equals(SearchOrderBy.SEVERITY)) {
            final SearchEntityOidsRequest searchOidsRequest = SearchEntityOidsRequest.newBuilder()
                    .addAllSearchParameters(searchParameters)
                    .addAllEntityOid(allEntityOids)
                    .build();
            return getServiceEntityPaginatedWithSeverity(inputDTO, nameQuery, paginationRequest,
                    expandedIds, searchOidsRequest);
        } else if (paginationRequest.getOrderBy().equals(SearchOrderBy.UTILIZATION)) {
            final SearchEntityOidsRequest searchOidsRequest = SearchEntityOidsRequest.newBuilder()
                    .addAllSearchParameters(searchParameters)
                    .addAllEntityOid(allEntityOids)
                    .build();
            return getServiceEntityPaginatedWithUtilization(inputDTO, nameQuery, paginationRequest,
                    expandedIds, searchOidsRequest, isGlobalScope);
        } else {
            // We don't use the RepositoryAPI utility because we do pagination,
            // and want to handle the pagination parameters.
            final SearchEntitiesRequest searchEntitiesRequest = SearchEntitiesRequest.newBuilder()
                .addAllSearchParameters(searchParameters)
                .addAllEntityOid(allEntityOids)
                .setReturnType(PartialEntity.Type.API)
                .setPaginationParams(paginationMapper.toProtoParams(paginationRequest))
                .build();
            final SearchEntitiesResponse response = searchServiceRpc.searchEntities(searchEntitiesRequest);
            List<ServiceEntityApiDTO> entities = response.getEntitiesList().stream()
                .map(PartialEntity::getApi)
                .map(serviceEntityMapper::toServiceEntityApiDTO)
                .collect(Collectors.toList());
            severityPopulator.populate(realtimeContextId, entities);
            return buildPaginationResponse(entities,
                    response.getPaginationResponse(), paginationRequest);
        }
    }

    /**
     * Implement search pagination with order by severity.  The query workflow will be: 1: The
     * query will first go to repository (if need) to get all candidates oids. 2: All candidates
     * oids will passed to Action Orchestrator and perform pagination based on entity severity,
     * and only return Top X candidates. 3: Query repository to get entity information only for
     * top X entities.
     * <p>
     * Because action severity data is already stored at Action Orchestrator, and it can be changed
     * when some action is executed. In this way, we can avoid store duplicate severity data and inconsistent
     * update operation.
     *
     * @param inputDTO a Description of what search to conduct.
     * @param nameQuery user specified search query for entity name.
     * @param paginationRequest {@link SearchPaginationRequest}
     * @param expandedIds a list of entity oids after expanded.
     * @param searchEntityOidsRequest {@link Search.SearchEntityOidsRequest}.
     * @return {@link SearchPaginationResponse}.
     */
    private SearchPaginationResponse getServiceEntityPaginatedWithSeverity(
            @Nonnull final GroupApiDTO inputDTO,
            @Nullable final String nameQuery,
            @Nonnull final SearchPaginationRequest paginationRequest,
            @Nonnull final Set<Long> expandedIds,
            @Nonnull final Search.SearchEntityOidsRequest searchEntityOidsRequest) {
        final Set<Long> candidates = getCandidateEntitiesForSearch(inputDTO, nameQuery, expandedIds,
                searchEntityOidsRequest);
        /*
         The search query with order by severity workflow will be: 1: The query will first go to
         repository (if need) to get all candidates oids. 2: All candidates oids will passed to Action
         Orchestrator and perform pagination based on entity severity, and only return Top X candidates.
         3: Query repository to get entity information only for top X entities.
         */
        MultiEntityRequest multiEntityRequest =
                MultiEntityRequest.newBuilder()
                        .addAllEntityIds(candidates)
                        .setTopologyContextId(realtimeContextId)
                        .setPaginationParams(paginationMapper.toProtoParams(paginationRequest))
                        .build();
        EntitySeveritiesResponse entitySeveritiesResponse =
                entitySeverityRpc.getEntitySeverities(multiEntityRequest);
        final Map<Long, String> paginatedEntitySeverities =
                entitySeveritiesResponse.getEntitySeverityList().stream()
                        .collect(Collectors.toMap(EntitySeverity::getEntityId,
                                entitySeverity -> ActionDTOUtil.getSeverityName(entitySeverity.getSeverity())));
        final Map<Long, ServiceEntityApiDTO> serviceEntityMap =
            repositoryApi.entitiesRequest(paginatedEntitySeverities.keySet())
                .getSEMap();
        // Should use entitySeveritiesResponse.getEntitySeverityList() here, because it has already contains
        // paginated entities with correct order.
        final List<ServiceEntityApiDTO> entities =
                entitySeveritiesResponse.getEntitySeverityList().stream()
                        .map(EntitySeverity::getEntityId)
                        .filter(serviceEntityMap::containsKey)
                        .map(serviceEntityMap::get)
                        .collect(Collectors.toList());
        return buildPaginationResponse(entities,
                entitySeveritiesResponse.getPaginationResponse(), paginationRequest);
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
     * @return
     */
    private SearchPaginationResponse getServiceEntityPaginatedWithUtilization(
            @Nonnull final GroupApiDTO inputDTO,
            @Nullable final String nameQuery,
            @Nonnull final SearchPaginationRequest paginationRequest,
            @Nonnull final Set<Long> expandedIds,
            @Nonnull final Search.SearchEntityOidsRequest searchRequest,
            @Nonnull final boolean isGlobalScope) {
        // if it is global scope and there is no other search criteria, it means all service entities
        // of same entity type are search candidates.
        final boolean isGlobalEntities =
                isGlobalScope && inputDTO.getCriteriaList().isEmpty() && StringUtils.isEmpty(nameQuery);
        // the search query with order by utilizaiton workflow will be:
        // 1: (if necessary) query repository componnet to get all candidate entity oids.
        // 2: query history component to get top X entity oids from the candidates sorted by price index.
        // 3: query repository to get full entity information only for top X entity oids.
        final EntityStatsScope.Builder statsScope = EntityStatsScope.newBuilder();
        if (!isGlobalEntities) {
            statsScope.setEntityList(EntityList.newBuilder()
                .addAllEntities(getCandidateEntitiesForSearch(inputDTO, nameQuery, expandedIds, searchRequest)));
        } else {
            statsScope.setEntityType(UIEntityType.fromString(inputDTO.getClassName()).typeNumber());
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
        final Map<Long, ServiceEntityApiDTO> serviceEntityMap =
            repositoryApi.entitiesRequest(Sets.newHashSet(nextPageIds))
                .getSEMap();
        // It is important to keep the order of entityIdsList, because they have already sorted by
        // utilization.
        final List<ServiceEntityApiDTO> entities = nextPageIds.stream()
                .filter(serviceEntityMap::containsKey)
                .map(serviceEntityMap::get)
                .collect(Collectors.toList());
        return buildPaginationResponse(entities, response.getPaginationResponse(), paginationRequest);
    }

    private Set<Long> getCandidateEntitiesForSearch(
            @Nonnull final GroupApiDTO inputDTO,
            @Nullable final String nameQuery,
            @Nonnull final Set<Long> expandedIds,
            @Nonnull final Search.SearchEntityOidsRequest searchRequest) {
        final Set<Long> candidates;
        // if query request doesn't contains any filter criteria, it can directly use expanded ids
        // as results. Otherwise, it needs to query repository to get matched entity oids.
        if (inputDTO.getCriteriaList().isEmpty() && StringUtils.isEmpty(nameQuery) && !expandedIds.isEmpty()) {
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
                .map(nexCursor -> paginationRequest.nextPageResponse((List<BaseApiDTO>) results, nexCursor))
                .orElseGet(() -> paginationRequest.finalPageResponse((List<BaseApiDTO>) results));
    }

    /**
     * Check if input scopes contains global scope or not.
     *
     * @param scopes a set of scopes ids.
     * @return true if input parameter contains global scope.
     */
    private boolean containsGlobalScope(@Nonnull final Set<String> scopes) {
        if (scopes.isEmpty()) {
            // if there is no specified scopes, it means it is a global scope.
            return true;
        }
        // if the scope list size is larger than default scope size, it will log a warn message.
        final int DEFAULT_MAX_SCOPE_SIZE = 50;
        if (scopes.size() >= DEFAULT_MAX_SCOPE_SIZE) {
            logger.warn("Search scope list size is too large: {}" + scopes.size());
        }
        return scopes.stream()
                .anyMatch(scope -> {
                    final boolean isMarket = UuidMapper.isRealtimeMarket(scope);
                    final Optional<Group> group = groupExpander.getGroup(scope);
                    return isMarket || (group.isPresent() && group.get().hasTempGroup()
                            && group.get().getTempGroup().getIsGlobalScopeGroup());
                });
    }

    /**
     * Provided an input SearchParameters object, resolve any cluster membership filters contained
     * inside and return a new SearchParameters object with the resolved filters. If there are no
     * cluster membership filters inside, return the original object.
     *
     * @param searchParameters A SearchParameters object that may contain cluster filters.
     * @return A SearchParameters object that has had any cluster filters in it resolved. Will be the
     * original object if there were no group filters inside.
     */
    @VisibleForTesting
    SearchParameters resolveClusterFilters(SearchParameters searchParameters) {
        // return the original object if no cluster member filters inside
        if (!searchParameters.getSearchFilterList().stream()
                .anyMatch(SearchFilter::hasClusterMembershipFilter)) {
            return searchParameters;
        }
        // We have one or more Cluster Member Filters to resolve. Rebuild the SearchParameters.
        SearchParameters.Builder searchParamBuilder = SearchParameters.newBuilder(searchParameters);
        // we will rebuild the search filters, resolving any cluster member filters we encounter.
        searchParamBuilder.clearSearchFilter();
        for (SearchFilter sf : searchParameters.getSearchFilterList()) {
            searchParamBuilder.addSearchFilter(convertClusterMemberFilter(sf));
        }

        return searchParamBuilder.build();
    }

    /**
     * Convert a cluster member filter to a static property filter. If the input filter does not
     * contain a cluster member filter, the input filter will be returned, unchanged.
     *
     * @param inputFilter The ClusterMemberFilter to convert.
     * @return A new SearchFilter with any ClusterMemberFilters converted to property filters. If
     * there weren't any ClusterMemberFilters to convert, the original filter is returned.
     */
    private SearchFilter convertClusterMemberFilter(SearchFilter inputFilter) {
        if (!inputFilter.hasClusterMembershipFilter()) {
            return inputFilter;
        }

        // find matching groups using the group service
        final Collection<String> oids =
            groupExpander.getGroupsWithMembers(
                    GetGroupsRequest.newBuilder()
                        .setPropertyFilters(
                            GroupPropertyFilterList.newBuilder()
                                .addPropertyFilters(
                                    inputFilter.getClusterMembershipFilter().getClusterSpecifier())
                                .build())
                        .addTypeFilter(Group.Type.CLUSTER)
                        .build())
                .flatMap(groupAndMembers -> groupAndMembers.members().stream())
                .distinct()
                .map(id -> Long.toString(id))
                .collect(Collectors.toList());
        return
            SearchFilter.newBuilder()
                .setPropertyFilter(
                    SearchProtoUtil.stringPropertyFilterExact(SearchableProperties.OID, oids))
                .build();
    }

    @Override
    public Map<String, Object> getGroupBuilderUsecases() {
        return groupUseCaseParser.getUseCases().entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    @Override
    public List<CriteriaOptionApiDTO> getCriteriaOptions(final String criteriaKey,
                                                         final List<String> scopes,
                                                         final String entityType,
                                                         final EnvironmentType envType) throws Exception {
        final List<CriteriaOptionApiDTO> optionApiDTOs = new ArrayList<>();

        switch (criteriaKey) {
            case STATE:
                // options should be all possible states
                Arrays.stream(UIEntityState.values())
                    .forEach(option -> {
                        final CriteriaOptionApiDTO optionApiDTO = new CriteriaOptionApiDTO();
                        optionApiDTO.setValue(option.apiStr());
                        optionApiDTOs.add(optionApiDTO);
                    });
                break;

            case StringConstants.TAGS_ATTR:
                if (GroupMapper.GROUP_CLASSES.contains(entityType)) {
                    // retrieve tags from the group service
                    final Map<String, TagValuesDTO> tags =
                            groupServiceRpc.getTags(
                                    GetTagsRequest.newBuilder().build())
                                .getTags().getTagsMap();

                    // map to desired output
                    tags.entrySet().forEach(tagEntry -> {
                        final CriteriaOptionApiDTO criteriaOptionApiDTO = new CriteriaOptionApiDTO();
                        criteriaOptionApiDTO.setValue(tagEntry.getKey());
                        criteriaOptionApiDTO.setSubValues(tagEntry.getValue().getValuesList());
                        optionApiDTOs.add(criteriaOptionApiDTO);
                     });
                } else {
                    // retrieve relevant tags from the tags service
                    final List<TagApiDTO> tags = tagsService.getTags(scopes, entityType, envType);

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
                break;

            case ACCOUNT_OID:
                // get all business accounts
                repositoryApi.newSearchRequest(SearchProtoUtil.makeSearchParameters(
                            SearchProtoUtil.entityTypeFilter(UIEntityType.BUSINESS_ACCOUNT.apiStr()))
                        .build())
                    .getMinimalEntities()
                    .forEach(ba -> {
                        // convert each business account to criteria option
                        CriteriaOptionApiDTO crOpt = new CriteriaOptionApiDTO();
                        crOpt.setDisplayName(ba.getDisplayName());
                        crOpt.setValue(String.valueOf(ba.getOid()));
                        optionApiDTOs.add(crOpt);
                    });
                break;
            default:
                throw new IllegalArgumentException("Unknown criterion key: " + criteriaKey);
        }

        return optionApiDTOs;
    }
}
