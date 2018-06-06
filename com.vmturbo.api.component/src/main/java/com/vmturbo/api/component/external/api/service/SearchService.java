package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.mapper.GroupMapper.CLUSTER;
import static com.vmturbo.api.component.external.api.mapper.GroupMapper.STORAGE_CLUSTER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.CollectionUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.ServiceEntitiesRequest;
import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.component.external.api.mapper.GroupUseCaseParser;
import com.vmturbo.api.component.external.api.mapper.MarketMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.SearchMapper;
import com.vmturbo.api.component.external.api.mapper.SeverityPopulator;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.market.MarketApiDTO;
import com.vmturbo.api.dto.search.CriteriaOptionApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.EntityDetailType;
import com.vmturbo.api.enums.EntityState;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.SearchOrderBy;
import com.vmturbo.api.pagination.SearchPaginationRequest;
import com.vmturbo.api.pagination.SearchPaginationRequest.SearchPaginationResponse;
import com.vmturbo.api.serviceinterfaces.ISearchService;
import com.vmturbo.common.protobuf.ActionDTOUtil;
import com.vmturbo.common.protobuf.PaginationProtoUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.MultiEntityRequest;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.NameFilter;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;

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

    private final GroupMapper groupMapper;

    private final SupplyChainFetcherFactory supplyChainFetcherFactory;

    private final GroupUseCaseParser groupUseCaseParser;

    private final UuidMapper uuidMapper;

    private final String REPO_OID_KEY_NAME = "oid";

    private final long realtimeContextId;

    SearchService(@Nonnull final RepositoryApi repositoryApi,
                  @Nonnull final MarketsService marketsService,
                  @Nonnull final GroupsService groupsService,
                  @Nonnull final TargetsService targetsService,
                  @Nonnull final SearchServiceBlockingStub searchServiceRpc,
                  @Nonnull final EntitySeverityServiceBlockingStub entitySeverityRpcService,
                  @Nonnull GroupExpander groupExpander,
                  @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                  @Nonnull final GroupMapper groupMapper,
                  @Nonnull final GroupUseCaseParser groupUseCaseParser,
                  @Nonnull UuidMapper uuidMapper,
                  final long realtimeTopologyContextId) {
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.marketsService = Objects.requireNonNull(marketsService);
        this.groupsService = Objects.requireNonNull(groupsService);
        this.targetsService = Objects.requireNonNull(targetsService);
        this.searchServiceRpc = Objects.requireNonNull(searchServiceRpc);
        this.entitySeverityRpc = Objects.requireNonNull(entitySeverityRpcService);
        this.groupExpander = Objects.requireNonNull(groupExpander);
        this.groupMapper = Objects.requireNonNull(groupMapper);
        this.groupUseCaseParser = groupUseCaseParser;
        this.supplyChainFetcherFactory = supplyChainFetcherFactory;
        this.uuidMapper = uuidMapper;
        this.realtimeContextId = realtimeTopologyContextId;
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
        return repositoryApi.getServiceEntityForUuid(Long.valueOf(uuidString));
    }

    @Override
    public SearchPaginationResponse getSearchResults(String query,
                                                     List<String> types,
                                                     List<String> scopes,
                                                     String state,
                                                     String groupType,
                                                     EnvironmentType environmentType,
                                                     // Ignored for now.
                                                     @Nullable EntityDetailType entityDetailType,
                                                     SearchPaginationRequest paginationRequest)
            throws Exception {
        List<BaseApiDTO> result = null;
        // Determine which of many (many) types of searches is requested.
        // NB: this method is heavily overloaded.  The REST endpoint to be redefined
        if (types == null && scopes == null && state == null && groupType == null) {
            // "search all" - gather answer from all possible sources in parallel
            return paginationRequest.allResultsResponse(searchAll());
        } else if (StringUtils.isNotEmpty(groupType)) {
            // if 'groupType' is specified, this MUST be a search over GROUPs

            final List<GroupApiDTO> groups = groupsService.getGroups();
            return paginationRequest.allResultsResponse(groups.stream()
                .filter(g -> groupType.equals(g.getGroupType()))
                .collect(Collectors.toList()));
        } else if (types != null) {
            // Check for a type that requires a query to a specific service, vs. Repository search.
            if (types.contains(GroupMapper.GROUP)) {
                final Collection<GroupApiDTO> groups = groupsService.getGroups();
                result = Lists.newArrayList(groups);
            } else if (types.contains(CLUSTER)) {
                final Collection<GroupApiDTO> groups = groupsService.getComputeClusters(
                    Lists.newArrayList());
                result = Lists.newArrayList(groups);
            } else if (types.contains(GroupMapper.STORAGE_CLUSTER)) {
                final Collection<GroupApiDTO> groups = groupsService.getStorageClusters(
                    Lists.newArrayList());
                result = Lists.newArrayList(groups);
            } else if (types.contains(MarketMapper.MARKET)) {
                final Collection<MarketApiDTO> markets = marketsService.getMarkets(scopes);
                result = Lists.newArrayList(markets);
            } else if (types.contains(TargetsService.TARGET)) {
                final Collection<TargetApiDTO> targets = targetsService.getTargets(null);
                result = Lists.newArrayList(targets);
            }
            // if this one of the specific service queries, return the result.
            if (!CollectionUtils.isEmpty(result)) {
                return paginationRequest.allResultsResponse(result);
            }
        }

        // Must be a search for a ServiceEntity
        if (scopes == null || scopes.size() <= 0 ||
                (scopes.get(0).equals(UuidMapper.UI_REAL_TIME_MARKET_STR))) {
            // Search with no scope requested; or a single scope == "Market"; then search in live Market
            result = Lists.newArrayList(repositoryApi.getSearchResults(query, types,
                    UuidMapper.UI_REAL_TIME_MARKET_STR, state, groupType));
        } else {
            // Fetch service entities matching the given specs
            Collection<ServiceEntityApiDTO> serviceEntities = repositoryApi.getSearchResults(query,
                    types, UuidMapper.UI_REAL_TIME_MARKET_STR, state, groupType);

            // expand to include the supplychain for the 'scopes', some of which may be groups or
            // clusters, and derive a list of ServiceEntities
            Set<String> scopeServiceEntityIds = Sets.newHashSet(scopes);
            SupplychainApiDTO supplychain = supplyChainFetcherFactory.newApiDtoFetcher()
                    .topologyContextId(uuidMapper.fromUuid(UuidMapper.UI_REAL_TIME_MARKET_STR).oid())
                    .addSeedUuids(scopeServiceEntityIds)
                    .entityTypes(types)
                    .environmentType(environmentType)
                    .includeHealthSummary(false)
                    .entityDetailType(EntityDetailType.entity)
                    .fetch();
            supplychain.getSeMap().forEach((entityType, supplychainEntryDTO) -> {
                if (types != null && types.contains(entityType)) {
                    scopeServiceEntityIds.addAll(supplychainEntryDTO.getInstances().keySet());
                }
            });

            // Restrict entities to those whose IDs are in the expanded 'scopeEntities'
            result = serviceEntities.stream()
                    .filter(se -> scopeServiceEntityIds.contains(se.getUuid()))
                    .collect(Collectors.toList());
        }

        return paginationRequest.allResultsResponse(result);
    }

    private static ExecutorService executor = Executors.newFixedThreadPool(3);

    private List<BaseApiDTO> searchAll() throws Exception {
        Future<Collection<ServiceEntityApiDTO>> entities = executor.submit(
                () -> repositoryApi.getSearchResults(
                        "", SearchMapper.SEARCH_ALL_TYPES, MarketMapper.MARKET, null, null));
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
                                                            SearchPaginationRequest paginationRequest)  {

        // the query input is called a GroupApiDTO even though this search can apply to any type
        // what sort of search is this
        final List<? extends BaseApiDTO> result;
        if (GroupMapper.GROUP.equals(inputDTO.getClassName())) {
            // this is a search for a group
            result = groupsService.getGroupsByFilter(inputDTO.getCriteriaList());
        } else if (CLUSTER.equals(inputDTO.getClassName())) {
            // this is a search for a Cluster
            result = groupsService.getComputeClusters(inputDTO.getCriteriaList());
        } else if (STORAGE_CLUSTER.equals(inputDTO.getClassName())) {
            // this is a search for a Cluster
            result = groupsService.getStorageClusters(inputDTO.getCriteriaList());
        } else {
            return searchEntitiesByParameters(inputDTO, query, paginationRequest);
        }
        return paginationRequest.allResultsResponse(result.stream()
                .filter(dto -> !StringUtils.isEmpty(query)
                        ? StringUtils.containsIgnoreCase(dto.getDisplayName(), query)
                        : true)
                .collect(Collectors.toList()));
    }

    /**
     * Send a search request to search rpc service by passing a list of search
     * parameters based on the parameters in the inputDTO. Convert the response
     * from a list of {@link com.vmturbo.common.protobuf.search.Search.Entity}
     * objects to list of {@link BaseApiDTO}.
     *
     * @param inputDTO a Description of what search to conduct
     * @param nameQuery user specified search query for entity name.
     * @return A list of {@link BaseApiDTO} will be sent back to client
     */
    private SearchPaginationResponse searchEntitiesByParameters(@Nonnull GroupApiDTO inputDTO,
                                                                @Nullable String nameQuery,
                                                                @Nonnull SearchPaginationRequest paginationRequest) {
        List<SearchParameters> searchParameters =
            groupMapper.convertToSearchParameters(inputDTO, inputDTO.getClassName(), nameQuery);

        // Convert any ClusterMemberFilters to static member filters
        Search.SearchRequest.Builder searchRequestBuilder = Search.SearchRequest.newBuilder();
        for (SearchParameters params : searchParameters) {
            searchRequestBuilder.addSearchParameters(resolveClusterFilters(params));
        }
        // match only the entity uuids which are part of the group or cluster
        // defined in the scope
        final Set<String> scopeList = Optional.ofNullable(inputDTO.getScope())
                .map(ImmutableSet::copyOf)
                .orElse(ImmutableSet.of());
        final boolean isGlobalScope = containsGlobalScope(scopeList);
        final Set<Long> expandedIds = groupExpander.expandUuids(scopeList);
        if (!expandedIds.isEmpty() && !isGlobalScope) {
            searchRequestBuilder.addAllEntityOid(expandedIds);
        }
        if (paginationRequest.getOrderBy().equals(SearchOrderBy.SEVERITY)) {
            // if pagination request is order by severity, it needs to query action orchestrator
            // to paginate by severity.
            final Search.SearchRequest searchRequest = searchRequestBuilder.build();
            return getServiceEntityPaginatedWithSeverity(inputDTO, nameQuery, paginationRequest,
                    expandedIds, searchRequest);
        } else {
            // TODO: Implement search entities order by utilization and cost.
            searchRequestBuilder.setPaginationParams(PaginationMapper.toProtoParams(paginationRequest));
            final Search.SearchRequest searchRequest = searchRequestBuilder.build();
            final SearchEntitiesResponse response = searchServiceRpc.searchEntities(searchRequest);
            List<ServiceEntityApiDTO> entities = response.getEntitiesList().stream()
                    .map(SearchMapper::seDTO)
                    .collect(Collectors.toList());
            SeverityPopulator.populate(entitySeverityRpc, realtimeContextId, entities);
            final List<? extends BaseApiDTO> results = entities;
            return PaginationProtoUtil.getNextCursor(
                    response.getPaginationResponse())
                    .map(nexCursor -> paginationRequest.nextPageResponse((List<BaseApiDTO>) results, nexCursor))
                    .orElseGet(() -> paginationRequest.finalPageResponse((List<BaseApiDTO>) results));
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
     * @param searchRequest {@link Search.SearchRequest}.
     * @return {@link SearchPaginationResponse}.
     */
    private SearchPaginationResponse getServiceEntityPaginatedWithSeverity(
            @Nonnull final GroupApiDTO inputDTO,
            @Nullable final String nameQuery,
            @Nonnull final SearchPaginationRequest paginationRequest,
            @Nonnull final Set<Long> expandedIds,
            @Nonnull final Search.SearchRequest searchRequest) {
        final Set<Long> candidates;
        // if query request doesn't contains any filter criteria, it can directly use expanded ids
        // as results. Otherwise, it needs to query repository to get matched entity oids.
        if (inputDTO.getCriteriaList().isEmpty() && StringUtils.isEmpty(nameQuery) && !expandedIds.isEmpty()) {
            candidates = expandedIds;
        } else {
            candidates = searchServiceRpc.searchEntityOids(searchRequest).getEntitiesList().stream()
                    .collect(Collectors.toSet());
        }
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
                        .setPaginationParams(PaginationMapper.toProtoParams(paginationRequest))
                        .build();
        EntitySeveritiesResponse entitySeveritiesResponse =
                entitySeverityRpc.getEntitySeverities(multiEntityRequest);
        final Map<Long, String> paginatedEntitySeverities =
                entitySeveritiesResponse.getEntitySeverityList().stream()
                        .collect(Collectors.toMap(EntitySeverity::getEntityId,
                                entitySeverity -> ActionDTOUtil.getSeverityName(entitySeverity.getSeverity())));
        final Map<Long, ServiceEntityApiDTO> serviceEntityMap =
                repositoryApi.getServiceEntitiesById(
                        ServiceEntitiesRequest.newBuilder(paginatedEntitySeverities.keySet()).build())
                        .entrySet().stream()
                        .filter(entry -> entry.getValue().isPresent())
                        .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().get()));
        // Should use entitySeveritiesResponse.getEntitySeverityList() here, because it has already contains
        // paginated entities with correct order.
        final List<ServiceEntityApiDTO> entities =
                entitySeveritiesResponse.getEntitySeverityList().stream()
                        .map(EntitySeverity::getEntityId)
                        .filter(serviceEntityMap::containsKey)
                        .map(serviceEntityMap::get)
                        .collect(Collectors.toList());
        entities.forEach(entity -> entity.setSeverity(paginatedEntitySeverities.get(Long.valueOf(entity.getUuid()))));

        final List<? extends BaseApiDTO> results = entities;
        return PaginationProtoUtil.getNextCursor(
                entitySeveritiesResponse.getPaginationResponse())
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
        // this has a cluster membership filter -- resolve plz
        StringFilter clusterSpecifierFilter = inputFilter.getClusterMembershipFilter()
                .getClusterSpecifier()
                .getStringFilter();
        logger.debug("Resolving ClusterMemberFilter {}", clusterSpecifierFilter.getStringPropertyRegex());
        // find matching groups and members using the group service
        List<GroupApiDTO> groups = groupsService.getGroupApiDTOS(GetGroupsRequest.newBuilder()
                .setNameFilter(NameFilter.newBuilder()
                        .setNameRegex(clusterSpecifierFilter.getStringPropertyRegex())
                        .setNegateMatch(!clusterSpecifierFilter.getMatch()))
                .setTypeFilter(Type.CLUSTER)
                .build());

        // build the replacement filter - a regex against /^oid1$|^oid2$|.../
        StringJoiner sj = new StringJoiner("$|^","^","$");
        groups.stream()
                .map(GroupApiDTO::getMemberUuidList)
                .flatMap(List::stream)
                .distinct()
                .forEach(sj::add);
        return SearchFilter.newBuilder()
                .setPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName("oid")
                        .setStringFilter(StringFilter.newBuilder()
                                .setStringPropertyRegex(sj.toString())))
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
        Arrays.stream(EntityState.values())
                        .forEach(option -> {
                            final CriteriaOptionApiDTO optionApiDTO = new CriteriaOptionApiDTO();
                            optionApiDTO.setValue(option.name());
                            optionApiDTOs.add(optionApiDTO);
                        });
        return optionApiDTOs;
    }
}
