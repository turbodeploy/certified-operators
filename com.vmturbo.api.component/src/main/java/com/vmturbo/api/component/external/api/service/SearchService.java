package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.mapper.GroupMapper.CLUSTER;
import static com.vmturbo.api.component.external.api.mapper.GroupMapper.STORAGE_CLUSTER;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.component.external.api.mapper.GroupUseCaseParser;
import com.vmturbo.api.component.external.api.mapper.MarketMapper;
import com.vmturbo.api.component.external.api.mapper.SearchMapper;
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
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.enums.SupplyChainDetailType;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.ISearchService;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.Entity;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;

/**
 * Service entry points to search the Repository.
 **/
public class SearchService implements ISearchService {

    private final RepositoryApi repositoryApi;

    private final MarketsService marketsService;

    private final GroupsService groupsService;
    private final GroupExpander groupExpander;

    private final TargetsService targetsService;

    private final SearchServiceBlockingStub searchServiceRpc;

    private final GroupMapper groupMapper;

    private final SupplyChainFetcherFactory supplyChainFetcherFactory;

    private final GroupUseCaseParser groupUseCaseParser;

    private final UuidMapper uuidMapper;

    private final String REPO_OID_KEY_NAME = "oid";

    SearchService(@Nonnull final RepositoryApi repositoryApi,
                  @Nonnull final MarketsService marketsService,
                  @Nonnull final GroupsService groupsService,
                  @Nonnull final TargetsService targetsService,
                  @Nonnull final SearchServiceBlockingStub searchServiceRpc,
                  @Nonnull GroupExpander groupExpander,
                  @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                  @Nonnull final GroupMapper groupMapper,
                  @Nonnull final GroupUseCaseParser groupUseCaseParser,
                  @Nonnull UuidMapper uuidMapper) {
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.marketsService = Objects.requireNonNull(marketsService);
        this.groupsService = Objects.requireNonNull(groupsService);
        this.targetsService = Objects.requireNonNull(targetsService);
        this.searchServiceRpc = Objects.requireNonNull(searchServiceRpc);
        this.groupExpander = Objects.requireNonNull(groupExpander);
        this.groupMapper = Objects.requireNonNull(groupMapper);
        this.groupUseCaseParser = groupUseCaseParser;
        this.supplyChainFetcherFactory = supplyChainFetcherFactory;
        this.uuidMapper = uuidMapper;
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
    public List<BaseApiDTO> getSearchResults(String query,
                                                   List<String> types,
                                                   List<String> scopes,
                                                   String state,
                                                   String groupType,
                                                   EnvironmentType environmentType) throws Exception {
        List<BaseApiDTO> result = null;

        // Determine which of many (many) types of searches is requested.
        // NB: this method is heavily overloaded.  The REST endpoint to be redefined
        if (types == null && scopes == null && state == null && groupType == null) {
            // "search all" - gather answer from all possible sources in parallel
            return searchAll();

        } else if (StringUtils.isNotEmpty(groupType)) {
            // if 'groupType' is specified, this MUST be a search over GROUPs

            final List<GroupApiDTO> groups = groupsService.getGroups();
            return groups.stream()
                .filter(g -> groupType.equals(g.getGroupType()))
                .collect(Collectors.toList());

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
                return result;
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
                    .supplyChainDetailType(SupplyChainDetailType.entity)
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

        return result;
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
     * @param inputDTO the specification of what to search
     * @return a list of DTOs based on the type of the search: ServiceEntityApiDTO or GroupApiDTO
     */
    @Override
    public List<BaseApiDTO> getMembersBasedOnFilter(GroupApiDTO inputDTO)  {

        // the query input is called a GroupApiDTO even though this search can apply to any type
        // what sort of search is this
        if (GroupMapper.GROUP.equals(inputDTO.getClassName())) {
            // this is a search for a group
            return Lists.newArrayList(groupsService.getGroupsByFilter(inputDTO.getCriteriaList()));
        } else if (CLUSTER.equals(inputDTO.getClassName())) {
            // this is a search for a Cluster
            return Lists.newArrayList(groupsService.getComputeClusters(inputDTO.getCriteriaList()));
        } else if (STORAGE_CLUSTER.equals(inputDTO.getClassName())) {
            // this is a search for a Cluster
            return Lists.newArrayList(groupsService.getStorageClusters(inputDTO.getCriteriaList()));
        } else {
            // this is a search for a ServiceEntity
            // Right now when UI send search requests, it use class name field to store entity type
            // when UI send group service requests, it use groupType fields to store entity type.
            return searchEntitiesByParameters(inputDTO);
        }
    }

    /**
     * Send a search request to search rpc service by passing a list of search
     * parameters based on the parameters in the inputDTO. Convert the response
     * from a list of {@link com.vmturbo.common.protobuf.search.Search.Entity}
     * objects to list of {@link BaseApiDTO}.
     *
     * @param inputDTO a Description of what search to conduct
     * @return A list of {@link BaseApiDTO} will be sent back to client
     */
    private List<BaseApiDTO> searchEntitiesByParameters(GroupApiDTO inputDTO) {
        List<SearchParameters> parameters =
            groupMapper.convertToSearchParameters(inputDTO, inputDTO.getClassName());
        final Search.SearchRequest.Builder searchRequestBuilder =
            Search.SearchRequest.newBuilder()
                .addAllSearchParameters(parameters);
        // match only the entity uuids which are part of the group or cluster
        // defined in the scope
        if ((inputDTO.getScope() != null) && (!inputDTO.getScope().isEmpty())) {
            String entityIdsToMatch =
                groupExpander.expandUuids(ImmutableSet.copyOf(inputDTO.getScope()))
                    .stream()
                    .map(uuid -> Long.toString(uuid))
                    // OR operation
                    .collect(Collectors.joining( "|" ) );
            if (!entityIdsToMatch.isEmpty()) {
                // This is clunky. We are setting a filter on an index in
                // repository(arangodb) which is not exposed anywhere.
                // The other option is to filter for the entities here on the api
                // client side. But doing the filter on the repository(server) side may
                // be more efficient.
                searchRequestBuilder.addSearchParameters(
                    SearchParameters.newBuilder()
                        .setStartingFilter(
                            SearchMapper.stringFilter(REPO_OID_KEY_NAME, entityIdsToMatch))
                        .build());
            }
        }
        Iterator<Entity> iterator = searchServiceRpc.searchEntities(searchRequestBuilder.build());
        List<BaseApiDTO> list = Lists.newLinkedList();
        while (iterator.hasNext()) {
            Entity entity = iterator.next();
            ServiceEntityApiDTO seDTO = SearchMapper.seDTO(entity);
            list.add(seDTO);
        }
        return list;
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
        // This is not implemented in XL yet, but if we throw an exception here search
        // does not work. Return an empty list instead - it's safe to do so, it's as if
        // no criteria options were found.
        return Collections.emptyList();
    }
}
