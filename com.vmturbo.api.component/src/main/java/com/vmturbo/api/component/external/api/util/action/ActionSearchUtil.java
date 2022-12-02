package com.vmturbo.api.component.external.api.util.action;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections4.CollectionUtils;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.ServiceProviderExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.enums.ActionDetailLevel;
import com.vmturbo.api.enums.ActionType;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.pagination.ActionPaginationRequest;
import com.vmturbo.api.pagination.ActionPaginationRequest.ActionPaginationResponse;
import com.vmturbo.common.protobuf.PaginationProtoUtil;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest.ActionQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse.TypeCase;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;

/**
 * Common functionality that has to do with action searching.
 */
public class ActionSearchUtil {

    private final ActionsServiceBlockingStub actionOrchestratorRpc;
    private final ActionSpecMapper actionSpecMapper;
    private final PaginationMapper paginationMapper;
    private final SupplyChainFetcherFactory supplyChainFetcherFactory;
    private final long realtimeTopologyContextId;
    private final GroupExpander groupExpander;
    private final ServiceProviderExpander serviceProviderExpander;

    /**
     * Flag that enables all action uuids come from the stable recommendation oid instead of the
     * unstable action instance id.
     */
    private final boolean useStableActionIdAsUuid;

    public ActionSearchUtil(
            @Nonnull ActionsServiceBlockingStub actionOrchestratorRpc,
            @Nonnull ActionSpecMapper actionSpecMapper,
            @Nonnull PaginationMapper paginationMapper,
            @Nonnull SupplyChainFetcherFactory supplyChainFetcherFactory,
            @Nonnull GroupExpander groupExpander,
            @Nonnull ServiceProviderExpander serviceProviderExpander,
            long realtimeTopologyContextId,
            boolean useStableActionIdAsUuid) {
        this.actionOrchestratorRpc = Objects.requireNonNull(actionOrchestratorRpc);
        this.actionSpecMapper = Objects.requireNonNull(actionSpecMapper);
        this.paginationMapper = Objects.requireNonNull(paginationMapper);
        this.supplyChainFetcherFactory = Objects.requireNonNull(supplyChainFetcherFactory);
        this.groupExpander = Objects.requireNonNull(groupExpander);
        this.serviceProviderExpander = Objects.requireNonNull(serviceProviderExpander);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.useStableActionIdAsUuid = useStableActionIdAsUuid;
    }

    /**
     * Get the actions related to the selected scope.
     *
     * @param scopeId scope
     * @param inputDto query
     * @param paginationRequest pagination request
     * @return a pagination response with {@link ActionApiDTO} objects
     * @throws OperationFailedException if the call to the supply chain service failed
     * @throws InterruptedException if thread is interrupted during processing.
     * @throws UnsupportedActionException translation to {@link ActionApiDTO} object failed for one object,
     *                                    because of action type that is not supported by the translation.
     * @throws ExecutionException translation to {@link ActionApiDTO} object failed for one object.
     * @throws ConversionException if error faced converting objects to API DTOs
     */
    @Nonnull
    public ActionPaginationResponse getActionsByScope(@Nonnull ApiId scopeId,
                                                      @Nonnull ActionApiInputDTO inputDto,
                                                      @Nonnull ActionPaginationRequest paginationRequest)
            throws  InterruptedException, OperationFailedException,
                    UnsupportedActionException, ExecutionException, ConversionException {
        Set<Long> scope = groupExpander.expandOids(Collections.singleton(scopeId));
        if (scope.isEmpty()) {
            return paginationRequest.finalPageResponse(Collections.emptyList(), 0);
        }

        // expand service providers to regions, expandServiceProviders will return a set of all
        // regions and original non serviceProvider scopes.
        scope = serviceProviderExpander.expand(scope);

        final Set<Long> expandedScope;
        final List<String> relatedEntityTypes = inputDto.getRelatedEntityTypes();
        final List<ActionType> actionTypeList = inputDto.getActionTypeList();

        // If the field "relatedEntityTypes" is not empty, then we need to fetch additional
        // entities from the scoped supply chain. Additionally, if call is being made from the UI,
        // then the boolean "forceExpansionOfAggregatedEntities" will be set to true. In that case we
        // will expand via AggregatedEntities for certain actions.
        if(!CollectionUtils.isEmpty(relatedEntityTypes) && relatedEntityTypes.contains("VirtualMachine") && !CollectionUtils.isEmpty(actionTypeList) && actionTypeList.contains(
                ActionType.MOVE) && paginationRequest.getForceExpansionOfAggregatedEntities()) {
            expandedScope = supplyChainFetcherFactory.expandAggregatedEntities(scope);
        }

        // If the field "relatedEntityTypes" is not empty, then we need to fetch additional
        // entities from the scoped supply chain
        else if (!CollectionUtils.isEmpty(inputDto.getRelatedEntityTypes())) {
            // get the scoped supply chain
            // extract entity oids from the supply chain and add them to the scope
            expandedScope = supplyChainFetcherFactory.expandScope(scope, inputDto.getRelatedEntityTypes());
        } else {
            // If there are no related entities, just get the aggregated entities, if they exist.
            expandedScope = supplyChainFetcherFactory.expandAggregatedEntities(scope);
        }

        if (!expandedScope.isEmpty()) {
            // create filter
            final ActionQueryFilter filter = actionSpecMapper.createActionFilter(inputDto,
                                                                                 Optional.of(expandedScope),
                                                                                 scopeId);

            // call the service and retrieve results
            return callActionService(filter, paginationRequest, inputDto.getDetailLevel(),
                    scopeId.getTopologyContextId());
        }

        return paginationRequest.finalPageResponse(Collections.emptyList(), 0);
    }

    /**
     * Get the actions related to each requested scope.
     *
     * @param scopeIds IDs of requested scopes
     * @param inputDto API query
     * @param topologyContextId topology context in which scope exists
     * @return a mapping of scope ID to associated {@link ActionApiDTO}s
     * @throws OperationFailedException if call to supply chain service fails
     * @throws InterruptedException if thread is interrupted during processing.
     * @throws UnsupportedActionException if action spec mapping fails because action is of unsupported type
     * @throws ExecutionException if action spec mapping cannot retrieve necessary entities
     * @throws ConversionException if error occurs converting objects to API DTOs
     */
    @Nullable
    public Map<Long, List<ActionApiDTO>> getActionsByScopes(@Nonnull Set<ApiId> scopeIds,
        @Nonnull ActionApiInputDTO inputDto, long topologyContextId)
        throws  InterruptedException, OperationFailedException,
        UnsupportedActionException, ExecutionException, ConversionException {

        final List<String> relatedTypes = inputDto.getRelatedEntityTypes();
        final boolean expandSupplyChainWithRelatedEntityTypes =
            !CollectionUtils.isEmpty(relatedTypes);

        final Map<ApiId, Set<Long>> originalToExpandedScope = new HashMap<>();
        for (final ApiId scopeId : scopeIds) {
            final Set<Long> byGroup = groupExpander.expandOids(Collections.singleton(scopeId));
            if (!byGroup.isEmpty()) {
                final Set<Long> byProvider = serviceProviderExpander.expand(byGroup);
                final Set<Long> bySupplyChain = expandSupplyChainWithRelatedEntityTypes ?
                    supplyChainFetcherFactory.expandScope(byProvider, relatedTypes) :
                    supplyChainFetcherFactory.expandAggregatedEntities(byProvider);
                if (!bySupplyChain.isEmpty()) {
                    originalToExpandedScope.put(scopeId, bySupplyChain);
                }
            }
        }
        if (originalToExpandedScope.isEmpty()) {
            return null;
        }
        final Set<ActionQuery> actionQueries = new HashSet<>();
        final AtomicInteger queryId = new AtomicInteger(0);
        final Map<Long, Long> filterIdToScopeId = new HashMap<>();

        originalToExpandedScope.forEach((apiId, expandedOidSet) -> {
            final long filterId = queryId.getAndIncrement();
            filterIdToScopeId.put(filterId, apiId.oid());
            actionQueries.add(ActionQuery.newBuilder()
                .setQueryId(filterId)
                .setQueryFilter(actionSpecMapper.createActionFilter(
                    inputDto, Optional.of(expandedOidSet), apiId)).build());
        });

        final Map<Long, ActionPaginationResponse> serviceResult = callActionServiceMultiFilter(
            actionQueries, inputDto.getDetailLevel(), topologyContextId);
        final Map<Long, List<ActionApiDTO>> resultsByScope = new HashMap<>();
        serviceResult.forEach((filter, response) ->
            resultsByScope.put(filterIdToScopeId.get(filter), response.getRawResults()));
        return resultsByScope;
    }

    /**
     * Call the action RPC with a constructed {@link ActionQueryFilter}.
     *
     * @param filter the filter
     * @param paginationRequest pagination request
     * @param detailLevelOpt detail of the actions to be returned
     *                       (if null, then defaults to {@link ActionDetailLevel#STANDARD})
     * @param contextId Real-time or plan topology context id.
     * @return the pagination response
     * @throws InterruptedException if thread is interrupted during processing.
     * @throws UnsupportedActionException translation to {@link ActionApiDTO} object failed for one object,
     *                                    because of action type that is not supported by the translation.
     * @throws ExecutionException translation to {@link ActionApiDTO} object failed for one object.
     * @throws ConversionException if error faced converting objects to API DTOs
     */
    @Nonnull
    public ActionPaginationResponse callActionService(@Nonnull ActionQueryFilter filter,
                                                      @Nonnull ActionPaginationRequest paginationRequest,
                                                      @Nullable ActionDetailLevel detailLevelOpt,
                                                      long contextId)
            throws  InterruptedException, UnsupportedActionException,
            ExecutionException, ConversionException {
        final ActionDetailLevel detailLevel = detailLevelOpt != null
                                                    ? detailLevelOpt
                                                    : ActionDetailLevel.STANDARD;

        // call the service and retrieve results
        Iterator<FilteredActionResponse> responseIterator = actionOrchestratorRpc.getAllActions(
                FilteredActionRequest.newBuilder()
                        .setTopologyContextId(contextId)
                        .addActionQuery(ActionQuery.newBuilder().setQueryFilter(filter))
                        .setPaginationParams(paginationMapper.toProtoParams(paginationRequest))
                        .build());

        final List<ActionApiDTO> results = new ArrayList<>();
        PaginationResponse paginationResponse = null;
        while (responseIterator.hasNext()) {
            final FilteredActionResponse response = responseIterator.next();
            // first response should be pagination response
            if (response.getTypeCase() == TypeCase.PAGINATION_RESPONSE) {
                paginationResponse = response.getPaginationResponse();
            } else {
                results.addAll(actionSpecMapper.mapActionSpecsToActionApiDTOs(
                        response.getActionChunk().getActionsList().stream()
                                .map(ActionOrchestratorAction::getActionSpec)
                                .collect(Collectors.toList()),
                        contextId,
                        detailLevel));
            }
        }

        if (paginationResponse != null) {
            final int totalRecordCount = paginationResponse.getTotalRecordCount();
            return PaginationProtoUtil.getNextCursor(paginationResponse)
                    .map(nextCursor -> paginationRequest.nextPageResponse(results, nextCursor, totalRecordCount))
                    .orElseGet(() -> paginationRequest.finalPageResponse(results, totalRecordCount));
        } else {
            // this should not happen, since enforceLimit it true by default
            return paginationRequest.allResultsResponse(results);
        }
    }

    /**
     * Call the action RPC with a set of {@link ActionQueryFilter}s.
     *
     * @param queries set of {@link ActionQuery}s containing {@link ActionQueryFilter}s
     * @param detailLevelOpt detail level of returned actions (if null, uses {@link ActionDetailLevel#STANDARD})
     * @param contextId Real-time or plan topology context id.
     * @return mapping of pagination response to the id of the filter associated with the response
     * @throws InterruptedException if thread is interrupted during processing.
     * @throws UnsupportedActionException if action spec mapping fails because action is of unsupported type
     * @throws ExecutionException if action spec mapping cannot retrieve necessary entities
     * @throws ConversionException if error occurs converting objects to API DTOs
     */
    private Map<Long, ActionPaginationResponse> callActionServiceMultiFilter(
            @Nonnull final Set<ActionQuery> queries,
            @Nullable final ActionDetailLevel detailLevelOpt,
            final long contextId) throws  InterruptedException, UnsupportedActionException,
                ExecutionException, ConversionException {
        final ActionDetailLevel detailLevel = detailLevelOpt != null ? detailLevelOpt :
            ActionDetailLevel.STANDARD;

        // call the service and retrieve results
        Iterator<FilteredActionResponse> responseIterator = actionOrchestratorRpc.getAllActions(
            FilteredActionRequest.newBuilder()
                .setTopologyContextId(contextId)
                .addAllActionQuery(queries)
                .setPaginationParams(PaginationParameters.newBuilder().setEnforceLimit(false))
                .build());

        final List<ActionSpec> specsToMap = new ArrayList<>();
        final Map<Long, Set<Long>> recsByFilter = new HashMap<>();
        while (responseIterator.hasNext()) {
            final FilteredActionResponse response = responseIterator.next();
            if (!response.hasActionChunk()) {
                continue;
            }
            final List<ActionSpec> specs = response.getActionChunk().getActionsList().stream()
                    .map(ActionOrchestratorAction::getActionSpec)
                    .collect(Collectors.toList());
                specsToMap.addAll(specs);
                recsByFilter.computeIfAbsent(response.getQueryId(), (arg) -> new HashSet<>())
                    .addAll(specs.stream()
                        .map(action -> actionSpecMapper.getActionId(action.getRecommendation().getId(),
                                action.getRecommendationId(), contextId))
                        .collect(Collectors.toList()));
        }
        final Map<Long, ActionApiDTO> actionsByRec =
            actionSpecMapper.mapActionSpecsToActionApiDTOs(specsToMap, contextId, detailLevel).stream()
                .collect(Collectors.toMap(ActionApiDTO::getActionID, dto -> dto));
        final Map<Long, ActionPaginationResponse> results = new HashMap<>();
        try {
            final ActionPaginationRequest emptyPaginationRequest =
                new ActionPaginationRequest(null, null, true, null);
            recsByFilter.forEach((filterId, recIds) -> {
                final List<ActionApiDTO> actions = recIds.stream()
                    .map(actionsByRec::get)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
                results.put(filterId, emptyPaginationRequest.finalPageResponse(actions, actions.size()));
            });
        } catch (InvalidOperationException e) {
            // this won't happen because the ActionPaginationRequest's params are not invalid
        }
        return results;
    }

    /**
     * Call the action RPC with a constructed {@link ActionQueryFilter}. No pagination involved.
     *
     * @param filter the filter
     * @return the result
     * @throws InterruptedException if thread is interrupted during processing.
     * @throws UnsupportedActionException translation to {@link ActionApiDTO} object failed for one object,
     *                                    because of action type that is not supported by the translation.
     * @throws ExecutionException translation to {@link ActionApiDTO} object failed for one object.
     * @throws ConversionException if error faced converting objects to API DTOs
     */
    @Nonnull
    public List<ActionApiDTO> callActionServiceWithNoPagination(@Nonnull ActionQueryFilter filter)
            throws  InterruptedException, UnsupportedActionException, ExecutionException, ConversionException {
        final Iterator<FilteredActionResponse> responseIterator = actionOrchestratorRpc.getAllActions(
                FilteredActionRequest.newBuilder()
                        .setTopologyContextId(realtimeTopologyContextId)
                        .addActionQuery(ActionQuery.newBuilder().setQueryFilter(filter))
                        // stream all actions
                        .setPaginationParams(PaginationParameters.newBuilder().setEnforceLimit(false))
                        .build());
        final List<ActionApiDTO> results = new ArrayList<>();
        while (responseIterator.hasNext()) {
            FilteredActionResponse response = responseIterator.next();
            if (response.getTypeCase() == TypeCase.ACTION_CHUNK) {
                results.addAll(actionSpecMapper.mapActionSpecsToActionApiDTOs(
                        response.getActionChunk().getActionsList().stream()
                                .map(ActionOrchestratorAction::getActionSpec)
                                .collect(Collectors.toList()),
                        realtimeTopologyContextId,
                        ActionDetailLevel.STANDARD));
            }
        }
        return results;
    }

    /**
     * Gets the instace id to use for the action based on the context and use stable id flag.
     *
     * @param uuid the uuid of the action.
     * @param contextId the topology context id.
     * @return the id of action if present.
     */
    @Nonnull
    public Optional<Long> getActionInstanceId(@Nonnull String uuid, @Nullable String contextId) {
        if (shouldUseRecommendationId(contextId)) {
            // look up the instance id based on the recommendation id
            return getInstanceIdForRecommendationIds(Collections.singleton(uuid), null)
                    .stream()
                    .findAny();
        } else {
            return Optional.of(Long.parseLong(uuid));
        }
    }

    /**
     * Gets the instance id to use for the actions based on the context and use stable id flag.
     *
     * @param uuids the uuids of the action.
     * @param contextId the topology context id.
     * @return the ids of action if present.
     */
    @Nonnull
    public List<Long> getInstanceIdForRecommendationIds(@Nonnull Collection<String> uuids,
                                                             @Nullable String contextId) {
        if (uuids.isEmpty()) {
            return Collections.emptyList();
        }

        final List<Long> actionIds;
        if (shouldUseRecommendationId(contextId)) {
            actionIds = actionOrchestratorRpc.getInstanceIdsForRecommendationIds(
                    ActionDTO.GetInstanceIdsForRecommendationIdsRequest
                            .newBuilder()
                            .setTopologyContextId(realtimeTopologyContextId)
                            .addAllRecommendationId(uuids.stream().map(Long::parseLong).collect(Collectors.toList()))
                            .build())
                    .getRecommendationIdToInstanceIdMap()
                    .values()
                    .stream()
                    .collect(Collectors.toList());
        } else {
            actionIds = uuids.stream()
                    .map(Long::valueOf)
                    .collect(Collectors.toList());
        }

        return actionIds;
    }

    private boolean shouldUseRecommendationId(String contextId) {
        return useStableActionIdAsUuid
                && (contextId == null
                || Long.parseLong(contextId) == realtimeTopologyContextId);
    }
}
