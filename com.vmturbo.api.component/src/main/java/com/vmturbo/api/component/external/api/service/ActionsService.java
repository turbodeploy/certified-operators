package com.vmturbo.api.component.external.api.service;

import java.time.Clock;
import java.util.Arrays;
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
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.springframework.validation.Errors;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.ExceptionMapper;
import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.ServiceProviderExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.action.ActionSearchUtil;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.action.ImmutableActionStatsQuery;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.action.ActionDetailsApiDTO;
import com.vmturbo.api.dto.action.ActionScopesApiInputDTO;
import com.vmturbo.api.dto.action.EntityActionsApiDTO;
import com.vmturbo.api.dto.action.NoDetailsApiDTO;
import com.vmturbo.api.dto.action.ScopeUuidsApiInputDTO;
import com.vmturbo.api.dto.notification.LogEntryApiDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.ActionDetailLevel;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnauthorizedObjectException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.ActionPaginationRequest;
import com.vmturbo.api.pagination.EntityActionPaginationRequest;
import com.vmturbo.api.pagination.EntityActionPaginationRequest.EntityActionPaginationResponse;
import com.vmturbo.api.serviceinterfaces.IActionsService;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.api.utils.UrlsHelp;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.GetInstanceIdsForRecommendationIdsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.MultiActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.utils.StringConstants;

/**
 * Service Layer to implement Actions.
 */
public class ActionsService implements IActionsService {
    private static final Logger logger = LogManager.getLogger();

    private final ActionStatsQueryExecutor actionStatsQueryExecutor;

    private final ActionsServiceBlockingStub actionOrchestratorRpc;

    private final RepositoryApi repositoryApi;

    private final ActionSpecMapper actionSpecMapper;

    private final ActionSearchUtil actionSearchUtil;

    private final SupplyChainFetcherFactory supplyChainFetcherFactory;

    private final MarketsService marketsService;

    private final long realtimeTopologyContextId;

    private final UuidMapper uuidMapper;

    private final ServiceProviderExpander serviceProviderExpander;

    private final Logger log = LogManager.getLogger();

    private final int validPaginationMaxLimit = 500;

    // Pagination request to be used for lists of actions that are returned as an inner list of a scope.
    private ActionPaginationRequest nestedDefaultPaginationRequest;

    /**
     * Flag that enables all action uuids come from the stable recommendation oid instead of the
     * unstable action instance id.
     */
    private final boolean useStableActionIdAsUuid;

    public ActionsService(@Nonnull final ActionsServiceBlockingStub actionOrchestratorRpcService,
                          @Nonnull final ActionSpecMapper actionSpecMapper,
                          @Nonnull final RepositoryApi repositoryApi,
                          final long realtimeTopologyContextId,
                          @Nonnull final ActionStatsQueryExecutor actionStatsQueryExecutor,
                          @Nonnull final UuidMapper uuidMapper,
                          @Nonnull final ServiceProviderExpander serviceProviderExpander,
                          @Nonnull final ActionSearchUtil actionSearchUtil,
                          @Nonnull final MarketsService marketsService,
                          @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                          final int apiPaginationMaxLimit, boolean useStableActionIdAsUuid) {
        this.actionOrchestratorRpc = Objects.requireNonNull(actionOrchestratorRpcService);
        this.actionSpecMapper = Objects.requireNonNull(actionSpecMapper);
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.actionStatsQueryExecutor = Objects.requireNonNull(actionStatsQueryExecutor);
        this.uuidMapper = uuidMapper;
        this.serviceProviderExpander = Objects.requireNonNull(serviceProviderExpander);
        this.actionSearchUtil = actionSearchUtil;
        this.marketsService = marketsService;
        this.supplyChainFetcherFactory = supplyChainFetcherFactory;
        this.useStableActionIdAsUuid = useStableActionIdAsUuid;

        try {
            // Default to 500 actions per entity scope which is the default max limit for pagination request.
            nestedDefaultPaginationRequest = new ActionPaginationRequest(null, apiPaginationMaxLimit < 1
                ? validPaginationMaxLimit : apiPaginationMaxLimit, true, StringConstants.SEVERITY);
        } catch (InvalidOperationException e) {
            // This should not happen as we are passing valid default configurations for APR.
            nestedDefaultPaginationRequest = null;
            logger.error("ActionsService nested default pagination request initialization failed: {}",
                    e.getLocalizedMessage());
        }
    }

    @Override
    public ActionApiDTO getActions() throws Exception {
        final ActionApiDTO actionApiDTO = new ActionApiDTO();
        // This method doesn't return actions, but returns information about
        // which routes to call to get actions for different kinds of objects
        UrlsHelp.setActionHelp(actionApiDTO);
        return actionApiDTO;
    }

    /**
     * Return the Action information given the id for the action.
     *
     * @param uuid          the ID for the Action to be returned
     * @param detailLevel   the level of Action details to be returned
     * @return the {@link ActionApiDTO} for the requested id
     * @throws Exception
     */
    @Override
    public ActionApiDTO getActionByUuid(@NonNull final String uuid, @Nullable final ActionDetailLevel detailLevel)
            throws Exception {
        log.debug("Fetching actions for: {}", uuid);

        final long id = getActionInstanceId(uuid).orElseThrow(() -> {
            logger.error("Cannot lookup action as one with ID {} cannot be found.", uuid);
            return new UnknownObjectException("Cannot find action with ID " + uuid);
        });

        ActionOrchestratorAction action = actionOrchestratorRpc.getAction(actionRequest(id));
        if (!action.hasActionSpec()) {
            throw new UnknownObjectException("Action with given action uuid: " + id + " not found");
        }

        log.debug("Mapping actions for: {}", id);
        final ActionApiDTO answer = actionSpecMapper.mapActionSpecToActionApiDTO(action.getActionSpec(),
                realtimeTopologyContextId, detailLevel);
        log.trace("Result: {}", answer::toString);
        return answer;
    }

    @Override
    public LogEntryApiDTO getNotificationByUuid(String uuid) {
        throw new NotImplementedException();
    }

    @Override
    public boolean executeAction(String uuid, boolean accept, boolean forMaintenanceWindow) throws Exception {
        final long id = getActionInstanceId(uuid).orElseThrow(() -> {
           logger.error("Cannot execute action as one with ID {} cannot be found.", uuid);
           return new UnknownObjectException("Cannot find action with ID " + uuid);
        });

        if (accept) {
            // accept the action
            try {
                log.info("Accepting action with id: {}", id);
                actionOrchestratorRpc.acceptAction(actionRequest(id));
                return true;
            } catch (StatusRuntimeException e) {
                log.error("Execute action error: {}", e.getMessage(), e);
                throw convertToApiException(e);
            }
        } else {
            // reject the action
            log.info("Rejecting action with id: {}", id);
            throw new NotImplementedException("!!!!!! Reject Action not implemented");
        }
    }

    @Nonnull
    private Optional<Long> getActionInstanceId(@Nonnull String uuid) {
        if (useStableActionIdAsUuid) {
            // look up the instance id based on the recommendation id
            final Map<Long, Long> recommendationIdToId = getInstanceIdForRecommendationIds(Collections.singleton(uuid));
            if (recommendationIdToId.isEmpty()) {
                return Optional.empty();
            } else {
                return Optional.of(recommendationIdToId.values().iterator().next());
            }
        } else {
            return Optional.of(Long.parseLong(uuid));
        }
    }

    @Nonnull
    private Map<Long, Long> getInstanceIdForRecommendationIds(@Nonnull Collection<String> actionIds) {
        if (actionIds.isEmpty()) {
            return Collections.emptyMap();
        }

        return actionOrchestratorRpc.getInstanceIdsForRecommendationIds(
                GetInstanceIdsForRecommendationIdsRequest
                        .newBuilder()
                        .setTopologyContextId(realtimeTopologyContextId)
                        .addAllRecommendationId(actionIds.stream().map(Long::parseLong).collect(Collectors.toList()))
                        .build())
                .getRecommendationIdToInstanceIdMap();
    }

    private static Exception convertToApiException(StatusRuntimeException e) {
        switch (e.getStatus().getCode()) {
            case NOT_FOUND:
                return new UnknownObjectException(e.getMessage());
            case INVALID_ARGUMENT:
                return new InvalidOperationException(e.getMessage());
            case PERMISSION_DENIED:
                return new UnauthorizedObjectException(e.getMessage());
            case INTERNAL:
            case FAILED_PRECONDITION:
            default:
                return new OperationFailedException(e.getMessage());
        }
    }

    private SingleActionRequest actionRequest(long actionId) {
        return SingleActionRequest.newBuilder()
            .setTopologyContextId(realtimeTopologyContextId)
            .setActionId(actionId)
            .build();
    }

    @Override
    public List<String> getAvailActionModes(String actionType, String seType) {
        // return an immutable list containing the "name()" string for each {@link ActionMode}
        return Arrays.stream(ActionMode.values())
                .map(ActionMode::name)
                .collect(Collectors.toList());
    }

    /**
     * Get the list of action statistics by multiple uuids using query parameters. And it based on
     * groupBy type to gather action stats. For example: the request needs to:
     * {"groupBy" : ["actionTypes", "actionModes"]} and also it only allow actionType: MOVE and
     * SUSPEND, and actionModes: RECOMMEND and MANUAL. First it will filer out actions which actionType is not
     * MOVE or SUSPEND and actionMode is not RECOMMEND or MANUAL. For those matched actions, will count
     * actions for different groupBy type, such as ["actionTypes" == "MOVE"], ["actionTypes" == "SUSPEND"],
     * ["actionModes" == "RECOMMEND"] and ["actionModes" == "MANUAL"].
     * Note: right now, we only implement group by action types, and will ignore action type filter lists.
     *
     * @param actionScopesApiInputDTO The object used to query the action statistics.
     * @return a list of EntityStatsApiDTO.
     * @throws Exception
     */
    @Override
    public List<EntityStatsApiDTO> getActionStatsByUuidsQuery(ActionScopesApiInputDTO actionScopesApiInputDTO)
            throws Exception {
        String currentTimeStamp = DateTimeUtil.toString(Clock.systemUTC().millis());
        if (actionScopesApiInputDTO.getScopes() == null) {
            return Collections.emptyList();
        }

        try {
            Set<ApiId> scopes;
            if (actionScopesApiInputDTO.getScopes() == null ||
                !UuidMapper.hasLimitedScope(actionScopesApiInputDTO.getScopes())) {
                scopes = Collections.singleton(uuidMapper.fromUuid(UuidMapper.UI_REAL_TIME_MARKET_STR));
            } else {
                scopes = actionScopesApiInputDTO.getScopes().stream()
                    .map(uuid -> {
                        try {
                            return uuidMapper.fromUuid(uuid);
                        } catch (OperationFailedException e) {
                            logger.error("Failed to map uuid {} to Api ID. Error: {}", uuid,
                                    e.getLocalizedMessage());
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());
            }

            // service providers do not have any stats, expand them to find the regions and get
            // stats for regions. expandServiceProviders will return a set of all regions and original
            // non serviceProvider scopes.
            final Set<Long> expandedScopes = serviceProviderExpander.expand(scopes.stream()
                    .map(ApiId::oid)
                    .collect(Collectors.toSet()));
            scopes = expandedScopes.stream()
                    .map(uuidMapper::fromOid)
                    .collect(Collectors.toSet());

            final Map<String, EntityStatsApiDTO> entityStatsByUuid = scopes.stream()
                .collect(Collectors.toMap(ApiId::uuid, scope -> {
                    final EntityStatsApiDTO entityDto = new EntityStatsApiDTO();
                    StatsMapper.populateEntityDataEntityStatsApiDTO(scope, entityDto);
                    return entityDto;
                }));

            final ImmutableActionStatsQuery.Builder queryBuilder = ImmutableActionStatsQuery.builder()
                .scopes(scopes)
                 // store the time when this api is triggered in the query and use it for current action record
                 // we want to make sure we have the action record with current timestamp
                .currentTimeStamp(currentTimeStamp)
                .actionInput(actionScopesApiInputDTO.getActionInput());
            if (actionScopesApiInputDTO.getRelatedType() != null) {
                queryBuilder.entityType(ApiEntityType.fromString(
                    actionScopesApiInputDTO.getRelatedType()).typeNumber());
            }
            final Map<ApiId, List<StatSnapshotApiDTO>> actionStatsByScope =
                actionStatsQueryExecutor.retrieveActionStats(queryBuilder.build());
            actionStatsByScope.forEach((scope, actionStats) -> {
                final EntityStatsApiDTO entityStats = entityStatsByUuid.get(scope.uuid());
                if (entityStats != null) {
                    entityStats.setStats(actionStats);
                }
            });

            return Lists.newArrayList(entityStatsByUuid.values());
        }  catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Code.NOT_FOUND)) {
                throw new UnknownObjectException(e.getStatus().getDescription());
            } else if (e.getStatus().getCode().equals(Code.INVALID_ARGUMENT)) {
                throw new InvalidOperationException(e.getStatus().getDescription());
            } else {
                throw e;
            }
        }
    }

    /**
     * Get a list of actions by multiple uuids using query parameters.
     *
     * @param actionScopesApiInputDTO The object used to query the actions
     * @return a list of actions by multiple uuids using query parameters
     */
    @Override
    public EntityActionPaginationResponse getActionsByUuidsQuery(ActionScopesApiInputDTO actionScopesApiInputDTO,
            EntityActionPaginationRequest paginationRequest)
        throws Exception {

        final Function<String, String> invalidScopeMessage =
            (uuid) -> "Failed to map uuid " + uuid + " to Api ID.";
        final Function<String, String> internalFailureMessage =
            (scopes) -> "Exception getting actions by scope for scopes [" + scopes + "]";

        if (actionScopesApiInputDTO.getActionInput() == null) {
            throw new IllegalArgumentException("Null ActionApiInputDTO is not allowed");
        }
        if (nestedDefaultPaginationRequest == null) {
            logger.error("Nested default pagination request for actions did not initialize properly");
            return paginationRequest.allResultsResponse(Collections.emptyList());
        }

        // Return if no scope is provided.
        if (actionScopesApiInputDTO.getScopes() == null) {
            return paginationRequest.allResultsResponse(Collections.emptyList());
        }

        try {
            Set<ApiId> scopes = actionScopesApiInputDTO.getScopes().stream()
                .map(uuid -> {
                    try {
                        return uuidMapper.fromUuid(uuid);
                    } catch (Exception e) {
                        final String message = invalidScopeMessage.apply(uuid);
                        logger.error(message, e);
                        throw new IllegalArgumentException(message, e);
                    }
                })
                .collect(Collectors.toSet());
            // relatedType indicates that scope should be expanded and per-entity actions returned
            // rather than aggregated.
            if (actionScopesApiInputDTO.getRelatedType() != null) {
                scopes = supplyChainFetcherFactory.expandScope(
                        scopes.stream()
                            .map(ApiId::oid)
                            .collect(Collectors.toSet()),
                        ImmutableList.of(actionScopesApiInputDTO.getRelatedType()))
                    .stream()
                        .map(uuidMapper::fromOid)
                        .collect(Collectors.toSet());
            }
            final List<EntityActionsApiDTO> entityActions = scopes.stream()
                .map(scope -> {
                    final EntityActionsApiDTO dto = new EntityActionsApiDTO();
                    dto.setUuid(scope.uuid());
                    dto.setDisplayName(scope.getDisplayName());
                    dto.setClassName(scope.getClassName());
                    return dto;
                })
                .sorted(paginationRequest.getOrderBy().getComparator(paginationRequest.isAscending()))
                .collect(Collectors.toList());
            final String cursor = paginationRequest.getCursor().orElse("0");
            final int totalCount = entityActions.size();
            final int skipCount;
            try {
                skipCount = Integer.parseInt(cursor);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Cursor " + cursor
                    + " is invalid. Should be an integer.");
            }
            final int limit = paginationRequest.getLimit();
            final Map<Long, Set<ApiId>> scopesByContext = new HashMap<>();
            final List<EntityActionsApiDTO> dtosWithPartialActions = entityActions.stream()
                .skip(skipCount)
                .limit(limit)
                .map(dto -> {
                    try {
                        final ApiId scope = uuidMapper.fromUuid(dto.getUuid());
                        if (scope.isRealtimeMarket() || scope.isPlan()) {
                            final List<ActionApiDTO> apiDtoList =
                                marketsService.getActionsByMarketUuid(scope.uuid(),
                                        actionScopesApiInputDTO.getActionInput(),
                                        nestedDefaultPaginationRequest)
                                    .getRawResults();
                            dto.setActions(apiDtoList);
                        } else {
                            scopesByContext.computeIfAbsent(scope.getTopologyContextId(),
                                (x) -> new HashSet<>()).add(scope);
                        }
                        return dto;
                    } catch (OperationFailedException e) {
                        final String message = invalidScopeMessage.apply(dto.getUuid());
                        logger.error(message, e);
                        throw new IllegalArgumentException(message, e);
                    } catch (Exception e) {
                        final String message = internalFailureMessage.apply(dto.getUuid());
                        logger.error(message, e);
                        throw new RuntimeException(message, e);
                    }
                })
                .collect(Collectors.toList());

            final Map<Long, List<ActionApiDTO>> actionsByScope = new HashMap<>();
            scopesByContext.forEach((contextId, scopesForContext) -> {
                try {
                    final Map<Long, List<ActionApiDTO>> contextResult =
                        actionSearchUtil.getActionsByScopes(scopesForContext,
                            actionScopesApiInputDTO.getActionInput(), contextId);
                    if (contextResult != null) {
                        actionsByScope.putAll(contextResult);
                    }
                } catch (Exception e) {
                    final String message = internalFailureMessage.apply(scopesForContext.stream()
                        .map(ApiId::uuid)
                        .collect(Collectors.joining(", "))) + " in context " + contextId;
                    logger.error(message, e);
                    throw new RuntimeException(message, e);
                }
            });

           final List<EntityActionsApiDTO> dtosWithAllActions =  dtosWithPartialActions.stream()
               .map(dto -> {
                   try {
                       // uuidMapper is necessary to handle special-case uuids e.g. Market
                       final long oid = uuidMapper.fromUuid(dto.getUuid()).oid();
                       if (actionsByScope.containsKey(oid)) {
                           dto.setActions(actionsByScope.get(oid));
                       }
                       return dto;
                   } catch (OperationFailedException e) {
                       final String message = invalidScopeMessage.apply(dto.getUuid());
                       logger.error(message, e);
                       throw new IllegalArgumentException(message, e);
                   }

               }).collect(Collectors.toList());
            if (limit + skipCount >= totalCount) {
                return paginationRequest.finalPageResponse(dtosWithAllActions, totalCount);
            } else {
                return paginationRequest.nextPageResponse(dtosWithAllActions,
                    Integer.toString(skipCount + limit), totalCount);
            }
        } catch (StatusRuntimeException e) {
            throw ExceptionMapper.translateStatusException(e);
        }
    }

    /**
     * Get details for an action.
     *
     * @param uuid Action ID.
     * @return Action details DTO.
     */
    @Override
    public ActionDetailsApiDTO getActionsDetailsByUuid(String uuid) throws UnknownObjectException {
        final long id = getActionInstanceId(uuid).orElseThrow(() -> {
            logger.error("Cannot execute action as one with ID {} cannot be found.", uuid);
            return new UnknownObjectException("Cannot find action with ID " + uuid);
        });

        ActionOrchestratorAction action = actionOrchestratorRpc.getAction(actionRequest(id));
        return getActionDetails(action, realtimeTopologyContextId);
    }

    /**
     * Gets details for an action.
     *
     * @param action action
     * @param topologyContextId the topology in which the action resides
     * @return details of the action
     */
    @Nonnull
    private ActionDetailsApiDTO getActionDetails(@Nonnull ActionOrchestratorAction action, final Long topologyContextId) {
        if (action.hasActionSpec()) {
            // create action details dto based on action api dto which contains "explanation" with
            // coverage information.
            ActionDetailsApiDTO actionDetailsApiDTO = actionSpecMapper.createActionDetailsApiDTO(
                    action,
                    Objects.isNull(topologyContextId) ? realtimeTopologyContextId : topologyContextId);
            if (actionDetailsApiDTO != null) {
                return actionDetailsApiDTO;
            }
        }
        return new NoDetailsApiDTO();
    }

    /**
     * Gets details for a list of actions.
     *
     * @param actions collection of actions
     * @param topologyContextId the topology in which the action resides
     * @return details of the action
     */
    @Nonnull
    private Map<String, ActionDetailsApiDTO> getActionDetails(@Nonnull Collection<ActionOrchestratorAction> actions, final Long topologyContextId) {

        Long contextId = Objects.isNull(topologyContextId) ? realtimeTopologyContextId : topologyContextId;
        return actionSpecMapper.createActionDetailsApiDTO(actions, contextId);
    }

    /**
     * Get the list of action details by multiple action uuids.
     *
     * @param inputDto contains information about requested action uuids
     * @return a map of action uuid to {@link ActionDetailsApiDTO}
     */
    @Override
    public Map<String, ActionDetailsApiDTO> getActionDetailsByUuids(ScopeUuidsApiInputDTO inputDto)
            throws OperationFailedException, IllegalArgumentException {
        // Use marketId field if it is set, otherwise use old deprecated topologyContextId
        final String inputDtoTopologyContextId = Strings.isNullOrEmpty(inputDto.getMarketId())
                ? inputDto.getTopologyContextId()
                : Long.toString(uuidMapper.fromUuid(inputDto.getMarketId()).oid());

        final List<Long> actionIds;
        if (useStableActionIdAsUuid
              && (inputDtoTopologyContextId == null
              || Long.parseLong(inputDtoTopologyContextId) == realtimeTopologyContextId)) {
            actionIds = getInstanceIdForRecommendationIds(inputDto.getUuids())
                    .values()
                    .stream()
                    .collect(Collectors.toList());
        } else {
            actionIds = inputDto.getUuids().stream()
                    .map(Long::valueOf)
                    .collect(Collectors.toList());
        }

        if (actionIds.isEmpty()) {
            return Collections.emptyMap();
        }

        Iterator<ActionOrchestratorAction> actionsIterator = actionOrchestratorRpc.getActions(
                multiActionRequest(actionIds, inputDtoTopologyContextId));
        Long topologyContextId = !Strings.isNullOrEmpty(inputDtoTopologyContextId)
                ? Long.parseLong(inputDtoTopologyContextId) : null;

        List<ActionOrchestratorAction> actions = Lists.newArrayList(actionsIterator);

        return getActionDetails(actions, topologyContextId);
    }

    private MultiActionRequest multiActionRequest(@Nonnull final List<Long> uuids,
                                                  @Nullable final String topologyContextId) {
        return MultiActionRequest.newBuilder()
                .setTopologyContextId(Strings.isNullOrEmpty(topologyContextId) ?
                    realtimeTopologyContextId : Long.valueOf(topologyContextId))
                .addAllActionIds(uuids)
                .build();
    }

    /**
     * Validates incoming action api input dto.
     *
     * @param actionApiInputDTO Object to validate
     * @param e                 Spring framework validation errors, not actually used in our validations
     */
    @Override
    public void validateInput(final ActionApiInputDTO actionApiInputDTO, final Errors e) {
        final StringBuilder errors = new StringBuilder();

        // description query
        if (actionApiInputDTO.getDescriptionQuery() != null && actionApiInputDTO.getDescriptionQuery().getType() == null) {
            errors.append("Description query is invalid. Query Type cannot be null.");
        }

        // risk query
        if (actionApiInputDTO.getRiskQuery() != null && actionApiInputDTO.getRiskQuery().getType() == null) {
            errors.append("Risk query is invalid. Query Type cannot be null.");
        }

        // savings amount range
        if (actionApiInputDTO.getSavingsAmountRange() != null
                && actionApiInputDTO.getSavingsAmountRange().getMinValue() == null
                && actionApiInputDTO.getSavingsAmountRange().getMaxValue() == null) {
            errors.append("Savings Amount Range is invalid. Either minValue or maxValue should be specified.");
        }

        final String errorMsg = errors.toString();
        if (StringUtils.isNotBlank(errorMsg)) {
            logger.error("Error validating action api input: {}", () -> errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }
    }
}
