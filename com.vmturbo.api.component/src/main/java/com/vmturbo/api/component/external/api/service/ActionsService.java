package com.vmturbo.api.component.external.api.service;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.ServiceProviderExpander;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.action.ImmutableActionStatsQuery;
import com.vmturbo.api.dto.action.ActionApiDTO;
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
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.IActionsService;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.api.utils.UrlsHelp;
import com.vmturbo.common.protobuf.action.ActionDTO.AcceptActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.MultiActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;

/**
 * Service Layer to implement Actions.
 */
public class ActionsService implements IActionsService {
    private static final Logger logger = LogManager.getLogger();

    private final ActionStatsQueryExecutor actionStatsQueryExecutor;

    private final ActionsServiceBlockingStub actionOrchestratorRpc;

    private final RepositoryApi repositoryApi;

    private final ActionSpecMapper actionSpecMapper;

    private final long realtimeTopologyContextId;

    private final UuidMapper uuidMapper;

    private final ServiceProviderExpander serviceProviderExpander;

    private final Logger log = LogManager.getLogger();

    public ActionsService(@Nonnull final ActionsServiceBlockingStub actionOrchestratorRpcService,
                          @Nonnull final ActionSpecMapper actionSpecMapper,
                          @Nonnull final RepositoryApi repositoryApi,
                          final long realtimeTopologyContextId,
                          @Nonnull final ActionStatsQueryExecutor actionStatsQueryExecutor,
                          @Nonnull final UuidMapper uuidMapper,
                          @Nonnull final ServiceProviderExpander serviceProviderExpander) {
        this.actionOrchestratorRpc = Objects.requireNonNull(actionOrchestratorRpcService);
        this.actionSpecMapper = Objects.requireNonNull(actionSpecMapper);
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.actionStatsQueryExecutor = Objects.requireNonNull(actionStatsQueryExecutor);
        this.uuidMapper = uuidMapper;
        this.serviceProviderExpander = Objects.requireNonNull(serviceProviderExpander);
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
        ActionOrchestratorAction action = actionOrchestratorRpc.getAction(actionRequest(uuid));
        if (!action.hasActionSpec()) {
            throw new UnknownObjectException("Action with given action uuid: " + uuid + " not found");
        }

        log.debug("Mapping actions for: {}", uuid);
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

        //TODO The forMaintenanceWindow flag will be used once RightTimeSizing has been implemented for XL
        if (accept) {
            // accept the action
            try {
                log.info("Accepting action with id: {}", uuid);
                AcceptActionResponse response = actionOrchestratorRpc
                        .acceptAction(actionRequest(uuid));
                if (response.hasError()) {
                    log.error("Error {}", response.getError());
                    throw new UnknownObjectException(response.getError());
                }
                return !response.hasError();
            } catch (RuntimeException e) {
                log.error("Execute action error: {}", e.getMessage(), e);
                throw new OperationFailedException("Execute action " + uuid + " attempt failed");
            }
        } else {
            // reject the action
            log.info("Rejecting action with id: {}", uuid);
            throw new NotImplementedException("!!!!!! Reject Action not implemented");
        }
    }

    private SingleActionRequest actionRequest(String actionId) {
        return SingleActionRequest.newBuilder()
            .setTopologyContextId(realtimeTopologyContextId)
            .setActionId(Long.valueOf(actionId))
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
                    entityDto.setUuid(scope.uuid());
                    return entityDto;
                }));

            final Set<Long> entityIds = scopes.stream()
                .filter(ApiId::isEntity)
                .map(ApiId::oid)
                .collect(Collectors.toSet());

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

            // Fill in extra information if the request belongs to an entity.
            if (!CollectionUtils.isEmpty(entityIds)) {
                repositoryApi.entitiesRequest(entityIds)
                    .getMinimalEntities()
                    .forEach(minEntity -> {
                        final EntityStatsApiDTO entityStatsApiDTO = entityStatsByUuid.get(Long.toString(minEntity.getOid()));
                        StatsMapper.populateEntityDataEntityStatsApiDTO(
                                minEntity, entityStatsApiDTO);
                    });
            }

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
    public List<EntityActionsApiDTO> getActionsByUuidsQuery(ActionScopesApiInputDTO actionScopesApiInputDTO) {
        return new ArrayList<>();
    }

    /**
     * Get details for an action.
     *
     * @param uuid Action ID.
     * @return Action details DTO.
     */
    @Override
    public ActionDetailsApiDTO getActionsDetailsByUuid(String uuid) {
        ActionOrchestratorAction action = actionOrchestratorRpc.getAction(actionRequest(uuid));
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
     * Get the list of action details by multiple action uuids.
     *
     * @param inputDto contains information about requested action uuids
     * @return a map of action uuid to {@link ActionDetailsApiDTO}
     */
    @Override
    public Map<String, ActionDetailsApiDTO> getActionDetailsByUuids(ScopeUuidsApiInputDTO inputDto) {
        if (inputDto.getUuids().isEmpty()) {
            return Collections.emptyMap();
        }
        List<Long> actionIds = inputDto.getUuids().stream()
                .map(Long::valueOf)
                .collect(Collectors.toList());
        Iterator<ActionOrchestratorAction> actionsIterator = actionOrchestratorRpc.getActions(
                multiActionRequest(actionIds, inputDto.getTopologyContextId()));
        String inputDtoTopologyContextId = inputDto.getTopologyContextId();
        Long topologyContextId = !Strings.isNullOrEmpty(inputDtoTopologyContextId)
                ? Long.parseLong(inputDtoTopologyContextId) : null;
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(actionsIterator, 0), false)
                .collect(Collectors.toMap(
                        action -> Long.toString(action.getActionId()),
                        action -> getActionDetails(action, topologyContextId)));
    }

    private MultiActionRequest multiActionRequest(@Nonnull final List<Long> uuids,
                                                  @Nullable final String topologyContextId) {
        return MultiActionRequest.newBuilder()
                .setTopologyContextId(Strings.isNullOrEmpty(topologyContextId) ?
                    realtimeTopologyContextId : Long.valueOf(topologyContextId))
                .addAllActionIds(uuids)
                .build();
    }
}
