package com.vmturbo.api.component.external.api.service;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ActionCountsMapper;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.action.ImmutableActionStatsQuery;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.action.ActionDetailsApiDTO;
import com.vmturbo.api.dto.action.ActionScopesApiInputDTO;
import com.vmturbo.api.dto.action.EntityActionsApiDTO;
import com.vmturbo.api.dto.action.NoDetailsApiDTO;
import com.vmturbo.api.dto.notification.LogEntryApiDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.IActionsService;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.api.utils.UrlsHelp;
import com.vmturbo.common.protobuf.action.ActionDTO.AcceptActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByEntityRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByEntityResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.TypeCount;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;

/**
 * Service Layer to implement Actions
 **/
public class ActionsService implements IActionsService {
    private static final Logger logger = LogManager.getLogger();

    private final ActionStatsQueryExecutor actionStatsQueryExecutor;

    private final ActionsServiceBlockingStub actionOrchestratorRpc;

    private final RepositoryApi repositoryApi;

    private final ActionSpecMapper actionSpecMapper;

    private final long realtimeTopologyContextId;

    private final UuidMapper uuidMapper;

    private final Logger log = LogManager.getLogger();

    public ActionsService(@Nonnull final ActionsServiceBlockingStub actionOrchestratorRpcService,
                          @Nonnull final ActionSpecMapper actionSpecMapper,
                          @Nonnull final RepositoryApi repositoryApi,
                          final long realtimeTopologyContextId,
                          @Nonnull final ActionStatsQueryExecutor actionStatsQueryExecutor,
                          @Nonnull final UuidMapper uuidMapper) {
        this.actionOrchestratorRpc = Objects.requireNonNull(actionOrchestratorRpcService);
        this.actionSpecMapper = Objects.requireNonNull(actionSpecMapper);
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.actionStatsQueryExecutor = Objects.requireNonNull(actionStatsQueryExecutor);
        this.uuidMapper = uuidMapper;
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
     * @param uuid the ID for the Action to be returned
     * @return the {@link ActionApiDTO} for the requested id
     * @throws Exception
     */
    @Override
    public ActionApiDTO getActionByUuid(String uuid) throws Exception {
        log.debug("Fetching actions for: {}", uuid);
        long actionId = Long.valueOf(uuid);
        ActionOrchestratorAction action = actionOrchestratorRpc.getAction(actionRequest(uuid));
        if (!action.hasActionSpec()) {
            throw new UnknownObjectException("Action with given action uuid: " + uuid + " not found");
        }

        log.debug("Mapping actions for: {}", uuid);
        final ActionApiDTO answer = actionSpecMapper.mapActionSpecToActionApiDTO(action.getActionSpec(),
                realtimeTopologyContextId);
        log.trace("Result: {}", () -> answer.toString());
        return answer;
    }

    @Override
    public LogEntryApiDTO getNotificationByUuid(String uuid) throws Exception {
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

    private final SingleActionRequest actionRequest(String actionId) {
        return SingleActionRequest.newBuilder()
            .setTopologyContextId(realtimeTopologyContextId)
            .setActionId(Long.valueOf(actionId))
            .build();
    }

    @Override
    public List<String> getAvailActionModes(String actionType, String seType) {
        // return an immutable list containing the "name()" string for each {@link ActionMode}
        return Arrays.asList(ActionMode.values()).stream()
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
            final Set<ApiId> scopes;
            if (actionScopesApiInputDTO.getScopes() == null ||
                !UuidMapper.hasLimitedScope(actionScopesApiInputDTO.getScopes())) {
                scopes = Collections.singleton(uuidMapper.fromUuid(UuidMapper.UI_REAL_TIME_MARKET_STR));
            } else {
                scopes = actionScopesApiInputDTO.getScopes().stream()
                    .map(uuid -> {
                        try {
                            return uuidMapper.fromUuid(uuid);
                        } catch (OperationFailedException e) {
                            logger.error("Failed to map uuid {} to Api ID. Error: {}", e.getLocalizedMessage());
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());
            }

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
                queryBuilder.entityType(UIEntityType.fromString(
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
                        entityStatsApiDTO.setDisplayName(minEntity.getDisplayName());
                        entityStatsApiDTO.setClassName(UIEntityType.fromType(minEntity.getEntityType()).apiStr());
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
     * Get action counts for given group uuid.
     * @param uuid of the group.
     * @param actionScopesApiInputDTO contains filter criteria for getting action stats.
     * @param groupMembers  are set of uuids of members of the given group uuid.
     * @return entity stats for all members in the group.
     */
    @Nonnull
    private EntityStatsApiDTO getActionStatsByUuidsQueryForGroup(String uuid,
                    ActionScopesApiInputDTO actionScopesApiInputDTO,
                    Optional<Set<Long>> groupMembers) {
        final EntityStatsApiDTO entityStatsApiDTO = new EntityStatsApiDTO();
        entityStatsApiDTO.setUuid(uuid);
        entityStatsApiDTO.setStats(getActionCountStatsForEntities(actionScopesApiInputDTO.getActionInput(),
                        groupMembers));
        return entityStatsApiDTO;
    }

    /**
     * Get a list of actions by multiple uuids using query parameters.
     *
     * @param actionScopesApiInputDTO The object used to query the actions
     * @return a list of actions by multiple uuids using query parameters
     * @throws Exception
     */
    @Override
    public List<EntityActionsApiDTO> getActionsByUuidsQuery(ActionScopesApiInputDTO actionScopesApiInputDTO)
        throws Exception {
        return new ArrayList<>();
    }

    /**
     * Send request to action orchestrator and get action counts by each entity.
     *
     * @param actionApiInputDTO contains filter criteria for getting action stats.
     * @param entityIds a list of entity ids.
     * @param entityStatsMap A map from entity id to EntityStatsApiDTO.
     */
    private void getActionCountStatsByUuid(@Nullable ActionApiInputDTO actionApiInputDTO,
                                           @Nonnull final Set<Long> entityIds,
                                           @Nonnull Map<Long, EntityStatsApiDTO> entityStatsMap) {
        final ActionQueryFilter filter =
            actionSpecMapper.createLiveActionFilter(actionApiInputDTO, Optional.of(entityIds));
        final GetActionCountsByEntityResponse response =
            actionOrchestratorRpc.getActionCountsByEntity(GetActionCountsByEntityRequest.newBuilder()
                .setTopologyContextId(realtimeTopologyContextId)
                .setFilter(filter)
                .build());
        response.getActionCountsByEntityList().stream()
            .forEach(actionCountsByEntity -> {
                final Long entityId = actionCountsByEntity.getEntityId();
                // we only want to keep requested entities
                if (entityStatsMap.containsKey(entityId)) {
                    entityStatsMap.get(entityId).getStats()
                        .addAll(convertCountsByTypeToApi(actionApiInputDTO.getGroupBy(),
                            actionCountsByEntity.getCountsByTypeList()));
                }
            });
    }

    /**
     *
     * Send request to action orchestrator and get action counts for entities in group.
     *
     * @param inputDto contains filter criteria for getting action stats.
     * @param entityIds set of entityIds
     * @return stats collected for entities in group.
     */
    @Nonnull
    private List<StatSnapshotApiDTO> getActionCountStatsForEntities(ActionApiInputDTO inputDto, Optional<Set<Long>> entityIds) {
        final ActionQueryFilter filter =
                actionSpecMapper.createLiveActionFilter(inputDto, entityIds);
        final GetActionCountsResponse response =
                actionOrchestratorRpc.getActionCounts(GetActionCountsRequest.newBuilder()
                    .setTopologyContextId(realtimeTopologyContextId)
                    .setFilter(filter)
                    .build());
        return ActionCountsMapper.countsByTypeToApi(response.getCountsByTypeList());
    }

    /**
     * For different groupBy type, convert its action orchestrator response to StatSnapshotApiDTO.
     *
     * @param groupTypes a list of types need to group by.
     * @param typeCounts Response from action orchestrator contains all group by counts.
     * @return list of StatSnapshotApiDTO
     */
    private List<StatSnapshotApiDTO> convertCountsByTypeToApi(@Nullable final List<String> groupTypes,
                                                              @Nonnull final List<TypeCount> typeCounts) {
        if (groupTypes == null) {
            return new ArrayList<>();
        }
        final List<StatSnapshotApiDTO> statSnapshotApiDTOS = new ArrayList<>();
        groupTypes.stream().forEach(groupByType -> {
            // TODO: Implement more groupBy types for gathering action stats.
            // Right now, we only implement group by action types.
            switch (groupByType) {
                case StringConstants.ACTION_TYPES:
                    statSnapshotApiDTOS.addAll(ActionCountsMapper.countsByTypeToApi(typeCounts));
                    break;
                default:
                    throw new NotImplementedException("Action stats groupBy " + groupByType +
                        " is not implemented yet.");
            }
        });
        return statSnapshotApiDTOS;
    }

    /**
     * Get details for an action.
     *
     * @param uuid
     * @return
     * @throws Exception
     */
    @Override
    public ActionDetailsApiDTO getActionsDetailsByUuid(String uuid) throws Exception {
        ActionOrchestratorAction action = actionOrchestratorRpc.getAction(actionRequest(uuid));
        ActionDetailsApiDTO actionDetailsApiDTO = null;
        if (action.hasActionSpec()) {
            // create action details dto based on action api dto which contains "explanation" with coverage information.
            actionDetailsApiDTO = actionSpecMapper.createActionDetailsApiDTO(action.getActionSpec());
            if (actionDetailsApiDTO == null) {
                return new NoDetailsApiDTO();
            }
            return actionDetailsApiDTO;
        }
        return new NoDetailsApiDTO();
    }
}
