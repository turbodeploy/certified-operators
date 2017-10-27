package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ActionCountsMapper;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.dto.notification.LogEntryApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.EntityActionsApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.action.ActionScopesApiInputDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.IActionsService;
import com.vmturbo.common.protobuf.action.ActionDTO.AcceptActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByEntityRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByEntityResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.TypeCount;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;

/**
 * Service Layer to implement Actions
 **/
public class ActionsService implements IActionsService {

    private final ActionsServiceBlockingStub actionOrchestratorRpc;

    private final RepositoryApi repositoryApi;

    private final ActionSpecMapper actionSpecMapper;

    private final long realtimeTopologyContextId;

    private final Logger log = LogManager.getLogger();

    public ActionsService(@Nonnull final ActionsServiceBlockingStub actionOrchestratorRpcService,
                          @Nonnull final ActionSpecMapper actionSpecMapper,
                          @Nonnull final RepositoryApi repositoryApi,
                          final long realtimeTopologyContextId) {
        this.actionOrchestratorRpc = Objects.requireNonNull(actionOrchestratorRpcService);
        this.actionSpecMapper = Objects.requireNonNull(actionSpecMapper);
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    /**
     * This API call is not spec'ed correctly - it doesn't return all actions.
     * @return
     * @throws Exception
     */
    @Override
    public ActionApiDTO getActions() throws Exception {
        // this is a placeholder.
        return new ActionApiDTO();
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
        final ActionApiDTO answer = actionSpecMapper.mapActionSpecToActionApiDTO(action.getActionSpec());
        log.trace("Result: {}", () -> answer.toString());
        return answer;
    }

    @Override
    public LogEntryApiDTO getNotificationByUuid(String uuid) throws Exception {
        throw new NotImplementedException();
    }

    @Override
    public boolean executeAction(String uuid, boolean accept) throws Exception {
        if (accept) {
            // accept the action
            try {
                log.info("Accepting action with id: {}", uuid);
                AcceptActionResponse response = actionOrchestratorRpc.acceptAction(actionRequest(uuid));
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
        if (actionScopesApiInputDTO.getScopes() == null) {
            return  new ArrayList<>();
        }
        final Set<Long> entityIds = actionScopesApiInputDTO.getScopes().stream()
            .map(Long::valueOf)
            .collect(Collectors.toSet());
        final Map<Long, EntityStatsApiDTO> entityStatsMap = new HashMap<>();
        for (Map.Entry<Long, Optional<ServiceEntityApiDTO>> entry : repositoryApi
            .getServiceEntitiesById(entityIds).entrySet()) {
            final EntityStatsApiDTO entityStatsApiDTO = new EntityStatsApiDTO();
            ServiceEntityApiDTO serviceEntity = entry.getValue().orElseThrow(()
                -> new UnknownObjectException(
                "ServiceEntity Not Found for oid: " + entry.getKey()));
            entityStatsApiDTO.setUuid(serviceEntity.getUuid());
            entityStatsApiDTO.setDisplayName(serviceEntity.getDisplayName());
            entityStatsApiDTO.setClassName(serviceEntity.getClassName());
            entityStatsApiDTO.setStats(new ArrayList<>());
            entityStatsMap.put(entry.getKey(), entityStatsApiDTO);
        }

        try {
            getActionCountStatsByUuid(actionScopesApiInputDTO.getActionInput(), entityIds,
                entityStatsMap);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Code.NOT_FOUND)) {
                throw new UnknownObjectException(e.getStatus().getDescription());
            } else if (e.getStatus().getCode().equals(Code.INVALID_ARGUMENT)) {
                throw new InvalidOperationException(e.getStatus().getDescription());
            } else {
                throw e;
            }
        }
        return Lists.newArrayList(entityStatsMap.values());
    }

    /**
     * Get a list of actions by multiple uuids using query parameters
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
            actionSpecMapper.createActionFilter(actionApiInputDTO, Optional.of(entityIds));
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
                case ActionCountsMapper.ACTION_TYPES_NAME:
                    statSnapshotApiDTOS.addAll(ActionCountsMapper.countsByTypeToApi(typeCounts));
                    break;
                default:
                    throw new NotImplementedException("Action stats groupBy " + groupByType +
                        " is not implemented yet.");
            }
        });
        return statSnapshotApiDTOS;
    }
}
