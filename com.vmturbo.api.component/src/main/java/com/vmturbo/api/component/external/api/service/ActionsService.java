package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.LogEntryApiDTO;
import com.vmturbo.api.dto.action.EntityActionsApiDTO;
import com.vmturbo.api.dto.input.statistic.ActionScopesApiInputDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.IActionsService;
import com.vmturbo.common.protobuf.action.ActionDTO.AcceptActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;

/**
 * Service Layer to implement Actions
 **/
public class ActionsService implements IActionsService {

    private final ActionsServiceBlockingStub actionOrchestratorRpc;

    private final ActionSpecMapper actionSpecMapper;

    private final long realtimeTopologyContextId;

    private final Logger log = LogManager.getLogger();

    public ActionsService(@Nonnull final ActionsServiceBlockingStub actionOrchestratorRpcService,
                          @Nonnull final ActionSpecMapper actionSpecMapper,
                          final long realtimeTopologyContextId) {
        this.actionOrchestratorRpc = Objects.requireNonNull(actionOrchestratorRpcService);
        this.actionSpecMapper = Objects.requireNonNull(actionSpecMapper);
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
     * Get the list of action statisitcs by multiple uuids using query parameters
     *
     * @param actionScopesApiInputDTO The object used to query the action statistics
     * @return
     * @throws Exception
     */
    @Override
    public List<EntityStatsApiDTO> getActionStatsByUuidsQuery(ActionScopesApiInputDTO actionScopesApiInputDTO)
        throws Exception {
        return new ArrayList<>();
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
}
