package com.vmturbo.api.component.external.api.service;


import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ActionCountsMapper;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.notification.LogEntryApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entity.TagApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.api.dto.settingspolicy.SettingsPolicyApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.UnauthorizedObjectException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.IEntitiesService;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsResponse;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;

/**
 * Service entrypoints to query supply chain values.
 *
 * TODO: The Repository Component will, in the future, implement a client api that will be called here, but
 * in the meanwhile, we issue HTTP requests to the Repository HTTP API:
 * GET /repository/supplychain/{oid}
 **/
public class EntitiesService implements IEntitiesService {

    Logger log = LogManager.getLogger();

    private final ActionsServiceBlockingStub actionOrchestratorRpcService;

    private final ActionSpecMapper actionSpecMapper;

    private final RepositoryApi repositoryApi;

    private final long realtimeTopologyContextId;

    public EntitiesService(@Nonnull final ActionsServiceBlockingStub actionOrchestratorRpcService,
                           @Nonnull final ActionSpecMapper actionSpecMapper,
                           @Nonnull final RepositoryApi repositoryApi,
                           final long realtimeTopologyContextId) {
        this.actionOrchestratorRpcService = Objects.requireNonNull(actionOrchestratorRpcService);
        this.actionSpecMapper = Objects.requireNonNull(actionSpecMapper);
        this.repositoryApi = repositoryApi;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    @Override
    public ServiceEntityApiDTO getEntities() throws Exception {
        throw ApiUtils.notImplementedInXL();
    }


    @Override
    public ServiceEntityApiDTO getEntityByUuid(String uuid, boolean includeAspects) throws Exception {
        // todo: implement includeAspcts
        return repositoryApi.getServiceEntityForUuid(Long.valueOf(uuid));
    }

    @Override
    public List<StatSnapshotApiDTO> getStatsByEntityUuid(String uuid,
                                                         String encodedQuery) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<LogEntryApiDTO> getNotificationsByEntityUuid(String uuid,
                                                             String starttime,
                                                             String endtime,
                                                             String category) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public LogEntryApiDTO getNotificationByEntityUuid(String uuid,
                                                      String notificationUuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    /**
     * Fetch a list of {@link ActionApiDTO} for the Actions generated for the given Service Entity uuid.
     *
     * The request for the {@link ActionSpec} objects from the Action Orchestrator for the given uuid. Map each
     * ActionSpec into the corresponding {@link ActionApiDTO}. This requires calls to the Repository component
     * to look up the displayName() for the corresponding entity, since the Action Orchestrator does not track that information.
     *
     * Note that the historical parameters are ignored. There is currently no history in the Action Orchestrator.
     *
     * Note that the filtering parameters are ignored. It would be relatively easy to implement these filters.
     *
     * @param uuid the unique id of Service Entity for which we are requesting actions.
     * @param inputDto A description of filter options on which actions to fetch.
     * @return a list of ActionApiDTOs for the Service Entity indicated by the given uuid.
     * @throws Exception if there is a communication exception
     */
    @Override
    public List<ActionApiDTO> getActionsByEntityUuid(String uuid,
                                                     ActionApiInputDTO inputDto) throws Exception {
        // The search will be on a long value, not a String
        final long entityId = Long.valueOf(uuid);

        final ActionQueryFilter filter = actionSpecMapper.createActionFilter(
                                     inputDto, Optional.of(Collections.singletonList(entityId)));

        Iterable<ActionOrchestratorAction> actions = () -> actionOrchestratorRpcService.getAllActions(
            FilteredActionRequest.newBuilder()
                .setTopologyContextId(realtimeTopologyContextId)
                .setFilter(filter)
                .build()
        );

        final Map<Long, ActionSpec> entityRelatedActionInfo = StreamSupport.stream(actions.spliterator(), false)
            .collect(Collectors.toMap(ActionOrchestratorAction::getActionId, ActionOrchestratorAction::getActionSpec));

        return actionSpecMapper.mapActionSpecsToActionApiDTOs(entityRelatedActionInfo.values());
    }

    @Override
    public ActionApiDTO getActionByEntityUuid(String uuid,
                                              String aUuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<PolicyApiDTO> getPoliciesByEntityUuid(String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    /**
     * Get the list of settings by entity.
     *
     * @param uuid The uuid of the entity.
     * @param includePolicies Include the group aspects in the response
     * @return the list of settings for the entity.
     * @throws Exception
     */
    @Override
    public List<SettingsManagerApiDTO> getSettingsByEntityUuid(String uuid, boolean includePolicies) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<StatSnapshotApiDTO> getStatsByEntityQuery(String uuid,
                                              StatPeriodApiInputDTO inputDto) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<BaseApiDTO> getGroupsByUuid(String uuid,
                                            Boolean path) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<StatSnapshotApiDTO> getActionCountStatsByUuid(String uuid,
                                                              ActionApiInputDTO inputDto)
            throws Exception {
        // The search will be on a long value, not a String
        final long entityId = Long.valueOf(uuid);

        try {
            final ActionQueryFilter filter =
                    actionSpecMapper.createActionFilter(inputDto,
                            Optional.of(Collections.singletonList(entityId)));
            final GetActionCountsResponse actionCountsResponse =
                    actionOrchestratorRpcService.getActionCounts(GetActionCountsRequest.newBuilder()
                            .setTopologyContextId(realtimeTopologyContextId)
                            .setFilter(filter)
                            .build());
            return ActionCountsMapper.countsByTypeToApi(actionCountsResponse.getCountsByTypeList());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Code.NOT_FOUND)) {
                return Collections.emptyList();
            } else {
                throw e;
            }
        }
    }

    @Override
    public List<StatSnapshotApiDTO> getNotificationCountStatsByUuid(final String s,
                                    final ActionApiInputDTO actionApiInputDTO) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<TagApiDTO> getTagsByEntityUuid(final String s) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<TagApiDTO> createTagByEntityUuid(final String s, final TagApiDTO tagApiDTO) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public void deleteTagByEntityUuid(final String s, final String s1) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public void deleteTagsByEntityUuid(final String s) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<SettingsPolicyApiDTO> getSettingPoliciesByEntityUuid(String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public Map<String, EntityAspect> getAspectsByEntityUuid(String uuid) throws UnauthorizedObjectException, UnknownObjectException {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public EntityAspect getAspectByEntityUuid(String uuid, String aspectTag) throws UnauthorizedObjectException, UnknownObjectException {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<StatSnapshotApiDTO> getStatsByEntityUuidAspect(String uuid, String aspectTag, String encodedQuery) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<StatSnapshotApiDTO> getStatsByEntityUuidAspectQuery(String uuid, String aspectTag, StatPeriodApiInputDTO inputDto) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ActionApiDTO> getCurrentActionsByEntityUuidAspect(String uuid, String aspectTag) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ActionApiDTO> getActionsByEntityUuidAspect(String uuid, String aspectTag, ActionApiInputDTO inputDto) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }
}

