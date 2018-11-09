package com.vmturbo.api.component.external.api.service;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.ServiceEntitiesRequest;
import com.vmturbo.api.component.external.api.mapper.ActionCountsMapper;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.util.DefaultCloudGroupProducer;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.constraints.ConstraintApiDTO;
import com.vmturbo.api.constraints.ConstraintApiInputDTO;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entity.TagApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.notification.LogEntryApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.api.dto.settingspolicy.SettingsPolicyApiDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.enums.EntityDetailType;
import com.vmturbo.api.exceptions.UnauthorizedObjectException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.ActionPaginationRequest;
import com.vmturbo.api.pagination.ActionPaginationRequest.ActionPaginationResponse;
import com.vmturbo.api.serviceinterfaces.IEntitiesService;
import com.vmturbo.common.protobuf.PaginationProtoUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCategoryStatsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCategoryStatsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsResponse;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.SearchTopologyEntityDTOsRequest;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Service entrypoints to query supply chain values.
 *
 * TODO: The Repository Component will, in the future, implement a client api that will be called here, but
 * in the meanwhile, we issue HTTP requests to the Repository HTTP API:
 * GET /repository/supplychain/{oid}
 **/
public class EntitiesService implements IEntitiesService {

    Logger logger = LogManager.getLogger();

    private final ActionsServiceBlockingStub actionOrchestratorRpcService;

    private final ActionSpecMapper actionSpecMapper;

    private final RepositoryApi repositoryApi;

    private final long realtimeTopologyContextId;

    private final SupplyChainFetcherFactory supplyChainFetcher;

    private final PaginationMapper paginationMapper;

    private final SearchServiceBlockingStub searchServiceRpc;

    private final EntityAspectMapper entityAspectMapper;

    public EntitiesService(@Nonnull final ActionsServiceBlockingStub actionOrchestratorRpcService,
                           @Nonnull final ActionSpecMapper actionSpecMapper,
                           @Nonnull final RepositoryApi repositoryApi,
                           final long realtimeTopologyContextId,
                           @Nonnull final SupplyChainFetcherFactory supplyChainFetcher,
                           @Nonnull final PaginationMapper paginationMapper,
                           @Nonnull final SearchServiceBlockingStub searchServiceRpc,
                           @Nonnull final EntityAspectMapper entityAspectMapper) {
        this.actionOrchestratorRpcService = Objects.requireNonNull(actionOrchestratorRpcService);
        this.actionSpecMapper = Objects.requireNonNull(actionSpecMapper);
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.supplyChainFetcher = Objects.requireNonNull(supplyChainFetcher);
        this.paginationMapper = Objects.requireNonNull(paginationMapper);
        this.searchServiceRpc = Objects.requireNonNull(searchServiceRpc);
        this.entityAspectMapper = Objects.requireNonNull(entityAspectMapper);
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
    public ActionPaginationResponse getActionsByEntityUuid(String uuid,
                                       ActionApiInputDTO inputDto,
                                       ActionPaginationRequest paginationRequest) throws Exception {
        // The search will be on a long value, not a String
        final long entityId = Long.valueOf(uuid);

        final ActionQueryFilter filter = actionSpecMapper.createActionFilter(
                                     inputDto, Optional.of(Collections.singleton(entityId)));

        final FilteredActionResponse response = actionOrchestratorRpcService.getAllActions(
                FilteredActionRequest.newBuilder()
                        .setTopologyContextId(realtimeTopologyContextId)
                        .setFilter(filter)
                        .setPaginationParams(paginationMapper.toProtoParams(paginationRequest))
                        .build());

        final List<ActionApiDTO> results = actionSpecMapper.mapActionSpecsToActionApiDTOs(
            response.getActionsList().stream()
                .map(ActionOrchestratorAction::getActionSpec)
                .collect(Collectors.toList()), realtimeTopologyContextId);
        return PaginationProtoUtil.getNextCursor(response.getPaginationResponse())
                .map(nextCursor -> paginationRequest.nextPageResponse(results, nextCursor))
                .orElseGet(() -> paginationRequest.finalPageResponse(results));
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
        if (!path) {
            // TODO: Get all groups which the service entity is member of
            throw ApiUtils.notImplementedInXL();
        }
        final List<String> entityTypes = Lists.newArrayList(
                ServiceEntityMapper.toUIEntityType(EntityType.DATACENTER_VALUE),
                ServiceEntityMapper.toUIEntityType(EntityType.CHASSIS_VALUE));
        final SupplychainApiDTO supplyChain = supplyChainFetcher.newApiDtoFetcher()
                .topologyContextId(realtimeTopologyContextId)
                .addSeedUuids(Lists.newArrayList(uuid))
                .entityTypes(entityTypes)
                .includeHealthSummary(false)
                .entityDetailType(EntityDetailType.entity)
                .fetch();
        final Set<Long> entityIds = supplyChain.getSeMap().entrySet().stream()
                .map(Entry::getValue)
                .flatMap(supplyChainEntryDTO -> supplyChainEntryDTO.getInstances().keySet().stream())
                .map(Long::valueOf)
                .collect(Collectors.toSet());

        if (entityIds.isEmpty()) {
            return Collections.emptyList();
        }

        final Map<Long, ServiceEntityApiDTO> serviceEntityMap =
                repositoryApi.getServiceEntitiesById(
                        ServiceEntitiesRequest.newBuilder(entityIds)
                                .build())
                        .entrySet().stream()
                        .filter(entry -> entry.getValue().isPresent())
                        .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().get()));
        return Lists.newArrayList(serviceEntityMap.values());
    }

    @Override
    public List<StatSnapshotApiDTO> getActionCountStatsByUuid(String uuid,
                                                              ActionApiInputDTO inputDto)
            throws Exception {

        try {

            // Handle cloud stats
           // TODO : We need to support cloud stats for all scopes e.g. Stats for a group of 2 AWS VM entities.
            if (uuid.equals(DefaultCloudGroupProducer.ALL_CLOULD_WORKLOAD_AWS_AND_AZURE_UUID) &&
                    !inputDto.getGroupBy().isEmpty() && inputDto.getGroupBy().get(0).equals(StringConstants.RISK_SUB_CATEGORY)) {
                    GetActionCategoryStatsResponse response =
                            actionOrchestratorRpcService.getActionCategoryStats(
                                    GetActionCategoryStatsRequest.newBuilder()
                                            .setTopologyContextId(realtimeTopologyContextId)
                                            .addEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                                            .addEntityType(EntityType.DATABASE_VALUE)
                                            .addEntityType(EntityType.DATABASE_SERVER_VALUE)
                                            .build());
                    List<StatSnapshotApiDTO> statSnapshotApiDTOS =
                            ActionCountsMapper.convertActionCategoryStatsToApiStatSnapshot(response.getActionStatsByCategoryList());
                    if (inputDto.getStartTime()!=null && inputDto.getEndTime()!=null) {
                        // If the request is for ProjectedActions, set all numEntities to zero.
                        // This hack is needed because UI expects it in this format.
                        statSnapshotApiDTOS.stream()
                                .flatMap(dto -> dto.getStatistics().stream())
                                .filter(dto -> dto.getName() == StringConstants.NUM_ENTITIES)
                                .forEach(statApiDTO -> {
                                    final StatValueApiDTO valueDto = new StatValueApiDTO();
                                    float statValue = 0;
                                    valueDto.setAvg(statValue);
                                    valueDto.setMax(statValue);
                                    valueDto.setMin(statValue);
                                    valueDto.setTotal(statValue);
                                    statApiDTO.setValues(valueDto);
                                });
                    }
                    return statSnapshotApiDTOS;
            }

            final long entityId = Long.valueOf(uuid);
            final ActionQueryFilter filter =
                    actionSpecMapper.createActionFilter(inputDto,
                            Optional.of(Collections.singleton(entityId)));
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
        } catch (NumberFormatException e) {
            // TODO Remove it when default Cloud group is supported
            return Collections.emptyList();
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
    public boolean loggingEntities(final List<String> arrayList) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public boolean loggingEntities() {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<SettingsPolicyApiDTO> getSettingPoliciesByEntityUuid(String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public Map<String, EntityAspect> getAspectsByEntityUuid(String uuid) throws UnauthorizedObjectException, UnknownObjectException {
        return entityAspectMapper.getAspectsByEntity(getTopologyEntityDTO(uuid));
    }

    @Override
    public EntityAspect getAspectByEntityUuid(String uuid, String aspectTag) throws UnauthorizedObjectException, UnknownObjectException {
        return entityAspectMapper.getAspectByEntity(getTopologyEntityDTO(uuid), aspectTag);
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

    @Override
    public List<ConstraintApiDTO> getConstraintsByEntityUuid(String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ServiceEntityApiDTO> getPotentialEntitiesByEntity(String uuid, ConstraintApiInputDTO inputDto) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    /**
     * Get TopologyEntityDTO based on provided oid.
     */
    private TopologyEntityDTO getTopologyEntityDTO(@Nonnull String uuid) throws UnknownObjectException {
        List<TopologyEntityDTO> entities = searchServiceRpc.searchTopologyEntityDTOs(
                SearchTopologyEntityDTOsRequest.newBuilder()
                        .addEntityOid(Long.valueOf(uuid))
                        .build()).getTopologyEntityDtosList();
        if (entities.size() > 1) {
            throw new UnknownObjectException("Found " + entities.size() + " entities of same id: " + uuid);
        }
        if (entities.size() == 0) {
            throw new UnknownObjectException("Entity: " + uuid + " not found");
        }
        return entities.get(0);
    }
}


