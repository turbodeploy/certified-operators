package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import com.vmturbo.api.MarketNotificationDTO.MarketNotification;
import com.vmturbo.api.MarketNotificationDTO.StatusNotification;
import com.vmturbo.api.MarketNotificationDTO.StatusNotification.Status;
import com.vmturbo.api.component.external.api.mapper.ActionCountsMapper;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.MarketMapper;
import com.vmturbo.api.component.external.api.mapper.PolicyMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.websocket.UINotificationChannel;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.market.MarketApiDTO;
import com.vmturbo.api.dto.notification.LogEntryApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiInputDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.enums.MergePolicyType;
import com.vmturbo.api.enums.PolicyType;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.IMarketsService;
import com.vmturbo.api.utils.ParamStrings.MarketOperations;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsResponse;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyResponse;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.CreatePlanRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.GetPlansOptions;
import com.vmturbo.common.protobuf.plan.PlanDTO.OptionalPlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;

/**
 * Service implementation of Markets.
 **/
public class MarketsService implements IMarketsService {
    private final Logger logger = LogManager.getLogger();

    private final PlanServiceBlockingStub planRpcService;

    private final ActionSpecMapper actionSpecMapper;

    private final UuidMapper uuidMapper;

    private final ActionsServiceBlockingStub actionRpcService;

    private final PolicyServiceBlockingStub policyRpcService;

    private final GroupServiceBlockingStub groupRpcService;

    private final PolicyMapper policyMapper;

    private final MarketMapper marketMapper;

    private final UINotificationChannel uiNotificationChannel;

    public MarketsService(@Nonnull final ActionSpecMapper actionSpecMapper,
                          @Nonnull final UuidMapper uuidMapper,
                          @Nonnull final ActionsServiceBlockingStub actionRpcService,
                          @Nonnull final PolicyServiceBlockingStub policyRpcService,
                          @Nonnull final PlanServiceBlockingStub planRpcService,
                          @Nonnull final PolicyMapper policyMapper,
                          @Nonnull final MarketMapper marketMapper,
                          @Nonnull final GroupServiceBlockingStub groupRpcService,
                          @Nonnull final UINotificationChannel uiNotificationChannel) {
        this.actionSpecMapper = Objects.requireNonNull(actionSpecMapper);
        this.uuidMapper = Objects.requireNonNull(uuidMapper);
        this.actionRpcService = Objects.requireNonNull(actionRpcService);
        this.policyRpcService = Objects.requireNonNull(policyRpcService);
        this.planRpcService = Objects.requireNonNull(planRpcService);
        this.policyMapper = Objects.requireNonNull(policyMapper);
        this.marketMapper = Objects.requireNonNull(marketMapper);
        this.uiNotificationChannel = Objects.requireNonNull(uiNotificationChannel);
        this.groupRpcService = Objects.requireNonNull(groupRpcService);
    }

    /**
     * Get all markets/plans from the plan orchestrator.
     *
     * @param scopeUuids this argument is currently ignored.
     * @return list of all markets
     */
    @Override
    @Nonnull
    public List<MarketApiDTO> getMarkets(@Nullable List<String> scopeUuids) {
        logger.debug("Get all markets in scopes: {}", scopeUuids);
        final Iterator<PlanInstance> plans =
                planRpcService.getAllPlans(GetPlansOptions.getDefaultInstance());
        final List<MarketApiDTO> result = new ArrayList<>();
        while (plans.hasNext()) {
            final PlanInstance plan = plans.next();
            final MarketApiDTO dto = marketMapper.dtoFromPlanInstance(plan);
            result.add(dto);
        }
        return result;
    }

    @Override
    public MarketApiDTO getMarketByUuid(String uuid) throws UnknownObjectException {
        final ApiId apiId = uuidMapper.fromUuid(uuid);
        if (apiId.isRealtimeMarket()) {
            // TODO (roman, Dec 21 2016): Not sure what the API
            // expects from the real-time market as far as state,
            // unplaced entities, state progress, and all that jazz.
            MarketApiDTO realtimeDto = new MarketApiDTO();
            realtimeDto.setUuid(apiId.uuid());
            realtimeDto.setState("SUCCEEDED");
            realtimeDto.setStateProgress(100);
            return realtimeDto;
        } else {
            OptionalPlanInstance response = planRpcService.getPlan(PlanId.newBuilder()
                    .setPlanId(Long.valueOf(uuid))
                    .build());
            if (!response.hasPlanInstance()) {
                throw new UnknownObjectException(uuid);
            }
            return marketMapper.dtoFromPlanInstance(response.getPlanInstance());
        }
    }

    @Override
    public MarketApiDTO executeOperation(String uuid, MarketOperations op) throws Exception {
        logger.debug("Trying to execute operation {} on {}.", op, uuid);
        if (op.equals(MarketOperations.save)) {
            // Not sure what to do with this in XL, since everything is saved automatically.
            return getMarketByUuid(uuid);
        } else if (op.equals(MarketOperations.stop)) {
            throw ApiUtils.notImplementedInXL();
        }
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<LogEntryApiDTO> getNotificationsByMarketUuid(String uuid, String starttime, String endtime, String filters) throws Exception {
        logger.debug("Get notifications from market {} with start time {} and end time {}",
                uuid, starttime, endtime);
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public LogEntryApiDTO getNotificationByMarketUuid(String uuid, String notificationUuid) throws Exception {
        logger.debug("Get notification {} from market {}", notificationUuid, uuid);
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ActionApiDTO> getCurrentActionsByMarketUuid(String uuid, EnvironmentType environmentType) throws Exception {
        ActionApiInputDTO inputDto = new ActionApiInputDTO();
        inputDto.setEnvironmentType(environmentType);
        return getActionsByMarketUuid(uuid, inputDto);
    }

    @Override
    public List<ActionApiDTO> getActionsByMarketUuid(String uuid,
                                                     ActionApiInputDTO inputDto) throws Exception {
        final ApiId apiId = uuidMapper.fromUuid(uuid);
        ActionQueryFilter filter =
                actionSpecMapper.createActionFilter(inputDto, Optional.empty());
        final Iterable<ActionOrchestratorAction> result =
                () -> actionRpcService.getAllActions(FilteredActionRequest.newBuilder()
                        .setTopologyContextId(apiId.oid())
                        .setFilter(filter)
                        .build());
        return actionSpecMapper.mapActionSpecsToActionApiDTOs(
            StreamSupport.stream(result.spliterator(), false)
                .filter(ActionOrchestratorAction::hasActionSpec)
                .map(ActionOrchestratorAction::getActionSpec)
                .collect(Collectors.toList()));
    }

    @Override
    public ActionApiDTO getActionByMarketUuid(String uuid, String aUuid) throws Exception {
        logger.debug("Request to get action by market UUID: {}, action ID: {}", uuid, aUuid);
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ServiceEntityApiDTO> getEntitiesByMarketUuid(String uuid) throws Exception {
        logger.debug("Request to get entities by market UUID: {}", uuid);
        throw ApiUtils.notImplementedInXL();
    }

    // Market UUID is ignored for now, since there is only one Market in XL.
    @Override
    public List<PolicyApiDTO> getPoliciesByMarketUuid(String uuid) throws Exception {
        try {
            final Iterator<PolicyDTO.PolicyResponse> allPolicyResps = policyRpcService
                    .getAllPolicies(PolicyDTO.PolicyRequest.getDefaultInstance());
            ImmutableList<PolicyResponse> policyRespList = ImmutableList.copyOf(allPolicyResps);

            Set<Long> groupingIDS = policyRespList.stream()
                    .filter(PolicyDTO.PolicyResponse::hasPolicy)
                    .flatMap(resp -> GroupProtoUtil.getPolicyGroupIds(resp.getPolicy()).stream())
                    .collect(Collectors.toSet());
            final Map<Long, Group> groupings = new HashMap<>();
            groupRpcService.getGroups(GetGroupsRequest.newBuilder()
                    .addAllId(groupingIDS)
                    .build())
                .forEachRemaining(group -> groupings.put(group.getId(), group));
            return policyRespList.stream()
                    .filter(PolicyDTO.PolicyResponse::hasPolicy)
                    .map(resp -> policyMapper.policyToApiDto(resp.getPolicy(), groupings))
                    .collect(Collectors.toList());
        } catch (RuntimeException e) {
            logger.error("Problem getting policies", e);
            throw e;
        }
    }

    @Override
    public PolicyApiDTO addPolicy(final String uuid,
                                  final String policyName,
                                  final List<String> uuids,
                                  final PolicyType type,
                                  final Integer maxCapacity,
                                  final Boolean enable,
                                  final MergePolicyType mergeType) throws Exception {
        try {
            // Reconstruct the PolicyApiInputDTO, it was deconstructed in the @Controller layer. :S
            PolicyApiInputDTO policyApiInputDTO = new PolicyApiInputDTO();
            policyApiInputDTO.setPolicyName(policyName);
            policyApiInputDTO.setType(type);
            policyApiInputDTO.setCapacity(maxCapacity);
            policyApiInputDTO.setEnabled(enable);
            policyApiInputDTO.setMergeType(mergeType);
            if (PolicyType.MERGE.equals(type)) {
                policyApiInputDTO.setMergeUuids(uuids);
            } else {
                if (uuids != null && uuids.size() == 2) {
                    policyApiInputDTO.setSellerUuid(uuids.get(0));
                    policyApiInputDTO.setBuyerUuid(uuids.get(1));
                } else {
                    throw new RuntimeException("The policy must contain the provider and consumer group IDs");
                }
            }

            final PolicyDTO.InputPolicy inputPolicy = policyMapper.policyApiInputDtoToProto(policyApiInputDTO);
            final PolicyDTO.PolicyCreateRequest createRequest = PolicyDTO.PolicyCreateRequest.newBuilder()
                    .setInputPolicy(inputPolicy)
                    .build();
            final PolicyDTO.PolicyCreateResponse policyCreateResp = policyRpcService.createPolicy(createRequest);
            final long createdPolicyID = policyCreateResp.getPolicyId();

            // Confirmed with the UI team that the UI doesn't really use the return value.
            // In the future, if they need to use it, we can change the `createPolicy` to return the policy.
            final PolicyApiDTO response = new PolicyApiDTO();
            response.setUuid(Long.toString(createdPolicyID));

            return response;
        } catch (RuntimeException e) {
            logger.error("Error while adding a policy with name " + policyName, e);
            throw e;
        }
    }

    @Override
    public PolicyApiDTO editPolicy(final String uuid,
                                   final String policyUuid,
                                   final String policyName,
                                   final List<String> uuids,
                                   final PolicyType type,
                                   final Integer maxCapacity,
                                   final Boolean enable) throws Exception {
        try {
            // Reconstruct the PolicyApiInputDTO, it was deconstructed in the @Controller layer. :S
            // Discussed with an UI team member, they will pass in the DTO rather than individual fields.
            // Also, edit is not completely functional because `mergeType` isn't being passed in.
            PolicyApiInputDTO policyApiInputDTO = new PolicyApiInputDTO();
            policyApiInputDTO.setPolicyName(policyName);
            policyApiInputDTO.setType(type);
            policyApiInputDTO.setCapacity(maxCapacity);
            policyApiInputDTO.setEnabled(enable);
            // policyApiInputDTO.setMergeType(mergeType);
            if (PolicyType.MERGE.equals(type)) {
                policyApiInputDTO.setMergeUuids(uuids);
            } else {
                if (uuids != null && uuids.size() == 2) {
                    policyApiInputDTO.setSellerUuid(uuids.get(0));
                    policyApiInputDTO.setBuyerUuid(uuids.get(1));
                } else {
                    throw new RuntimeException("The policy must contain the provider and consumer group IDs");
                }
            }

            final PolicyDTO.InputPolicy inputPolicy = policyMapper.policyApiInputDtoToProto(policyApiInputDTO);
            final PolicyDTO.PolicyEditRequest policyEditRequest = PolicyDTO.PolicyEditRequest.newBuilder()
                    .setInputPolicy(inputPolicy)
                    .setPolicyId(Long.valueOf(policyUuid))
                    .build();
            policyRpcService.editPolicy(policyEditRequest);

            return new PolicyApiDTO();
        } catch (RuntimeException e) {
            logger.error("Fail to edit policy " + policyUuid + " with name " + policyName, e);
            throw e;
        }
    }

    @Override
    // uuid is not used because there is only one Market in XL.
    public Boolean deletePolicy(String uuid, String policy_uuid) throws Exception {
        final PolicyDTO.PolicyDeleteRequest policyDeleteRequest = PolicyDTO.PolicyDeleteRequest.newBuilder()
                .setPolicyId(Long.valueOf(policy_uuid))
                .build();

        try {
            policyRpcService.deletePolicy(policyDeleteRequest);
            return true;
        } catch (RuntimeException e) {
            logger.error("Fail to delete policy " + policy_uuid, e);
            return false;
        }
    }

    @Override
    public List<DemandReservationApiDTO> getReservationsByMarketUuid(String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public MarketApiDTO deleteMarketByUuid(String uuid) throws Exception {
        logger.debug("Deleting market with UUID: {}", uuid);
        // TODO (roman, June 16 2017) OM-20328: Plan deletion is currently synchronous in XL.
        // The delete plan call won't return until the plan actually got deleted. The API
        // treats it asynchronously, which is why it expects the DELETING -> DELETED status updates
        // over websocket. We should switch to doing it asynchronously as well, to give the
        // Plan Orchestrator more room to deal with the various parts of deleting a plan and
        // error-handling (e.g. if the Action Orchestrator is down and we can't erase the plan's
        // action plan).
        try {
            // We don't broadcast a DELETING notification, because that significantly complicates
            // error handling from the RPC call. If the deletePlan method returned an exception,
            // what's the state of the plan? It may be deleting (i.e. partially deleted) or
            // fully intact.
            final PlanInstance oldPlanInstance = planRpcService.deletePlan(PlanId.newBuilder()
                    .setPlanId(Long.parseLong(uuid))
                    .build());
            uiNotificationChannel.broadcastMarketNotification(MarketNotification.newBuilder()
                    .setMarketId(uuid)
                    .setStatusNotification(StatusNotification.newBuilder()
                            .setStatus(Status.DELETED))
                    .build());
            return marketMapper.dtoFromPlanInstance(oldPlanInstance);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Code.NOT_FOUND)) {
                throw new UnknownObjectException(e.getStatus().getDescription());
            } else {
                throw e;
            }
        }
    }

    @Override
    public MarketApiDTO applyAndRunScenario(@Nonnull String marketUuid,
                                            @Nonnull Long scenarioId,
                                            @Nullable Boolean ignoreConstraints,
                                            // TODO (roman, June 6 2017): The plan market name
                                            // should be the display name of the plan. However,
                                            // the UI doesn't currently use it, so postponing
                                            // this for later.
                                            @Nullable String planMarketName) throws Exception {
        logger.info("Running scenario. Market UUID: {}, Scenario ID: {}, " +
                        "Ignore Constraints: {}, Plan Market Name: {}",
                marketUuid, scenarioId, ignoreConstraints, planMarketName);

        // note that, for XL, the "marketUuid" in the request is interpreted as the Plan Instance ID
        final ApiId planInstanceId;
        // todo: The 'ignoreConstraints' parameter will move into the Scenario in OM-18012
        try {
            planInstanceId = uuidMapper.fromUuid(marketUuid);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid market id: " + marketUuid);
        }
        final PlanInstance planInstance;
        if (planInstanceId.isRealtimeMarket()) {
            // for realtime market create a new plan; topologyId is not set, defaulting to "0"
            planInstance = planRpcService.createPlan(CreatePlanRequest.newBuilder()
                    .setScenarioId(scenarioId)
                    .build());
        } else {
            // plan market: create new plan where source topology is the prev projected topology
            PlanDTO.PlanScenario planScenario = PlanDTO.PlanScenario.newBuilder()
                    .setPlanId(planInstanceId.oid())
                    .setScenarioId(scenarioId)
                    .build();
            planInstance = planRpcService.createPlanOverPlan(planScenario);
        }
        final PlanInstance updatedInstance = planRpcService.runPlan(PlanId.newBuilder()
                .setPlanId(planInstance.getPlanId())
                .build());
        return marketMapper.dtoFromPlanInstance(updatedInstance);
    }

    @Override
    public List<StatSnapshotApiDTO> getStatsByMarketUuid(final String uuid, final String encodedQuery) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<StatSnapshotApiDTO> getStatsByMarketQuery(final String uuid,
                                                          final StatPeriodApiInputDTO inputDto)
            throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<StatSnapshotApiDTO> getActionCountStatsByUuid(String uuid, ActionApiInputDTO actionApiInputDTO) throws Exception {
        final ApiId apiId = uuidMapper.fromUuid(uuid);
        final ActionQueryFilter filter =
                actionSpecMapper.createActionFilter(actionApiInputDTO, Optional.empty());
        try {
            final GetActionCountsResponse actionCountsResponse =
                    actionRpcService.getActionCounts(GetActionCountsRequest.newBuilder()
                            .setTopologyContextId(apiId.oid())
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
                                                        final ActionApiInputDTO actionApiInputDTO)
                throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ServiceEntityApiDTO> getUnplacedEntitiesByMarketUuid(String uuid) throws Exception {
        // This has to return something instead of throwing an error,
        // because if this throws an error the entire Environment Summary
        // widget doesn't render (which includes the before-and-after entity
        // counts).
        return Collections.emptyList();
    }
}
