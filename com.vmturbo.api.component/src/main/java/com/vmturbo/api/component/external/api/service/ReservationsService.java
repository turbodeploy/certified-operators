package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import com.vmturbo.api.component.external.api.mapper.ReservationMapper;
import com.vmturbo.api.component.external.api.mapper.ReservationMapper.PlacementInfo;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.reservation.DemandReservationApiDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiInputDTO;
import com.vmturbo.api.enums.ReservationAction;
import com.vmturbo.api.enums.ReservationEditAction;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.IReservationsService;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.InitialPlacementRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.InitialPlacementResponse;
import com.vmturbo.common.protobuf.plan.PlanDTO.OptionalPlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceFutureStub;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceBlockingStub;

/**
 * XL implementation of IReservationAndDeployService
 **/
public class ReservationsService implements IReservationsService {
    private static final Logger logger = LogManager.getLogger();

    private final ReservationServiceBlockingStub reservationService;

    private final ReservationMapper reservationMapper;

    private final PlanServiceFutureStub planServiceFutureStub;

    private final PlanServiceBlockingStub planServiceBlockingStub;

    private final ActionsServiceBlockingStub actionOrchestratorService;

    private final long initialPlacementTimeoutSeconds;

    private final long THREAD_SLEEP_INTERVAL_MS = 500;

    public ReservationsService(@Nonnull final ReservationServiceBlockingStub reservationService,
                               @Nonnull final ReservationMapper reservationMapper,
                               final long initialPlacementTimeoutSeconds,
                               @Nonnull final PlanServiceBlockingStub planServiceBlockingStub,
                               @Nonnull final PlanServiceFutureStub planServiceFutureStub,
                               @Nonnull final ActionsServiceBlockingStub actionOrchestratorService) {
        this.reservationService = Objects.requireNonNull(reservationService);
        this.reservationMapper = Objects.requireNonNull(reservationMapper);
        this.initialPlacementTimeoutSeconds = initialPlacementTimeoutSeconds;
        this.planServiceFutureStub = Objects.requireNonNull(planServiceFutureStub);
        this.planServiceBlockingStub = Objects.requireNonNull(planServiceBlockingStub);
        this.actionOrchestratorService = Objects.requireNonNull(actionOrchestratorService);
    }

    @Override
    public List<DemandReservationApiDTO> getAllReservations(Map<String, String> queryParams)
                    throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public DemandReservationApiDTO getReservationByID(String reservationID) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public DemandReservationApiDTO createReservationForDemand(
            @Nonnull Boolean apiCallBlock,
            @Nonnull ReservationAction demandAction,
            @Nonnull DemandReservationApiInputDTO demandApiInputDTO) throws Exception {
        switch (demandAction) {
            case PLACEMENT:
                final List<ScenarioChange> scenarioChange =
                        reservationMapper.placementToScenarioChange(demandApiInputDTO.getParameters());
                return processInitialPlacement(scenarioChange);
            case RESERVATION:
                throw ApiUtils.notImplementedInXL();
            case DEPLOYMENT:
                throw ApiUtils.notImplementedInXL();
            default:
                throw new UnsupportedOperationException("Invalid action " + demandAction);
        }
    }

    @Override
    public DemandReservationApiDTO doActionOnReservationByID(Boolean callBlock,
                                                             ReservationEditAction action, String reservationID) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public Boolean deleteReservationByID(String reservationID) {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public DemandReservationApiDTO deployReservationByID(Boolean callBlock, String reservationID)
                    throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    /**
     * Because UI initial placement call is synchronous, but in XL, Market analysis is asynchronous.
     * This method will continuously check initial placement status until it succeed or failed.
     * At the end, it will send a delete request to delete initial placement results.
     *
     * @param scenarioChanges a list of {@link ScenarioChange} contains required parameters for
     *                        initial placement.
     * @return {@link DemandReservationApiDTO}.
     * @throws OperationFailedException
     * @throws UnknownObjectException
     */
    private DemandReservationApiDTO processInitialPlacement(@Nonnull final List<ScenarioChange> scenarioChanges)
            throws OperationFailedException, UnknownObjectException {
        // send request to run initial placement
        final long planId = sendRequestForInitialPlacement(scenarioChanges);
        try {
            //TODO: After UI change initial placement call to asynchronous, we should use UI notification
            // to send initial placement results back to UI.
            final CompletableFuture<Boolean> initialPlacementFuture = CompletableFuture.supplyAsync(() -> {
                try {
                    return waitingInitialPlacementFinish(planId);
                } catch (InterruptedException | UnknownObjectException | ExecutionException e) {
                    return false;
                }
            }, Executors.newSingleThreadExecutor());
            final boolean isSucceed = initialPlacementFuture.get(initialPlacementTimeoutSeconds,
                    TimeUnit.SECONDS);
            if (!isSucceed) {
                logger.error("Initial placement operation failed: {}.", planId);
                throw new OperationFailedException("Initial placement operation failed.");
            }
            final List<PlacementInfo> placementInfos = getPlacementResults(planId);
            // get topology addition from list of scenarioChanges.
            final TopologyAddition topologyAddition = getTopologyAddition(scenarioChanges);
            DemandReservationApiDTO demandReservationApiDTO =
                    reservationMapper.convertToDemandReservationApiDTO(topologyAddition, placementInfos);
            return demandReservationApiDTO;
        } catch (TimeoutException e) {
            logger.error("Initial placement: {} operation timeout. Error: {}", planId,
                    e.getMessage());
            throw new OperationFailedException("Initial placement operation time out.");
        }
        catch (InterruptedException | ExecutionException e) {
            logger.error("Initial placement: {} operation failed. Error: {}", planId,
                    e.getMessage());
            throw new OperationFailedException("Initial placement operation failed.");
        } finally {
            // TODO: There is bug (OM-28557) that when plan finished, if immediately delete this plan
            // will cause delete operation stuck. At here uses future stub instead of blocking stub
            // will avoid this stuck.
            planServiceFutureStub.deletePlan(PlanId.newBuilder()
                    .setPlanId(planId)
                    .build());
        }
    }

    private long sendRequestForInitialPlacement(@Nonnull final List<ScenarioChange> scenarioChanges) {
        InitialPlacementRequest request = InitialPlacementRequest.newBuilder()
                .setScenarioInfo(ScenarioInfo.newBuilder()
                        .addAllChanges(scenarioChanges))
                .build();
        InitialPlacementResponse response = reservationService.initialPlacement(request);
        return response.getPlanId();
    }

    private TopologyAddition getTopologyAddition(
            @Nonnull final List<ScenarioChange> scenarioChanges) throws OperationFailedException {
        return scenarioChanges.stream()
                .filter(scenarioChange -> scenarioChange.hasTopologyAddition())
                .findFirst()
                .map(scenarioChange -> scenarioChange.getTopologyAddition())
                .orElseThrow(() -> {
                    logger.error("Can not find topology addition for scenario change: {}",
                            scenarioChanges);
                    return new OperationFailedException("Initial placement operation failed.");
                });
    }

    /**
     * Continuously checking if initial placement plan is succeeded or failed.
     *
     * @param planId id of initial placement plan.
     * @return boolean, true means plan succeeded, false means plan failed.
     * @throws InterruptedException
     * @throws UnknownObjectException
     */
    private boolean waitingInitialPlacementFinish(final long planId)
            throws InterruptedException, UnknownObjectException, ExecutionException {
        while (true) {
            // thread sleep in order to avoid too many requests to send
            Thread.sleep(THREAD_SLEEP_INTERVAL_MS);
            final OptionalPlanInstance planInstance = planServiceBlockingStub.getPlan(PlanId.newBuilder()
                    .setPlanId(planId)
                    .build());
            if (!planInstance.hasPlanInstance()) {
                throw new UnknownObjectException("Plan is not found!");
            }
            if (planInstance.getPlanInstance().getStatus() == PlanStatus.SUCCEEDED ||
                    planInstance.getPlanInstance().getStatus() == PlanStatus.FAILED) {
                return planInstance.getPlanInstance().getStatus() == PlanStatus.SUCCEEDED;
            }
        }
    }

    /**
     * After initial placement finished, this method will query Action Orchestrator to get placement
     * results, it will based on "Move" action to determine if there is any entity been placed, and
     * which ones are their providers.
     *
     * @param planId id of initial placement plan.
     * @return a list of {@link PlacementInfo}.
     */
    private List<PlacementInfo> getPlacementResults(final long planId) {
        final Iterable<ActionOrchestratorAction> response = () ->
                actionOrchestratorService.getAllActions(FilteredActionRequest.newBuilder()
                        .setTopologyContextId(planId)
                        .build());
        final Multimap<Long, Long> entityToProviders = ArrayListMultimap.create();
        StreamSupport.stream(response.spliterator(), false)
                .map(ActionOrchestratorAction::getActionSpec)
                .map(ActionSpec::getRecommendation)
                .map(Action::getInfo)
                .filter(actionInfo -> actionInfo.getActionTypeCase().equals(ActionTypeCase.MOVE))
                .map(ActionInfo::getMove)
                .forEach(move -> entityToProviders.put(move.getTargetId(), move.getDestinationId()));
        final List<PlacementInfo> placementInfos = createPlacementInfos(entityToProviders);
        return placementInfos;
    }

    private List<PlacementInfo> createPlacementInfos(@Nonnull Multimap<Long, Long> entityToProviders) {
        final List<PlacementInfo> placementInfoList = new ArrayList<>();
        entityToProviders.asMap().entrySet().stream()
                .forEach(entry -> {
                    final PlacementInfo placementInfo = new PlacementInfo(entry.getKey(),
                            ImmutableList.copyOf(entry.getValue()));
                    placementInfoList.add(placementInfo);
                });
        return placementInfoList;
    }
}
