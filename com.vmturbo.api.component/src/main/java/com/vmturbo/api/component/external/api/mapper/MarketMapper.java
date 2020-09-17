package com.vmturbo.api.component.external.api.mapper;

import static com.vmturbo.api.MarketNotificationDTO.StatusNotification.Status.CREATED;
import static com.vmturbo.api.MarketNotificationDTO.StatusNotification.Status.READY_TO_START;
import static com.vmturbo.api.MarketNotificationDTO.StatusNotification.Status.RUNNING;
import static com.vmturbo.api.MarketNotificationDTO.StatusNotification.Status.STOPPED;
import static com.vmturbo.api.MarketNotificationDTO.StatusNotification.Status.SUCCEEDED;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.api.MarketNotificationDTO.MarketNotification;
import com.vmturbo.api.MarketNotificationDTO.StatusNotification;
import com.vmturbo.api.dto.market.MarketApiDTO;
import com.vmturbo.api.dto.scenario.ScenarioApiDTO;
import com.vmturbo.api.dto.user.UserApiDTO;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanStatusNotification.StatusUpdate;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProject;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProject.PlanProjectStatus;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.Scenario;

/**
 * Converts {@link PlanInstance} objects to the plan-related API objects - namely
 * {@link MarketApiDTO} and {@link MarketNotification}.
 */
public class MarketMapper {

    private final ScenarioMapper scenarioMapper;

    public static final String MARKET = "Market";

    /**
     * Progress indicator percentages.
     */
    private static final int PROGRESS_NONE = 0;
    private static final int PROGRESS_COMPLETED = 100;
    private static final int PROGRESS_JUST_STARTED = 10;
    private static final int PROGRESS_ALMOST_QUARTER = 20;
    private static final int PROGRESS_ALMOST_HALF = 40;
    private static final int PROGRESS_ALMOST_DONE = 80;

    public MarketMapper(@Nonnull final ScenarioMapper scenarioMapper) {
        this.scenarioMapper = Objects.requireNonNull(scenarioMapper);
    }

    @Nonnull
    public MarketApiDTO dtoFromPlanInstance(@Nonnull final PlanInstance instance)
            throws ConversionException, InterruptedException {
        return dtoFromPlanInstance(instance, instance.getScenario());
    }

    /**
     * Gets MarketApiDTO given the plan instance and scenario.
     *
     * @param instance Plan instance to convert to DTO.
     * @param scenario Associated plan scenario.
     * @return MarketApiDTO to be sent back to UI.
     * @throws ConversionException Thrown on some issue with data conversion to DTO.
     * @throws InterruptedException Throw on thread interruption.
     */
    @Nonnull
    public MarketApiDTO dtoFromPlanInstance(@Nonnull final PlanInstance instance,
                                            @Nonnull final Scenario scenario)
            throws ConversionException, InterruptedException {
        final MarketApiDTO retDto = new MarketApiDTO();
        retDto.setClassName(MARKET);
        retDto.setUuid(Long.toString(instance.getPlanId()));
        retDto.setState(stateFromStatus(instance.getStatus()).name());
        retDto.setStateProgress(progressFromStatus(instance.getStatus()));

        final ScenarioApiDTO scenarioApiDTO = scenarioMapper.toScenarioApiDTO(scenario);
        UserApiDTO userApiDTO = new UserApiDTO();
        userApiDTO.setUuid(instance.getCreatedByUser());
        scenarioApiDTO.setOwners(Collections.singletonList(userApiDTO));
        retDto.setScenario(scenarioApiDTO);

        //We want the name to come from the plan
        //For backwards compatibility we fallback to the scenario name
        retDto.setDisplayName(instance.hasName() ? instance.getName()
                : scenarioApiDTO.getDisplayName());

        retDto.setSaved(true);
        if (instance.hasStartTime()) {
            retDto.setRunDate(DateTimeUtil.toString(instance.getStartTime()));
        }
        if (instance.hasEndTime()) {
            retDto.setRunCompleteDate(DateTimeUtil.toString(instance.getEndTime()));
        }
        return retDto;
    }

    /**
     * Main and related plans of a plan project as well as the user scenario are converted to
     * {@link MarketApiDTO} for return to the UI.
     *
     * @param planProject Main plan project that contains the plans.
     * @param mainPlan Plan considered as primary for project. E.g Consumption plan for migration.
     * @param relatedPlans Additional optional plans that are part of the project, e.g Allocation.
     * @param scenario User created scenario info, contains id of scenario.
     * @return MarketApiDTO sent back to UI.
     * @throws ConversionException Thrown on API conversion issue.
     * @throws InterruptedException On thread interruption.
     */
    @Nonnull
    public MarketApiDTO dtoFromPlanProject(@Nonnull final PlanProject planProject,
                                           @Nonnull final PlanInstance mainPlan,
                                           @Nonnull final List<PlanInstance> relatedPlans,
                                           @Nonnull final Scenario scenario)
            throws ConversionException, InterruptedException {
        MarketApiDTO projectMarketDto = dtoFromPlanInstance(mainPlan, scenario);
        if (!relatedPlans.isEmpty()) {
            List<MarketApiDTO> relatedMarkets = new ArrayList<>();
            for (PlanInstance otherPlan : relatedPlans) {
                relatedMarkets.add(dtoFromPlanInstance(otherPlan));
            }
            projectMarketDto.setRelatedPlanMarkets(relatedMarkets);
        }
        // Overwrite the status progress with that of the whole project
        projectMarketDto.setStateProgress(progressFromStatus(planProject.getStatus()));
        projectMarketDto.setState(stateFromStatus(planProject.getStatus()).name());
        return projectMarketDto;
    }

    /**
     * Progress from plan project status.
     *
     * @param status plan project status
     * @return Current status percentage.
     */
    private static int progressFromStatus(@Nonnull final PlanProjectStatus status) {
        switch (status) {
            case SUCCEEDED:
            case FAILED:
                return PROGRESS_COMPLETED;
            case RUNNING:
                return PROGRESS_ALMOST_HALF;
            default:
                return PROGRESS_NONE;
        }
    }

    /**
     * Gets the plan project state based on the current progress status.
     *
     * @param status {@link PlanProjectStatus}
     * @return status from MarketNotificationDTO.StatusNotification.
     * @throws ConversionException Thrown on unsupported status.
     */
    @Nonnull
    private static StatusNotification.Status stateFromStatus(@Nonnull final PlanProjectStatus status)
            throws ConversionException {
        switch (status) {
            case READY:
                return CREATED;
            case RUNNING:
                return RUNNING;
            case SUCCEEDED:
                return SUCCEEDED;
            case FAILED:
                return STOPPED;
            default:
                throw new ConversionException("Unexpected plan project status: " + status);
        }
    }


    /**
     * Create a {@link MarketNotification} for a plan state transition given a plan status update.
     * @param statusUpdate The plan status update.
     * @return The {@link MarketNotification} to send to the UI.
     */
    public static MarketNotification notificationFromPlanStatus(@Nonnull final StatusUpdate statusUpdate) {
        final StatusNotification status = StatusNotification.newBuilder()
                .setProgressPercentage(progressFromStatus(statusUpdate.getNewPlanStatus()))
                .setStatus(stateFromStatus(statusUpdate.getNewPlanStatus()))
                .build();
        final MarketNotification.Builder retBuilder = MarketNotification.newBuilder();
        retBuilder.setMarketId(Long.toString(statusUpdate.getPlanId()));
        if (status.getStatus().equals(SUCCEEDED) || status.getStatus().equals(STOPPED)) {
            retBuilder.setStatusNotification(status);
        } else {
            retBuilder.setStatusProgressNotification(status);
        }
        return retBuilder.build();
    }

    private static int progressFromStatus(@Nonnull final PlanInstance.PlanStatus status) {
        switch (status) {
            case SUCCEEDED:
            case FAILED:
            case STOPPED:
                return PROGRESS_COMPLETED;
            case QUEUED:
                return PROGRESS_JUST_STARTED;
            case CONSTRUCTING_TOPOLOGY:
                return PROGRESS_ALMOST_QUARTER;
            case RUNNING_ANALYSIS:
                return PROGRESS_ALMOST_HALF;
            case WAITING_FOR_RESULT:
                return PROGRESS_ALMOST_DONE;
            default:
                // For READY state as well.
                return PROGRESS_NONE;
        }
    }

    @Nonnull
    private static StatusNotification.Status stateFromStatus(@Nonnull final PlanInstance.PlanStatus status) {
        switch (status) {
            case READY:
                return CREATED;
            case QUEUED:
                return READY_TO_START;
            case CONSTRUCTING_TOPOLOGY:
            case RUNNING_ANALYSIS:
            case WAITING_FOR_RESULT:
            case STARTING_BUY_RI:
            case BUY_RI_COMPLETED:
                return RUNNING;
            case SUCCEEDED:
                return SUCCEEDED;
            case FAILED:
            case STOPPED:
                return STOPPED;
            default:
                throw new IllegalArgumentException("Unexpected plan status: " + status);
        }
    }
}
