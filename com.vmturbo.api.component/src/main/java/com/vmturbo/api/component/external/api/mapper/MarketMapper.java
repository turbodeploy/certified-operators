package com.vmturbo.api.component.external.api.mapper;

import static com.vmturbo.api.MarketNotificationDTO.StatusNotification.Status.CREATED;
import static com.vmturbo.api.MarketNotificationDTO.StatusNotification.Status.READY_TO_START;
import static com.vmturbo.api.MarketNotificationDTO.StatusNotification.Status.RUNNING;
import static com.vmturbo.api.MarketNotificationDTO.StatusNotification.Status.STOPPED;
import static com.vmturbo.api.MarketNotificationDTO.StatusNotification.Status.SUCCEEDED;

import java.util.Collections;
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

/**
 * Converts {@link PlanInstance} objects to the plan-related API objects - namely
 * {@link MarketApiDTO} and {@link MarketNotification}.
 */
public class MarketMapper {

    private final ScenarioMapper scenarioMapper;

    public static final String MARKET = "Market";

    public MarketMapper(@Nonnull final ScenarioMapper scenarioMapper) {
        this.scenarioMapper = Objects.requireNonNull(scenarioMapper);
    }

    @Nonnull
    public MarketApiDTO dtoFromPlanInstance(@Nonnull final PlanInstance instance)
            throws ConversionException, InterruptedException {
        final MarketApiDTO retDto = new MarketApiDTO();
        retDto.setClassName(MARKET);
        retDto.setUuid(Long.toString(instance.getPlanId()));
        retDto.setState(stateFromStatus(instance.getStatus()).name());
        retDto.setStateProgress(progressFromStatus(instance.getStatus()));

        final ScenarioApiDTO scenarioApiDTO =
                scenarioMapper.toScenarioApiDTO(instance.getScenario());

        UserApiDTO userApiDTO = new UserApiDTO();
        userApiDTO.setUuid(instance.getCreatedByUser());
        scenarioApiDTO.setOwners(Collections.singletonList(userApiDTO));
        retDto.setScenario(scenarioApiDTO);

        //We want the name to come from the plan
        //For backwards compatibility we fallback to the scenario name
        retDto.setDisplayName(instance.hasName() ? instance.getName() : scenarioApiDTO.getDisplayName());

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
            case SUCCEEDED: case FAILED: case STOPPED:
                return 100;
            case READY:
                return 0;
            case QUEUED:
                return 10;
            case CONSTRUCTING_TOPOLOGY:
                return 20;
            case RUNNING_ANALYSIS:
                return 40;
            case WAITING_FOR_RESULT:
                return 80;
            default:
                return 0;
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
