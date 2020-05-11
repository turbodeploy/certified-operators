package com.vmturbo.action.orchestrator.action;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.common.protobuf.action.ActionDTO;

/**
 * The internal representation of an action execution schedule.
 */
@Immutable
public class ActionSchedule {
    /*
     * The timestamp of the next occurrence action execution window.
     */
    private final Long scheduleStartTimestamp;

    /*
     * The timestamp for when the active execution window ends if execution window is active or
     * next execution window ends otherwise.
     */
    private final Long scheduleEndTimestamp;

    /*
     * The timezone for the schedule associated to this action.
     */
    private final String scheduleTimeZoneId;

    /*
     * The OID for the schedule associated to execution window of this action.
     */
    private final long scheduleId;

    /*
     * The display name for the schedule associated with this account.
     */
    private final String scheduleDisplayName;

    /*
     * Keeps the action mode for action in the execution window.
     */
    private final ActionDTO.ActionMode executionWindowActionMode;

    /*
     * The user that have accepted this action for the execution window.
     */
    private final String acceptingUser;

    /**
     * The constructor for {@link ActionSchedule} class.
     *
     * @param scheduleStartTimestamp the timestamp of the next occurrence action execution window.
     * @param scheduleEndTimestamp the timestamp for when the active execution window ends if
     *                             execution window is active or next execution window ends
     *                             otherwise.
     * @param scheduleTimeZoneId the timezone for the schedule associated to this action.
     * @param scheduleId the ID for the schedule associated to execution window of this action.
     * @param scheduleDisplayName The display name for the schedule associated to execution window
     *                            of this action.
     * @param executionWindowActionMode the action mode for action in the execution window.
     * @param acceptingUser the user that have accepted this action for the execution window or
     *                      null otherwise.
     */
    public ActionSchedule(@Nullable Long scheduleStartTimestamp,
                          @Nullable Long scheduleEndTimestamp,
                          @Nonnull String scheduleTimeZoneId, long scheduleId,
                          @Nonnull String scheduleDisplayName,
                          @Nonnull ActionDTO.ActionMode executionWindowActionMode,
                          @Nullable String acceptingUser) {
        this.scheduleStartTimestamp = scheduleStartTimestamp;
        this.scheduleEndTimestamp = scheduleEndTimestamp;
        this.scheduleTimeZoneId = Objects.requireNonNull(scheduleTimeZoneId);
        this.scheduleId = scheduleId;
        this.scheduleDisplayName = Objects.requireNonNull(scheduleDisplayName);
        this.executionWindowActionMode = Objects.requireNonNull(executionWindowActionMode);
        this.acceptingUser = acceptingUser;
    }

    /**
     * Returns the timestamp of the next occurrence action execution window.
     * @return the timestamp of the next occurrence action execution window.
     */
    @Nullable
    public Long getScheduleStartTimestamp() {
        return this.scheduleStartTimestamp;
    }

    /**
     * Returns the timestamp for when the active execution window ends if execution window is
     * active or next execution window ends otherwise.
     * @return the timestamp for when the active execution window ends.
     */
    @Nullable
    public Long getScheduleEndTimestamp() {
        return this.scheduleEndTimestamp;
    }

    /**
     * Returns the timezone for the schedule associated to this action.
     * @return the timezone for the schedule associated to this action.
     */
    @Nonnull
    public String getScheduleTimeZoneId() {
        return this.scheduleTimeZoneId;
    }

    /**
     * Returns the ID for the schedule associated to execution window of this action.
     * @return the ID for the schedule associated to execution window of this action.
     */
    public long getScheduleId() {
        return this.scheduleId;
    }

    /**
     * Returns the display name for the schedule associated to execution window of this action.
     * @return the display name for the schedule associated to execution window of this action.
     */
    @Nonnull
    public String getScheduleDisplayName() {
        return this.scheduleDisplayName;
    }

    /**
     * Returns the action mode for action in the execution window.
     * @return the action mode for action in the execution window.
     */
    @Nonnull
    public ActionDTO.ActionMode getExecutionWindowActionMode() {
        return executionWindowActionMode;
    }

    /**
     * Returns the user that have accepted this action for the execution window or null otherwise.
     * @return the user that have accepted this action for the execution window.
     */
    @Nullable
    public String getAcceptingUser() {
        return acceptingUser;
    }

    /**
     * Generates the protobuf translation of this object.
     *
     * @return the protobuf translation of this object.
     */
    @Nonnull
    public ActionDTO.ActionSpec.ActionSchedule getTranslation() {
        ActionDTO.ActionSpec.ActionSchedule.Builder actionScheduleBuilder =
            ActionDTO.ActionSpec.ActionSchedule.newBuilder()
            .setScheduleTimezoneId(scheduleTimeZoneId)
            .setScheduleId(scheduleId)
            .setScheduleDisplayName(scheduleDisplayName)
            .setExecutionWindowActionMode(executionWindowActionMode);

        if (scheduleStartTimestamp != null) {
            actionScheduleBuilder.setStartTimestamp(scheduleStartTimestamp);
        }

        if (scheduleEndTimestamp != null) {
            actionScheduleBuilder.setEndTimestamp(scheduleEndTimestamp);
        }

        if (acceptingUser != null) {
            actionScheduleBuilder.setAcceptingUser(acceptingUser);
        }

        return actionScheduleBuilder.build();
    }

    @Override
    public String toString() {
        return "ActionSchedule{"
            + "scheduleStartTimestamp=" + scheduleStartTimestamp
            + ", scheduleEndTimestamp=" + scheduleEndTimestamp
            + ", scheduleTimeZoneId='" + scheduleTimeZoneId + '\''
            + ", scheduleId=" + scheduleId
            + ", scheduleDisplayName=" + scheduleDisplayName + '\''
            + ", executionWindowActionMode=" + executionWindowActionMode
            +  ", acceptingUser='" + acceptingUser + '\''
            + '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ActionSchedule that = (ActionSchedule)o;
        return Objects.equals(scheduleStartTimestamp, that.scheduleStartTimestamp)
            && Objects.equals(scheduleEndTimestamp, that.scheduleEndTimestamp)
            && Objects.equals(scheduleTimeZoneId, that.scheduleTimeZoneId)
            && scheduleId == that.scheduleId
            && Objects.equals(scheduleDisplayName, that.scheduleDisplayName)
            && executionWindowActionMode == that.executionWindowActionMode
            && Objects.equals(acceptingUser, that.acceptingUser);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scheduleStartTimestamp, scheduleEndTimestamp, scheduleTimeZoneId,
            scheduleId, scheduleDisplayName, executionWindowActionMode, acceptingUser);
    }
}
