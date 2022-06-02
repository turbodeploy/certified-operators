package com.vmturbo.cost.component.savings;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.ZoneId;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow;
import com.vmturbo.common.protobuf.action.ActionDTO.UpdateActionChangeWindowRequest.ActionLivenessInfo;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.components.api.TimeUtil;

/**
 * General entity savings related utility methods.
 */
public class SavingsUtil {
    /**
     * Inner instance, not meant to be called.
     */
    private SavingsUtil() {
    }

    /**
     * Util to convert epoch millis to LocalDateTime before storing into DB.
     *
     * @param timeMillis epoch millis.
     * @param clock UTC clock.
     * @return LocalDateTime created from millis.
     */
    @Nonnull
    public static LocalDateTime getLocalDateTime(long timeMillis, final Clock clock) {
        return Instant.ofEpochMilli(timeMillis).atZone(clock.getZone()).toLocalDateTime();
    }

    /**
     * Epoch millis to display timestamp.
     *
     * @param timeMillis Time since epoch in millis.
     * @return LocalDateTime for display.
     */
    @Nonnull
    public static LocalDateTime getLocalDateTime(long timeMillis) {
        return Instant.ofEpochMilli(timeMillis).atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

    /**
     * Given a day time, returns the time (epoch millis) of the month end.
     *
     * @param dayTime Time of day, e.g '2021-02-16 20:00:00'
     * @param clock Clock to use (UTC).
     * @return Epoch millis for month end '2021-02-28 00:00:00'
     */
    public static long getMonthEndTime(@Nonnull final LocalDateTime dayTime, final Clock clock) {
        YearMonth month = YearMonth.from(dayTime);
        return TimeUtil.localDateToMilli(month.atEndOfMonth(), clock);
    }

    /**
     * Gets 12:00 AM on the start of the day of the given input time.
     *
     * @param timeMillis Epoch time in millis. E.g. for a time like 'Feb 12, 2022 1:30:00 PM'
     * @param clock Clock to use.
     * @return Day start time, e.g. 'Feb 12, 2022 12:00:00 AM'
     */
    public static LocalDateTime getDayStartTime(long timeMillis, final Clock clock) {
        Instant eachInstant = Instant.ofEpochMilli(timeMillis);
        LocalDateTime eachDateUtc = eachInstant.atZone(clock.getZone()).toLocalDateTime();
        return eachDateUtc.toLocalDate().atStartOfDay();
    }

    /**
     * Gets current date.
     *
     * @param clock Clock to use.
     * @return Current date.
     */
    @Nonnull
    public static LocalDateTime getCurrentDateTime(final Clock clock) {
        return LocalDateTime.now(clock);
    }

    /**
     * Gets entity id if present in action spec.
     *
     * @param changeWindow Change window having the action spec.
     * @return Entity id if present.
     */
    @Nonnull
    public static Optional<Long> getEntityId(@Nullable final ExecutedActionsChangeWindow changeWindow) {
        if (changeWindow == null) {
            return Optional.empty();
        }
        if (!changeWindow.hasActionSpec() || !changeWindow.getActionSpec().hasRecommendation()) {
            return Optional.empty();
        }
        return ActionDTOUtil.getPrimaryEntityIfPresent(
                changeWindow.getActionSpec().getRecommendation()).map(ActionEntity::getId);
    }

    /**
     * Gets the source provider id from action spec if present.
     *
     * @param changeWindow Action change window having the spec.
     * @return Provider id if present.
     */
    @Nonnull
    public static Optional<Long> getSourceProviderId(
            @Nullable final ExecutedActionsChangeWindow changeWindow) {
        return getProviderIds(changeWindow).map(Pair::getKey);
    }

    /**
     * Gets the destination provider id from action spec if present.
     *
     * @param changeWindow Action change window having the spec.
     * @return Provider id if present.
     */
    @Nonnull
    public static Optional<Long> getDestinationProviderId(
            @Nullable final ExecutedActionsChangeWindow changeWindow) {
        return getProviderIds(changeWindow).map(Pair::getValue);
    }

    /**
     * Helper to get source and destination provider ids from action spec.
     *
     * @param changeWindow Action change window having the spec.
     * @return Source and destination provider ids if present, underlying values could be null.
     */
    @Nonnull
    private static Optional<Pair<Long, Long>> getProviderIds(
            @Nullable final ExecutedActionsChangeWindow changeWindow) {
        if (changeWindow == null) {
            return Optional.empty();
        }
        if (!changeWindow.hasActionSpec() || !changeWindow.getActionSpec().hasRecommendation()) {
            return Optional.empty();
        }
        final Optional<ChangeProvider> changeProvider = ActionDTOUtil
                .getPrimaryChangeProvider(changeWindow.getActionSpec()
                .getRecommendation());
        if (changeProvider.isPresent()) {
            Long sourceId = changeProvider.get().hasSource()
                    ? changeProvider.get().getSource().getId() : null;
            Long destinationId = changeProvider.get().hasDestination()
                    ? changeProvider.get().getDestination().getId() : null;
            return Optional.of(ImmutablePair.of(sourceId, destinationId));
        }
        return Optional.empty();
    }

    /**
     * Helper method to convert liveness info into display string.
     *
     * @param livenessInfo Info for which details are required.
     * @param clock Clock for time display.
     * @return String for logging.
     */
    public static String toString(@Nonnull final ActionLivenessInfo livenessInfo,
            @Nonnull final Clock clock) {
        return "actionId: " + livenessInfo.getActionOid() + ", timestamp: "
                + livenessInfo.getTimestamp() + " ("
                + com.vmturbo.components.common.utils.TimeUtil.millisToLocalDateTime(
                livenessInfo.getTimestamp(), clock) + ")" + ", state: "
                + livenessInfo.getLivenessState();
    }

    /**
     * Helper method to convert action change window into display string.
     *
     * @param changeWindow Change window for which details are required.
     * @param clock Clock for time display.
     * @return String for logging.
     */
    public static String toString(@Nonnull final ExecutedActionsChangeWindow changeWindow,
            @Nonnull final Clock clock) {
        return "actionId: " + changeWindow.getActionOid() + ", entityId: "
                + changeWindow.getEntityOid()
                + (changeWindow.hasStartTime() ? (", start time: " + changeWindow.getStartTime()
                + " (" + com.vmturbo.components.common.utils.TimeUtil.millisToLocalDateTime(
                        changeWindow.getStartTime(), clock) + ")") : "")
                + (changeWindow.hasEndTime() ? (", end time: " + changeWindow.getEndTime()
                + " (" + com.vmturbo.components.common.utils.TimeUtil.millisToLocalDateTime(
                changeWindow.getEndTime(), clock) + ")") : "")
                + ", state: " + changeWindow.getLivenessState();
    }
}
