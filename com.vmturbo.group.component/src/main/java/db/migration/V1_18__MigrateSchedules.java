package db.migration;

import static java.time.ZoneOffset.UTC;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableBiMap;
import com.google.protobuf.InvalidProtocolBufferException;

import net.fortuna.ical4j.model.Recur.Frequency;
import net.fortuna.ical4j.model.WeekDay;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.api.migration.jdbc.JdbcMigration;
import org.springframework.beans.factory.annotation.Value;

import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.Schedule;
import com.vmturbo.common.protobuf.setting.SettingProto.Schedule.DayOfWeek;
import com.vmturbo.group.identity.IdentityProvider;

/**
 * Class for migrating schedule info in setting policies. Extracts schedule info from setting policy
 * records and creates schedule records.
 */
public class V1_18__MigrateSchedules implements JdbcMigration {
    private static final String INSERT_SCHEDULE_SQL =
            "INSERT INTO schedule (id, display_name, start_time, end_time, last_date, recur_rule, time_zone_id) "
                    + "VALUES (?,?,?,?,?,?,?)";
    private static final String INSERT_SETTING_POLICY_SCHEDULE =
        "INSERT INTO setting_policy_schedule (setting_policy_id, schedule_id) VALUES (?,?)";

    private static final ImmutableBiMap<DayOfWeek, WeekDay> weekdays = ImmutableBiMap
        .<DayOfWeek, WeekDay>builder()
        .put(DayOfWeek.MONDAY, WeekDay.MO)
        .put(DayOfWeek.TUESDAY, WeekDay.TU)
        .put(DayOfWeek.WEDNESDAY, WeekDay.WE)
        .put(DayOfWeek.THURSDAY, WeekDay.TH)
        .put(DayOfWeek.FRIDAY, WeekDay.FR)
        .put(DayOfWeek.SATURDAY, WeekDay.SA)
        .put(DayOfWeek.SUNDAY, WeekDay.SU)
        .build();

    private final Logger logger = LogManager.getLogger(getClass());
    @Value("${identityGeneratorPrefix}")
    private long identityGeneratorPrefix;

    private final IdentityProvider identityProvider = new IdentityProvider(identityGeneratorPrefix);

    @Override
    public void migrate(final Connection connection) throws Exception {
        connection.setAutoCommit(false);
        PreparedStatement schedulePrepStmt = null;
        PreparedStatement settingPolicySchedMappingStmt = null;
        try {
            schedulePrepStmt = connection.prepareStatement(INSERT_SCHEDULE_SQL);
            settingPolicySchedMappingStmt = connection.prepareStatement(INSERT_SETTING_POLICY_SCHEDULE);
            final ResultSet rs = connection.createStatement()
                .executeQuery("SELECT id, setting_policy_data FROM setting_policy");
            while (rs.next()) {
                parseScheduleInfo(schedulePrepStmt, settingPolicySchedMappingStmt,
                    rs.getLong("id"), rs.getBytes("setting_policy_data"));
            }
            schedulePrepStmt.executeBatch();
            settingPolicySchedMappingStmt.executeBatch();
            connection.commit();
        } catch (InvalidProtocolBufferException | SQLException e) {
            logger.warn("Failure executing schedule migration", e);
            connection.rollback();
            throw e;
        } finally {
            try {
                schedulePrepStmt.close();
                settingPolicySchedMappingStmt.close();
            } catch (SQLException sqle) {
                logger.warn("Exception caught while trying to close prepared statements", sqle);
            }
        }
        connection.setAutoCommit(true);
    }

    /**
     * Parse schedule info from setting policy protobuf structure.
     * @param schedulePrepStmt Prepared statement for schedule inserts
     * @param settingPolicySchedulePrepStmt Prepared statement for setting policy schedule mappings
     * @param settingPolicyId Setting policy ID
     * @param settingPolicydata Setting policy info protobuf structure
     * @throws InvalidProtocolBufferException Exception while parsing protobuf data
     * @throws SQLException SQL exceptions
     */
    private void parseScheduleInfo(@Nonnull final PreparedStatement schedulePrepStmt,
           @Nonnull final PreparedStatement settingPolicySchedulePrepStmt,
           final long settingPolicyId, @Nonnull final byte[] settingPolicydata)
        throws InvalidProtocolBufferException, SQLException {
        final SettingProto.SettingPolicyInfo settingPolicyInfo = SettingProto.SettingPolicyInfo
            .parseFrom(settingPolicydata);
        if (settingPolicyInfo.hasDeprecatedSchedule()) {
            final Schedule scheduleInfo = settingPolicyInfo.getDeprecatedSchedule();
            final long nextScheduleId = identityProvider.next();
            //id
            schedulePrepStmt.setLong(1, nextScheduleId);
            // display name
            schedulePrepStmt.setString(2,
                    settingPolicyInfo.hasDisplayName()
                            ? settingPolicyInfo.getDisplayName() + " Schedule"
                            : settingPolicyInfo.getName() + " Schedule");
            // start time
            schedulePrepStmt.setTimestamp(3, new Timestamp(scheduleInfo.getStartTime()));
            // end time
            schedulePrepStmt.setTimestamp(4, new Timestamp(getEndTime(scheduleInfo)));
            // last date
            if (scheduleInfo.hasPerpetual() || scheduleInfo.getLastDate() == 0) {
                schedulePrepStmt.setTimestamp(5, null);
            } else {
                schedulePrepStmt.setTimestamp(5, new Timestamp(scheduleInfo.getLastDate()));
            }
            // recurrence rule
            schedulePrepStmt.setString(6, generateRecurrence(scheduleInfo));
            // time zone ID - default
            schedulePrepStmt.setString(7, ZoneOffset.UTC.toString());
            schedulePrepStmt.addBatch();

            // now insert setting policy -> schedule mapping
            settingPolicySchedulePrepStmt.setLong(1, settingPolicyId);
            settingPolicySchedulePrepStmt.setLong(2, nextScheduleId);
            settingPolicySchedulePrepStmt.addBatch();
        }

    }

    /**
     * Convert Schedule info recurrence from protobuf structure to recurrence rule.
     * @param scheduleInfo Protobuf schedule info
     * @return Recurrence rule
     */
    @Nonnull
    private String generateRecurrence(@Nonnull final Schedule scheduleInfo) {
        StringBuilder recurrence = new StringBuilder("");
        if (scheduleInfo.hasDaily() || scheduleInfo.hasWeekly() || scheduleInfo.hasMonthly()) {
            recurrence.append("FREQ=");
            if (scheduleInfo.hasDaily()) {
                recurrence.append(Frequency.DAILY);
            } else if (scheduleInfo.hasWeekly()) {
                //The day(s) of the week on which the policy should be active
                // If not set, this defaults to the day of the start_date
                recurrence.append(Frequency.WEEKLY)
                    .append(";BYDAY=")
                    .append(scheduleInfo.getWeekly().getDaysOfWeekList().stream()
                    .map(d -> weekdays.get(d).getDay().name())
                    .collect(Collectors.joining(",")));
            } else {
                // The day(s) of the month on which the policy should be active.
                recurrence.append(Frequency.MONTHLY)
                    .append(";BYMONTHDAY=")
                    .append(scheduleInfo.getMonthly().getDaysOfMonthList().stream()
                    .map(String::valueOf)
                    .collect(Collectors.joining(",")));
            }
            recurrence.append(";INTERVAL=1");
        }
        return recurrence.toString();
    }

    /**
     * Get scdule end time.
     * @param scheduleInfo Protobuf schedule info
     * @return Schedule end time (in milliseconds since Unix epoch)
     */
    @Nonnull
    private long getEndTime(@Nonnull final Schedule scheduleInfo) {
        if (scheduleInfo.hasEndTime()) {
            return scheduleInfo.getEndTime();
        } else {
            // Schedules uses duration in minutes
            // Although not sure how it can be defined in the UI
            return Instant.ofEpochMilli(scheduleInfo.getStartTime())
                .atOffset(UTC).plusMinutes(scheduleInfo.getMinutes()).toInstant().toEpochMilli();
        }
    }
}
