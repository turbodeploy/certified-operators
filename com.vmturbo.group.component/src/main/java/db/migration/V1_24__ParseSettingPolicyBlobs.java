package db.migration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.api.migration.jdbc.JdbcMigration;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.components.common.setting.EntitySettingSpecs;

/**
 * This migration is aimed to parse internal blobs, storing {@link SettingPolicyInfo}
 * protobuf serialized message and put the fields in to a relational DB columns.
 */
public class V1_24__ParseSettingPolicyBlobs implements JdbcMigration {

    private static final float DEFAULT_VALUE_OLD = 10000.0f;

    private static final float DEFAULT_VALUE_NEW = 20.0f;

    private final Logger logger = LogManager.getLogger(getClass());

    @Override
    public void migrate(Connection connection) throws Exception {
        connection.setAutoCommit(false);
        try {
            migrateData(connection);
            connection.commit();
        } finally {
            connection.setAutoCommit(true);
        }
    }

    @Nonnull
    private Set<Long> existingScheduleIds(@Nonnull Connection connection) throws SQLException {
        final ResultSet rs = connection.createStatement().executeQuery("SELECT id FROM schedule");
        final Set<Long> scheduleIds = new HashSet<>();
        while (rs.next()) {
            final long scheduleId = rs.getLong(1);
            scheduleIds.add(scheduleId);
        }
        return Collections.unmodifiableSet(scheduleIds);
    }

    @Nonnull
    private Set<Long> existingGroupIds(@Nonnull Connection connection) throws SQLException {
        final ResultSet rs = connection.createStatement().executeQuery("SELECT id FROM grouping");
        final Set<Long> groupIds = new HashSet<>();
        while (rs.next()) {
            final long groupId = rs.getLong(1);
            groupIds.add(groupId);
        }
        return Collections.unmodifiableSet(groupIds);
    }

    private void migrateData(@Nonnull Connection connection)
            throws SQLException, InvalidProtocolBufferException {
        final ResultSet rs = connection.createStatement()
                .executeQuery("SELECT id, setting_policy_data FROM setting_policy");
        final PreparedStatement updatePolicy = connection.prepareStatement("UPDATE setting_policy"
                + " SET name=?, display_name=?, entity_type=?, enabled=?, schedule_id=? WHERE id=?");
        final PreparedStatement groups = connection.prepareStatement(
                "INSERT INTO setting_policy_groups"
                        + " (group_id, setting_policy_id) values (?, ?)");
        final PreparedStatement settings = connection.prepareStatement(
                "INSERT INTO setting_policy_setting"
                        + " (policy_id, setting_name, setting_type, setting_value)"
                        + " VALUES (?, ?, ?, ?)");
        final PreparedStatement settingsOids = connection.prepareStatement(
                "INSERT INTO setting_policy_setting_oids"
                        + " (policy_id, setting_name, oid_number, oid)" + " VALUES (?, ?, ?, ?)");

        final Set<Long> existingSchedules = existingScheduleIds(connection);
        final Set<Long> existingGroups = existingGroupIds(connection);
        while (rs.next()) {
            final long id = rs.getLong(1);
            final byte[] data = rs.getBytes(2);
            final SettingPolicyInfo policy = SettingPolicyInfo.parseFrom(data);
            final String name = policy.hasName() ? policy.getName() : Long.toString(id);
            updatePolicy.setString(1, name);
            final String displayName = policy.hasDisplayName() ? policy.getDisplayName() : name;
            updatePolicy.setString(2, displayName);
            if (policy.hasEntityType()) {
                updatePolicy.setInt(3, policy.getEntityType());
            } else {
                logger.error("Entity type is not set for policy {}-{}. Skipping it", id, name);
                continue;
            }
            updatePolicy.setBoolean(4, policy.getEnabled());
            if (policy.hasScheduleId()) {
                final long scheduleId = policy.getScheduleId();
                if (existingSchedules.contains(scheduleId)) {
                    updatePolicy.setLong(5, scheduleId);
                } else {
                    updatePolicy.setNull(5, Types.BIGINT);
                    logger.error("Policy {} is referencing absent schedule {}. Resetting to NULL",
                            id, scheduleId);
                }
            } else {
                updatePolicy.setNull(5, Types.BIGINT);
            }
            updatePolicy.setLong(6, id);
            updatePolicy.addBatch();
            addGroups(groups, existingGroups, id, policy);
            addSettingPolicies(settings, settingsOids, id, policy.getSettingsList());
        }
        updatePolicy.executeBatch();
        groups.executeBatch();
        settings.executeBatch();
        settingsOids.executeBatch();
    }

    private void addGroups(@Nonnull PreparedStatement groups, @Nonnull Set<Long> existingGroups,
            long policyId, @Nonnull SettingPolicyInfo policy) throws SQLException {
        for (Long groupId : policy.getScope().getGroupsList()) {
            if (existingGroups.contains(groupId)) {
                groups.setLong(1, groupId);
                groups.setLong(2, policyId);
                groups.addBatch();
            } else {
                logger.error(
                        "Group {} found at policy {} is not present. Skipping. Policy will be still added",
                        groupId, policyId);
            }
        }
    }

    private void addSettingPolicies(@Nonnull PreparedStatement stmt,
            @Nonnull PreparedStatement settingOids, long policyId,
            @Nonnull Collection<Setting> settings) throws SQLException {
        for (Setting setting : settings) {
            stmt.setLong(1, policyId);
            stmt.setString(2, setting.getSettingSpecName());
            stmt.setInt(3, setting.getValueCase().getNumber());
            final String value;
            switch (setting.getValueCase()) {
                case BOOLEAN_SETTING_VALUE:
                    value = Boolean.toString(setting.getBooleanSettingValue().getValue());
                    break;
                case ENUM_SETTING_VALUE:
                    value = setting.getEnumSettingValue().getValue();
                    break;
                case NUMERIC_SETTING_VALUE:
                    // Convert transaction capacity default value.
                    // Code is moved from V_01_00_03__Change_Default_Transactions_Capacity
                    if (setting.getSettingSpecName()
                            .equals(EntitySettingSpecs.TransactionsCapacity.getSettingName())
                            && setting.getNumericSettingValue().getValue() == DEFAULT_VALUE_OLD) {
                        value = Float.toString(DEFAULT_VALUE_NEW);
                    } else {
                        value = Float.toString(setting.getNumericSettingValue().getValue());
                    }
                    break;
                case STRING_SETTING_VALUE:
                    value = setting.getStringSettingValue().getValue();
                    break;
                case SORTED_SET_OF_OID_SETTING_VALUE:
                    value = "-";
                    addSettingOids(settingOids, policyId, setting.getSettingSpecName(),
                            setting.getSortedSetOfOidSettingValueOrBuilder().getOidsList());
                    break;
                default:
                    logger.error("Unknown setting type {} found for setting {} of policy {}",
                            setting.getValueCase(), setting.getSettingSpecName(), policyId);
                    continue;
            }
            stmt.setString(4, value);
            stmt.addBatch();
        }
    }

    private void addSettingOids(@Nonnull PreparedStatement settingOids, long policyId,
            @Nonnull String settingName, @Nonnull List<Long> oids) throws SQLException {
        int counter = 0;
        for (long oid : oids) {
            settingOids.setLong(1, policyId);
            settingOids.setString(2, settingName);
            settingOids.setInt(3, counter++);
            settingOids.setLong(4, oid);
            settingOids.addBatch();
        }
    }
}
