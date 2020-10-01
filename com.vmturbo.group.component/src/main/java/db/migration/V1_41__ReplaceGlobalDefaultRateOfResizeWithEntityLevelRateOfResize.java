package db.migration;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.api.migration.jdbc.JdbcMigration;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Remove VM and container rate of resize global settings.
 * Add VM rate of resize to VM default setting.
 * Add container rate of resize to container default setting.
 */
public class V1_41__ReplaceGlobalDefaultRateOfResizeWithEntityLevelRateOfResize
    extends BaseJdbcMigration implements JdbcMigration {

    private final Logger logger = LogManager.getLogger();

    @Override
    protected void performMigrationTasks(final Connection connection) throws SQLException, IOException {
        connection.setAutoCommit(false);
        final String defaultRateOfResize = "RATE_OF_RESIZE";
        final String containerRateOfResize = "containerRateOfResize";

        // Get existing rate of resize settings.
        final String selectGlobalSettingQuery = "SELECT name, setting_data FROM global_settings"
            + " WHERE name = \"" + defaultRateOfResize + "\""
            + " OR name = \"" + containerRateOfResize + "\"";
        logger.info("Executing query {}", selectGlobalSettingQuery);

        // Store existing rate of resize settings in the map.
        final Map<Integer, Float> entityTypeToRateOfResizeSettingValue = new HashMap<>();
        try (ResultSet rs = connection.createStatement().executeQuery(selectGlobalSettingQuery)) {
            while (rs.next()) {
                final String name = rs.getString("name");
                final Setting settingData = Setting.parseFrom(rs.getBytes("setting_data"));

                if (defaultRateOfResize.equals(name)) {
                    entityTypeToRateOfResizeSettingValue.put(
                        EntityType.VIRTUAL_MACHINE_VALUE, settingData.getNumericSettingValue().getValue());
                } else if (containerRateOfResize.equals(name)) {
                    entityTypeToRateOfResizeSettingValue.put(
                        EntityType.CONTAINER_VALUE, settingData.getNumericSettingValue().getValue());
                }
            }
        }

        // Get policy ids of virtual machine default and container default.
        final String selectSettingPolicyQuery =
            "SELECT id, entity_type from setting_policy where policy_type = \"default\""
            + " AND (entity_type = " + EntityType.VIRTUAL_MACHINE_VALUE
            + " OR entity_type = " + EntityType.CONTAINER_VALUE + ")";
        logger.info("Executing query {}", selectSettingPolicyQuery);
        try (ResultSet rs = connection.createStatement().executeQuery(selectSettingPolicyQuery)) {
            while (rs.next()) {
                final long policyId = rs.getLong("id");
                final int entityType = rs.getInt("entity_type");

                // Insert new rate of resize setting.
                if (entityTypeToRateOfResizeSettingValue.containsKey(entityType)) {
                    addRateOfResizeSetting(connection, policyId, entityTypeToRateOfResizeSettingValue.get(entityType));
                }
            }
        }

        // Delete old rate of resize settings.
        final String deleteQuery = "DELETE FROM global_settings"
            + " WHERE name = \"" + defaultRateOfResize + "\""
            + " OR name = \"" + containerRateOfResize + "\"";
        logger.info("Executing query {}", deleteQuery);
        connection.createStatement().executeQuery(deleteQuery);
        connection.setAutoCommit(true);
    }

    private void addRateOfResizeSetting(final Connection connection, final long policyId, final float settingValue)
            throws SQLException {
        final String insertQuery =
            "INSERT INTO setting_policy_setting VALUES ("
            + policyId + ", \"" + EntitySettingSpecs.RateOfResize.getSettingName()
            + "\", " + Setting.NUMERIC_SETTING_VALUE_FIELD_NUMBER + ", " + settingValue + ")";
        logger.info("Executing query {}", insertQuery);
        connection.createStatement().executeQuery(insertQuery);
    }
}
