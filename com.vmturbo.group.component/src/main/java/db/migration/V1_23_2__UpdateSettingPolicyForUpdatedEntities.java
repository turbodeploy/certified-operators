package db.migration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.flywaydb.core.api.migration.jdbc.JdbcMigration;

import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Migration to go over the policy settings and:
 * 1) Update custom App/AppServer policies to AppComponent;
 * 2) Delete default App/AppServer policies;
 * 3) Set 'resize' parameter to ex-App policies;
 * 4) Set 'autoSetResponseTimeCapacity' to Business App policies.
 */
public class V1_23_2__UpdateSettingPolicyForUpdatedEntities extends BaseJdbcMigration
        implements JdbcMigration {

    private final Logger logger = LogManager.getLogger(getClass());
    private static final String AUTO_SET_RESPONSE_TIME_CAPACITY = "autoSetResponseTimeCapacity";

    /**
     * Performs all migration tasks.
     * @param connection connection to DB
     *
     */
    @Override
    protected void performMigrationTasks(Connection connection)
            throws InvalidProtocolBufferException, SQLException {
        connection.setAutoCommit(false);
        migrateAppRelatedPolicies(connection);
        migrateBusinessAppPolicies(connection);
    }

    /**
     * Performs migration of Application and Application Server policies.
     * @param connection connection to DB
     * @throws SQLException error during work with queries
     * @throws InvalidProtocolBufferException parsing data exception
     */
    private void migrateAppRelatedPolicies(Connection connection)
            throws SQLException, InvalidProtocolBufferException {
        final ResultSet rs = connection.createStatement()
                .executeQuery("SELECT id, setting_policy_data " +
                        "FROM setting_policy WHERE policy_type <> 'default' AND (entity_type = " +
                        EntityType.APPLICATION_VALUE +
                        " OR entity_type = " +
                        EntityType.APPLICATION_SERVER_VALUE +
                        ")");
        while (rs.next()) {
            final long oid = rs.getLong("id");
            final byte[] settingPolicyDataBin = rs.getBytes("setting_policy_data");
            SettingPolicyInfo settingPolicyInfo =
                    SettingPolicyInfo.parseFrom(settingPolicyDataBin);
            if (isOldApplicationEntityType(settingPolicyInfo)) {

                // For ex APPLICATION and APPLICATION_SERVER policies
                // changing entity type to APPLICATION_COMPONENT
                SettingPolicyInfo.Builder updatedSettingPolicyInfoBuilder =
                        settingPolicyInfo.toBuilder().setEntityType(EntityType.APPLICATION_COMPONENT_VALUE);

                // Adding resize setting for ex-APPLICATION policies
                if (settingPolicyInfo.getEntityType() == EntityType.APPLICATION_VALUE) {
                    EnumSettingValue resizeSettingValue = EnumSettingValue.newBuilder()
                            .setValue("MANUAL")
                            .build();
                    SettingProto.Setting resizeSetting = SettingProto.Setting.newBuilder()
                            .setSettingSpecName("resize")
                            .setEnumSettingValue(resizeSettingValue)
                            .build();
                    updatedSettingPolicyInfoBuilder.addSettings(resizeSetting);
                }

                // Perform policies updating
                final PreparedStatement stmt = connection.prepareStatement(
                        "UPDATE setting_policy SET setting_policy_data=?, entity_type=" +
                                EntityType.APPLICATION_COMPONENT_VALUE + " WHERE id=?");
                stmt.setBytes(1, updatedSettingPolicyInfoBuilder.build().toByteArray());
                stmt.setLong(2, oid);
                stmt.execute();
            }
        }
        // Removing default APPLICATION and APPLICATION_SERVER policies
        connection.createStatement()
                .execute("DELETE FROM setting_policy WHERE policy_type = 'default' AND (entity_type = " +
                        EntityType.APPLICATION_VALUE +
                        " OR entity_type = " +
                        EntityType.APPLICATION_SERVER_VALUE +
                        ")");
    }

    /**
     * Checks if setting policy type is applied to Application or Application Server.
     * @param settingPolicyInfo policy info
     * @return if it is old app-related policy info
     */
    private boolean isOldApplicationEntityType(SettingPolicyInfo settingPolicyInfo) {
        return settingPolicyInfo.hasEntityType() && (settingPolicyInfo.getEntityType() == EntityType.APPLICATION_VALUE
                || settingPolicyInfo.getEntityType() == EntityType.APPLICATION_SERVER_VALUE);
    }

    /**
     * Performs migration of business applications policies.
     * @param connection connection to DB
     * @throws SQLException error during work with queries
     * @throws InvalidProtocolBufferException parsing data exception
     */
    private void migrateBusinessAppPolicies(Connection connection)
            throws SQLException, InvalidProtocolBufferException {
        final ResultSet rs = connection.createStatement()
                .executeQuery("SELECT id, setting_policy_data " +
                        "FROM setting_policy WHERE entity_type = " +
                        EntityType.BUSINESS_APPLICATION_VALUE);
        while (rs.next()) {
            final long oid = rs.getLong("id");
            final byte[] settingPolicyDataBin = rs.getBytes("setting_policy_data");
            SettingPolicyInfo settingPolicyInfo =
                    SettingPolicyInfo.parseFrom(settingPolicyDataBin);

            // Setting autoSetResponseTimeCapacity setting (false by default) if not exist
            if (settingPolicyInfo.getSettingsList()
                    .stream()
                    .noneMatch(s -> AUTO_SET_RESPONSE_TIME_CAPACITY.equals(s.getSettingSpecName()))) {
                SettingProto.BooleanSettingValue newSettingValue = SettingProto.BooleanSettingValue.newBuilder()
                        .setValue(false).build();
                SettingProto.Setting newSetting = SettingProto.Setting.newBuilder()
                        .setSettingSpecName(AUTO_SET_RESPONSE_TIME_CAPACITY)
                        .setBooleanSettingValue(newSettingValue)
                        .build();
                SettingPolicyInfo updatedSettingPolicyInfo =
                        settingPolicyInfo.toBuilder().addSettings(newSetting)
                                .build();

                final PreparedStatement stmt = connection.prepareStatement(
                        "UPDATE setting_policy SET setting_policy_data=? WHERE id=?");
                stmt.setBytes(1, updatedSettingPolicyInfo.toByteArray());
                stmt.setLong(2, oid);
                stmt.execute();
            }
        }
    }
}
