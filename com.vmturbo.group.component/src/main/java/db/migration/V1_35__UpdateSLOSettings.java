package db.migration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.annotation.Nonnull;

import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.api.migration.jdbc.JdbcMigration;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Migration to change SLO settings names for BAs, BTs, Services, App Components and DB Servers policies.
 */
public class V1_35__UpdateSLOSettings extends BaseJdbcMigration
        implements JdbcMigration {

    private final Logger logger = LogManager.getLogger();

    // policy types
    private static final String DEFAULT_POLICY = "default";
    private static final String USER_POLICY = "user";

    // Old names.
    private static final String RESPONSE_TIME_CAPACITY = "responseTimeCapacity";
    private static final String AUTO_SET_RESPONSE_TIME_CAPACITY = "autoSetResponseTimeCapacity";
    private static final String TRANSACTION_CAPACITY = "transactionsCapacity";
    private static final String AUTO_SET_TRANSACTION_CAPACITY = "autoSetTransactionsCapacity";

    // New names.
    private static final String RESPONSE_TIME_SLO = "responseTimeSLO";
    private static final String RESPONSE_TIME_SLO_ENABLED = "responseTimeSLOEnabled";
    private static final String TRANSACTION_SLO = "transactionSLO";
    private static final String TRANSACTION_SLO_ENABLED = "transactionSLOEnabled";

    private static final String CHANGE_SETTING_NAME_COMMAND =
            "UPDATE setting_policy_setting SET setting_name=?"
            + " WHERE policy_id=? AND setting_name=?";

    private static final String CHANGE_SETTING_VALUE_COMMAND =
            "UPDATE setting_policy_setting SET setting_value=?"
            + " WHERE policy_id=? AND setting_name=? AND setting_value=?";

    /**
     * Performs all migration tasks.
     *
     * @param connection connection to DB
     *
     */
    @Override
    protected void performMigrationTasks(Connection connection)
            throws InvalidProtocolBufferException, SQLException {
        connection.setAutoCommit(false);
        updateSLOSettings(connection);
    }

    /**
     * Change the SLO setting names (and values if needed).
     *
     * @param connection connection to DB
     * @throws SQLException error during SQL queries
     */
    private void updateSLOSettings(Connection connection)
            throws SQLException {
        String query = "SELECT id, entity_type, policy_type FROM setting_policy"
                + " WHERE (policy_type = '" + DEFAULT_POLICY + "'"
                + " OR policy_type = '" + USER_POLICY + "')"
                + " AND (entity_type = " + EntityType.BUSINESS_APPLICATION_VALUE
                + " OR entity_type = " + EntityType.BUSINESS_TRANSACTION_VALUE
                + " OR entity_type = " + EntityType.SERVICE_VALUE
                + " OR entity_type = " + EntityType.APPLICATION_COMPONENT_VALUE
                + " OR entity_type = " + EntityType.DATABASE_SERVER_VALUE
                + ")";
        logger.info("Executing query {}", query);
        final ResultSet rs = connection.createStatement().executeQuery(query);
        while (rs.next()) {
            final long policyId = rs.getLong("id");
            final int entityType = rs.getInt("entity_type");
            final String policyType = rs.getString("policy_type");

            logger.info("changing SLO setting names for {} policy {} for entity type {}",
                    policyType, policyId, entityType);

            changeSettingName(connection, policyId, RESPONSE_TIME_CAPACITY, RESPONSE_TIME_SLO);
            changeSettingName(connection, policyId, AUTO_SET_RESPONSE_TIME_CAPACITY, RESPONSE_TIME_SLO_ENABLED);
            changeSettingName(connection, policyId, TRANSACTION_CAPACITY, TRANSACTION_SLO);
            changeSettingName(connection, policyId, AUTO_SET_TRANSACTION_CAPACITY, TRANSACTION_SLO_ENABLED);

            // In user policies we need to change the value as well.
            // For example, if the user set "Disable SLO = true" then we need to change it
            // to "Enable SLO = false"
            if (USER_POLICY.equals(policyType)) {
                changeSettingValue(connection, policyId, RESPONSE_TIME_SLO_ENABLED, "false", "true");
                changeSettingValue(connection, policyId, RESPONSE_TIME_SLO_ENABLED, "true", "false");
                changeSettingValue(connection, policyId, TRANSACTION_SLO_ENABLED, "false", "true");
                changeSettingValue(connection, policyId, TRANSACTION_SLO_ENABLED, "true", "false");
            } else {
                // In default policies, change the "Enable SLO" value only if the user changed the
                // default value (false) to true.
                changeSettingValue(connection, policyId, RESPONSE_TIME_SLO_ENABLED, "true", "false");
                changeSettingValue(connection, policyId, TRANSACTION_SLO_ENABLED, "true", "false");
            }
        }
    }

    /**
     * Change setting name.
     *
     * @param connection connection to DB
     * @param policyId policy ID
     * @param oldName the old setting name
     * @param newName the new setting name
     * @throws SQLException error during SQL query
     */
    private void changeSettingName(Connection connection,
                                   long policyId,
                                   @Nonnull final String oldName,
                                   @Nonnull final String newName) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(CHANGE_SETTING_NAME_COMMAND);
        stmt.setString(1, newName);
        stmt.setLong(2, policyId);
        stmt.setString(3, oldName);
        stmt.execute();
    }

    /**
     * Change setting value.
     *
     * @param connection connection to DB
     * @param policyId policy ID
     * @param settingName the setting name
     * @param oldValue the old setting name
     * @param newValue the new setting name
     * @throws SQLException error during SQL query
     */
    private void changeSettingValue(Connection connection,
                                   long policyId,
                                   @Nonnull final String settingName,
                                   @Nonnull final String oldValue,
                                   @Nonnull final String newValue) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(CHANGE_SETTING_VALUE_COMMAND);
        stmt.setString(1, newValue);
        stmt.setLong(2, policyId);
        stmt.setString(3, settingName);
        stmt.setString(4, oldValue);
        stmt.execute();
    }
}
