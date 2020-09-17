package db.migration;

import static com.vmturbo.common.protobuf.setting.SettingProto.Setting.ValueCase.ENUM_SETTING_VALUE;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.annotation.ParametersAreNonnullByDefault;

import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.group.db.enums.SettingPolicyPolicyType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * The migration for fixing "Action Automation" settings for automated policies created by a user.
 * In 7.22.3 Application and ApplicationServer policies have one setting 'resize'.
 * In 7.22.5 ApplicationComponent policies have 2 settings: 'ResizeDownHeap' and 'ResizeUpHeap'.
 * Steps of the migration:
 * - select policies which have entity type APPLICATION_COMPONENT, were created by a user, have a setting 'resize';
 * - add settings 'ResizeDownHeap' and 'ResizeUpHeap' with the value of the current 'resize';
 * - delete 'resize' setting for these policies.
 */
public class V1_34__FixAppComponentActionAutomation extends BaseJdbcMigration {

    @Override
    protected void performMigrationTasks(Connection connection) throws SQLException {
        final String selectQuery = String.format(""
                        + "SELECT policy_id, setting_value FROM setting_policy_setting "
                        + "WHERE setting_name='%s' "
                        + "AND policy_id IN ( SELECT * FROM ( "
                        + "   SELECT id FROM setting_policy WHERE entity_type=%d AND policy_type='%s' "
                        + ") temp_table ) "
                        + "AND policy_id NOT IN ( SELECT * FROM ( "
                        + "    SELECT policy_id FROM setting_policy_setting where setting_name IN ('resizeDownHeap', 'resizeUpHeap') "
                        + ") temp_table2 )  ",
                ConfigurableActionSettings.Resize.getSettingName(),
                EntityType.APPLICATION_COMPONENT.getNumber(),
                SettingPolicyPolicyType.user.getLiteral());
        try (ResultSet resultSet = connection.createStatement().executeQuery(selectQuery)) {
            while (resultSet.next()) {
                final long id = resultSet.getLong("policy_id");
                final String value = resultSet.getString("setting_value");
                try (Statement insertBatch = connection.createStatement()) {
                    insertBatch.addBatch(
                            getSettingPolicyInsertQuery(id, ConfigurableActionSettings.ResizeDownHeap.getSettingName(), value));
                    insertBatch.addBatch(
                            getSettingPolicyInsertQuery(id, ConfigurableActionSettings.ResizeUpHeap.getSettingName(), value));
                    insertBatch.addBatch(
                            getSettingPolicyDeleteQuery(id, ConfigurableActionSettings.Resize.getSettingName()));
                    insertBatch.executeBatch();
                }
            }
        }
    }

    @ParametersAreNonnullByDefault
    private String getSettingPolicyInsertQuery(long id, String name, String value) {
        return String.format(""
                + "INSERT INTO setting_policy_setting "
                + "( policy_id, setting_name, setting_type, setting_value ) "
                + "VALUES "
                + "( %d, '%s', %d, '%s' )", id, name, ENUM_SETTING_VALUE.getNumber(), value);
    }

    @ParametersAreNonnullByDefault
    private String getSettingPolicyDeleteQuery(long id, String name) {
        return String.format(""
                + "DELETE FROM setting_policy_setting "
                + "WHERE policy_id=%d AND setting_name='%s'", id, name);
    }
}
