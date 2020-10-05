package db.migration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.migration.MigrationInfoProvider;
import org.flywaydb.core.api.migration.jdbc.JdbcMigration;

import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.Scenario;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.SettingOverride;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.common.setting.EntitySettingSpecs;

/**
 * Migration to add rate of resize setting to the scenarios of the plans. We store scenarios
 * in two tables: the scenario table and the plan instance table. We first add rate of resize
 * setting to the Scenario table. We then keep track of that Scenario id with the new
 * corresponding Scenario Info in the table scenarioIdToInfo.
 * We then iterate the plan_instance table and check for every ScenarioInfo. if the ScenarioInfo is
 * contained in the table, it gets substituted with the new ScenarioInfo.
 */
public class V2v20AddRateOfResizeScenarioChange implements JdbcMigration, MigrationInfoProvider {

    @Override
    public void migrate(final Connection connection) throws SQLException, InvalidProtocolBufferException {
        connection.setAutoCommit(false);
        final Map<Long, ScenarioInfo> scenarioIdToInfo = updateScenarioTable(connection);
        updatePlanInstanceTable(connection, scenarioIdToInfo);
        connection.setAutoCommit(true);
    }

    private Map<Long, ScenarioInfo> updateScenarioTable(final Connection connection)
            throws SQLException, InvalidProtocolBufferException {
        final Map<Long, ScenarioInfo> scenarioIdToInfo = new HashMap<>();
        try (ResultSet rs = connection.createStatement()
            .executeQuery("SELECT id, scenario_info FROM scenario")) {
            while (rs.next()) {
                final long scenarioId = rs.getLong("id");
                final byte[] binaryScenarioInfo = rs.getBytes("scenario_info");
                final ScenarioInfo scenarioInfo = ScenarioInfo.parseFrom(binaryScenarioInfo);

                if (scenarioInfo.hasDeprecatedPlanGlobalSetting()
                    && scenarioInfo.getDeprecatedPlanGlobalSetting().hasDeprecatedRateOfResize()) {
                    final NumericSettingValue rateOfResizeValue =
                        scenarioInfo.getDeprecatedPlanGlobalSetting().getDeprecatedRateOfResize().getNumericSettingValue();
                    final ScenarioInfo.Builder newScenarioInfo = scenarioInfo.toBuilder();
                    newScenarioInfo.getDeprecatedPlanGlobalSettingBuilder().clearDeprecatedRateOfResize();
                    newScenarioInfo.addChanges(ScenarioChange.newBuilder()
                        .setSettingOverride(SettingOverride.newBuilder()
                            .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                            .setSetting(Setting.newBuilder()
                                .setSettingSpecName(EntitySettingSpecs.RateOfResize.getSettingName())
                                .setNumericSettingValue(rateOfResizeValue)))
                        .build());

                    scenarioIdToInfo.put(scenarioId, newScenarioInfo.build());
                    final PreparedStatement stmt = connection.prepareStatement(
                        "UPDATE scenario SET scenario_info=? WHERE id=?");
                    stmt.setBytes(1, scenarioInfo.toByteArray());
                    stmt.setLong(2, scenarioId);
                    stmt.addBatch();
                    stmt.executeBatch();
                }
            }
        }

        return scenarioIdToInfo;
    }

    private void updatePlanInstanceTable(final Connection connection, final Map<Long, ScenarioInfo> scenarioIdToInfo)
            throws SQLException, InvalidProtocolBufferException {
        try (ResultSet planResults = connection.createStatement()
            .executeQuery("SELECT id, plan_instance FROM plan_instance")) {
            while (planResults.next()) {
                final long planId = planResults.getLong("id");
                final byte[] planInstanceBinary = planResults.getBytes("plan_instance");
                PlanInstance planInstance = PlanInstance.parseFrom(planInstanceBinary);
                if (planInstance.hasScenario() && scenarioIdToInfo.containsKey(planInstance.getScenario().getId())) {
                    Long oid = planInstance.getScenario().getId();
                    Scenario newScenario =
                        Scenario.newBuilder()
                            .setId(planInstance.getScenario().getId())
                            .setScenarioInfo(scenarioIdToInfo.get(oid))
                            .build();
                    planInstance = planInstance
                        .toBuilder()
                        .clearScenario()
                        .setScenario(newScenario).build();
                    final PreparedStatement stmt = connection.prepareStatement(
                        "UPDATE plan_instance SET plan_instance=? WHERE id=?");
                    stmt.setBytes(1, planInstance.toByteArray());
                    stmt.setLong(2, planId);
                    stmt.addBatch();
                    stmt.executeBatch();
                }
            }
        }
    }

    @Override
    public MigrationVersion getVersion() {
        return MigrationVersion.fromVersion("2.20");
    }

    @Override
    public String getDescription() {
        return "Add RateOfResize scenario change.";
    }
}
