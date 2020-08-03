package db.migration;

import java.lang.reflect.Method;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.flywaydb.core.api.migration.MigrationChecksumProvider;
import org.flywaydb.core.api.migration.jdbc.JdbcMigration;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.Scenario;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.SettingOverride;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;

/**
 * Migration to remove all the Resize settings from the scenario of the plans. We store scenarios
 * in two tables: the scenario table and the plan instance table. We first iterate the Scenario
 * table to find any Resize settings for vms and substitute them with the vmResizeSettings. We then
 * keep track of that Scenario id with the new corresponding Scenario Info in the table scenarioIdToInfo.
 * We then iterate the plan_instance table and check for every ScenarioInfo. if the ScenarioInfo is
 * contained in the table, it gets substituted with the new ScenarioInfo.
 */
public class V2_13__remove_resize_for_vms implements JdbcMigration, MigrationChecksumProvider {

    private static final ImmutableSet<EntitySettingSpecs> vmResizeSettings =
        ImmutableSet.of(
            EntitySettingSpecs.ResizeVcpuAboveMaxThreshold,
            EntitySettingSpecs.ResizeVcpuBelowMinThreshold,
            EntitySettingSpecs.ResizeVcpuUpInBetweenThresholds,
            EntitySettingSpecs.ResizeVcpuDownInBetweenThresholds,
            EntitySettingSpecs.ResizeVmemAboveMaxThreshold,
            EntitySettingSpecs.ResizeVmemBelowMinThreshold,
            EntitySettingSpecs.ResizeVmemUpInBetweenThresholds,
            EntitySettingSpecs.ResizeVmemDownInBetweenThresholds);

    private final Logger logger = LogManager.getLogger(getClass());

    @Override
    public void migrate(final Connection connection) throws Exception {
        connection.setAutoCommit(false);
        ResultSet rs;
        List<Long> failedScenarioIds = new ArrayList<>();
        try {
            rs = connection.createStatement()
                .executeQuery("SELECT id, scenario_info FROM scenario");
            Map<Long, ScenarioInfo> scenarioIdToInfo = new HashMap<>();
            long scenarioId = 0;
            while (rs.next()) {
                try {
                    scenarioId = rs.getLong("id");
                    final byte[] binaryScenarioInfo = rs.getBytes("scenario_info");
                    ScenarioInfo scenarioInfo = ScenarioInfo.parseFrom(binaryScenarioInfo);
                    List<ScenarioChange> newScenarioChanges = new ArrayList<>();
                    EnumSettingValue vmResizeValue = null;
                    for (ScenarioChange change : scenarioInfo.getChangesList()) {
                        if (change.hasSettingOverride() &&
                            change.getSettingOverride().getEntityType() == EntityType.VIRTUAL_MACHINE.getValue()) {
                            if (change.getSettingOverride().hasSetting()
                                && change.getSettingOverride().getSetting().getSettingSpecName().equals(EntitySettingSpecs.Resize.getSettingName())) {
                                vmResizeValue =
                                    change.getSettingOverride().getSetting().getEnumSettingValue();
                                continue;
                            }
                        }
                        newScenarioChanges.add(change);
                    }
                    if (vmResizeValue != null) {
                        addResizeSettings(newScenarioChanges, vmResizeValue);
                        scenarioInfo =
                            scenarioInfo.toBuilder().clearChanges().addAllChanges(newScenarioChanges).build();
                        scenarioIdToInfo.put(scenarioId, scenarioInfo);
                        final PreparedStatement stmt = connection.prepareStatement(
                            "UPDATE scenario SET scenario_info=? WHERE id=?");
                        stmt.setBytes(1, scenarioInfo.toByteArray());
                        stmt.setLong(2, scenarioId);
                        stmt.addBatch();
                        stmt.executeBatch();
                    }
                    updatePlanInstanceTable(connection, scenarioIdToInfo);
                } catch (InvalidProtocolBufferException | SQLException e) {
                    failedScenarioIds.add(scenarioId);
                    logger.debug("Failed performing migration for scenario with id " + scenarioId);
                }
            }
        } catch (SQLException e) {
            logger.warn("Failed performing migration", e);
        }
        if (!failedScenarioIds.isEmpty()) {
            logger.warn("Failed to update " + failedScenarioIds.size() + " scenarios");
        }
        connection.setAutoCommit(true);
    }

    /**
     * By default, flyway JDBC migrations do not provide chekcpoints, but we do so here.
     *
     * <p>The goal is to prevent any change to this migration from ever being made after it goes into release.
     * We do that by gathering some information that would, if it were to change, signal that this class definition
     * has been changed, and then computing a checksum value from that information. It's not as fool-proof as a
     * checksum on the source code, but there's no way to reliably obtain the exact source code at runtime.</p>
     *
     * @return checksum for this migration
     */
    @Override
    public Integer getChecksum() {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            // include this class's fully qualified name
            md5.update(getClass().getName().getBytes());
            // and the closest I know how to get to a source line count
            // add in the method signatures of all the
            md5.update(getMethodSignature("migrate").getBytes());
            md5.update(getMethodSignature("getChecksum").getBytes());
            md5.update(getMethodSignature("getMethodSignature").getBytes());
            return new HashCodeBuilder().append(md5.digest()).hashCode();
        } catch (Exception e) {
            if (!(e instanceof IllegalStateException)) {
                e = new IllegalStateException(e);
            }
            throw (IllegalStateException)e;
        }
    }

    /**
     * Get a rendering of a named method's signature.
     *
     * <p>We combine the method's name with a list of the fully-qualified class names of all its parameters.</p>
     *
     * <p>This works only when the method is declared by this class, and it is the only method with that name
     * declared by the class.</p>
     *
     * @param name name of method
     * @return method's signature (e.g. "getMethodSignature(java.lang.String name)"
     */
    private String getMethodSignature(String name) {
        List<Method> candidates = Stream.of(getClass().getDeclaredMethods())
            .filter(m -> m.getName().equals(name))
            .collect(Collectors.toList());
        if (candidates.size() == 1) {
            String parms = Stream.of(candidates.get(0).getParameters())
                .map(p -> p.getType().getName() + " " + p.getName())
                .collect(Collectors.joining(","));
            return candidates.get(0).getName() + "(" + parms + ")";
        } else {
            throw new IllegalStateException(
                String.format("Failed to obtain method signature for method '%s': %d methods found",
                    name, candidates.size()));
        }
    }

    private void addResizeSettings(List<ScenarioChange> newScenarioChanges,
                                   EnumSettingValue vmResizeValue) {
        for (EntitySettingSpecs resizeSetting : vmResizeSettings) {
            newScenarioChanges.add(ScenarioChange.newBuilder().setSettingOverride(SettingOverride.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE.getValue())
                .setSetting(Setting.newBuilder().setSettingSpecName(resizeSetting.getSettingName())
                    .setEnumSettingValue(vmResizeValue).build())
                .build()).build());
        }
    }

    private void updatePlanInstanceTable(final Connection connection, Map<Long, ScenarioInfo> scenarioIdToInfo) throws SQLException,
        InvalidProtocolBufferException {
        final ResultSet planResults = connection.createStatement()
            .executeQuery("SELECT id, plan_instance FROM plan_instance");
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
