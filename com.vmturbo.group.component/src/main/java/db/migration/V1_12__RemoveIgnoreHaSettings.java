package db.migration;

import java.lang.reflect.Method;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.flywaydb.core.api.migration.MigrationChecksumProvider;
import org.flywaydb.core.api.migration.jdbc.JdbcMigration;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;

/**
 * Migration to go over the policy settings and remove all the ignoreHa settings.
 */
public class V1_12__RemoveIgnoreHaSettings implements JdbcMigration, MigrationChecksumProvider {

    private final Logger logger = LogManager.getLogger(getClass());

    @Override
    public void migrate(Connection connection) throws Exception {
        boolean autoCommit = connection.getAutoCommit();
        try {
            connection.setAutoCommit(false);
            final ResultSet rs = connection.createStatement()
                .executeQuery("SELECT id, setting_policy_data FROM group_component.setting_policy");
            while (rs.next()) {
                final long oid = rs.getLong("id");
                final byte[] settingPolicyDataBin = rs.getBytes("setting_policy_data");
                SettingPolicyInfo settingPolicyInfo =
                    SettingPolicyInfo.parseFrom(settingPolicyDataBin);
                List<Setting> filteredSettings = settingPolicyInfo.getSettingsList()
                    .stream()
                    .filter(setting -> !setting.getSettingSpecName().equals("ignoreHa")
                    ).collect(Collectors.toList());
                settingPolicyInfo =
                    settingPolicyInfo.toBuilder().clearSettings().addAllSettings(filteredSettings).build();
                final PreparedStatement stmt = connection.prepareStatement(
                    "UPDATE group_component.setting_policy SET setting_policy_data=? WHERE id=?");
                stmt.setBytes(1, settingPolicyInfo.toByteArray());
                stmt.setLong(2, oid);
                stmt.addBatch();
                stmt.executeBatch();
            }
        } catch (InvalidProtocolBufferException | SQLException e) {
            logger.warn("Failed performing migration", e);
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(autoCommit);
        }

    }

    /**
     * By default, flyway JDBC migrations do not provide checkpoints, but we do so here.
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
}
