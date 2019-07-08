package com.vmturbo.auth.component.migration;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

import com.vmturbo.auth.api.usermgmt.SecurityGroupDTO;
import com.vmturbo.auth.component.store.AuthProvider;
import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.kvstore.KeyValueStore;

/**
 * Migrate the existing active groups if the group's oid is not set. It will assign oid to the
 * existing group and save to consul. This migration script is added because we don't assign oid
 * to active directory group before, now we need it since the name field may contain slash which
 * is not supported in the API url path. But we have some customers which are already using active
 * directory, and the groups don't have oid in consul. So we need to assign oid to those groups.
 */
public class V_01_00_00__Assign_Oid_To_Security_Groups implements Migration {

    private final Logger logger = LogManager.getLogger();

    private static final Gson GSON = new GsonBuilder().create();

    private final KeyValueStore keyValueStore;

    private final Object migrationInfoLock = new Object();

    @GuardedBy("migrationInfoLock")
    private final MigrationProgressInfo.Builder migrationInfo = MigrationProgressInfo.newBuilder();

    public V_01_00_00__Assign_Oid_To_Security_Groups(@Nonnull KeyValueStore keyValueStore) {
        this.keyValueStore = Objects.requireNonNull(keyValueStore);
    }

    public MigrationStatus getMigrationStatus() {
        synchronized (migrationInfoLock) {
            return migrationInfo.getStatus();
        }
    }

    public MigrationProgressInfo getMigrationInfo() {
        synchronized (migrationInfoLock) {
            return migrationInfo.build();
        }
    }

    public MigrationProgressInfo startMigration() {
        logger.info("Starting migration of active directory groups.");

        final Map<String, Long> migratedGroups = new HashMap<>();

        getSecurityGroups()
            .filter(securityGroup -> securityGroup.getOid() == null)
            .forEach(securityGroup -> {
                final String groupConsulKey = AuthProvider.composeExternalGroupInfoKey(
                    securityGroup.getDisplayName());
                // delete old active directory group
                keyValueStore.removeKey(groupConsulKey);
                // assign oid to this active directory group
                final Long oid = IdentityGenerator.next();
                // create new active directory group from old one
                final SecurityGroupDTO newSecurityGroup = new SecurityGroupDTO(
                    securityGroup.getDisplayName(), securityGroup.getType(),
                    securityGroup.getRoleName(), securityGroup.getScopeGroups(), oid);
                keyValueStore.put(groupConsulKey, GSON.toJson(newSecurityGroup));

                logger.info("Assigned oid: " + oid + " to active directory group: " +
                    securityGroup.getDisplayName());
                migratedGroups.put(securityGroup.getDisplayName(), oid);
            });

        logger.info("Finished migration of active directory groups.");

        final String migratedGroupsInfo = migratedGroups.entrySet().stream()
            .map(entry -> "[name: " + entry.getKey() + ", oid: " + entry.getValue() + "]")
            .collect(Collectors.joining(","));
        return createMigrationProgressInfo(MigrationStatus.SUCCEEDED, 100,
            "Successfully migrated " + migratedGroups.size() + " security groups: " +
                migratedGroupsInfo);
    }

    /**
     * Get the stored security groups from consul.
     */
    private Stream<SecurityGroupDTO> getSecurityGroups() {
        return keyValueStore.getByPrefix(AuthProvider.PREFIX_GROUP).values().stream()
            .map(jsonData -> {
                try {
                    return GSON.fromJson(jsonData, SecurityGroupDTO.class);
                } catch (JsonSyntaxException e) {
                    logger.error("Error parsing group json: {}", jsonData);
                    return null;
                }
            }).filter(Objects::nonNull);
    }

    /**
     * Update the migrationInfo and return a new MigrationProgressInfo with the updated status.
     */
    private MigrationProgressInfo createMigrationProgressInfo(@Nonnull MigrationStatus status,
                                                              @Nonnull float completionPercentage,
                                                              @Nonnull String msg) {
        synchronized (migrationInfoLock) {
            return migrationInfo
                    .setStatus(status)
                    .setCompletionPercentage(completionPercentage)
                    .setStatusMessage(msg)
                    .build();
        }
    }
}
