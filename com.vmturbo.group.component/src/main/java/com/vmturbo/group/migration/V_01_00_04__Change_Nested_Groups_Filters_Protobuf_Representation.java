package com.vmturbo.group.migration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupPropertyFilterList;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupPropertyFilterList.GroupPropertyFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.NestedGroupInfo;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.group.db.Tables;
import com.vmturbo.group.db.tables.records.GroupingRecord;

/**
 * Since the database contains group-representing protobuf messages as binary objects,
 * a change in the protobuf representation of groups must be accompanied by a migration
 * that translates the old-style representation to the new one.
 * <p/>
 * This migration translates nested groups from their 7.16 representation to their 7.17
 * representation.
 */
public class V_01_00_04__Change_Nested_Groups_Filters_Protobuf_Representation implements Migration {
    private static final Logger logger = LogManager.getLogger();
    private final DSLContext dslContext;
    private final Object migrationInfoLock = new Object();

    // list of DB records of nested groups whose group-info blob
    // parsed into a protobuf object of the 7.16 format
    // these groups must be translated to the 7.17 format
    private List<GroupingRecord> recordsToUpdate;

    // list of DB records of nested groups whose group-info blob
    // failed to parse to a protobuf object
    // or parsed to an protobuf object that cannot be recognized
    // as belonging to either the 7.16 or the 7.17 format
    // these groups must be deleted
    private List<GroupingRecord> recordsToDelete;

    @GuardedBy("migrationInfoLock")
    private final MigrationProgressInfo.Builder migrationInfo = MigrationProgressInfo.newBuilder();

    public V_01_00_04__Change_Nested_Groups_Filters_Protobuf_Representation(@Nonnull DSLContext dslContext) {
        this.dslContext = Objects.requireNonNull(dslContext);
    }

    @Override
    public MigrationStatus getMigrationStatus() {
        synchronized (migrationInfoLock) {
            return migrationInfo.getStatus();
        }
    }

    @Override
    public MigrationProgressInfo getMigrationInfo() {
        synchronized (migrationInfoLock) {
            return migrationInfo.build();
        }
    }

    @Override
    public MigrationProgressInfo startMigration() {
        logger.info("Starting migration...");
        synchronized (migrationInfoLock) {
            migrationInfo.setStatus(MigrationStatus.RUNNING);
        }

        recordsToUpdate = new ArrayList<>();
        recordsToDelete = new ArrayList<>();

        try {
            dslContext.transaction(config -> {
                final DSLContext transactionContext = DSL.using(config);
                transactionContext
                    .selectFrom(Tables.GROUPING)
                    .where(Tables.GROUPING.TYPE.eq(Type.NESTED_GROUP_VALUE))
                    .fetch().forEach(this::handleGroup);
                logger.info("Found {} groups to update and {} groups to delete",
                        recordsToUpdate.size(), recordsToDelete.size());
                long errorsUpdating =
                        Arrays.stream(transactionContext.batchUpdate(recordsToUpdate).execute())
                            .filter(numRows -> numRows != 1)
                            .count();
                if (errorsUpdating > 0) {
                    throw new IllegalStateException("Failed to update " + errorsUpdating + " groups");
                }
                long errorsDeleting =
                        Arrays.stream(transactionContext.batchDelete(recordsToDelete).execute())
                            .filter(numRows -> numRows != 1)
                            .count();
                if (errorsDeleting > 0) {
                    throw new IllegalStateException("Failed to delete " + errorsDeleting + " groups");
                }
            });
        } catch (DataAccessException e) {
            logger.error("Failed to update the database", e);
            return migrationFailed(e.getMessage());
        } catch (Exception e) {
            logger.error("Unexpected error occurred", e);
            return migrationFailed(e.getMessage());
        }

        return migrationSucceeded();
    }

    private void handleGroup(@Nonnull GroupingRecord group) {
        try {
            // parse the blob into a NestedGroupInfo protobuf object
            logger.info("Found nested group named {}", group.getName());
            byte[] representationData = group.getGroupData();
            logger.debug("Representation in byte array:\n{}", representationData);
            final NestedGroupInfo protobufObject =
                    NestedGroupInfo.parseFrom(group.getGroupData());
            logger.debug("Successfully parsed group into protobuf object:\n{}",
                    protobufObject.toString());

            // basic validation: the information must contain a non-empty name,
            // and a cluster type
            // if not, then the group information is illegal
            // and the record must be removed
            if (!protobufObject.hasName() || protobufObject.getName().isEmpty()
                    || !protobufObject.hasCluster()) {
                logger.error("Invalid nested group info: record will be deleted");
                recordsToDelete.add(group);
                return;
            }

            // if this is a static group, do nothing:
            // static groups have the same representation
            if (protobufObject.hasStaticGroupMembers()) {
                logger.info("Static nested group: no translation needed");
                return;
            }

            if (isDynamic716(protobufObject)) {
                logger.info("Object to be translated from 7.16 to 7.17 format");
            } else if (isDynamic717(protobufObject)) {
                logger.info("Object is in valid 7.17 format: no translation needed");
                return;
            } else {
                logger.error("Invalid dynamic nested group info: record will be deleted");
                recordsToDelete.add(group);
                return;
            }

            // translation to 7.16
            final byte[] translation =
                    nestedGroupInfo716ToNestedGroupInfo(protobufObject).toByteArray();
            logger.debug(
                    "Representation of the translation in byte array:\n{}", representationData);
            final NestedGroupInfo newRepresentation = NestedGroupInfo.parseFrom(translation);
            logger.debug("Successfully translated group to new format:\n{}",
                    newRepresentation);
            group.setGroupData(translation);
            recordsToUpdate.add(group);

        } catch (InvalidProtocolBufferException | IllegalArgumentException e) {
            logger.error("Group data from nested group " + group.getName()
                    + " cannot be converted to 7.17 format", e);
            recordsToDelete.add(group);
        }
    }

    @Nonnull
    private MigrationProgressInfo migrationSucceeded() {
        return migrationInfo
                 .setStatus(MigrationStatus.SUCCEEDED)
                 .setCompletionPercentage(100)
                 .build();
    }

    @Nonnull
    private MigrationProgressInfo migrationFailed(@Nonnull String errorMessage) {
        return migrationInfo
                .setStatus(MigrationStatus.FAILED)
                .setStatusMessage("Migration failed: " + errorMessage)
                .build();
    }

    /**
     * Checks a {@link NestedGroupInfo} protobuf object and decides if it represents
     * a valid dynamic nested group according to the 7.16 version of the message.
     * To qualify, an object must have
     * <ul>
     *   <li>
     *       exactly one object of type {@link GroupPropertyFilter} in position 1
     *   </li>
     *   <li>
     *       nothing in position 2
     *   </li>
     * </ul>
     *
     * @param nestedGroupInfo the {@link NestedGroupInfo} object to be tested.
     * @return true if and only if this is valid 7.16-version representation of
     *         nested group info.
     */
    @VisibleForTesting
    public boolean isDynamic716(@Nonnull NestedGroupInfo nestedGroupInfo) {
        return nestedGroupInfo.hasPropertyFilterList()
                && nestedGroupInfo.getPropertyFilterList().getDeprecatedPropertyFiltersOldCount() == 1
                && nestedGroupInfo.getPropertyFilterList().getPropertyFiltersCount() == 0;
    }

    /**
     * Checks a {@link NestedGroupInfo} protobuf object and decides if it represents
     * a valid dynamic nested group according to the 7.16 version of the message.
     * To qualify, an object must have
     * <ul>
     *   <li>
     *       nothing in position 1
     *   </li>
     *   <li>
     *       a non-empty list of {@link PropertyFilter} objects in position 2.
     *       All of them must have a non-empty property name
     *   </li>
     * </ul>
     *
     * @param nestedGroupInfo the {@link NestedGroupInfo} object to be tested.
     * @return true if and only if this is valid 7.16-version representation of
     *         nested group info.
     */
    @VisibleForTesting
    public boolean isDynamic717(@Nonnull NestedGroupInfo nestedGroupInfo) {
        return nestedGroupInfo.hasPropertyFilterList()
                && nestedGroupInfo.getPropertyFilterList().getDeprecatedPropertyFiltersOldCount() == 0
                && nestedGroupInfo.getPropertyFilterList().getPropertyFiltersCount() > 0
                && nestedGroupInfo.getPropertyFilterList().getPropertyFiltersList().stream()
                        .noneMatch(filter ->
                                     !filter.hasPropertyName() || filter.getPropertyName().isEmpty());
    }

    /**
     * Translates a valid 7.16 dynamic nested info protobuf object to its corresponding
     * 7.17 representation.
     *
     * @param nestedGroupInfo716 object to be translated.
     * @return translation.
     */
    @Nonnull
    private NestedGroupInfo nestedGroupInfo716ToNestedGroupInfo(
            @Nonnull NestedGroupInfo nestedGroupInfo716) {
        return NestedGroupInfo.newBuilder()
                    .setName(nestedGroupInfo716.getName())
                    .setCluster(ClusterInfo.Type.forNumber(nestedGroupInfo716.getCluster().getNumber()))
                    .setPropertyFilterList(
                        GroupPropertyFilterList.newBuilder()
                            .addPropertyFilters(translatePropertyFilter(
                                                    nestedGroupInfo716.getPropertyFilterList()
                                                        .getDeprecatedPropertyFiltersOld(0))))
                    .build();
    }

    @Nonnull
    private PropertyFilter translatePropertyFilter(@Nonnull GroupPropertyFilter groupPropertyFilter716) {
        return PropertyFilter.newBuilder()
                   .setPropertyName(SearchableProperties.DISPLAY_NAME)
                   .setStringFilter(groupPropertyFilter716.getDeprecatedNameFilter())
                   .build();
    }
}
