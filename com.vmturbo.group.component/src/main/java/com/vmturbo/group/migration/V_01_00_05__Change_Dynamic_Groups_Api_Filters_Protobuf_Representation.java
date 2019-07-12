package com.vmturbo.group.migration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchParameters.FilterSpecs;
import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.group.db.Tables;
import com.vmturbo.group.db.tables.records.GroupingRecord;

/**
 * This migration fixes the API filters stored in the database,
 * in the serializations of GroupInfo objects.
 * In particular, string regex matching filters in 7.16 contain
 * {@link FilterSpecs} objects whose operator is either
 * "EQ" (regex matches) or "NEQ" (regex does not match).
 * In 7.17, these operator names must change to "RXEQ" and "RXNEQ"
 * respectively.
 */
public class V_01_00_05__Change_Dynamic_Groups_Api_Filters_Protobuf_Representation implements Migration {
    private static final Logger logger = LogManager.getLogger();
    private final DSLContext dslContext;
    private final Object migrationInfoLock = new Object();

    // this migration must change the filter operator for certain API filter types
    // this set contains the filter types that do *not* need the change
    // they are numeric filters, tag filters, and exact string matching filters
    private final Set<String> filterTypesThatWork =
        ImmutableSet.<String>builder()
            .add("vmsByState").add("vmsByTag").add("vmsByNumCPUs").add("vmsByBusinessAccountUuid")
            .add("vmsByMem").add("vdcsByTag").add("pmsByState").add("pmsByTag").add("pmsByMem")
            .add("pmsByNumVms").add("pmsByNumCPUs").add("storageByState").add("storageByTag")
            .add("databaseByTag").add("databaseByBusinessAccountUuid").add("databaseServerByTag")
            .add("datacentersByTag").add("databaseServerByBusinessAccountUuid")
            .build();

    @GuardedBy("migrationInfoLock")
    private final MigrationProgressInfo.Builder migrationInfo = MigrationProgressInfo.newBuilder();

    public V_01_00_05__Change_Dynamic_Groups_Api_Filters_Protobuf_Representation(
            @Nonnull DSLContext dslContext) {
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

        // list of DB records whose group-info blob must be updated
        final List<GroupingRecord> recordsToUpdate = new ArrayList<>();

        // list of DB records whose group-info blob is invalid and must be deleted
        final List<GroupingRecord> recordsToDelete = new ArrayList<>();

        try {
            dslContext.transaction(config -> {
                final DSLContext transactionContext = DSL.using(config);

                // fetch and handle related groups:
                // only simple and nested groups are affected
                transactionContext
                    .selectFrom(Tables.GROUPING).where(Tables.GROUPING.TYPE.eq(Type.GROUP_VALUE))
                    .fetch().forEach(groupingRecord -> {
                            if (handleRecord(groupingRecord)) {
                                recordsToUpdate.add(groupingRecord);
                            } else {
                                recordsToDelete.add(groupingRecord);
                            }
                        });

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
     * Handles a DB record for a simple dynamic group.
     *
     * @param group The mutable DB record.
     *              If the record needs to be updated,
     *              a setter will be called on this object.
     * @return true if the record is to be updated and
     *         false if it is to be deleted
     */
    private boolean handleRecord(@Nonnull GroupingRecord group) {
        try {
            // parse the blob into a GroupInfo protobuf object
            logger.info("Found simple group named {}", group.getName());
            byte[] representationData = group.getGroupData();
            logger.debug("Representation in byte array:\n{}", representationData);
            final GroupInfo protobufObject = GroupInfo.parseFrom(representationData);
            logger.debug("Successfully parsed group into protobuf object:\n{}",
                    protobufObject.toString());

            // basic validation: the information must contain a non-empty name
            if (!protobufObject.hasName() || protobufObject.getName().isEmpty()) {
                logger.error("Invalid group info (missing name): record will be deleted");
                return false;
            }

            // if this is a static group, do nothing:
            // static groups have the same representation
            if (protobufObject.hasStaticGroupMembers()) {
                logger.info("Static simple group: no translation needed");
                return false;
            }

            // make sure this is a valid dynamic group
            if (!protobufObject.hasSearchParametersCollection()) {
                logger.error("Dynamic simple group with no filters: record will be deleted");
                return false;
            }

            // fix all filter specs inside the search parameters
            final SearchParametersCollection newSearchParameters =
                SearchParametersCollection.newBuilder()
                    .addAllSearchParameters(
                        protobufObject.getSearchParametersCollection().getSearchParametersList().stream()
                            .map(this::fixFilterSpecs)
                            .collect(Collectors.toList()))
                    .build();
            final GroupInfo newProtobufObject = GroupInfo.newBuilder(protobufObject)
                                                    .setSearchParametersCollection(newSearchParameters)
                                                    .build();
            group.setGroupData(newProtobufObject.toByteArray());
        } catch (InvalidProtocolBufferException | IllegalArgumentException e) {
            logger.error("Group data from group " + group.getName() + " cannot be converted", e);
            return false;
        }
        return true;
    }

    @Nonnull
    private SearchParameters fixFilterSpecs(@Nonnull SearchParameters searchParameters) {
        // if this is one of the API filters that do not need fixing: return
        if (!searchParameters.hasSourceFilterSpecs() ||
                filterTypesThatWork.contains(searchParameters.getSourceFilterSpecs().getFilterType())) {
            return searchParameters;
        }

        // translate string comparators to their equivalent regular expression comparators
        final FilterSpecs.Builder filterSpecsBuilder =
            FilterSpecs.newBuilder(searchParameters.getSourceFilterSpecs());
        if (filterSpecsBuilder.getExpressionType().equals("EQ")) {
            filterSpecsBuilder.setExpressionType("RXEQ");
        }
        if (filterSpecsBuilder.getExpressionType().equals("NEQ")) {
            filterSpecsBuilder.setExpressionType("RXNEQ");
        }
        return SearchParameters.newBuilder(searchParameters)
                    .setSourceFilterSpecs(filterSpecsBuilder.build())
                    .build();
    }
}
