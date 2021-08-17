package db.migration;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.security.MessageDigest;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.api.migration.MigrationChecksumProvider;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.GroupFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Migrate corrupted dynamic groups of groups. For dynamic groups of groups which have missed related entries
 * in "group_expected_members_groups" table we need to add relevant records.
 *
 * <p>Note: We are not adding unit tests for database migrations because it is difficult to
 * initialize the database in the test with specific state and then run new migration and check
 * results.</p>
 */
public class V1_57__MigrateDynamicGroupsOfGroups extends BaseJdbcMigration
        implements MigrationChecksumProvider {

    private final Logger logger = LogManager.getLogger();

    @Override
    protected void performMigrationTasks(Connection connection) throws SQLException, IOException {
        migrateCorruptedDynamicGroupsOfGroups(connection);
    }

    private void addMissedEntry(@Nonnull PreparedStatement preparedStatement, long group_id,
            @Nonnull GroupType membersGroupType) throws SQLException {
        preparedStatement.setLong(1, group_id);
        preparedStatement.setInt(2, membersGroupType.getNumber());
        preparedStatement.addBatch();
    }

    /**
     * Add relevant records into "group_expected_members_groups" for dynamic groups of groups which
     * missed these related entries.
     *
     * @param connection - JDBC connection.
     * @throws SQLException - in case of any SQL error.
     * @throws IOException - in case of any error while reading BLOBs.
     */
    private void migrateCorruptedDynamicGroupsOfGroups(@Nonnull Connection connection) throws SQLException, IOException {
        final String selectCorruptedDynamicGroupOfGroups =
                "SELECT DISTINCT gr.id, gr.group_filters FROM grouping gr"
                        + " LEFT JOIN group_expected_members_groups gemg ON (gr.id = gemg.group_id)"
                        + " WHERE gemg.group_type IS NULL AND gr.group_filters IS NOT NULL";

        // Populate corrupted dynamic groups of groups with related group filters
        final Map<Long, Blob> groupFiltersMap = new HashMap<>();
        try (ResultSet result1 = connection.createStatement().executeQuery(
                selectCorruptedDynamicGroupOfGroups)) {
            while (result1.next()) {
                final Blob groupFiltersBlob = result1.getBlob("group_filters");
                final long groupId = result1.getLong("id");
                groupFiltersMap.put(groupId, groupFiltersBlob);
            }
        }

        // Add related entries into "group_expected_members_groups" table for corrupted dynamic groups of groups
        int addedEntriesCount = 0;
        final PreparedStatement insertGroupExpectedMembersGroupsEntriesStmt =
                connection.prepareStatement(
                        "INSERT INTO group_expected_members_groups (group_id, group_type, direct_member) VALUES (?,?,true)");
        for (Entry<Long, Blob> entry: groupFiltersMap.entrySet()) {
            final GroupFilters groupFilters = readGroupFilter(entry.getValue());
            final Optional<GroupFilter> groupFilter =
                    groupFilters.getGroupFilterList().stream().findFirst();
            if (groupFilter.isPresent()) {
                final GroupType groupType = groupFilter.get().getGroupType();
                addMissedEntry(insertGroupExpectedMembersGroupsEntriesStmt,
                        entry.getKey(), groupType);
                addedEntriesCount++;
            } else {
                // shouldn't happen
                logger.error("Failed to get group filter from group_filters blob for {} group",
                        entry.getKey());
            }
        }

        // execute insertion of required entries if needed
        if (addedEntriesCount != 0) {
            insertGroupExpectedMembersGroupsEntriesStmt.executeBatch();
            logger.info(
                    "Successfully added missed entries into 'group_expected_members_groups' table for {} corrupted dynamic groups of groups.",
                    addedEntriesCount);
        } else {
            logger.info("There are no corrupted dynamic groups of groups for migration.");
        }
    }

    /**
     * Reads bytes from a BLOB and create {@link com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.GroupFilters}
     * using them.
     *
     * @param blob - BLOB object to read.
     * @return an {@link GroupFilters} instance.
     * @throws SQLException - in case of any SQL error.
     * @throws IOException - in case of any error while reading BLOBs.
     */
    @Nonnull
    private GroupFilters readGroupFilter(@Nonnull Blob blob) throws IOException, SQLException {
        final InputStream stream = blob.getBinaryStream();
        final byte[] targetArray = new byte[stream.available()];
        stream.read(targetArray);
        return GroupFilters.parseFrom(targetArray);
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
            md5.update(getMethodSignature("performMigrationTasks").getBytes());
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

