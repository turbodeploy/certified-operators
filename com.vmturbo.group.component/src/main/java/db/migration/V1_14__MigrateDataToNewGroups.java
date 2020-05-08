package db.migration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.api.migration.jdbc.JdbcMigration;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters.EntityFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.GroupFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.Builder;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.ListFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.ListFilter.ListElementTypeCase;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchParameters.FilterSpecs;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition.StoppingConditionTypeCase;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.group.migration.v1_13.V113Migration.ClusterInfo;
import com.vmturbo.group.migration.v1_13.V113Migration.Group.Type;
import com.vmturbo.group.migration.v1_13.V113Migration.GroupInfo;
import com.vmturbo.group.migration.v1_13.V113Migration.GroupPropertyFilterList;
import com.vmturbo.group.migration.v1_13.V113Migration.GroupPropertyFilterList.GroupPropertyFilter;
import com.vmturbo.group.migration.v1_13.V113Migration.NestedGroupInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Class for migrating data from GroupDTO to Grouping. Performs processing of BLOBS containing
 * Protobuf messages. Because of obsolete migration V_01_00_00 we do not rely on entity_type column
 * - it may be empty yet.
 */
@SuppressWarnings("deprecation")
public class V1_14__MigrateDataToNewGroups implements JdbcMigration {

    private final Logger logger = LogManager.getLogger(getClass());
    /**
     * This set contains the filter types that do *not* need the change.
     * they are numeric filters, tag filters, and exact string matching filters
     * This is part of obsolete migration V_01_00_05__Change_Dynamic_Groups_Api_Filters_Protobuf_Representation
     */
    private final Set<String> filterTypesThatWork = ImmutableSet.<String>builder().add("vmsByState")
            .add("vmsByTag")
            .add("vmsByNumCPUs")
            .add("vmsByBusinessAccountUuid")
            .add("vmsByMem")
            .add("vdcsByTag")
            .add("pmsByState")
            .add("pmsByTag")
            .add("pmsByMem")
            .add("pmsByNumVms")
            .add("pmsByNumCPUs")
            .add("storageByState")
            .add("storageByTag")
            .add("databaseByTag")
            .add("databaseByBusinessAccountUuid")
            .add("databaseServerByTag")
            .add("datacentersByTag")
            .add("databaseServerByBusinessAccountUuid")
            .build();

    private final Map<ClusterInfo.Type, EntityType> clusterTypeToEntityType =
            ImmutableMap.<ClusterInfo.Type, EntityType>builder().put(ClusterInfo.Type.COMPUTE,
                    EntityType.PHYSICAL_MACHINE)
                    .put(ClusterInfo.Type.COMPUTE_VIRTUAL_MACHINE, EntityType.VIRTUAL_MACHINE)
                    .put(ClusterInfo.Type.STORAGE, EntityType.STORAGE)
                    .build();
    private final Map<ClusterInfo.Type, GroupType> clusterTypeToGroupType =
            ImmutableMap.<ClusterInfo.Type, GroupType>builder().put(ClusterInfo.Type.COMPUTE,
                    GroupType.COMPUTE_HOST_CLUSTER)
                    .put(ClusterInfo.Type.COMPUTE_VIRTUAL_MACHINE,
                            GroupType.COMPUTE_VIRTUAL_MACHINE_CLUSTER)
                    .put(ClusterInfo.Type.STORAGE, GroupType.STORAGE_CLUSTER)
                    .build();

    @Override
    public void migrate(Connection connection) throws Exception {
        connection.setAutoCommit(false);
        try {
            final ResultSet rs = connection.createStatement()
                    .executeQuery("SELECT id, group_data, type FROM grouping");
            while (rs.next()) {
                final long oid = rs.getLong("id");
                final byte[] groupData = rs.getBytes("group_data");
                final int groupType = rs.getInt("type");
                if (groupType == Type.GROUP.getNumber()) {
                    parseGroupInfo(connection, oid, groupData);
                } else if (groupType == Type.CLUSTER.getNumber()) {
                    parseClusterInfo(connection, oid, groupData);
                } else if (groupType == Type.NESTED_GROUP.getNumber()) {
                    parseNestedInfo(connection, oid, groupData);
                }
            }
            // Fill in expected group types from static member groups. Expected types are
            // calculated here for user groups with static member groups. We determine expected
            // types based on real types of members.
            connection.createStatement()
                    .execute(
                            "INSERT INTO group_expected_members_groups (group_id, group_type, direct_member) "
                                    + "SELECT DISTINCT gsg.parent_group_id, child.group_type, true"
                                    + "   FROM group_static_members_groups gsg"
                                    + "     LEFT JOIN grouping child ON (gsg.child_group_id = child.id)"
                                    + "     LEFT JOIN grouping parent ON (gsg.parent_group_id = parent.id)"
                                    + "   WHERE parent.origin_discovered_src_id IS NULL");
            // Resolve transitive expected entity member types for nested groups - i.e. groups of groups
            connection.createStatement()
                    .execute(
                            "INSERT INTO group_expected_members_entities (group_id, entity_type, direct_member) "
                                    + "SELECT DISTINCT parent.id, sm.entity_type, false"
                                    + "  FROM grouping parent JOIN group_static_members_groups gsg ON (parent.id = gsg.parent_group_id)"
                                    + "    JOIN group_static_members_entities sm ON (sm.group_id = gsg.child_group_id) "
                                    + "  WHERE parent.origin_discovered_src_id IS NULL");
            connection.commit();
        } catch (InvalidProtocolBufferException | SQLException e) {
            logger.warn("Failed performing migration", e);
            connection.rollback();
            throw e;
        }
        connection.setAutoCommit(true);
    }

    private void parseGroupInfo(@Nonnull Connection connection, long oid, @Nonnull byte[] groupData)
            throws InvalidProtocolBufferException, SQLException {
        final GroupInfo groupInfo = GroupInfo.parseFrom(groupData);
        final String displayName =
                groupInfo.hasDisplayName() ? groupInfo.getDisplayName() : groupInfo.getName();
        final boolean isHidden = groupInfo.getIsHidden();
        final PreparedStatement stmt = connection.prepareStatement(
                "UPDATE grouping SET display_name=?, is_hidden=? WHERE id=?");
        stmt.setString(1, displayName);
        stmt.setBoolean(2, isHidden);
        stmt.setLong(3, oid);
        stmt.addBatch();
        stmt.executeBatch();
        final int entityType = groupInfo.getEntityType();
        createExpectedMemberEntities(connection, oid, entityType);

        switch (groupInfo.getSelectionCriteriaCase()) {
            case STATIC_GROUP_MEMBERS:
                createStaticMembers(connection, oid, entityType, groupInfo.getStaticGroupMembers());
                break;
            case SEARCH_PARAMETERS_COLLECTION:
                // Migrate parameters from the previous versions, if required
                // It is obsolete migration V_01_00_02__String_Filters_Replace_Contains_With_Full_Match
                final MigratedSearchParams filterEditor =
                        new MigratedSearchParams(groupInfo.getSearchParametersCollection());
                final SearchParametersCollection searchParametersCollection =
                        fixExpressionType(filterEditor.getMigratedParams());
                final EntityFilters entityFilters = EntityFilters.newBuilder()
                        .addEntityFilter(EntityFilter.newBuilder()
                                .setEntityType(entityType)
                                .setSearchParametersCollection(searchParametersCollection))
                        .build();
                final byte[] entitiesFilter = entityFilters.toByteArray();
                final PreparedStatement search = connection.prepareStatement(
                        "UPDATE grouping SET entity_filters = ? WHERE id = ?");
                search.setBytes(1, entitiesFilter);
                search.setLong(2, oid);
                search.execute();
                break;
            default:
                logger.warn("Found a group without a selection criteria: " + oid
                        + " will be converted to empty static group");
        }
    }

    private void parseClusterInfo(@Nonnull Connection connection, long oid,
            @Nonnull byte[] groupData) throws InvalidProtocolBufferException, SQLException {
        final ClusterInfo clusterInfo = ClusterInfo.parseFrom(groupData);
        final String displayName = clusterInfo.getDisplayName();
        final int entityType = clusterTypeToEntityType.getOrDefault(clusterInfo.getClusterType(),
                EntityType.PHYSICAL_MACHINE).getNumber();

        final GroupType groupType = clusterTypeToGroupType.getOrDefault(clusterInfo.getClusterType(),
                GroupType.COMPUTE_HOST_CLUSTER);
        final PreparedStatement stmt = connection.prepareStatement(
                "UPDATE grouping SET display_name=?, group_type=? WHERE id=?");
        stmt.setString(1, displayName);
        stmt.setInt(2, groupType.ordinal());
        stmt.setLong(3, oid);
        stmt.execute();
        createStaticMembers(connection, oid, entityType, clusterInfo.getMembers());
        createExpectedMemberEntities(connection, oid, entityType);
    }

    private void parseNestedInfo(@Nonnull Connection connection, long oid,
            @Nonnull byte[] groupData) throws InvalidProtocolBufferException, SQLException {
        final NestedGroupInfo groupInfo = NestedGroupInfo.parseFrom(groupData);
        final String displayName = groupInfo.getName();
        final PreparedStatement stmt =
                connection.prepareStatement("UPDATE grouping SET display_name=? WHERE id=?");
        stmt.setString(1, displayName);
        stmt.setLong(2, oid);
        stmt.execute();
        switch (groupInfo.getSelectionCriteriaCase()) {
            case STATIC_GROUP_MEMBERS:
                createStaticMembersGroups(connection, oid, groupInfo.getStaticGroupMembers());
                break;
            case PROPERTY_FILTER_LIST:
                final GroupPropertyFilterList filterList =
                        nestedGroupInfo716ToNestedGroupInfo(groupInfo.getPropertyFilterList());
                final GroupFilters filter = GroupFilters.newBuilder()
                        .addGroupFilter(GroupFilter.newBuilder()
                                .addAllPropertyFilters(filterList.getPropertyFiltersList()))
                        .build();
                final PreparedStatement search = connection.prepareStatement(
                        "UPDATE grouping SET group_filters = ? WHERE id = ?");
                search.setBytes(1, filter.toByteArray());
                search.setLong(2, oid);
                search.execute();
                createExpectedMemberGroups(connection, oid,
                        Sets.newHashSet(GroupType.STORAGE_CLUSTER, GroupType.COMPUTE_HOST_CLUSTER,
                                GroupType.COMPUTE_VIRTUAL_MACHINE_CLUSTER));
                break;
            default:
                logger.warn("Found a nested group without a selection criteria: " + oid
                        + " will be converted to empty static group");
        }
    }

    /**
     * Translates a valid 7.16 dynamic nested info protobuf object to its corresponding
     * 7.17 representation if necessary.
     *
     * @param nestedGroupInfo716 object to be translated.
     * @return translation
     */
    @Nonnull
    private GroupPropertyFilterList nestedGroupInfo716ToNestedGroupInfo(
            @Nonnull GroupPropertyFilterList nestedGroupInfo716) {
        // If the obsolete field is used, then data came from 7.16 and needs to be migrated
        if (nestedGroupInfo716.getDeprecatedPropertyFiltersOldCount() != 0) {
            return GroupPropertyFilterList.newBuilder()
                    .addPropertyFilters(translatePropertyFilter(
                            nestedGroupInfo716.getDeprecatedPropertyFiltersOld(0)))
                    .build();
        } else {
            return nestedGroupInfo716;
        }
    }

    @Nonnull
    private PropertyFilter translatePropertyFilter(
            @Nonnull GroupPropertyFilter groupPropertyFilter716) {
        return PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.DISPLAY_NAME)
                .setStringFilter(groupPropertyFilter716.getDeprecatedNameFilter())
                .build();
    }

    private void createStaticMembers(@Nonnull Connection connection, long oid, int entityType,
            @Nonnull StaticGroupMembers staticMembers) throws SQLException {
        final Collection<Long> members = staticMembers.getStaticMemberOidsList();
        final PreparedStatement membersStmt = connection.prepareStatement(
                "INSERT IGNORE INTO group_static_members_entities (group_id, entity_type, "
                        + "entity_id) VALUES (?, ?, ?)");
        for (Long member : members) {
            membersStmt.setLong(1, oid);
            membersStmt.setInt(2, entityType);
            membersStmt.setLong(3, member);
            membersStmt.addBatch();
        }
        membersStmt.executeBatch();
    }

    private void createStaticMembersGroups(@Nonnull Connection connection, long oid,
            @Nonnull StaticGroupMembers staticMembers) throws SQLException {
        final Collection<Long> members = staticMembers.getStaticMemberOidsList();
        final PreparedStatement membersStmt = connection.prepareStatement(
                "INSERT IGNORE INTO group_static_members_groups (parent_group_id, child_group_id) "
                        + "VALUES (?, ?)");
        for (Long member : members) {
            membersStmt.setLong(1, oid);
            membersStmt.setLong(2, member);
            membersStmt.addBatch();
        }
        membersStmt.executeBatch();
    }

    private void createExpectedMemberEntities(@Nonnull Connection connection, long oid,
            int expectedEntityType) throws SQLException {
        final PreparedStatement membersStmt = connection.prepareStatement(
                "INSERT INTO group_expected_members_entities (group_id, entity_type, direct_member) VALUES (?,?,true)");
        membersStmt.setLong(1, oid);
        membersStmt.setInt(2, expectedEntityType);
        membersStmt.execute();
    }

    private void createExpectedMemberGroups(@Nonnull Connection connection, long oid,
            @Nonnull Set<GroupType> expectedGroupsTypes) throws SQLException {
        final PreparedStatement membersStmt = connection.prepareStatement(
                "INSERT INTO group_expected_members_groups (group_id, group_type, direct_member) VALUES (?,?,true)");
        for (GroupType memberType: expectedGroupsTypes) {
            membersStmt.setLong(1, oid);
            membersStmt.setInt(2, memberType.ordinal());
            membersStmt.addBatch();
        }
        membersStmt.executeBatch();
    }

    /**
     * Represents the migrated search parameters of a dynamic group.
     *
     * <p>Internally, it will actually migrate the search parameters, and isolates the logic
     * required to do so.
     */
    @Immutable
    private static class MigratedSearchParams {

        private final SetOnce<Boolean> anyPropertyChanged = new SetOnce<>();

        private final SearchParametersCollection searchParams;

        private MigratedSearchParams(@Nonnull final SearchParametersCollection searchParams) {
            this.searchParams = migrateStringFiltersInParams(searchParams);
        }

        private boolean isAnyPropertyChanged() {
            return anyPropertyChanged.getValue().orElse(false);
        }

        /**
         * Returns the migrated params or an original set of parameters if there is nothing to
         * migrate.
         *
         * @return good search parameters for the group
         */
        @Nonnull
        public SearchParametersCollection getMigratedParams() {
            return searchParams;
        }

        private void tryMigrateStringFilter(@Nonnull final StringFilter.Builder strFilter) {
            final String prefix;
            if (!strFilter.getStringPropertyRegex().startsWith("^")) {
                prefix = "^.*";
            } else {
                prefix = "";
            }

            final String suffix;
            if (!strFilter.getStringPropertyRegex().endsWith("$")) {
                suffix = ".*$";
            } else {
                suffix = "";
            }

            if (!prefix.isEmpty() || !suffix.isEmpty()) {
                strFilter.setStringPropertyRegex(
                        prefix + strFilter.getStringPropertyRegex() + suffix);
                anyPropertyChanged.trySetValue(true);
            }
        }

        @Nonnull
        private SearchParametersCollection migrateStringFiltersInParams(
                @Nonnull final SearchParametersCollection searchParametersCollection) {
            final SearchParametersCollection.Builder updatedParamsBldr =
                    searchParametersCollection.toBuilder();
            final Queue<Builder> propertyFilters = new LinkedList<>();

            // "Seed" the property filter queues with all the top-level properties in the search
            // params.
            updatedParamsBldr.getSearchParametersBuilderList().forEach(param -> {
                propertyFilters.add(param.getStartingFilterBuilder());
                param.getSearchFilterBuilderList().forEach(searchFilter -> {
                    switch (searchFilter.getFilterTypeCase()) {
                        case TRAVERSAL_FILTER:
                            final StoppingCondition.Builder stoppingCondition =
                                    searchFilter.getTraversalFilterBuilder()
                                            .getStoppingConditionBuilder();
                            if (stoppingCondition.getStoppingConditionTypeCase()
                                    == StoppingConditionTypeCase.STOPPING_PROPERTY_FILTER) {
                                propertyFilters.add(
                                        stoppingCondition.getStoppingPropertyFilterBuilder());
                            }
                            break;
                        case PROPERTY_FILTER:
                            propertyFilters.add(searchFilter.getPropertyFilterBuilder());
                            break;
                        case GROUP_FILTER:
                            propertyFilters.add(searchFilter.getGroupFilterBuilder()
                                    .getGroupSpecifierBuilder());
                            break;
                        default:
                            break;
                    }
                });
            });

            // Traverse through all nested property filters, and migrate any encountered string
            // filters.
            while (!propertyFilters.isEmpty()) {
                final PropertyFilter.Builder nextFilter = propertyFilters.poll();
                switch (nextFilter.getPropertyTypeCase()) {
                    case STRING_FILTER:
                        tryMigrateStringFilter(nextFilter.getStringFilterBuilder());
                        break;
                    case LIST_FILTER:
                        final ListFilter.Builder listFilter = nextFilter.getListFilterBuilder();
                        if (listFilter.getListElementTypeCase()
                                == ListElementTypeCase.OBJECT_FILTER) {
                            listFilter.getObjectFilterBuilder()
                                    .getFiltersBuilderList()
                                    .forEach(propertyFilters::add);
                        } else if (listFilter.getListElementTypeCase()
                                == ListElementTypeCase.STRING_FILTER) {
                            tryMigrateStringFilter(listFilter.getStringFilterBuilder());
                        }
                        break;
                    case OBJECT_FILTER:
                        nextFilter.getObjectFilterBuilder()
                                .getFiltersBuilderList()
                                .forEach(propertyFilters::add);
                        break;
                    default:
                        // Do nothing.
                }
            }

            if (isAnyPropertyChanged()) {
                return updatedParamsBldr.build();
            } else {
                // Return exactly the input, to avoid the overhead of building the updated params
                // and to ensure byte-level equality.
                return searchParametersCollection;
            }
        }
    }

    /**
     * This migration fixes the API filters stored in the database,
     * in the serializations of GroupInfo objects.
     * In particular, string regex matching filters in 7.16 contain
     * {@link FilterSpecs} objects whose operator is either
     * "EQ" (regex matches) or "NEQ" (regex does not match).
     * In 7.17, these operator names must change to "RXEQ" and "RXNEQ"
     * respectively.
     * This is a part of obsolete migration V_01_00_05__Change_Dynamic_Groups_Api_Filters_Protobuf_Representation
     *
     * @param src search parameters collection to convert
     * @return converted search parameters
     */
    @Nonnull
    private SearchParametersCollection fixExpressionType(@Nonnull SearchParametersCollection src) {
        return SearchParametersCollection.newBuilder()
                .addAllSearchParameters(src.getSearchParametersList()
                        .stream()
                        .map(this::fixFilterSpecs)
                        .collect(Collectors.toList()))
                .build();
    }

    @Nonnull
    private SearchParameters fixFilterSpecs(@Nonnull SearchParameters searchParameters) {
        // if this is one of the API filters that do not need fixing: return
        if (!searchParameters.hasSourceFilterSpecs() || filterTypesThatWork.contains(
                searchParameters.getSourceFilterSpecs().getFilterType())) {
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
