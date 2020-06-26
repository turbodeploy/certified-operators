package db.migration;

import static com.vmturbo.common.protobuf.search.SearchableProperties.ENTITY_TYPE;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.apache.commons.collections4.SetUtils;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters.EntityFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * The class for migration of deprecated entity types in the Group Component data models.
 * 1 Policies:
 *   1.1 Placement (remove all deprecated):
 *       - remove ones that scoped by static deprecated groups;
 *       - remove ones that scoped by dynamic deprecated groups;
 *   1.2 Automation (keep ones that are connected to dynamic groups):
 *       - remove ones that scoped by static deprecated groups;
 *       - remove ones that scoped by dynamic deprecated groups which cannot be migrated;
 *       - update 'group_component.setting_policy'.
 * 2 Groups:
 *   2.1 Migrate dynamic groups which can be migrated:
 *       - update 'group_component.grouping.entity_filters';
 *       - update 'group_component.group_expected_members_entities.entity_type';
 *   2.2 Remove all static deprecated groups:
 *       - table 'grouping';
 *       - table 'group_static_members_entities';
 *   2.3 Remove all dynamic deprecated groups which cannot be migrated:
 *       - table 'grouping';
 *       - table 'group_static_members_entities';
 */
public class V1_32__DeprecatedEntityTypeMigration extends BaseJdbcMigration {

    private static Set<EntityType> deprecatedTypes = Sets.newHashSet(EntityType.APPLICATION,
            EntityType.APPLICATION_SERVER, EntityType.LOAD_BALANCER, EntityType.VIRTUAL_APPLICATION
    );

    private static Map<EntityType, EntityType> migrationTypeMapping = Maps.newHashMap();

    static {
        migrationTypeMapping.put(EntityType.APPLICATION, EntityType.APPLICATION_COMPONENT);
        migrationTypeMapping.put(EntityType.APPLICATION_SERVER, EntityType.APPLICATION_COMPONENT);
        migrationTypeMapping.put(EntityType.VIRTUAL_APPLICATION, EntityType.SERVICE);
    }

    @Override
    protected void performMigrationTasks(Connection connection) throws SQLException, IOException {
        final Multimap<EntityType, Long> deprecatedDynamicGroupsMultimap = getDeprecatedDynamicGroups(connection);
        final Set<Long> deprecatedStaticGroups = getDeprecatedStaticGroups(connection);
        final Set<Long> allDeprecatedGroups = SetUtils.union(deprecatedStaticGroups, Sets.newHashSet(deprecatedDynamicGroupsMultimap.values()));
        final Set<Long> deprecatedDynamicGroupsToMigrate = getDynamicGroupsToMigrate(deprecatedDynamicGroupsMultimap);
        final Set<Long> deprecatedDynamicGroupsToRemove = getDynamicGroupsToRemove(deprecatedDynamicGroupsMultimap);
        // Policies
        removeDeprecatedPlacementPolicies(allDeprecatedGroups, connection);
        removeDeprecatedAutomationPolicies(deprecatedStaticGroups, connection);
        removeDeprecatedAutomationPolicies(deprecatedDynamicGroupsToRemove, connection);
        migrateAutomationPolicies(connection);
        // Groups
        migrateDynamicGroups(connection, deprecatedDynamicGroupsToMigrate);
        removeDeprecatedGroups(connection, deprecatedDynamicGroupsToRemove);
        removeDeprecatedGroups(connection, deprecatedStaticGroups);
    }

    @Nonnull
    @ParametersAreNonnullByDefault
    private Set<Long> getDynamicGroupsToMigrate(Multimap<EntityType, Long> deprecatedDynamicGroupsMultimap) {
        final Set<Long> dynamicGroupsToMigrate = Sets.newHashSet();
        for (EntityType type : migrationTypeMapping.keySet()) {
            if (deprecatedDynamicGroupsMultimap.containsKey(type)) {
                dynamicGroupsToMigrate.addAll(deprecatedDynamicGroupsMultimap.get(type));
            }
        }
        return dynamicGroupsToMigrate;
    }

    @Nonnull
    @ParametersAreNonnullByDefault
    private Set<Long> getDynamicGroupsToRemove(Multimap<EntityType, Long> deprecatedDynamicGroupsMultimap) {
        final Set<Long> dynamicGroupsToRemove = Sets.newHashSet();
        final Set<EntityType> typesToRemove = Sets.difference(deprecatedTypes, migrationTypeMapping.keySet());
        for (EntityType type : typesToRemove) {
            if (deprecatedDynamicGroupsMultimap.containsKey(type)) {
                dynamicGroupsToRemove.addAll(deprecatedDynamicGroupsMultimap.get(type));
            }
        }
        return dynamicGroupsToRemove;
    }

    /**
     * Remove all Placement Policies which are connected to deprecated groups (static and dynamic).
     * 1) Get all policies IDs connected to deprecated groups from the table 'policy_group';
     * 2) Remove policies from tables 'policy' and 'policy_group'.
     *
     * @param deprecatedGroups - IDs of deprecated groups.
     * @param connection       - JDBC connection.
     * @throws SQLException - in case of any SQL error.
     */
    @ParametersAreNonnullByDefault
    private void removeDeprecatedPlacementPolicies(Set<Long> deprecatedGroups,
                                                   Connection connection) throws SQLException {
        if (deprecatedGroups.isEmpty()) {
            return;
        }
        final String selectQuery = String.format("SELECT policy_id FROM group_component.policy_group " +
                "WHERE group_id IN (%s)", createJoiningString(deprecatedGroups));
        final ResultSet resultSet = connection.createStatement().executeQuery(selectQuery);
        final Set<Long> deprecatedPolicies = Sets.newHashSet();
        while (resultSet.next()) {
            deprecatedPolicies.add(resultSet.getLong("policy_id"));
        }
        if (deprecatedPolicies.isEmpty()) {
            return;
        }
        final String policiesIds = createJoiningString(deprecatedPolicies);
        final Statement statement = connection.createStatement();
        statement.addBatch(String.format("DELETE FROM group_component.policy WHERE id IN (%s);", policiesIds));
        statement.addBatch(String.format("DELETE FROM group_component.policy_group WHERE policy_id IN (%s);", policiesIds));
        statement.executeBatch();
    }

    /**
     * Remove all Automated Policies which are connected to deprecated static groups.
     * 1) Get all policies IDs connected to deprecated static groups from the table 'setting_policy_groups';
     * 2) Remove policies from tables 'setting_policy', 'setting_policy_groups' and 'setting_policy_setting'.
     *
     * @param deprecatedGroups - IDs of deprecated groups. All static groups or dynamic groups which can not be converted.
     * @param connection       - JDBC connection.
     * @throws SQLException - in case of any SQL error.
     */
    @ParametersAreNonnullByDefault
    private void removeDeprecatedAutomationPolicies(Set<Long> deprecatedGroups,
                                                    Connection connection) throws SQLException {
        if (deprecatedGroups.isEmpty()) {
            return;
        }
        final String selectQuery = String.format("SELECT setting_policy_id FROM group_component.setting_policy_groups " +
                "WHERE group_id IN (%s)", createJoiningString(deprecatedGroups));
        final ResultSet resultSet = connection.createStatement().executeQuery(selectQuery);
        final Set<Long> deprecatedPolicies = Sets.newHashSet();
        while (resultSet.next()) {
            deprecatedPolicies.add(resultSet.getLong("setting_policy_id"));
        }
        if (deprecatedPolicies.isEmpty()) {
            return;
        }
        final String policiesIds = createJoiningString(deprecatedPolicies);
        final Statement statement = connection.createStatement();
        statement.addBatch(String.format("DELETE FROM group_component.setting_policy WHERE id IN (%s);", policiesIds));
        statement.addBatch(String.format("DELETE FROM group_component.setting_policy_groups WHERE setting_policy_id IN (%s);", policiesIds));
        statement.addBatch(String.format("DELETE FROM group_component.setting_policy_setting WHERE policy_id IN (%s);", policiesIds));
        statement.executeBatch();
    }

    /**
     * Update the table 'setting_policy': set new entity type.
     *
     * @param connection - JDBC connection.
     * @throws SQLException - in case of any SQL error.
     */
    private void migrateAutomationPolicies(Connection connection) throws SQLException {
        final Statement statement = connection.createStatement();
        for (final Entry<EntityType, EntityType> entry : migrationTypeMapping.entrySet()) {
            final int deprecatedType = entry.getKey().getNumber();
            final int newType = entry.getValue().getNumber();
                final String updateSql = String.format("" +
                        "UPDATE group_component.setting_policy " +
                        "SET entity_type=%d " +
                        "WHERE id IN (" +
                        "    SELECT id " +
                        "    FROM group_component.setting_policy " +
                        "    WHERE entity_type=%d" +
                    ")", newType, deprecatedType);
                statement.addBatch(updateSql);
        }
        statement.executeBatch();
    }

    @Nonnull
    private String createJoiningString(@Nonnull Set<Long> ids) {
        return ids.stream().map(String::valueOf).collect(Collectors.joining(","));
    }

    /**
     * Remove all groups which have static member with deprecated entity types.
     * 1) remove from the table 'grouping';
     * 2) remove from the table 'group_static_members_entities'.
     *
     * @param connection - JDBC connection.
     * @param groupIds   - IDs of deprecated static groups.
     * @throws SQLException - in case of any SQL error.
     */
    @ParametersAreNonnullByDefault
    private void removeDeprecatedGroups(Connection connection, Set<Long> groupIds) throws SQLException {
        if (!groupIds.isEmpty()) {
            final Statement statement = connection.createStatement();
            final String ids = createJoiningString(groupIds);
            statement.addBatch(String.format("DELETE FROM group_component.grouping WHERE id IN (%s);", ids));
            statement.addBatch(String.format("DELETE FROM group_component.group_static_members_entities WHERE group_id IN (%s)", ids));
            statement.addBatch(String.format("DELETE FROM group_component.group_expected_members_entities WHERE group_id IN (%s)", ids));
            statement.executeBatch();
        }
    }

    /**
     * Returns IDs of static groups which members have deprecated entity types.
     *
     * @param connection - JDBC connection.
     * @return a set of deprecated static groups IDs.
     * @throws SQLException - in case of any SQL error.
     */
    @Nonnull
    private Set<Long> getDeprecatedStaticGroups(@Nonnull Connection connection) throws SQLException {
        final String deprecatedTypesNumbers = deprecatedTypes.stream().map(EntityType::getNumber)
                .map(String::valueOf).collect(Collectors.joining(","));
        final String query = String.format("SELECT distinct(group_id) FROM group_component.group_static_members_entities " +
                "WHERE entity_type IN (%s)", deprecatedTypesNumbers);
        final ResultSet result = connection.createStatement().executeQuery(query);
        final Set<Long> ids = Sets.newHashSet();
        while (result.next()) {
            ids.add(result.getLong(1));
        }
        return ids;
    }

    /**
     * Returns IDs of dynamic groups which members have deprecated entity type.
     * It is defined by reading {@link EntityFilters} model from 'entity_filters' column.
     *
     * @param connection - JDBC connection.
     * @return a set of deprecated dynamic groups IDs.
     * @throws SQLException - in case of any SQL error.
     * @throws IOException  - in case of any error while reading BLOBs.
     */
    @Nonnull
    private Multimap<EntityType, Long> getDeprecatedDynamicGroups(@Nonnull Connection connection) throws SQLException, IOException {
        final Set<Integer> deprecatedTypeNumbers = deprecatedTypes.stream().map(EntityType::getNumber).collect(Collectors.toSet());
        final Multimap<EntityType, Long> multimap = ArrayListMultimap.create();
        final ResultSet result = connection.createStatement().executeQuery(
                "SELECT id, entity_filters FROM group_component.grouping WHERE entity_filters IS NOT NULL");
        while (result.next()) {
            final EntityFilters entityFilters = readEntityFilter(result.getBlob("entity_filters"));
            final EntityType deprecatedType = findDeprecatedType(entityFilters, deprecatedTypeNumbers);
            if (Objects.nonNull(deprecatedType)) {
                multimap.put(deprecatedType, result.getLong("id"));
            }
        }
        return multimap;
    }

    /**
     * Check if the {@link EntityFilters} instance has a filter with deprecated entity type.
     *
     * @param filters         - entity filter to check.
     * @param deprecatedTypes - deprecated entity types numbers.
     * @return EntityType if a deprecated type is found.
     */
    @Nullable
    @ParametersAreNonnullByDefault
    private EntityType findDeprecatedType(EntityFilters filters, Set<Integer> deprecatedTypes) {
        for (final EntityFilter filter : filters.getEntityFilterList()) {
            if (deprecatedTypes.contains(filter.getEntityType())) {
                return EntityType.forNumber(filter.getEntityType());
            }
        }
        return null;
    }

    /**
     * Update deprecated dynamic groups: configure then with new entity types.
     * 1) get 'entity_filters' values of deprecated dynamic groups;
     * 2) convert them with new entity types;
     * 3) update 'grouping' table with new values;
     * 4) update 'group_expected_members_entities' table: set new 'entity_type' column values.
     *
     * @param connection - JDBC connection.
     * @param deprecatedDynamicGroups - IDs of deprecated dynamic groups.
     * @throws SQLException - in case of any SQL error.
     * @throws IOException - in case of any error while reading BLOBs.
     */
    @ParametersAreNonnullByDefault
    private void migrateDynamicGroups(Connection connection, Set<Long> deprecatedDynamicGroups) throws SQLException, IOException {
        if (deprecatedDynamicGroups.isEmpty()) {
            return;
        }
        final String ids = createJoiningString(deprecatedDynamicGroups);
        final String selectQuery = String.format("SELECT id, entity_filters FROM group_component.grouping WHERE id IN (%s)", ids);
        final PreparedStatement updGrouping = connection.prepareStatement("UPDATE group_component.grouping SET entity_filters=? WHERE id=?");
        final ResultSet result = connection.createStatement().executeQuery(selectQuery);
        while (result.next()) {
            final Blob filtersBlob = result.getBlob("entity_filters");
            if (filtersBlob != null) {
                final EntityFilters entityFilters = readEntityFilter(filtersBlob);
                final List<EntityFilter> convertedList = entityFilters.getEntityFilterList().stream()
                        .map(this::convertFilter).collect(Collectors.toList());
                final EntityFilters entityFiltersConverted = entityFilters.toBuilder()
                        .clearEntityFilter().addAllEntityFilter(convertedList).build();
                updGrouping.setObject(1, entityFiltersConverted.toByteArray());
                updGrouping.setObject(2, result.getLong("id"));
                updGrouping.addBatch();
            }
        }
        updGrouping.executeBatch();
        final Statement updMembers = connection.createStatement();
        for (final Entry<EntityType, EntityType> entry : migrationTypeMapping.entrySet()) {
            final int deprecatedType = entry.getKey().getNumber();
            final int newType = entry.getValue().getNumber();
            final String updateSql = String.format("" +
                    "UPDATE group_component.group_expected_members_entities " +
                    "SET entity_type=%d " +
                    "WHERE group_id IN (" +
                    "    SELECT group_id " +
                    "    FROM group_component.group_expected_members_entities " +
                    "    WHERE entity_type=%d" +
                    ")", newType, deprecatedType);
            updMembers.addBatch(updateSql);
        }
        updMembers.executeBatch();
    }

    @Nonnull
    private EntityFilter convertFilter(@Nonnull EntityFilter filter) {
        final EntityType oldType = EntityType.forNumber(filter.getEntityType());
        final EntityType newType = migrationTypeMapping.get(oldType);
        if (newType != null) {
            final EntityFilter.Builder builder = filter.toBuilder();
            // Set new entity type.
            builder.setEntityType(newType.getNumber());
            if (filter.hasSearchParametersCollection()) {
                // Update SearchParametersCollection: a starting_filter (PropertyFilter) in a 'SearchParameters'
                // also have en entity type definition which should be changed.
                final SearchParametersCollection searchParametersCollectionConverted =
                        convertSearchParameterCollection(filter.getSearchParametersCollection());
                builder.clearSearchParametersCollection()
                        .setSearchParametersCollection(searchParametersCollectionConverted);
            }
            return builder.build();
        }
        return filter;
    }

    /**
     * Update entity type in 'starting_filter' of SearchParameters. This 'starting_filter' has name
     * 'entityType' and NumericFilter with value of deprecated entity type.
     *
     * @param parameter - {@link SearchParameters} instance.
     * @return {@link SearchParameters} instance with changes entity type.
     */
    @Nonnull
    private SearchParameters convertSearchParameter(@Nonnull SearchParameters parameter) {
        if (parameter.hasStartingFilter()) {
            final PropertyFilter startingFilter = parameter.getStartingFilter();
            if (startingFilter.getPropertyName().equals(ENTITY_TYPE)) {
                final PropertyFilter.Builder startingFilterBuilder = startingFilter.toBuilder();
                if (startingFilter.hasNumericFilter()) {
                    final NumericFilter numericFilter = startingFilter.getNumericFilter();
                    final EntityType entityType = EntityType.forNumber((int)numericFilter.getValue());
                    if (migrationTypeMapping.containsKey(entityType)) {
                        final EntityType newEntityType = migrationTypeMapping.get(entityType);
                        startingFilterBuilder.clearNumericFilter().setNumericFilter(
                                numericFilter.toBuilder().setValue(newEntityType.getNumber()).build()
                        );
                    }
                }
                return parameter.toBuilder().clearStartingFilter()
                        .setStartingFilter(startingFilterBuilder).build();
            }
        }
        return parameter;
    }

    @Nonnull
    private SearchParametersCollection convertSearchParameterCollection(
            @Nonnull SearchParametersCollection collection) {
        final List<SearchParameters> searchParametersConverted = collection
                .getSearchParametersList().stream().map(this::convertSearchParameter)
                .collect(Collectors.toList());
        return collection.toBuilder().clearSearchParameters()
                .addAllSearchParameters(searchParametersConverted).build();
    }

    /**
     * Reads bytes from a BLOB and create {@link EntityFilters} using them.
     *
     * @param blob - BLOB object to read.
     * @return an {@link EntityFilters} instance.
     * @throws SQLException - in case of any SQL error.
     * @throws IOException  - in case of any error while reading BLOBs.
     */
    @Nonnull
    private EntityFilters readEntityFilter(@Nonnull Blob blob) throws IOException, SQLException {
        final InputStream stream = blob.getBinaryStream();
        final byte[] targetArray = new byte[stream.available()];
        stream.read(targetArray);
        return EntityFilters.parseFrom(targetArray);
    }

}
