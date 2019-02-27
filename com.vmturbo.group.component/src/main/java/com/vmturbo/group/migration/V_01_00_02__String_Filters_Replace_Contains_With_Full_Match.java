package com.vmturbo.group.migration;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo.SelectionCriteriaCase;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.ListFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.ListFilter.ListElementTypeCase;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition.StoppingConditionTypeCase;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.group.common.DuplicateNameException;
import com.vmturbo.group.common.ImmutableUpdateException.ImmutableGroupUpdateException;
import com.vmturbo.group.common.ItemNotFoundException.GroupNotFoundException;
import com.vmturbo.group.group.GroupStore;

/**
 * This migration affects the string regexes in a group definition.
 *
 * As part of the change for OM-42806, all user-created search string regexes - including those
 * used for group definitions - now start with ^ and end with $, thereby matching the entire
 * property value. This migration is responsible for replacing all string regexes in existing
 * group definitions to use the new format.
 *
 * To migrate a string regex, we add "^.*" as a prefix and ".*$" as a suffix. Note - we only do this
 * for regexes that don't already begin with "^" or end with "$".
 *
 * Suppose Barney has two VMs - "myVM", "myVM1". Before the OM-42806 change if he created a group
 * with "name equals 'myVM'" the group would have two members - myVM and myVM1. After the change
 * if he creates a group with "name equals 'myVM'"  the group will have one member - myVM.
 *
 * However, if the Barney saved the group (name equals 'myVM') before the change, we assume he WANTS
 * to have "myVM" and "myVM1" in the group. Why? Because that's what the preview would have shown!
 * So we change it to "^.*myVM.*$".
 *
 * The reason we need to change it is if Barney goes back to the UI and loads the group definition,
 * we want to make sure he sees the members he expects ("myVM" and "myVM1"). If we don't migrate the
 * group definition, the system will work fine (because under the covers 'myVM' is equivalent to
 * '^.*myVM.*$' anyway) but when Barney loads the group "Groups" page in the UI - to edit it,
 * for example - the preview of the members will be wrong (because the API will add ^ and $ to
 * "myVM").
 */
public class V_01_00_02__String_Filters_Replace_Contains_With_Full_Match implements Migration {

    private static final Logger logger = LogManager.getLogger();

    private final GroupStore groupStore;

    private final SearchParamsMigrator editorFactory;

    private final Object migrationInfoLock = new Object();

    @GuardedBy("migrationInfoLock")
    private final MigrationProgressInfo.Builder migrationInfo =
        MigrationProgressInfo.newBuilder();

    public V_01_00_02__String_Filters_Replace_Contains_With_Full_Match(@Nonnull final GroupStore groupStore) {
        this(groupStore, MigratedSearchParams::new);
    }

    @VisibleForTesting
    V_01_00_02__String_Filters_Replace_Contains_With_Full_Match(
            @Nonnull final GroupStore groupStore,
            @Nonnull final SearchParamsMigrator editorFactory) {
        this.groupStore = Objects.requireNonNull(groupStore);
        this.editorFactory = Objects.requireNonNull(editorFactory);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MigrationStatus getMigrationStatus() {
        synchronized (migrationInfoLock) {
            return migrationInfo.getStatus();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MigrationProgressInfo getMigrationInfo() {
        synchronized (migrationInfoLock) {
            return migrationInfo.build();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MigrationProgressInfo startMigration() {
        logger.info("Starting migration...");
        synchronized (migrationInfoLock) {
            migrationInfo.setStatus(MigrationStatus.RUNNING)
                .setStatusMessage("Calculating group updates...");
        }

        final Map<Long, GroupInfo> changedGroupInfosById = new HashMap<>();
        logger.info("Calculating groups to change...");
        groupStore.getAll().stream()
            // We only care about dynamic, user-created groups.
            .filter(group -> group.getType() == Type.GROUP)
            .filter(group -> group.getOrigin() == Origin.USER)
            .filter(group -> group.getGroup().getSelectionCriteriaCase()
                == SelectionCriteriaCase.SEARCH_PARAMETERS_COLLECTION)
            .forEach(group -> {
                final MigratedSearchParams filterEditor =
                    editorFactory.migrateSearchParams(group.getGroup().getSearchParametersCollection());
                // We only care about migrating groups where some property needed to be changed.
                if (filterEditor.isAnyPropertyChanged()) {
                    final GroupInfo.Builder groupInfoBuilder = group.getGroup().toBuilder();
                    groupInfoBuilder.setSearchParametersCollection(
                        filterEditor.getMigratedParams());
                    changedGroupInfosById.put(group.getId(), groupInfoBuilder.build());
                }
            });

        logger.info("{} groups need migration.", changedGroupInfosById.size());
        synchronized (migrationInfoLock) {
            migrationInfo.setStatus(MigrationStatus.RUNNING)
                .setCompletionPercentage(50)
                .setStatusMessage("Applying group updates...");
        }

        changedGroupInfosById.forEach((groupId, groupInfo) -> {
            try {
                // Update the groups one at a time - there shouldn't be too many groups, so we
                // don't need to try to do it in bulk.
                logger.info("Updating group {}...", groupId);
                groupStore.updateUserGroup(groupId, groupInfo);
            } catch (ImmutableGroupUpdateException e) {
                // This shouldn't happen because we only edit user groups.
                // Don't error out completely because we want to try to migrate the other groups.
                logger.error("Unexpected immutable update exception when updating " +
                    "user group {}. Error: {}", groupId, e.getMessage());
            } catch (GroupNotFoundException e) {
                // This might happen if the group somehow got deleted while we're running
                // the migration. If the group doesn't exist anymore - no problem! Don't need
                // to worry about its filters. Realistically this should never happen because
                // we run migrations before the user can interact with the system.
                logger.warn("Group {} not found for string filter update. Error: {}",
                    groupId, e.getMessage());
            } catch (DuplicateNameException e) {
                // This could maybe happen if group names changed underneath somehow.
                // Realistically this should never happen because
                // we run migrations before the user can interact with the system.
                logger.error("Unexpected duplicate name exception when updating user group {}. Error: {}",
                    groupId, e.getMessage());
            }
        });

        logger.info("Finished migration!");

        synchronized (migrationInfoLock) {
            return migrationInfo.setStatus(MigrationStatus.SUCCEEDED)
                .setCompletionPercentage(100)
                .setStatusMessage("Replaced all relevant string filters")
                .build();
        }
    }

    /**
     * Factory for {@link MigratedSearchParams}s, for isolation during unit testing.
     */
    @VisibleForTesting
    @FunctionalInterface
    interface SearchParamsMigrator {

        @Nonnull
        MigratedSearchParams migrateSearchParams(@Nonnull final SearchParametersCollection searchParams);

    }

    /**
     * Default implementation of {@link SearchParamsMigrator}.
     */
    static class DefaultSearchParamsMigrator implements SearchParamsMigrator {

        @Nonnull
        @Override
        public MigratedSearchParams migrateSearchParams(@Nonnull final SearchParametersCollection searchParams) {
            return new MigratedSearchParams(searchParams);
        }
    }

    /**
     * Represents the migrated search parameters of a dynamic group.
     *
     * Internally, it will actually migrate the search parameters, and isolates the logic
     * required to do so.
     */
    @VisibleForTesting
    @Immutable
    static class MigratedSearchParams {

        private final SetOnce<Boolean> anyPropertyChanged = new SetOnce<>();

        private final SearchParametersCollection searchParams;

        private MigratedSearchParams(@Nonnull final SearchParametersCollection searchParams) {
            this.searchParams = migrateStringFiltersInParams(searchParams);
        }

        /**
         * Returns the migrated params. If {@link MigratedSearchParams#isAnyPropertyChanged()} is
         * false, returns the original params.
         */
        @Nonnull
        public SearchParametersCollection getMigratedParams() {
            return searchParams;
        }

        /**
         * @return Whether or not the migration modified any properties of the input search
         *         parameters.
         */
        public boolean isAnyPropertyChanged() {
            return anyPropertyChanged.getValue().orElse(false);
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
                strFilter.setStringPropertyRegex(prefix + strFilter.getStringPropertyRegex() + suffix);
                anyPropertyChanged.trySetValue(true);
            }
        }

        @Nonnull
        private SearchParametersCollection migrateStringFiltersInParams(
                @Nonnull final SearchParametersCollection searchParametersCollection) {
            final SearchParametersCollection.Builder updatedParamsBldr = searchParametersCollection.toBuilder();
            final Queue<PropertyFilter.Builder> propertyFilters = new LinkedList<>();

            // "Seed" the property filter queues with all the top-level properties in the search
            // params.
            updatedParamsBldr.getSearchParametersBuilderList().forEach(param -> {
                propertyFilters.add(param.getStartingFilterBuilder());
                param.getSearchFilterBuilderList().forEach(searchFilter -> {
                    switch (searchFilter.getFilterTypeCase()) {
                        case TRAVERSAL_FILTER:
                            final StoppingCondition.Builder stoppingCondition =
                                searchFilter.getTraversalFilterBuilder().getStoppingConditionBuilder();
                            if (stoppingCondition.getStoppingConditionTypeCase() ==
                                    StoppingConditionTypeCase.STOPPING_PROPERTY_FILTER) {
                                propertyFilters.add(stoppingCondition.getStoppingPropertyFilterBuilder());
                            }
                            break;
                        case PROPERTY_FILTER:
                            propertyFilters.add(searchFilter.getPropertyFilterBuilder());
                            break;
                        case CLUSTER_MEMBERSHIP_FILTER:
                            propertyFilters.add(searchFilter.getClusterMembershipFilterBuilder().getClusterSpecifierBuilder());
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
                        if (listFilter.getListElementTypeCase() == ListElementTypeCase.OBJECT_FILTER) {
                            listFilter.getObjectFilterBuilder()
                                .getFiltersBuilderList().forEach(propertyFilters::add);
                        } else if (listFilter.getListElementTypeCase() == ListElementTypeCase.STRING_FILTER) {
                            tryMigrateStringFilter(listFilter.getStringFilterBuilder());
                        }
                        break;
                    case OBJECT_FILTER:
                        nextFilter.getObjectFilterBuilder()
                            .getFiltersBuilderList().forEach(propertyFilters::add);
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
}
