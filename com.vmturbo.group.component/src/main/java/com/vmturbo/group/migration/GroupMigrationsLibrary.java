package com.vmturbo.group.migration;

import java.util.Objects;
import java.util.SortedMap;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSortedMap;

import org.jooq.DSLContext;

import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.group.setting.SettingStore;

/**
 * Container for all the migrations for the group component.
 */
public class GroupMigrationsLibrary {

    private final DSLContext dslContext;

    private final SettingStore settingStore;

    public GroupMigrationsLibrary(@Nonnull final DSLContext dslContext,
                                  @Nonnull final SettingStore settingStore) {
        this.dslContext = Objects.requireNonNull(dslContext);
        this.settingStore = Objects.requireNonNull(settingStore);
    }

    /**
     * Get the list of migrations, in the order they should be run.
     *
     * @return Sorted map of migrations by name.
     */
    public SortedMap<String, Migration> getMigrationsList(){
        return ImmutableSortedMap.<String, Migration>naturalOrder()
            .put(V_01_00_03__Change_Default_Transactions_Capacity.class.getSimpleName(),
                new V_01_00_03__Change_Default_Transactions_Capacity(settingStore))
            .build();
        // Note, that migrations up to V_01_00_05 has been previously removed. The next migration
        // MUST be no less then V_01_00_06 in order to avoid collisions at customers' side
    }
}
