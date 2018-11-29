package com.vmturbo.group.migration;

import java.util.Objects;
import java.util.SortedMap;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.google.common.collect.ImmutableSortedMap;

import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.group.group.GroupStore;

/**
 * Container for all the migrations for the group component.
 */
public class GroupMigrationsLibrary {

    private final DSLContext dslContext;

    public GroupMigrationsLibrary(@Nonnull final DSLContext dslContext) {
        this.dslContext = Objects.requireNonNull(dslContext);
    }

    /**
     * Get the list of migrations, in the order they should be run.
     *
     * @return Sorted map of migrations by name.
     */
    public SortedMap<String, Migration> getMigrationsList(){
        return ImmutableSortedMap.<String, Migration>naturalOrder()
            .put(V_01_00_00__Group_Table_Add_Entity_Type.class.getSimpleName(),
                 new V_01_00_00__Group_Table_Add_Entity_Type(dslContext))
            .put(V_01_00_01__Drop_Discovered_Groups_Policies.class.getSimpleName(),
                    new V_01_00_01__Drop_Discovered_Groups_Policies(dslContext))
            .build();
    }
}
