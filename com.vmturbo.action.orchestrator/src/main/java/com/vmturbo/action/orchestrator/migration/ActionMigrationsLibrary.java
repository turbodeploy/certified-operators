package com.vmturbo.action.orchestrator.migration;

import com.google.common.collect.ImmutableSortedMap;
import com.vmturbo.components.common.migration.Migration;
import org.jooq.DSLContext;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.SortedMap;

/**
 * Container for all the migrations for the action-orchestrator component.
 */
public class ActionMigrationsLibrary {

    private final DSLContext dslContext;

    public ActionMigrationsLibrary(@Nonnull final DSLContext dslContext) {
        this.dslContext = Objects.requireNonNull(dslContext);
    }

    /**
     * Get the list of migrations, in the order they should be run.
     *
     * @return Sorted map of migrations by name.
     */
    public SortedMap<String, Migration> getMigrationsList(){
        return ImmutableSortedMap.<String, Migration>naturalOrder()
            .put(V_01_00_00__Provision_Action_Reason.class.getSimpleName(),
                 new V_01_00_00__Provision_Action_Reason(dslContext))
            .build();
    }
}
