package com.vmturbo.auth.component.migration;

import java.util.Objects;
import java.util.SortedMap;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSortedMap;

import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.kvstore.KeyValueStore;

public class MigrationsLibrary {

    private final KeyValueStore keyValueStore;

    public MigrationsLibrary(@Nonnull KeyValueStore keyValueStore) {
        this.keyValueStore = Objects.requireNonNull(keyValueStore);
    }

    public SortedMap<String, Migration> getMigrationsList() {
        return ImmutableSortedMap.of(
            "V_01_00_00__Assign_Oid_To_Security_Groups",
            new V_01_00_00__Assign_Oid_To_Security_Groups(keyValueStore)
        );
    }
}
