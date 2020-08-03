package com.vmturbo.group.setting;

import java.util.Collection;
import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * A result of a collection update. 2 collections are formed: one to delete (by OIDs) and another
 * one is to insert.
 *
 * @param <T> type of objects
 */
public class CollectionsUpdate<T> {
    private final Collection<T> objectsToAdd;
    private final Collection<Long> objectsToDelete;

    /**
     * Constructs collections update object.
     *
     * @param objectsToAdd collection of objects to add
     * @param objectsToDelete collection of objects to remove
     */
    public CollectionsUpdate(@Nonnull Collection<T> objectsToAdd,
            @Nonnull Collection<Long> objectsToDelete) {
        this.objectsToAdd = Objects.requireNonNull(objectsToAdd);
        this.objectsToDelete = Objects.requireNonNull(objectsToDelete);
    }

    public Collection<T> getObjectsToAdd() {
        return objectsToAdd;
    }

    public Collection<Long> getObjectsToDelete() {
        return objectsToDelete;
    }
}
