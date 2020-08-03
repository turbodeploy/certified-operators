package com.vmturbo.api.component.external.api.util;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

/**
 * Class containing page of some objects.
 *
 * @param <T> objects type of the page
 */
public class ObjectsPage<T> {
    private final List<T> objects;
    private final int totalCount;
    private final int nextCursor;

    /**
     * Constructs objects page.
     *
     * @param objects objects this page owns
     * @param totalCount total count of objects (including object in the page).
     * @param nextCursor next cursor to query
     */
    public ObjectsPage(@Nonnull List<T> objects, int totalCount, int nextCursor) {
        this.objects = Collections.unmodifiableList(objects);
        if (nextCursor > totalCount) {
            throw new IllegalArgumentException(String.format(
                    "Next cursor %d must not be larger then total count %d for objects: %s",
                    nextCursor, totalCount, objects));
        }
        this.totalCount = totalCount;
        this.nextCursor = nextCursor;
    }

    @Nonnull
    public List<T> getObjects() {
        return objects;
    }

    public int getTotalCount() {
        return totalCount;
    }

    /**
     * Returns a cursor value for the next page. If next cursor equals total count, this means
     * there will be no further pages.
     *
     * @return next cursor
     */
    public int getNextCursor() {
        return nextCursor;
    }

    /**
     * Creates an empty page - containing no elements.
     *
     * @param <T> type of objects for the page
     * @return objects page for empty collection
     */
    @Nonnull
    public static <T> ObjectsPage<T> createEmptyPage() {
        return new ObjectsPage<T>(Collections.emptyList(), 0, 0);
    }
}
