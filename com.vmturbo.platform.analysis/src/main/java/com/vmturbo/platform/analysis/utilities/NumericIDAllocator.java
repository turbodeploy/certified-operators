package com.vmturbo.platform.analysis.utilities;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.checkerframework.checker.javari.qual.PolyRead;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

/**
 * Allocates consecutive unique numerical IDs to string IDs starting from 0 and translates between
 * them.
 */
public final class NumericIDAllocator {
    // Fields
    private final @NonNull Map<@NonNull String, @NonNull Integer> ids_ = new LinkedHashMap<>();
    private final @NonNull List<@NonNull String> names_ = new ArrayList<>();

    // Constructors

    /**
     * Constructs an empty numeric ID allocator.
     */
    public NumericIDAllocator() {
        // nothing to do
    }

    // Methods

    /**
     * Allocates a new numerical ID for the specified name and returns it or returns the already
     * allocated ID for this name if one exists.
     *
     * @param name The name with which the new ID will be associated.
     * @return The ID associated with the specified name (either newly allocated or not).
     */
    public int allocate(@NonNull String name) {
        Integer id = ids_.get(name);

        if (id != null) {
            return id;
        } else {
            ids_.put(name, names_.size());
            names_.add(name);
            return names_.size()-1;
        }
    }

    /**
     * Returns the ID associated with a specified name or {@code null} if none is associated.
     *
     * @param name The name for which to query the ID. It must have been used with
     *             {@link #allocate(String)}.
     * @return The numerical ID associated with name.
     */
    @Pure
    public int getId(@ReadOnly NumericIDAllocator this, @NonNull String name) {
        return ids_.get(name);
    }

    /**
     * Returns the name with which the specified numerical ID is associated.
     *
     * @param id The ID for which to query the name. It must be allocated.
     * @return The name associated with ID.
     */
    @Pure
    public @NonNull String getName(@ReadOnly NumericIDAllocator this, int id) {
        return names_.get(id);
    }

    /**
     * @see Collection#size()
     */
    @Pure
    public int size(@ReadOnly NumericIDAllocator this) {
        return names_.size();
    }

    /**
     * @see Map#entrySet() The returned set is unmodifiable.
     */
    @Pure
    public @PolyRead Set<Entry<@NonNull String, @NonNull Integer>> entrySet(@PolyRead NumericIDAllocator this) {
        return Collections.unmodifiableSet(ids_.entrySet());
    }

} // end NumericIdAllocator class
