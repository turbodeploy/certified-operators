package com.vmturbo.platform.analysis.utilities;

import java.util.ArrayList;
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
public final class NumericIDAllocator implements UnmodifiableNumericIDAllocator {
    // Fields
    private final @NonNull Map<@NonNull String, @NonNull Integer> ids_ = new LinkedHashMap<>();
    private final @NonNull List<@NonNull String> names_ = new ArrayList<>();

    // Cached data

    // Cached unmodifiable view of the ids_.entrySet Set.
    private final @NonNull Set<Entry<@NonNull String, @NonNull Integer>> unmodifiableEntrySet =
                                                        Collections.unmodifiableSet(ids_.entrySet());

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

    @Pure
    @Override
    public int getId(@ReadOnly NumericIDAllocator this, @NonNull String name) {
        return ids_.get(name);
    }

    @Pure
    @Override
    public @NonNull String getName(@ReadOnly NumericIDAllocator this, int id) {
        return names_.get(id);
    }

    @Pure
    @Override
    public int size(@ReadOnly NumericIDAllocator this) {
        return names_.size();
    }

    @Pure
    @Override
    public boolean isEmpty(@ReadOnly NumericIDAllocator this) {
        return names_.isEmpty();
    }

    @Pure
    @Override
    public @ReadOnly @NonNull Set<Entry<@NonNull String, @NonNull Integer>> entrySet(@PolyRead NumericIDAllocator this) {
        return unmodifiableEntrySet;
    }

    public void dumpTable() {
        for (Entry<String,Integer> entry : ids_.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
    }
} // end NumericIdAllocator class
