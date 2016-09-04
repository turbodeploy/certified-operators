package com.vmturbo.platform.analysis.utilities;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

/**
 * An unmodifiable view of a {@link NumericIDAllocator}.
 *
 * <p>
 *  It includes all read-only operations.
 * </p>
 */
public interface UnmodifiableNumericIDAllocator {

    /**
     * Returns the ID associated with a specified name.
     *
     * @param name The name for which to query the ID. It must already be associated with an ID.
     * @return The numerical ID associated with name.
     */
    @Pure
    int getId(@ReadOnly UnmodifiableNumericIDAllocator this, @NonNull String name);

    /**
     * Returns the name with which the specified numerical ID is associated.
     *
     * @param id The ID for which to query the name. It must be allocated to some name.
     * @return The name associated with ID.
     */
    @Pure
    @NonNull String getName(@ReadOnly UnmodifiableNumericIDAllocator this, int id);

    /**
     * @see Collection#size()
     */
    @Pure
    int size(@ReadOnly UnmodifiableNumericIDAllocator this);

    /**
     * @see Collection#isEmpty()
     */
    @Pure
    boolean isEmpty(@ReadOnly UnmodifiableNumericIDAllocator this);

    /**
     * @see Map#entrySet() The returned set is unmodifiable.
     */
    @Pure
    @NonNull Set<Entry<@NonNull String, @NonNull Integer>> entrySet(@ReadOnly UnmodifiableNumericIDAllocator this);

} // end UnmodifiableNumericIDAllocator interface
