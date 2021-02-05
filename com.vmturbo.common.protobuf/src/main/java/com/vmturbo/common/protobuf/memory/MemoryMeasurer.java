package com.vmturbo.common.protobuf.memory;

import java.util.Collections;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.StringUtil;
import com.vmturbo.common.protobuf.memory.MemoryVisitor.TotalSizesAndCountsVisitor;

/**
 * Easy-to-locate utility to help measure how much memory an object takes up.
 */
public class MemoryMeasurer {

    private MemoryMeasurer() { }

    /**
     * Measure the input object; should work across all JVMs.
     *
     * <p>This does a traversal using reflection, so it is not fast and should be used sparingly
     * (or not at all) in production.</p>
     *
     * <p>Excluded class objects cause all visited instances of that class to be excluded.</p>
     *
     * <p>Excluded non-class objets are identified, during the traversal, by object identity,
     * ignoring the object's implementation of {@link Object#equals(Object)}.</p>
     *
     * <p>Class and non-class exclusions can be mixed as needed.</p>
     *
     * @param obj        The {@link Object}.
     * @param exclusions Objects and/or classes to be explicitly omitted from the measurement
     * @return The {@link MemoryMeasurement}.
     */
    @Nonnull
    public static MemoryMeasurement measure(@Nonnull final Object obj, @Nonnull final Set<Object> exclusions) {
        final TotalSizesAndCountsVisitor visitor =
                // limit traversal to 100 deep, and apply exclusions at all depths
                new TotalSizesAndCountsVisitor(exclusions, 0, 100);
        new FastMemoryWalker(visitor).traverse(obj);
        return new MemoryMeasurement(visitor);
    }

    /**
     * Measure the input object without any exclusions.
     *
     * @param obj the {@link Object} tobe measured
     * @return the {@link MemoryMeasurement}
     */
    @Nonnull
    public static MemoryMeasurement measure(@Nonnull final Object obj) {
        return measure(obj, Collections.emptySet());
    }

    /**
     * Information about the memory required by a particular object.
     */
    public static class MemoryMeasurement {
        private final long totalSize;

        private MemoryMeasurement(@Nonnull final TotalSizesAndCountsVisitor visitor) {
            this.totalSize = visitor.totalSize();
        }

        /**
         * Get the total size of the object being measured, in bytes.
         *
         * @return The bytes.
         */
        public long getTotalSizeBytes() {
            return totalSize;
        }

        @Override
        public String toString() {
            return StringUtil.getHumanReadableSize(getTotalSizeBytes());
        }
    }
}
