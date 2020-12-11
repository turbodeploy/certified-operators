package com.vmturbo.common.protobuf.memory;

import java.util.Collections;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.StringUtil;
import com.vmturbo.common.protobuf.memory.MemoryVisitor.TotalSizesAndCountsVisitor;

/**
 * Easy-to-locate utility to help measure how much memory an object takes up.
 */
public class MemoryMeasurer {

    private MemoryMeasurer() { }

    /**
     * Measure the input object. Should work across all JVMs. This does a traversal using reflection,
     * so it is not fast and should be used sparingly (or not at all) in production.
     *
     * @param obj The {@link Object}.
     * @return The {@link MemoryMeasurement}.
     */
    @Nonnull
    public static MemoryMeasurement measure(@Nonnull final Object obj) {
        final TotalSizesAndCountsVisitor visitor =
                new TotalSizesAndCountsVisitor(Collections.emptySet(), 100, 100);
        new FastMemoryWalker(visitor).traverse(obj);
        return new MemoryMeasurement(visitor);
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
