package com.vmturbo.protoc.pojo.gen;

import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * We signify that Object fields are unset by setting them to null,
 * but we track whether primitive fields are set by bit-masking
 * a bit set. This is a helper class in establishing the bitmask
 * and field name for a primtive field on a POJO.
 *
 * <p/>These self-tracked bitsets are similar to how the official
 * Protobufs track fields, although they also use the bitsets
 * for object fields and not just primitives (not exactly sure why).
 */
public class PrimitiveFieldBits {
    private int primitiveFieldIndex;

    /**
     * The name for the bit field.
     */
    public static final String BIT_FIELD_NAME = "bitField";

    /**
     * The number of positions in a bit field.
     */
    public static final int BIT_FIELD_POSITIONS = 32;

    /**
     * Create a new {@link PrimitiveFieldBits} helper.
     */
    public PrimitiveFieldBits() {
        primitiveFieldIndex = 0;
    }

    /**
     * Get the name for the current bitField.
     *
     * @return the name for the current bitField.
     */
    public String getCurrentBitfieldName() {
        return bitFieldNameForIndex(primitiveFieldIndex, BIT_FIELD_POSITIONS);
    }

    /**
     * Get the current index.
     *
     * @return the current index.
     */
    public int getCurrentIndex() {
        return primitiveFieldIndex;
    }

    /**
     * Get the names for all the bitFields given all the indexes that have been occupied so far.
     *
     * @return the names for all the bitFields given all the indexes that have been occupied so far.
     */
    public Stream<String> getAllBitFieldNames() {
        if (primitiveFieldIndex == 0) {
            return Stream.empty();
        }

        final int maxField = Math.max(0, (getCurrentIndex() - 1) / BIT_FIELD_POSITIONS);
        return IntStream.rangeClosed(0, maxField)
            .mapToObj(i -> bitFieldNameForIndex(i, 1));
    }

    private static String bitFieldNameForIndex(final int index, final int divisor) {
        return BIT_FIELD_NAME + (index / divisor) + "_";
    }

    /**
     * Get the bitmask for the current index for use with the current bitField.
     *
     * @return the bitmask for the current index.
     */
    public String getCurrentBitmask() {
        return "0x" + Integer.toHexString(1 << primitiveFieldIndex % BIT_FIELD_POSITIONS);
    }

    /**
     * Increment the current index. Returns the value of the index prior to
     * incrementing.
     *
     * @return the value of the index prior to incrementing.
     */
    public int increment() {
        return primitiveFieldIndex++;
    }

    /**
     * Whether the next increment should set up a new bitField.
     *
     * @return whether the next increment should set up a new bitField.
     */
    public boolean nextIncrementRequiresNewBitField() {
        return (primitiveFieldIndex % BIT_FIELD_POSITIONS) == 0;
    }
}
