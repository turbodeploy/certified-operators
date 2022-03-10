package com.vmturbo.common.protobuf.topology;

import java.util.StringJoiner;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.EntityPipelineErrors.StitchingErrorCode;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityView;

/**
 * Represents a collection of possible errors encountered during stitching (normally applies to a
 * single entity).
 */
public class StitchingErrors {

    private static StitchingErrors NONE = new StitchingErrors();

    /**
     * The set of errors is represented by a bunch of bits. 0 (empty) means no errors.
     * See {@link StitchingErrorCode}.
     */
    private int errCode;

    public StitchingErrors() {
        this.errCode = 0;
    }

    private StitchingErrors(final int errCode) {
        this.errCode = errCode;
    }

    @Nonnull
    public static StitchingErrors fromProtobuf(@Nonnull final TopologyEntityDTO entity) {
        // It's 0 (none) in the default instance. In that case we can save an allocation.
        if (!entity.getPipelineErrors().hasStitchingErrors()) {
            return NONE;
        } else {
            return new StitchingErrors(entity.getPipelineErrors().getStitchingErrors());
        }
    }

    @Nonnull
    public static StitchingErrors fromTopologyEntityView(@Nonnull final TopologyEntityView entity) {
        // It's 0 (none) in the default instance. In that case we can save an allocation.
        if (!entity.getPipelineErrors().hasStitchingErrors()) {
            return NONE;
        } else {
            return new StitchingErrors(entity.getPipelineErrors().getStitchingErrors());
        }
    }

    /**
     * Add an error code to this collection of errors. No effect if this code has already been
     * added.
     *
     * @param code The {@link StitchingErrorCode}.
     */
    public void add(@Nonnull final StitchingErrorCode code) {
        this.errCode = this.errCode | 1 << code.getNumber();
    }

    /**
     * Add another set of errors into this collection. This collection will contain the union
     * of errors in the two collections.
     *
     * @param error The other {@link StitchingErrors}.
     */
    public void add(@Nonnull final StitchingErrors error) {
        this.errCode = this.errCode | error.errCode;
    }

    /**
     * Return whether or not this collection of errors contains all the provided error codes.
     * @param errors A variable number of {@link StitchingErrorCode}s.
     * @return True if all the provided error codes are in this error.
     */
    public boolean contains(@Nonnull final StitchingErrorCode... errors) {
        if (errors.length == 0) {
            return true;
        }

        for (final StitchingErrorCode code : errors) {
            if (!contains(code)) {
                return false;
            }
        }
        return true;
    }

    private boolean contains(@Nonnull final StitchingErrorCode code) {
        // Check if the bit is set.
        return (this.errCode & (1 << code.getNumber())) != 0;
    }

    /**
     * Get the integer representation of this collection.
     */
    public int getCode() {
        return errCode;
    }

    /**
     * Return true if this collection contains no errors.
     */
    public boolean isNone() {
        return errCode == 0;
    }

    @Nonnull
    public static StitchingErrors none() {
        return NONE;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(errCode);
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) return true;
        if (other == null || !(other instanceof StitchingErrors)) return false;
        return ((StitchingErrors) other).errCode == errCode;
    }

    @Override
    public String toString() {
        final StringJoiner readableJoiner = new StringJoiner(", ");
        for (final StitchingErrorCode code : StitchingErrorCode.values()) {
            if (contains(code)) {
                readableJoiner.add(code.name());
            }
        }
        return readableJoiner.toString();
    }
}
