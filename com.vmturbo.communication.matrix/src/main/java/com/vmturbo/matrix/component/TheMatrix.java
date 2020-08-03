package com.vmturbo.matrix.component;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.matrix.component.external.MatrixInterface;

/**
 * The {@link TheMatrix} exposes a singleton instance
 * of the {@link CommunicationMatrix}.
 */
public final class TheMatrix {
    /**
     * The Matrix instance.
     */
    private final CommunicationMatrix matrix_;

    /**
     * The cache.
     */
    @VisibleForTesting
    static final Map<Long, MatrixInterface> CACHE_ = Collections.synchronizedMap(new HashMap<>());

    /**
     * This is a singleton.
     */
    private TheMatrix() {
        matrix_ = new CommunicationMatrix();
    }

    /**
     * The lazy holder pattern.
     */
    private static class Holder {
        static final TheMatrix SINGLETON_ = new TheMatrix();
    }

    /**
     * Returns the singleton instance of the {@link CommunicationMatrix}.
     *
     * @return The singleton instance of the {@link CommunicationMatrix}.
     */
    public static @Nonnull MatrixInterface instance() {
        return Holder.SINGLETON_.matrix_;
    }

    /**
     * Returns the named instance of the {@link CommunicationMatrix}.
     *
     * @param id The id of the matrix.
     * @return The named instance of the {@link CommunicationMatrix}.
     */
    public static @Nonnull Optional<MatrixInterface> instance(final @Nonnull Long id) {
        return Optional.ofNullable(CACHE_.get(id));
    }

    /**
     * Sets the named instance of the {@link CommunicationMatrix}.
     *
     * @param id     The id of the matrix.
     * @param matrix The matrix.
     */
    public static void setInstance(final @Nonnull Long id,
                                   final @Nonnull MatrixInterface matrix) {
        CACHE_.put(id, matrix);
    }

    /**
     * Clears the named instance of the {@link CommunicationMatrix}.
     *
     * @param id The name of the matrix.
     */
    public static void clearInstance(final @Nonnull Long id) {
        CACHE_.remove(id);
    }

    /**
     * Returns a new instance of a Communication Matrix.
     *
     * @return A new instance of a Communication Matrix.
     */
    public static MatrixInterface newInstance() {
        return new CommunicationMatrix();
    }
}
