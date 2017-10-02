package com.vmturbo.topology.processor.api;

import java.util.Date;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Represents target operation, that is beeing executed in TopologyProcessor.
 */
public interface OperationStatus {

    /**
     * Id of the operation.
     *
     * @return operation's id
     */
    long getId();

    /**
     * Returns id of the target this result is reported for.
     *
     * @return id of the target.
     */
    long getTargetId();

    /**
     * Returns whether the operation is completed.
     *
     * @return {@code true} if operation is completed, {@code false} otherwise
     */
    boolean isCompleted();

    /**
     * Returns whether the operation succeeded. It is meaningful only when the operation is completed.
     *
     * @return {@code true} if operation succeeded, {@code false} otherwise
     */
    boolean isSuccessful();

    /**
     * Return list of error messages, if any. There may be error messages even if operation
     * succeeded (some warnings).
     *
     * @return list of error messages
     */
    @Nonnull
    List<String> getErrorMessages();

    /**
     * Returns time of operation start.
     *
     * @return time of operation start
     */
    @Nonnull
    Date getStartTime();

    /**
     * Returns time of operation completion. .
     *
     * @return time of operation completion
     */
    @Nullable
    Date getCompletionTime();
}
