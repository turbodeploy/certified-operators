package com.vmturbo.components.common;

/**
 * The different execution states for an {@link IVmtComponent}.
 *
 * <p>A component is created in {@code NEW}, and only transitions to {@code STARTING} on
 * receipt of the "start()" method call.
 * <br>Once the component is completely initialized and open for business, it transitions
 * internally to the {@code RUNNING} state.
 * <br>The "pause()" method causes a transition from {@code RUNNING} to {@code PAUSED}.
 * <br>The "resume()" call transitions back to {@code RUNNING}.
 * <br>The "stop()" method initiates a transition to {@code STOPPING}, and once the
 * Component is completely shut down, internally transitions to {@code TERMINATED}.
 * <br>There may be internal processing exceptions that cause the Component to transition
 * to the {@code FAILED} state. The only way out of the {@code FAILED} state is to "stop()"
 *
 */
public enum ExecutionStatus {
    /**
     * The initial component state.
     */
    NEW,

    /**
     * A component is starting up.
     */
    STARTING,

    /**
     * Component is running data migrations.
     */
    MIGRATING, // Running data migrations.

    /**
     * The component context is constructed. Certain parts of the components are running
     * initialization code.
     */
    INITIALIZING_DATA,

    /**
     * The component finished startup, and is ready to serve requests.
     */
    RUNNING,

    /**
     * Component is paused (currently unused?).
     */
    PAUSED,

    /**
     * Component is shutting down.
     */
    STOPPING,

    /**
     * Component is terminated.
     */
    TERMINATED,

    /**
     * Component startup failed (currently unused?).
     */
    FAILED;
    // TODO: add legalNextState(nextState) to encode the above semantics
}
