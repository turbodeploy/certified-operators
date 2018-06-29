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
    NEW,
    STARTING,
    RUNNING,
    PAUSED,
    STOPPING,
    MIGRATING, // Running data migrations.
    TERMINATED,
    FAILED;
    // TODO: add legalNextState(nextState) to encode the above semantics
}
