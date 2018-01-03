package com.vmturbo.clustermgr;

/**
 * This enum represents a component state, as reported to the user.
 * This class only exists for data transfer to the UI component, and will probably be refactored
 * when we revisit how component health / status are checked for Kubernetes.
 *
 * The UI recognizes three states:
 *   UNKNOWN: the component state is unknown.
 *   UNHEALTHY: the component is online, but reported as unhealthy as per the HealthCheck result.
 *   RUNNING: the component is online and healthy.
 */
public enum ComponentState {
    UNKNOWN,
    UNHEALTHY,
    RUNNING
}
