package com.vmturbo.cost.component.scope;

import java.time.Instant;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * Represents a unique alias to real oid mapping along with the timestamp of when it was first discovered.
 */
@HiddenImmutableImplementation
@Immutable
public interface OidMapping {

    /**
     * Returns the oid mapping key, the alias to real oid mapping.
     *
     * @return the oid mapping key, the alias to real oid mapping.
     */
    OidMappingKey oidMappingKey();

    /**
     * Returns the time in utc ms  the alias to real oid mapping was first discovered.
     *
     * @return the time in utc ms the alias to real oid mapping was first discovered.
     */
    Instant firstDiscoveredTimeMsUtc();
}