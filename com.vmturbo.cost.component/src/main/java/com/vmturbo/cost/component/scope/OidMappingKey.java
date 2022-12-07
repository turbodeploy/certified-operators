package com.vmturbo.cost.component.scope;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * Represents a unique alias to real oid mapping.
 */
@HiddenImmutableImplementation
@Immutable
public interface OidMappingKey {

    /**
     * Returns the real oid part of the mapping.
     *
     * @return the real oid part of the mapping.
     */
    long realOid();

    /**
     * Returns the alias oid part of the mapping.
     *
     * @return the alias oid part of the mapping.
     */
    long aliasOid();
}