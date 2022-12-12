package com.vmturbo.cost.component.scope;

import org.immutables.value.Value;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * Persistence statistics for a single Scope id replacement data segment.
 */
@HiddenImmutableImplementation
@Value.Immutable
public interface ScopeIdReplacementPersistenceStats {

    /**
     * Returns the number of alias oids with valid scope data.
     *
     * @return the number of alias oids with valid scope data.
     */
    long numAliasOidWithValidScopes();

    /**
     * Returns the number of real oids persisted to cloud_scope table.
     *
     * @return the number of real oids persisted to cloud_scope table.
     */
    long numRealOidScopesPersisted();

    /**
     * Returns the number of update queries created for scope id replacement.
     *
     * @return the number of update queries created for scope id replacement.
     */
    long numUpdateQueriesCreated();

    /**
     * Returns the number of scope id replacements logged.
     *
     * @return the number of scope id replacements logged.
     */
    long numScopeIdReplacementsLogged();
}