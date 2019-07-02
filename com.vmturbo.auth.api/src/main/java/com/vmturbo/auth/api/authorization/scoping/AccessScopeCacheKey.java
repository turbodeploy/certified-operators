package com.vmturbo.auth.api.authorization.scoping;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

/**
 * An access scope cache entry is going to be keyed by a set of scope group oids + flag on whether
 * infrastructure is included or not.
 */
public class AccessScopeCacheKey {
    // TODO: optimize this a bit by just storing the hash for the scope group oids instead of an
    // entire copy of the set. This would require some refactoring though, since this object is
    // also being relied on to pass the scope group oids around (which also muddies the intent
    // of the class in the first place)
    private final Set<Long> scopeGroupOids;
    /**
     * flag indicating whether this scope would include infrastructure entities or not. Infrastructure
     * entities are those below the VM layer, such as Hosts, Storage, Datacenters, etc.
     */
    private final boolean excludeInfrastructureEntities;

    public AccessScopeCacheKey(@Nonnull Collection<Long> scopeGroupOids, boolean excludeInfrastructureEntities) {
        Objects.requireNonNull(scopeGroupOids);
        this.scopeGroupOids = new HashSet<>(scopeGroupOids);
        this.excludeInfrastructureEntities = excludeInfrastructureEntities;
    }

    public Set<Long> getScopeGroupOids() {
        return scopeGroupOids;
    }

    public boolean excludesInfrastructureEntities() {
        return excludeInfrastructureEntities;
    }

    @Override
    public int hashCode() {
        int booleanHash = excludeInfrastructureEntities ? 1231 : 1237;
        return (31 * booleanHash) + scopeGroupOids.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) return true;
        if (obj == null) return false;

        if (! (obj instanceof AccessScopeCacheKey)) return false;

        AccessScopeCacheKey other = (AccessScopeCacheKey) obj;
        if (other.excludeInfrastructureEntities != excludeInfrastructureEntities) return false;

        Set<Long> otherScopeGroups = other.getScopeGroupOids();
        if (otherScopeGroups.size() != scopeGroupOids.size()) return false;

        return scopeGroupOids.containsAll(otherScopeGroups);
    }
}
