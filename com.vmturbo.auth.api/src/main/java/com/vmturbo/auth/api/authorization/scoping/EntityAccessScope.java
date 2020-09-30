package com.vmturbo.auth.api.authorization.scoping;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.springframework.util.CollectionUtils;

import com.vmturbo.components.common.identity.OidFilter;
import com.vmturbo.components.common.identity.OidSet;
import com.vmturbo.components.common.identity.OidSet.AllOidsSet;
import com.vmturbo.components.common.identity.RoaringBitmapOidSet;

/**
 * EntityAccessScope models an access restriction list based on an "entity scope". The "access scope"
 * is defined by a set of group id's, and all entities that participate in the supply chain for the
 * entities in those groups are included in the "access scope."
 *
 * If there are no groups defining the scope, then the entity access scope is unrestricted. Note
 * that we expect this to be the case the majority of the time, so this unrestricted scope is also
 * the default behavior.
 */
public class EntityAccessScope implements OidFilter {
    // the default access filter doesn't filter any oids out.
    private static final OidFilter DEFAULT_ACCESS_FILTER = AllOidsFilter.ALL_OIDS_FILTER;

    public static final EntityAccessScope DEFAULT_ENTITY_ACCESS_SCOPE = new EntityAccessScope();

    // the collection of groups to base the access scope on -- may, and usually will, be empty
    @Nonnull
    private final Collection<Long> scopeGroupIds;

    // the collection of scope group member oids. This is cached for convenience, and used by the
    // supply chain query.
    @Nonnull
    private final OidSet scopeGroupMemberOids;

    // the oid access filter to apply for this EntityAccessScope. Created lazily.
    @Nonnull
    private final OidFilter accessFilter;

    // a breakdown of accessible oids by entity type. Only really relevant for non-empty and non-all scopes.
    private final Map<String, OidSet> accessibleOidsByEntityType;

    // create a scope with no restrictions
    private EntityAccessScope() {
        this.accessFilter = DEFAULT_ACCESS_FILTER;
        this.scopeGroupIds = Collections.EMPTY_LIST;
        this.scopeGroupMemberOids = OidSet.EMPTY_OID_SET;
        this.accessibleOidsByEntityType = Collections.EMPTY_MAP;
    }

    public EntityAccessScope(@Nullable Collection<Long> groupIds, @Nullable OidSet scopeGroupMemberOids,
                             @Nullable OidFilter accessFilter, @Nullable Map<String, OidSet> oidsByEntityType) {
        this.scopeGroupIds = groupIds != null ? groupIds : Collections.EMPTY_LIST;
        this.scopeGroupMemberOids = scopeGroupMemberOids != null ? scopeGroupMemberOids : OidSet.EMPTY_OID_SET;
        this.accessFilter = accessFilter != null ? accessFilter : DEFAULT_ACCESS_FILTER;
        this.accessibleOidsByEntityType = oidsByEntityType != null ? oidsByEntityType : Collections.EMPTY_MAP;
    }

    @Nonnull
    public Collection<Long> getScopeGroupIds() {
        return scopeGroupIds;
    }

    @Nonnull
    public OidSet getScopeGroupMembers() {
        return scopeGroupMemberOids;
    }

    @Nonnull
    public OidFilter getEntityAccessFilter() {
        return accessFilter;
    }

    @Override
    public boolean containsAll() {
        return accessFilter.containsAll();
    }

    @Override
    public boolean contains(final long oid) {
        return accessFilter.contains(oid);
    }

    @Override
    public boolean contains(final Collection<Long> oids) {
        return accessFilter.contains(oids);
    }

    @Override
    public OidSet filter(final long[] inputOids) {
        return accessFilter.filter(inputOids);
    }

    @Override
    public OidSet filter(final OidSet inputSet) {
        return accessFilter.filter(inputSet);
    }

    @Override
    public Set<Long> filter(final Set<Long> inputOids) {
        return accessFilter.filter(inputOids);
    }

    public OidSet accessibleOids() {
        if (accessFilter instanceof OidSet) {
            return (OidSet) accessFilter;
        }
        throw new IllegalStateException("iterator only available on non-empty access scopes.");
    }

    /**
     * Get an OidSet of the accessible oids for the specified entity type. This set is only relevant
     * if EntityAccessScope.containsAll() is false -- if the scope contains "all" entities then this
     * method will just return an {@link AllOidsSet} that is not iterable.
     *
     * @param entityType The string entity type to look for
     * @return an {@link OidSet} of entity oids of the requested type in the accessible set.
     */
    public OidSet getAccessibleOidsByEntityType(String entityType) {
        // special case for "all oids".
        if (accessFilter.containsAll()) {
            return AllOidsSet.ALL_OIDS_SET;
        }

        return accessibleOidsByEntityType.getOrDefault(entityType, OidSet.EMPTY_OID_SET);
    }

    /**
     * Get an OidSet of the accessible oids for the specified collection of entityTypes. This set is only relevant
     * if EntityAccessScope.containsAll() is false -- if the scope contains "all" entities then this
     * method will just return an {@link AllOidsSet} that is not iterable.
     *
     * @param entityTypes The string entity type to look for
     * @return an {@link OidSet} of entity oids of the requested type in the accessible set.
     */
    public OidSet getAccessibleOidsByEntityTypes(Collection<String> entityTypes) {
        // special case for "all oids".
        if (accessFilter.containsAll()) {
            return AllOidsSet.ALL_OIDS_SET;
        }
        if (CollectionUtils.isEmpty(entityTypes)) {
            return accessibleOids();
        }
        OidSet oids = OidSet.EMPTY_OID_SET;
        for (String entityType : entityTypes) {
            oids = oids.union(accessibleOidsByEntityType.get(entityType));
        }
        return oids;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder("Scope(groups:");
        sb.append(scopeGroupIds).append(" containing ");
        if (containsAll()) {
            sb.append("All");
        } else {
            sb.append(accessibleOids().size());
        }
        sb.append(" oids)");
        return sb.toString();
    }
}
