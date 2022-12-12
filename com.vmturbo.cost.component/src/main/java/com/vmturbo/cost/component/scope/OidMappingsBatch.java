package com.vmturbo.cost.component.scope;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

/**
 * Batch of new {@link OidMapping} instances to be used for carrying out scope id replacement.
 */
public class OidMappingsBatch {

    private final Map<Long, Set<OidMapping>> newOidMappingsByAliasId;

    /**
     * Creates an instance of {@link OidMappingsBatch}.
     *
     * @param newOidMappingsByAliasId map containing new {@link OidMapping} instances indexed by alias id. Grouping the
     *                                {@link OidMapping} instances by alias id is essential as it helps to find previous
     *                                replacements and succeeding replacements for a given {@link OidMapping} needed for
     *                                constructing update queries.
     */
    public OidMappingsBatch(@Nonnull final Map<Long, Set<OidMapping>> newOidMappingsByAliasId) {
        this.newOidMappingsByAliasId = Objects.requireNonNull(newOidMappingsByAliasId);
    }

    /**
     * Return all alias oids from this data segment.
     *
     * @return all alias oids from this data segment.
     */
    @Nonnull
    public Collection<Long> getAliasOids() {
        return newOidMappingsByAliasId.keySet();
    }

    /**
     * Return all {@link OidMapping} instances with the provided alias oids.
     *
     * @param aliasOids for which to retrieve the {@link OidMapping} instances.
     * @return all {@link OidMapping} instances with the provided alias oids.
     */
    @Nonnull
    public Collection<OidMapping> getOidMappingsForAliasOids(@Nonnull final Collection<Long> aliasOids) {
        return aliasOids.stream()
            .map(newOidMappingsByAliasId::get)
            .filter(Objects::nonNull)
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());
    }
}