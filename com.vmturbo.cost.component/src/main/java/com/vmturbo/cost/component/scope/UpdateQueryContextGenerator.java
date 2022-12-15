package com.vmturbo.cost.component.scope;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Creates instances of {@link UpdateQueryContext} for the provided {@link OidMapping} instances.
 */
public class UpdateQueryContextGenerator {

    private final Logger logger = LogManager.getLogger();
    private final Map<Long, Set<OidMapping>> replacedMappingsByAlias;
    private final Map<Long, Set<OidMapping>> toReplaceMappingsByAlias;

    /**
     * Creates an instance of {@link UpdateQueryContextGenerator}.
     *
     * @param replacedMappingsByAlias oid mapping instances that have been previously replaced.
     * @param toReplaceMappings new oid mappings that need to be replaced.
     */
    public UpdateQueryContextGenerator(@Nonnull final Map<Long, Set<OidMapping>> replacedMappingsByAlias,
                                       @Nonnull final Collection<OidMapping> toReplaceMappings) {
        this.replacedMappingsByAlias = Objects.requireNonNull(replacedMappingsByAlias);
        this.toReplaceMappingsByAlias = Objects.requireNonNull(toReplaceMappings).stream()
            .collect(Collectors.groupingBy(mapping -> mapping.oidMappingKey().aliasOid(), Collectors.toSet()));
    }

    /**
     * Creates and returns a list of {@link UpdateQueryContext}s.
     *
     * <p>The following logic is used to create an {@link UpdateQueryContext} instance,
     * For every alias oid,
     * 1. Find the last persisted replacement {@link OidMapping} instance (from a previous replacement cycle), this will
     * be the lastReplacedOidMappingForAlias argument for the {@link UpdateQueryContext} instance.
     * 2. For every {@link OidMapping} that needs to replaced for the above alias oid:
     *    (i) Find the chronologically succeeding {@link OidMapping} instance if any (this will happen only if back to
     *    back {@link OidMapping}s are detected before a replacement), this will be the nextOidMappingForAlias argument
     *    for the {@link UpdateQueryContext} instance.
     *    (ii) Create a {@link UpdateQueryContext} instance using the mapping from 1., and 2(i) for the
     *    {@link OidMapping} under consideration as the currentOidMapping argument for the {@link UpdateQueryContext}
     *    instance.
     * <p/>
     *
     * @return a list of {@link UpdateQueryContext}s.
     */
    public List<UpdateQueryContext> getUpdateQueryContexts() {

        final List<UpdateQueryContext> contexts = new ArrayList<>();

        toReplaceMappingsByAlias.forEach((alias, mappings) -> {

            if (!mappings.isEmpty()) {

                final OidMapping lastReplacedOidMappingForAlias = Optional.ofNullable(
                        replacedMappingsByAlias.get(alias)).map(set -> set.stream()
                        .max(Comparator.comparing(OidMapping::firstDiscoveredTimeMsUtc)))
                    .flatMap(opt -> opt).orElse(null);

                final Set<OidMapping> mappingsInChronologicalOrder = new TreeSet<>(
                    Comparator.comparing(OidMapping::firstDiscoveredTimeMsUtc));
                mappingsInChronologicalOrder.addAll(mappings);

                final Iterator<OidMapping> iterator = mappingsInChronologicalOrder.iterator();
                OidMapping currentMapping = iterator.next();

                while (iterator.hasNext()) {

                    final OidMapping nextMapping = iterator.next();

                    if (!currentMapping.firstDiscoveredTimeMsUtc().truncatedTo(ChronoUnit.DAYS)
                        .equals(nextMapping.firstDiscoveredTimeMsUtc().truncatedTo(ChronoUnit.DAYS))) {

                        contexts.add(new UpdateQueryContext(currentMapping,
                            lastReplacedOidMappingForAlias, nextMapping));

                    } else {
                        logger.info("Skipping {} and using {} as it is the more recent mapping.",
                            currentMapping, nextMapping);
                    }

                    currentMapping = nextMapping;
                }

                contexts.add(new UpdateQueryContext(currentMapping, lastReplacedOidMappingForAlias, null));
            }
        });
        return contexts;
    }

    /**
     * Oid mapping data that can be used to create Update queries. It contains the Oid mapping under consideration for
     * replacement, previously replaced mapping for the same alias oid, and next replacement for the same alias oid.
     *
     * <p>This context models all the possible scenarios:
     * <p/>
     *
     * <p>1. currentOidMapping: {real_oid: 111, alias_oid: 777}, lastReplacedOidMappingForAlias: null,
     *       nextOidMappingForAlias: null
     *       Use cases: New entities discovered with new resource uris (new subscription target or entity creation)
     * <p/>
     *
     * <p>2. currentOidMapping: {real_oid: 111, alias_oid: 777},
     *       lastReplacedOidMappingForAlias: {real_oid: 222, alias_oid: 777},
     *       nextOidMappingForAlias: null
     *       Use cases: Earlier entity deleted (previously replaced), new entity created with the same resource uri
     *v<p/>
     *
     * <p>3. currentOidMapping: {real_oid: 111, alias_oid: 777},
     *       lastReplacedOidMappingForAlias: null
     *       nextOidMappingForAlias: {real_oid: 222, alias_oid: 777}
     *       Use cases: Back to back recreation of entities with the same resource uri, with a date change in the
     *       middle. This can happen if the replacement process happens after the billing data upload.
     * <p/>
     *
     * <p>4. currentOidMapping: {real_oid: 111, alias_oid: 777},
     *       lastReplacedOidMappingForAlias: {real_oid: 222, alias_oid: 777}
     *       nextOidMappingForAlias: {real_oid: 333, alias_oid: 777}
     *       Use cases: Earlier entity deleted (previously replaced) + case 3
     * <p/>
     */
    public static class UpdateQueryContext {
        private final OidMapping currentOidMapping;
        private final OidMapping lastReplacedOidMappingForAlias;
        private final OidMapping nextOidMappingForAlias;

        /**
         * Creates an instance of {@link UpdateQueryContext}.
         *
         * @param currentOidMapping {@link OidMapping} instance for which the {@link UpdateQueryContext} is
         *                                                    created
         * @param lastReplacedOidMappingForAlias most recent {@link OidMapping} instance that was replaced
         *                                                   for the same alias oid
         * @param nextOidMappingForAlias {@link OidMapping} instance that will be the next in line for replacement for the same
         *                                          alias oid
         */
        public UpdateQueryContext(@Nonnull final OidMapping currentOidMapping,
                                  @Nullable final OidMapping lastReplacedOidMappingForAlias,
                                  @Nullable final OidMapping nextOidMappingForAlias) {
            this.currentOidMapping = Objects.requireNonNull(currentOidMapping);
            this.lastReplacedOidMappingForAlias = lastReplacedOidMappingForAlias;
            this.nextOidMappingForAlias = nextOidMappingForAlias;
        }

        @Nonnull
        public OidMapping getCurrentOidMapping() {
            return currentOidMapping;
        }

        @Nonnull
        public Optional<OidMapping> getLastReplacedOidMappingForAlias() {
            return Optional.ofNullable(lastReplacedOidMappingForAlias);
        }

        @Nonnull
        public Optional<OidMapping> getNextOidMappingForAlias() {
            return Optional.ofNullable(nextOidMappingForAlias);
        }
    }
}