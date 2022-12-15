package com.vmturbo.cost.component.scope;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.InsertReturningStep;
import org.jooq.impl.DSL;

import com.vmturbo.components.common.RequiresDataInitialization;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.OidMappingRecord;

/**
 * Responsible for persisting and retrieving {@link OidMapping} instances from the Cost DB. Also contains an in-memory
 * write through cache for efficient detection of newly encountered {@link OidMapping} instances.
 */
public class SqlOidMappingStore implements RequiresDataInitialization {

    private static final Logger logger = LogManager.getLogger();

    private final AtomicBoolean cacheInitialized = new AtomicBoolean();
    private final Map<OidMappingKey, OidMapping> registeredOidMappings = new ConcurrentHashMap<>();
    private final DSLContext dslContext;

    /**
     * Creates an instance of {@link SqlOidMappingStore}.
     *
     * @param dslContext instance for executing queries.
     */
    public SqlOidMappingStore(@Nonnull final DSLContext dslContext) {
        this.dslContext = Objects.requireNonNull(dslContext);
    }

    @Override
    public void initialize() throws InitializationException {
        logger.info("Initializing oid mapping store cache.");
        try {
            registeredOidMappings.clear();
            registeredOidMappings.putAll(
                dslContext.selectFrom(Tables.OID_MAPPING).fetch().stream()
                    .map(this::recordToOidMapping)
                    .collect(Collectors.toMap(OidMapping::oidMappingKey, Function.identity()))
            );
            cacheInitialized.set(true);
        } catch (Exception ex) {
            logger.warn("Cache initialization failed for oid mapping store.", ex);
        }
    }

    /**
     * Store {@link OidMapping}s if not previously stored.
     *
     * @param oidMappings to be stored.
     */
    public void registerOidMappings(@Nonnull final Collection<OidMapping> oidMappings) {
        final Collection<OidMapping> newOidMappings = oidMappings.stream()
            .filter(oidMapping -> !registeredOidMappings.containsKey(oidMapping.oidMappingKey()))
            .collect(Collectors.toSet());
        if (!newOidMappings.isEmpty()) {
            logger.info("Persisting {} new oid mappings.", oidMappings.size());
            persistOidMappings(newOidMappings);
        }
    }

    private void persistOidMappings(@Nonnull final Collection<OidMapping> oidMappings) {
        final Set<InsertReturningStep<OidMappingRecord>> inserts = oidMappings.stream()
            .map(this::oidMappingToInsertStatement)
            .collect(Collectors.toSet());
        dslContext.transaction(config -> {
            final DSLContext context = DSL.using(config);
            context.batch(inserts).execute();
            registeredOidMappings.putAll(oidMappings.stream()
                .collect(Collectors.toMap(OidMapping::oidMappingKey, Function.identity())));
        });
    }

    private InsertReturningStep<OidMappingRecord> oidMappingToInsertStatement(final OidMapping oidMapping) {
        return dslContext.insertInto(Tables.OID_MAPPING)
            .columns(Tables.OID_MAPPING.REAL_ID, Tables.OID_MAPPING.ALIAS_ID,
                Tables.OID_MAPPING.FIRST_DISCOVERED_TIME_MS_UTC)
            .values(oidMapping.oidMappingKey().realOid(), oidMapping.oidMappingKey().aliasOid(),
                oidMapping.firstDiscoveredTimeMsUtc())
            .onDuplicateKeyIgnore();
    }

    /**
     * Returns new {@link OidMapping} instances that are not present in the existingOidMappings collection.
     *
     * @param existingOidMappings {@link OidMapping} instances seen before.
     * @return new {@link OidMapping} instances that are not present in the existingOidMappings collection.
     */
    @Nonnull
    public Collection<OidMapping> getNewOidMappings(@Nonnull final Collection<OidMappingKey> existingOidMappings) {
        return registeredOidMappings.values().stream()
            .filter(mapping -> !existingOidMappings.contains(mapping.oidMappingKey()))
            .collect(Collectors.toSet());
    }

    private OidMapping recordToOidMapping(final OidMappingRecord record) {
        return ImmutableOidMapping.builder()
            .oidMappingKey(ImmutableOidMappingKey.builder()
                .realOid(record.getRealId())
                .aliasOid(record.getAliasId())
                .build())
            .firstDiscoveredTimeMsUtc(record.getFirstDiscoveredTimeMsUtc())
            .build();
    }
}