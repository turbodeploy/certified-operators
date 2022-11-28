package com.vmturbo.history.db.bulk;

import static com.vmturbo.history.db.bulk.DbInserters.excludeFieldsUpserter;
import static com.vmturbo.history.db.bulk.DbInserters.simpleUpserter;
import static com.vmturbo.history.db.bulk.DbInserters.valuesInserter;
import static com.vmturbo.history.schema.abstraction.Tables.APPLICATION_SERVICE_DAYS_EMPTY;
import static com.vmturbo.history.schema.abstraction.Tables.CLUSTER_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.Tables.ENTITIES;
import static com.vmturbo.history.schema.abstraction.Tables.HIST_UTILIZATION;
import static com.vmturbo.history.schema.abstraction.Tables.MARKET_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.Tables.VM_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.Tables.VOLUME_ATTACHMENT_HISTORY;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.RecordTransformer;
import com.vmturbo.history.db.bulk.BulkInserterFactory.TableOperation;
import com.vmturbo.history.db.bulk.DbInserters.DbInserter;
import com.vmturbo.history.db.jooq.JooqUtils;
import com.vmturbo.sql.utils.jooq.ProxyKey;
import com.vmturbo.sql.utils.partition.IPartitioningManager;
import com.vmturbo.sql.utils.partition.PartitionProcessingException;

/**
 * This class uses {@link BulkInserterFactory} to create bulk loaders that are automatically
 * configured for the needs of specific tables.
 *
 * <p>Currently, the following tables get special handling:</p>
 * <dl>
 * <dt>*_stats_latest</dt>
 * <dd>
 * A transformer is configured that computes rollup keys.
 * </dd>
 * <dt>entities</dt>
 * <dd>
 * Uses an inserter based on the Jooq batchStore method, which performs a mix of inserts
 * and updates based on the state of the records.
 * </dd>
 * </dl>
 *
 * <p>For all other tables, no transformer is applied, and the values inserter is used.</p>
 */
public class SimpleBulkLoaderFactory implements AutoCloseable {
    private static final Logger logger = LogManager.getLogger();

    // these column names are common to all the stats_latest tables
    private static final String HOUR_KEY_FIELD_NAME = VM_STATS_LATEST.HOUR_KEY.getName();
    private static final String DAY_KEY_FIELD_NAME = VM_STATS_LATEST.DAY_KEY.getName();
    private static final String MONTH_KEY_FIELD_NAME = VM_STATS_LATEST.MONTH_KEY.getName();

    private final DSLContext dsl;

    // we delegate to this factory for all the writers we create
    private final BulkInserterFactory factory;
    private final IPartitioningManager partitioningManager;

    /**
     * Create a new instance.
     *
     * @param dsl                     base db utilities
     * @param defaultConfig           config to be used by default when creating inserters
     * @param partitioningManager     partitioning manager
     * @param executorServiceSupplier executor service to manage concurrent statement executions
     */
    public SimpleBulkLoaderFactory(final @Nonnull DSLContext dsl,
            final @Nonnull BulkInserterConfig defaultConfig,
            IPartitioningManager partitioningManager,
            final @Nonnull Supplier<ExecutorService> executorServiceSupplier) {
        this(dsl, new BulkInserterFactory(dsl, defaultConfig, executorServiceSupplier),
                partitioningManager);
    }

    /**
     * Create a new instance, supplying a BulkInserterFactory instance.
     *
     * @param dsl                 base db utilities
     * @param factory             underlying BulkInserterFactory instance
     * @param partitioningManager partitioning manager
     */
    public SimpleBulkLoaderFactory(final @Nonnull DSLContext dsl,
            final @Nonnull BulkInserterFactory factory, IPartitioningManager partitioningManager) {
        this.dsl = dsl;
        this.factory = factory;
        this.partitioningManager = partitioningManager;
    }

    /**
     * Get access to the underlying {@link BulkInserterFactory} used by this instance.
     *
     * <p>Clients can use this to create bulk loaders that are configured differently than what
     * this class would create. But keep in mind that only one bulk loader for a given table can be
     * created by any factory, so if a bulk loader has already been created for a given table, this
     * factory will only ever return that loader for that table, regardless of specified
     * configuration options.
     *
     * @return the bulk loader factory underlying this factory
     */
    public BulkInserterFactory getFactory() {
        return factory;
    }

    /**
     * Get a loader for records of the given table, configured appropriately.
     *
     * @param table the table into which records will be inserted
     * @param <R>   the underlying record type
     * @return an appropriately configured writer
     */
    public <R extends Record> BulkLoader<R> getLoader(final @Nonnull Table<R> table) {
        BulkInserter<R, R> loader = factory.getInserter(table, table, getRecordTransformer(table),
                getDbInserter(table, Collections.emptySet()));
        if (dsl.dialect() == SQLDialect.POSTGRES
                || FeatureFlags.OPTIMIZE_PARTITIONING.isEnabled()) {
            // we always do this for Postgres because our "legacy" partitioning implementation
            // in that case is the one based on `pg_partman`, which does on-demand partition
            // provisioning in the same manner as the new implementation. And we do it under
            // the feature flag cuze that means we're using the new implementation
            setPreInstallHook(table, loader);
        }
        return loader;
    }

    /**
     * Get loader for records of the given table, configured appropriately.
     *
     * @param table           the table into which records will be inserted
     * @param fieldsToExclude set of fields that should not be updated
     * @param <R>             the underlying record type
     * @return an appropriately configured writer
     */
    //TODO(OM-64657): replace fieldsToExclude with a more generic parameter.
    public <R extends Record> BulkLoader<R> getLoader(
            @Nonnull final Table<R> table, @Nonnull final Set<Field<?>> fieldsToExclude) {
        final String key = table.getName() + "/" + fieldsToExclude;
        BulkInserter<R, R> loader = factory.getInserter(key, table, table,
                getRecordTransformer(table),
                getDbInserter(table, fieldsToExclude));
        setPreInstallHook(table, loader);
        return loader;
    }

    private <R extends Record> void setPreInstallHook(@NotNull Table<R> table,
            BulkInserter<R, R> loader) {
        Table<?> latestTable = EntityType.fromTable(table)
                .flatMap(EntityType::getLatestTable)
                .orElse(null);
        if (latestTable != null && latestTable != CLUSTER_STATS_LATEST) {
            loader.setPreInsertionHook(r -> {
                try {
                    Timestamp timestamp = r.getValue(StringConstants.SNAPSHOT_TIME,
                            Timestamp.class);
                    partitioningManager.prepareForInsertion(table, timestamp);
                    return true;
                } catch (PartitionProcessingException e) {
                    logger.error("Partition preparation failed for table {}", table, e);
                    return false;
                }
            });
        }
    }

    /**
     * Get a "transient" loader for the given table.
     *
     * <p>A transient loader is a loader that loads records not into the given table, but into a
     * copy of that table created specifically for this request, and with the same record structure
     * as the given table. This is similar in concept to the "temporary table" facilities ofr MySQL,
     * but the tables are not bound to the connection from which they were created, which makes them
     * far more suitable for use with bulk loaders.</p>
     *
     * <p>The jOOQ {@link Table} object returned can in many cases be used where a primary table
     * object can be used, and will access the transient table. But there are some cases where this
     * does not hold. In particular, when using a transient table in as a FROM table in a SELECT
     * statement, you should use <code>DSL.table(transientTable.getName())</code>.</p>
     *
     * <p>If indexes need to be created on the transient table, or any other post-processing is
     * needed, that can be accomplished by providing a <code>postCreateTableOp</code> function.</p>
     *
     * <p>Unlike non-transient tables, the factory does not give back shared instances to multiple
     * requests for the same table. This method will ALWAYS create a new transient table and return
     * a loader bound to that table.</p>
     *
     * <p>When the loader is closed, the transient table is dropped automatically.</p>
     *
     * @param table             the table whose structure will be copied when creating the
     *                          transient
     * @param postTableCreateOp a function to perform operations on the table after it has been
     *                          created
     * @param <R>               record type of underlying and transient tables
     * @return the transient table, as a jOOQ {@link Table} instance
     * @throws InstantiationException if we can't create the jOOQ table object
     * @throws DataAccessException    if there's a problem with DB connection
     * @throws IllegalAccessException if we can't create the jOOQ table object
     */
    public <R extends Record> BulkLoader<R> getTransientLoader(
            final @Nonnull Table<R> table, TableOperation<R> postTableCreateOp)
            throws InstantiationException, DataAccessException, IllegalAccessException {
        return factory.getTransientInserter(
                table, table, getRecordTransformer(table), getDbInserter(table,
                        Collections.emptySet()),
                postTableCreateOp);
    }

    private <R extends Record> RecordTransformer<R, R> getRecordTransformer(Table<R> table) {
        final Optional<EntityType> entityType = EntityType.fromTable(table);
        if (entityType.map(EntityType::rollsUp).orElse(false)
                || table.equals(MARKET_STATS_LATEST)) {
            return new RollupKeyTransfomer<>();
        } else {
            return RecordTransformer.identity();
        }
    }

    private <R extends Record> DbInserter<R> getDbInserter(Table<R> table,
            Set<Field<?>> fieldsToExclude) {
        if (ENTITIES.equals(table) || HIST_UTILIZATION.equals(table)) {
            // Entities table uses upserts so that previously existing entities get any changes
            // to display name that show up in the topology.
            return simpleUpserter();
        } else if (VOLUME_ATTACHMENT_HISTORY.equals(table)
                || APPLICATION_SERVICE_DAYS_EMPTY.equals(table)) {
            return excludeFieldsUpserter(fieldsToExclude);
        } else {
            // nothing else currently using bulk loader should ever have a primary key collision,
            // so straight inserts are used.
            return valuesInserter();
        }
    }

    /**
     * Get the stats object for the underlying inserter factory.
     *
     * @return inserter factory's stats object
     */
    public BulkInserterFactoryStats getStats() {
        return factory.getStats();
    }

    /**
     * Flush all inserters created by the underlying {@link BulkInserterFactory}.
     *
     * @throws InterruptedException if interrupted
     */
    public void flushAll() throws InterruptedException {
        factory.flushAll();
    }

    /**
     * Close the underlying {@link BulkInserterFactory}.
     *
     * @throws InterruptedException if interrupted
     */
    public void close() throws InterruptedException {
        factory.close();
    }

    /**
     * Close the underlying {@link BulkInserterFactory} with stats logged to the given logger.
     *
     * @param logger where to log stats
     * @throws InterruptedException if interrupted
     */
    public void close(final Logger logger) throws InterruptedException {
        factory.close(logger);
    }

    /**
     * RecordTransformer that adds rollup keys to a stats record.
     *
     * @param <R> Type of stats record
     */
    class RollupKeyTransfomer<R extends Record> implements RecordTransformer<R, R> {
        @Override
        public Optional<R> transform(final R record, final Table<R> inTable,
                final Table<R> outTable) {
            Builder<String, Object> rollupKeyMap = ImmutableMap.builder();

            if (inTable.equals(MARKET_STATS_LATEST)) {
                if (FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled()) {
                    // market-stats gets a key in non-legacy scenarios
                    String key = getMarketStatsKey(record);
                    rollupKeyMap.put(MARKET_STATS_LATEST.TIME_SERIES_KEY.getName(), key);
                }
            } else if (dsl.dialect() == SQLDialect.POSTGRES) {
                // and entity-stats tables only get a single key stored in latest.hour_key if we're
                // using Postgres
                String key = getEntityStatsKey(record, inTable, null);
                rollupKeyMap.put(HOUR_KEY_FIELD_NAME, key);
            } else {
                // entity stats tables in MariaDB get hour_key, day_key, and month_key that include
                // a timestamp, because they've always been that way, and we can't safely change
                // up key calculations. If we did that, then records upserted before and after
                // an upgrade would have different keys, so they wouldn't map to the same rollup
                // records as required.
                rollupKeyMap.put(HOUR_KEY_FIELD_NAME,
                        getEntityStatsKey(record, inTable, HOUR_KEY_FIELD_NAME));
                rollupKeyMap.put(DAY_KEY_FIELD_NAME,
                        getEntityStatsKey(record, inTable, DAY_KEY_FIELD_NAME));
                rollupKeyMap.put(MONTH_KEY_FIELD_NAME,
                        getEntityStatsKey(record, inTable, MONTH_KEY_FIELD_NAME));
            }
            record.fromMap(rollupKeyMap.build());
            return Optional.of(record);
        }

        @NotNull
        private String getMarketStatsKey(R record) {
            return ProxyKey.getKeyAsHex(record,
                    MARKET_STATS_LATEST.TOPOLOGY_CONTEXT_ID,
                    MARKET_STATS_LATEST.ENTITY_TYPE, MARKET_STATS_LATEST.ENVIRONMENT_TYPE,
                    MARKET_STATS_LATEST.PROPERTY_TYPE, MARKET_STATS_LATEST.PROPERTY_SUBTYPE,
                    MARKET_STATS_LATEST.RELATION);
        }

        private String getEntityStatsKey(R record, Table<R> inTable, String keyName) {
            Timestamp timestamp = getTimestamp(record, keyName);
            List<Field<?>> fields = new ArrayList<>();
            if (timestamp != null) {
                fields.add(DSL.inline(timestamp));
            }
            fields.add(JooqUtils.getStringField(inTable, StringConstants.UUID));
            fields.add(JooqUtils.getStringField(inTable, StringConstants.PRODUCER_UUID));
            fields.add(JooqUtils.getStringField(inTable, StringConstants.PROPERTY_TYPE));
            fields.add(JooqUtils.getStringField(inTable, StringConstants.PROPERTY_SUBTYPE));
            fields.add(JooqUtils.getRelationTypeField(inTable, StringConstants.RELATION));
            fields.add(JooqUtils.getStringField(inTable, StringConstants.COMMODITY_KEY));
            return ProxyKey.getKeyAsHex(record, fields.toArray(new Field<?>[0]));
        }

        private Timestamp getTimestamp(R record, String keyName) {
            if (keyName == null) {
                return null;
            }
            Instant latestTime = Instant.ofEpochMilli(
                    record.getValue(StringConstants.SNAPSHOT_TIME, Timestamp.class).getTime());
            if (keyName.equals((HOUR_KEY_FIELD_NAME))) {
                return Timestamp.from(latestTime.truncatedTo(ChronoUnit.HOURS));
            } else if (keyName.equals(DAY_KEY_FIELD_NAME)) {
                return Timestamp.from(latestTime.truncatedTo(ChronoUnit.DAYS));
            } else {
                // monthly - we need the start of the last day in the current month
                return Timestamp.from(
                        OffsetDateTime.ofInstant(latestTime, ZoneOffset.UTC)
                                // start-of-day
                                .truncatedTo(ChronoUnit.DAYS)
                                // start of this day next month
                                .plus(1L, ChronoUnit.MONTHS)
                                // start of first day of next month
                                .withDayOfMonth(1)
                                // start of last day of this month
                                .minusDays(1L)
                                .toInstant());
            }
        }
    }
}
