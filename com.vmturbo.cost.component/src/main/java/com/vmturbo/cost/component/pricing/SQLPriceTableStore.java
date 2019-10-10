package com.vmturbo.cost.component.pricing;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstancePriceTable;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.pricing.PriceTableMerge.PriceTableMergeFactory;

/**
 * A {@link PriceTableStore} backed by MySQL.
 */
@ThreadSafe
public class SQLPriceTableStore implements PriceTableStore {

    private static final Logger logger = LogManager.getLogger();

    private final DSLContext dsl;

    private final Clock clock;

    private final PriceTableMergeFactory mergeFactory;

    public SQLPriceTableStore(@Nonnull final Clock clock,
                              @Nonnull final DSLContext dsl,
                              @Nonnull final PriceTableMergeFactory mergeFactory) {
        this.clock = Objects.requireNonNull(clock);
        this.dsl = Objects.requireNonNull(dsl);
        this.mergeFactory = Objects.requireNonNull(mergeFactory);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public PriceTable getMergedPriceTable() {
        // It may be worth it to cache the merged price table. We can calculate the
        // merged table during putProbePriceTables.
        final Set<PriceTable> priceTables = dsl.select(Tables.PRICE_TABLE.PRICE_TABLE_DATA)
                .from(Tables.PRICE_TABLE)
                .fetchSet(Tables.PRICE_TABLE.PRICE_TABLE_DATA);
        final PriceTableMerge merge = mergeFactory.newMerge();
        return merge.merge(priceTables);
    }

    @Nonnull
    @Override
    public ReservedInstancePriceTable getMergedRiPriceTable() {
        // It may be worth it to cache the merged price table. We can calculate the
        // merged table during putProbePriceTables.
        final Set<ReservedInstancePriceTable> priceTables = dsl.select(Tables.PRICE_TABLE.RI_PRICE_TABLE_DATA)
                .from(Tables.PRICE_TABLE)
                .fetchSet(Tables.PRICE_TABLE.RI_PRICE_TABLE_DATA);
        final PriceTableMerge merge = mergeFactory.newMerge();
        return merge.mergeRi(priceTables);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putProbePriceTables(@Nonnull final Map<String, PriceTables> tablesByProbeType) {
        dsl.transaction(context -> {
            final DSLContext transactionContext = DSL.using(context);
            final LocalDateTime curTime = LocalDateTime.now(clock);
            final Set<String> existingProbeTypes = transactionContext.select(
                Tables.PRICE_TABLE.ASSOCIATED_PROBE_TYPE)
                    .from(Tables.PRICE_TABLE)
                    .fetchSet(Tables.PRICE_TABLE.ASSOCIATED_PROBE_TYPE);
            final Set<String> probeTypesToRemove = Sets.difference(existingProbeTypes, tablesByProbeType.keySet());
            if (!probeTypesToRemove.isEmpty()) {
                final int deletedRows = transactionContext.deleteFrom(Tables.PRICE_TABLE)
                        .where(Tables.PRICE_TABLE.ASSOCIATED_PROBE_TYPE.in(probeTypesToRemove))
                        .execute();
                if (deletedRows != probeTypesToRemove.size()) {
                    logger.error("Wanted to delete {} rows, but deleted {}",
                           probeTypesToRemove.size(), deletedRows);
                }
            }

            tablesByProbeType.forEach((probeType, tables) -> {
                final PriceTable priceTable = tables.getPriceTable();

                final ReservedInstancePriceTable riPriceTable = tables.getRiPriceTable();

                final int modifiedRows = transactionContext.insertInto(Tables.PRICE_TABLE)
                        .set(Tables.PRICE_TABLE.ASSOCIATED_PROBE_TYPE, probeType)
                        .set(Tables.PRICE_TABLE.LAST_UPDATE_TIME, curTime)
                        .set(Tables.PRICE_TABLE.PRICE_TABLE_DATA, priceTable)
                        .set(Tables.PRICE_TABLE.RI_PRICE_TABLE_DATA, riPriceTable)
                        .onDuplicateKeyUpdate()
                        .set(Tables.PRICE_TABLE.LAST_UPDATE_TIME, curTime)
                        .set(Tables.PRICE_TABLE.PRICE_TABLE_DATA, priceTable)
                        .set(Tables.PRICE_TABLE.RI_PRICE_TABLE_DATA, riPriceTable)
                        .execute();
                if (modifiedRows != 1) {
                    logger.error("Expected 1 modified row after insert/update. Got: {}",
                        modifiedRows);
                }
            });
        });
    }
}
