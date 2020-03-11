package com.vmturbo.cost.component.pricing;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Param;
import org.jooq.TableField;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableKey;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstancePriceTable;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.PriceTableRecord;
import com.vmturbo.cost.component.identity.PriceTableKeyIdentityStore;
import com.vmturbo.cost.component.pricing.PriceTableMerge.PriceTableMergeFactory;
import com.vmturbo.cost.component.pricing.utils.PriceTableKeySerializationHelper;
import com.vmturbo.identity.exceptions.IdentityStoreException;

/**
 * A {@link PriceTableStore} backed by MySQL.
 */
@ThreadSafe
public class SQLPriceTableStore implements PriceTableStore {

    private static final Logger logger = LogManager.getLogger();

    private final DSLContext dsl;

    private final Clock clock;

    private final PriceTableMergeFactory mergeFactory;

    private final PriceTableKeyIdentityStore priceTableKeyIdentityStore;

    public SQLPriceTableStore(@Nonnull final Clock clock,
                              @Nonnull final DSLContext dsl,
                              @Nonnull final PriceTableKeyIdentityStore priceTableKeyIdentityStore,
                              @Nonnull final PriceTableMergeFactory mergeFactory) {
        this.clock = Objects.requireNonNull(clock);
        this.dsl = Objects.requireNonNull(dsl);
        this.priceTableKeyIdentityStore = Objects.requireNonNull(priceTableKeyIdentityStore);
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
        // Get only the price tables which are being used by business accounts in the system
        final Set<Long> oids = dsl.selectDistinct(Tables.BUSINESS_ACCOUNT_PRICE_TABLE_KEY.PRICE_TABLE_KEY_OID)
                .from(Tables.BUSINESS_ACCOUNT_PRICE_TABLE_KEY).fetchSet(Tables.BUSINESS_ACCOUNT_PRICE_TABLE_KEY.PRICE_TABLE_KEY_OID);
        final Set<ReservedInstancePriceTable> priceTables = dsl.select(Tables.PRICE_TABLE.RI_PRICE_TABLE_DATA)
                .from(Tables.PRICE_TABLE)
                .where(filterByOidsCondition(oids))
                .fetchSet(Tables.PRICE_TABLE.RI_PRICE_TABLE_DATA);
        final PriceTableMerge merge = mergeFactory.newMerge();
        return merge.mergeRi(priceTables);
    }

    @Nonnull
    @Override
    public Map<Long, PriceTable> getPriceTables(final Collection<Long> oids) {
        return dsl.select(Tables.PRICE_TABLE.OID, Tables.PRICE_TABLE.PRICE_TABLE_DATA)
                .from(Tables.PRICE_TABLE)
                .where(filterByOidsCondition(oids)).fetchMap(Tables.PRICE_TABLE.OID,
                        Tables.PRICE_TABLE.PRICE_TABLE_DATA);
    }

    @Nonnull
    @Override
    public Map<Long, ReservedInstancePriceTable> getRiPriceTables(final Collection<Long> oids) {
        final Map<Long, ReservedInstancePriceTable> priceTables =
                dsl.select(Tables.PRICE_TABLE.OID, Tables.PRICE_TABLE.RI_PRICE_TABLE_DATA)
                        .from(Tables.PRICE_TABLE)
                        .where(filterByOidsCondition(oids))
                        .fetchMap(Tables.PRICE_TABLE.OID, Tables.PRICE_TABLE.RI_PRICE_TABLE_DATA);
        return priceTables;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putProbePriceTables(@Nonnull final Map<PriceTableKey, PriceTables> priceTableToPriceTableKeyMap) {
        dsl.transaction(context -> {
            final DSLContext transactionContext = DSL.using(context);
            final LocalDateTime curTime = LocalDateTime.now(clock);
            priceTableToPriceTableKeyMap.forEach((priceTableKey, table) -> {
                final PriceTable priceTable = table.getPriceTable();
                final ReservedInstancePriceTable riPriceTable = table.getRiPriceTable();
                final Long checkSum = table.getCheckSum();
                try {
                    final long oid = priceTableKeyIdentityStore.fetchOrAssignOid(priceTableKey);
                    final String serializedPriceTableKey = PriceTableKeySerializationHelper
                            .serializeProbeKeyMaterial(priceTableKey);
                    // We avoid using 'InsertOnDuplicateStep.onDuplicateKeyUpdate()' because it can
                    // cause an exceeding of 'max_allowed_packet' size.
                    final boolean duplicateKey = transactionContext
                            .fetchCount(Tables.PRICE_TABLE, Tables.PRICE_TABLE.OID.eq(oid)) > 0;
                    final Param<PriceTable> pPriceTable
                            = getParameter(Tables.PRICE_TABLE.PRICE_TABLE_DATA, priceTable);
                    final Param<ReservedInstancePriceTable> pRiPriceTable
                            = getParameter(Tables.PRICE_TABLE.RI_PRICE_TABLE_DATA, riPriceTable);
                    int modified;
                    if (duplicateKey) {
                        modified = transactionContext.update(Tables.PRICE_TABLE)
                                .set(Tables.PRICE_TABLE.PRICE_TABLE_KEY, serializedPriceTableKey)
                                .set(Tables.PRICE_TABLE.LAST_UPDATE_TIME, curTime)
                                .set(Tables.PRICE_TABLE.PRICE_TABLE_DATA, pPriceTable)
                                .set(Tables.PRICE_TABLE.RI_PRICE_TABLE_DATA, pRiPriceTable)
                                .set(Tables.PRICE_TABLE.CHECKSUM, checkSum)
                                .where(Tables.PRICE_TABLE.OID.eq(oid)).execute();
                    } else {
                        modified = transactionContext.insertInto(Tables.PRICE_TABLE)
                                .set(Tables.PRICE_TABLE.OID, oid)
                                .set(Tables.PRICE_TABLE.PRICE_TABLE_KEY, serializedPriceTableKey)
                                .set(Tables.PRICE_TABLE.LAST_UPDATE_TIME, curTime)
                                .set(Tables.PRICE_TABLE.PRICE_TABLE_DATA, pPriceTable)
                                .set(Tables.PRICE_TABLE.RI_PRICE_TABLE_DATA, pRiPriceTable)
                                .set(Tables.PRICE_TABLE.CHECKSUM, checkSum).execute();
                    }
                    logger.info("Modified {} row after insert/update.", modified);
                } catch (InvalidProtocolBufferException e) {
                    logger.error("unable to de-serialize priceTable : {}:", priceTableKey, e);
                } catch (IdentityStoreException e) {
                    logger.error("Exception when trying to persist OID for pricetableKey {}",
                            priceTableKey, e);
                }
            });
        });
    }

    private <T> Param<T> getParameter(TableField<PriceTableRecord, T> field, T value) {
        Param<T> param = DSL.param(field.getName(), field);
        param.setValue(value);
        return param;
    }

    /**
     * {@inheritDoc}
     */

    @Override
    @Nonnull
    public Map<PriceTableKey, Long> getChecksumByPriceTableKeys(
            @Nonnull final Collection<PriceTableKey> priceTableKeyList) {
        Map<PriceTableKey, Long> priceTableKeyLongMap = Maps.newHashMap();
        Map<String, Long> result = dsl
                .select(Tables.PRICE_TABLE.PRICE_TABLE_KEY, Tables.PRICE_TABLE.CHECKSUM)
                .from(Tables.PRICE_TABLE)
                .where(filterCondition(priceTableKeyList))
                .fetchMap(Tables.PRICE_TABLE.PRICE_TABLE_KEY, Tables.PRICE_TABLE.CHECKSUM);
        result.forEach((priceTableKey, value) -> {
            try {
                priceTableKeyLongMap.put(PriceTableKeySerializationHelper
                        .deserializeProbeKeyMaterial(priceTableKey), value);
            } catch (InvalidProtocolBufferException e) {
                logger.info("Unable to de-serialize priceTableKey {}", priceTableKey);
            }
        });
        return priceTableKeyLongMap;
    }

    /**
     * If {@param priceTableKeyList } is empty all rows should be returned.
     *
     * @param priceTableKeyList preselected list of priceTableKeys.
     * @return condition for query
     */
    @Nonnull
    private Condition filterCondition(@Nonnull final Collection<PriceTableKey> priceTableKeyList) {
        Set<String> priceTableKeySet = Sets.newHashSet();
        priceTableKeyList.forEach(priceTableKey -> {
            try {
                priceTableKeySet.add(PriceTableKeySerializationHelper.serializeProbeKeyMaterial(priceTableKey));
            } catch (InvalidProtocolBufferException e) {
                logger.error("Unable to serialize priceTableKey {}. Continuing.", priceTableKey);
            }
        });
        logger.debug("Fetching {} priceTableKey.", priceTableKeySet.isEmpty() ?
                "all" : priceTableKeySet.size());
        return priceTableKeySet.isEmpty() ?
                DSL.trueCondition() :
                Tables.PRICE_TABLE.PRICE_TABLE_KEY.in(priceTableKeySet);
    }

    /**
     * Condition to filter the Price Table by the list of oids.
     *
     * @param oids The oids.
     * @return The condition.
     */
    private Condition filterByOidsCondition(final Collection oids) {
        return oids.isEmpty() ? DSL.trueCondition() : Tables.PRICE_TABLE.OID.in(oids);
    }
}
