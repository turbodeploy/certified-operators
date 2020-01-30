package com.vmturbo.cost.component.pricing;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.Sets;
import com.google.gson.Gson;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.cost.Pricing.BusinessAccountPriceTableKey;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableKey;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.BusinessAccountPriceTableKeyRecord;
import com.vmturbo.cost.component.identity.PriceTableKeyIdentityStore;
import com.vmturbo.identity.attributes.IdentityMatchingAttributes;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.sql.utils.DbException;

/**
 * Persistence for Business Account oids to their respective price table key.
 * persisted in {@link Tables#BUSINESS_ACCOUNT_PRICE_TABLE_KEY}.
 */
@ThreadSafe
public class BusinessAccountPriceTableKeyStore implements DiagsRestorable {
    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();
    private static final Logger logger = LogManager.getLogger();
    private final DSLContext dsl;
    private final PriceTableKeyIdentityStore priceTableKeyIdentityStore;

    /**
     * Constructor to initialize BusinessAccountPriceTableKey store. Backed by persistence.
     *
     * @param dsl                        the dsl context.
     * @param priceTableKeyIdentityStore priceTableKeyIdentityStore used to create/fetch new priceTableOids.
     */
    public BusinessAccountPriceTableKeyStore(final DSLContext dsl,
                                             final PriceTableKeyIdentityStore priceTableKeyIdentityStore) {
        this.dsl = dsl;
        this.priceTableKeyIdentityStore = priceTableKeyIdentityStore;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void collectDiags(@Nonnull DiagnosticsAppender appender) throws DiagnosticsException {
        for (Entry<Long, Long> entry : fetchPriceTableKeyOidsByBusinessAccount(
                Collections.emptySet()).entrySet()) {
            final BusinessAccountOidToPriceTableKey priceTableKeyOid =
                    new BusinessAccountOidToPriceTableKey(entry.getKey(), entry.getValue());
            final String string =
                    GSON.toJson(priceTableKeyOid, BusinessAccountOidToPriceTableKey.class);
            appender.appendString(string);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags) throws DiagnosticsException {
        try {
            dsl.transaction(configuration -> {
                DSLContext context = DSL.using(configuration);
                removeAllOids();
                collectedDiags.forEach(diag -> {
                    final BusinessAccountOidToPriceTableKey businessAccountOidToPriceTableKey
                            = GSON.fromJson(diag, BusinessAccountOidToPriceTableKey.class);
                    context.insertInto(Tables.BUSINESS_ACCOUNT_PRICE_TABLE_KEY)
                            .set(Tables.BUSINESS_ACCOUNT_PRICE_TABLE_KEY.BUSINESS_ACCOUNT_OID,
                                    businessAccountOidToPriceTableKey.id)
                            .set(Tables.BUSINESS_ACCOUNT_PRICE_TABLE_KEY.PRICE_TABLE_KEY_OID,
                                    businessAccountOidToPriceTableKey.priceTableKey)
                            .onDuplicateKeyIgnore()
                            .execute();
                });
            });
        } catch (DataAccessException e) {
            throw new DiagnosticsException(String.format("Restoring BusinessAccountOidToPriceTableKey" +
                    " to database failed. %s", e));
        }
    }

    @Nonnull
    @Override
    public String getFileName() {
        return getClass().getSimpleName();
    }

    private void removeAllOids() {
        dsl.delete(Tables.BUSINESS_ACCOUNT_PRICE_TABLE_KEY).execute();
    }

    /**
     * Upload businessAccount to PriceTableKey mapping.
     *
     * @param businessAccountPriceTableKey map of business account OID to {@link PriceTableKey}.
     */
    public void uploadBusinessAccount(final BusinessAccountPriceTableKey businessAccountPriceTableKey) {
        try {
            dsl.transaction((configuration) -> {
                DSLContext transactionDsl = DSL.using(configuration);
                Map<IdentityMatchingAttributes, Long> currentPriceTableKeys = priceTableKeyIdentityStore
                        .fetchAllOidMappings(transactionDsl);
                Set<Query> queries = businessAccountPriceTableKey
                        .getBusinessAccountPriceTableKeyMap().entrySet()
                        .stream()
                        .map(priceTableKeyEntry -> createPriceTableKeyEntryQuery(transactionDsl, priceTableKeyEntry,
                                currentPriceTableKeys))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toSet());
                transactionDsl.batch(queries).execute();
            });
            } catch (DataAccessException e) {
                logger.error("Exception while trying to upload businessAccount to priceTable mappings.", e);
        }
    }

    /**
     * Retrieve mapping of all businessAccount OIDs to {@link PriceTableKey} indexed by BA OIDs;
     * if not OIDs are specified.
     * If businessAccount OIDs are specified in args, only those mapping are retrieved.
     *
     * @param businessAccountOIDs list of business account OIDs to retrieve.
     * @return map of BA OIDs to {@link PriceTableKey}.
     */
    @Nonnull
    public Map<Long, Long> fetchPriceTableKeyOidsByBusinessAccount(@Nonnull final Set<Long> businessAccountOIDs) {
        return dsl
                .select(Tables.BUSINESS_ACCOUNT_PRICE_TABLE_KEY.BUSINESS_ACCOUNT_OID,
                        Tables.BUSINESS_ACCOUNT_PRICE_TABLE_KEY.PRICE_TABLE_KEY_OID)
                .from(Tables.BUSINESS_ACCOUNT_PRICE_TABLE_KEY)
                .where(filterCondition(businessAccountOIDs))
                .fetchMap(Tables.BUSINESS_ACCOUNT_PRICE_TABLE_KEY.BUSINESS_ACCOUNT_OID,
                        Tables.BUSINESS_ACCOUNT_PRICE_TABLE_KEY.PRICE_TABLE_KEY_OID);
    }

    /**
     * Remove BA oid from {@link Tables#BUSINESS_ACCOUNT_PRICE_TABLE_KEY} and remove orphaned
     * priceTableOids. We determine priceTableOids which are still being used in
     * {@link Tables#BUSINESS_ACCOUNT_PRICE_TABLE_KEY} and remove it from the list of
     * priceTableOids to be removed from {@link PriceTableKeyIdentityStore}.
     *
     * @param businessAccountOIDs businessAccount to be removed.
     * @throws DbException if exception during reading/removing from DB.
     */
    public void removeBusinessAccountAndPriceTableKeyOid(@Nonnull Set<Long> businessAccountOIDs)
            throws DbException {
        Map<Long, Long> baOidToPriceTableKeyMap = fetchPriceTableKeyOidsByBusinessAccount(businessAccountOIDs);
        Collection<Long> priceTableKeyOids = baOidToPriceTableKeyMap.values();
        try {
            dsl.transaction(configuration -> {
                DSLContext transactionDsl = DSL.using(configuration);
                transactionDsl.deleteFrom(Tables.BUSINESS_ACCOUNT_PRICE_TABLE_KEY)
                        .where(Tables.BUSINESS_ACCOUNT_PRICE_TABLE_KEY.BUSINESS_ACCOUNT_OID
                                .in(businessAccountOIDs)).execute();
                // get the row which still exist with same priceTables OIDs.
                // We should not delete these pricetablesOIDs yet as they are still being used.
                List<Long> priceTableKeyOidNoDelete = transactionDsl.select(
                        Tables.BUSINESS_ACCOUNT_PRICE_TABLE_KEY.PRICE_TABLE_KEY_OID)
                        .from(Tables.BUSINESS_ACCOUNT_PRICE_TABLE_KEY)
                        .where(Tables.BUSINESS_ACCOUNT_PRICE_TABLE_KEY.PRICE_TABLE_KEY_OID
                                .in(baOidToPriceTableKeyMap.values()))
                        .fetch(Tables.BUSINESS_ACCOUNT_PRICE_TABLE_KEY.PRICE_TABLE_KEY_OID);
                priceTableKeyOids.removeAll(priceTableKeyOidNoDelete);
                priceTableKeyIdentityStore.removeOidMappings(transactionDsl,
                        Sets.newHashSet(priceTableKeyOids));
            });
        } catch (DataAccessException e) {
            throw new DbException("Error deleting BA Oid Mappings", e);
        }
    }

    /**
     * Creates a query with {@link BusinessAccountPriceTableKeyRecord}; updates on duplicate.
     *
     * @param context                dsl context.
     * @param longPriceTableKeyEntry BA oid to {@link PriceTableKey} entry.
     * @param currentPriceTableKeys collection of current priceTableKeys.
     * @return Insert Query used during batch insert. null if any exception occurs.
     */
    @Nullable
    private Query createPriceTableKeyEntryQuery(
            @Nonnull final DSLContext context,
            @Nonnull final Entry<Long, PriceTableKey> longPriceTableKeyEntry,
            final Map<IdentityMatchingAttributes, Long> currentPriceTableKeys) {
        PriceTableKey priceTableKey = longPriceTableKeyEntry.getValue();
        Long businessAccountOID = longPriceTableKeyEntry.getKey();
        try {
            // assign oid using priceTableKeyIdentityStore.
            Entry<IdentityMatchingAttributes, Long> newPriceTableKeyOidEntry = priceTableKeyIdentityStore
                    .assignPriceTableKeyOid(context, priceTableKey, currentPriceTableKeys);
            currentPriceTableKeys.put(newPriceTableKeyOidEntry.getKey(),
                    newPriceTableKeyOidEntry.getValue());
            BusinessAccountPriceTableKeyRecord businessAccountPriceTableKeyRecord =
                    new BusinessAccountPriceTableKeyRecord(businessAccountOID,
                            newPriceTableKeyOidEntry.getValue());
            return context.insertInto(Tables.BUSINESS_ACCOUNT_PRICE_TABLE_KEY)
                    .set(businessAccountPriceTableKeyRecord)
                    .onDuplicateKeyUpdate()
                    .set(businessAccountPriceTableKeyRecord);
        } catch (IdentityStoreException e) {
            logger.error("Was unable to create a valid query for entry BA OID: {} to pricetablekey {}.",
                    businessAccountOID, priceTableKey);
            return null;
        }
    }

    @Nonnull
    private Condition filterCondition(@Nonnull final Collection<Long> businessOids) {
        return businessOids.isEmpty() ?
                DSL.trueCondition() :
                Tables.BUSINESS_ACCOUNT_PRICE_TABLE_KEY.BUSINESS_ACCOUNT_OID.in(businessOids);
    }

    /**
     * Bean class to map DB records {@link BusinessAccountPriceTableKeyRecord}.
     */
    private static class BusinessAccountOidToPriceTableKey {
        private long id;
        private long priceTableKey;

        /**
         * Constructor for methods used during collecting,restoring diags.
         *
         * @param id            BA oids.
         * @param priceTableKey {@link PriceTableKey}.
         */
        BusinessAccountOidToPriceTableKey(final long id, final long priceTableKey) {
            this.id = id;
            this.priceTableKey = priceTableKey;
        }
    }
}
