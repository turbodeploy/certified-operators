package com.vmturbo.cost.component.identity;

import static com.vmturbo.cost.component.identity.PriceTableKeyExtractor.PRICE_TABLE_KEY_IDENTIFIERS;

import java.text.MessageFormat;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.cost.Pricing.PriceTableKey;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.PriceTableKeyOidRecord;
import com.vmturbo.identity.attributes.IdentityMatchingAttributes;
import com.vmturbo.identity.attributes.SimpleMatchingAttributes;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.sql.utils.DbException;

/**
 * Persistence for price table key oids for {@link Tables#PRICE_TABLE}.
 */
public class PriceTableKeyIdentityStore implements DiagsRestorable<Void> {
    private static final Logger logger = LogManager.getLogger();
    private final DSLContext dsl;
    private final IdentityProvider identityProvider;

    /**
     * Constructor for the PriceTableKeyIdentityStore.
     *
     * @param dsl              The dsl context.
     * @param identityProvider The identity provider.
     */
    public PriceTableKeyIdentityStore(@Nonnull final DSLContext dsl,
                                      @Nonnull final IdentityProvider identityProvider) {
        this.dsl = Objects.requireNonNull(dsl);
        this.identityProvider = Objects.requireNonNull(identityProvider);
    }

    /**
     * Fetch all the price Table OIDs, priceTableKey rows.
     *
     * @return Map of generated {@link SimpleMatchingAttributes} to OIDs
     * from {@link Tables#PRICE_TABLE_KEY_OID}.
     * @throws DbException if DB access fails.
     */
    @Nonnull
    public Map<IdentityMatchingAttributes, Long> fetchAllOidMappings() throws DbException {
        try {
            return fetchAllOidMappings(dsl);
        } catch (DataAccessException e) {
            throw new DbException("Error fetching all OID mappings", e);
        }
    }

    /**
     * Fetch all the price Table OIDs, priceTableKey rows.
     *
     * @param dsl DB access context.
     * @return Map of generated {@link SimpleMatchingAttributes} to OIDs.
     */
    @Nonnull
    public Map<IdentityMatchingAttributes, Long> fetchAllOidMappings(DSLContext dsl) {
        final Map<IdentityMatchingAttributes, Long> identityMatchingAttributesLongMap = Maps.newHashMap();
        dsl.select()
                .from(Tables.PRICE_TABLE_KEY_OID)
                .fetchInto(PriceTableKeyOidRecord.class)
                .forEach(entry -> identityMatchingAttributesLongMap.put(
                        PriceTableKeyExtractor.getMatchingAttributes(entry.getPriceTableKey()),
                        entry.getId()));
        return identityMatchingAttributesLongMap;
    }

    /**
     * Assigns/fetches an OID for a given priceTableKey. This OID is used in PriceTable as well.
     *
     * @param newPriceTableKey pricetablekey received in request.
     * @return OID which is either newly assigned or fetched if
     * {@link IdentityMatchingAttributes } matches the given priceTableKey.
     * @throws IdentityStoreException If unable to fetch rows or generate a new OID.
     */
    public long fetchOrAssignOid(@Nonnull final PriceTableKey newPriceTableKey) throws
            IdentityStoreException {
        try {
            return dsl.transactionResult(configuration -> {
                DSLContext transactionDsl = DSL.using(configuration);
                final Map<IdentityMatchingAttributes, Long> currentPriceKeys =
                        fetchAllOidMappings(transactionDsl);
                return assignPriceTableKeyOid(transactionDsl, newPriceTableKey, currentPriceKeys)
                        .getValue();
            });
        } catch (DataAccessException e) {
            throw new IdentityStoreException("Exception while fetching new OID", e);
        }
    }

    /**
     * Generates a new and unique OID using {@link IdentityProvider}.
     *
     * @param previousPriceTableKeyValues current values in DB.
     * @return new {@link long} oid.
     * @throws IdentityStoreException if fails to fetch from DB.
     */
    private long generateNewOid(@Nonnull final Set<Long> previousPriceTableKeyValues)
            throws IdentityStoreException {
        final AtomicLong newPriceTableKeyOid = new AtomicLong(identityProvider.next());
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            Future<AtomicLong> future = executorService.submit(() -> {
                while (true) {
                    if (!previousPriceTableKeyValues.contains(newPriceTableKeyOid.longValue())) {
                        return newPriceTableKeyOid;
                    } else {
                        newPriceTableKeyOid.set(identityProvider.next());
                    }
                }
            });
            future.get(10, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            //should never happen
            logger.error("Unable to generate a new oid in 10 seconds. " +
                    "Current priceTable oids are : {}", previousPriceTableKeyValues);
            throw new IdentityStoreException("Unable to generate a new oid", e);
        } finally {
            executorService.shutdownNow();
        }
        return newPriceTableKeyOid.longValue();
    }

    /**
     * Removes oid mapping to price table keys.
     * Also cascade deletes row from {@link Tables#PRICE_TABLE}.
     *
     * @param oidsToRemove set of oids to remove.
     * @throws DbException if unable to remove OID from DB.
     */
    public void removeOidMappings(@Nonnull final Set<Long> oidsToRemove) throws DbException {
        removeOidMappings(dsl, oidsToRemove);
    }

    /**
     * Removes oid mapping to price table keys.
     * Also cascade deletes row from {@link Tables#PRICE_TABLE}.
     *
     * @param oidsToRemove set of oids to remove.
     * @param dsl transaction dsl context.
     * @throws DbException if unable to remove OID from DB.
     */
    public void removeOidMappings(@Nonnull final DSLContext dsl, @Nonnull final Set<Long> oidsToRemove)
            throws DbException {
        try {
            dsl.transaction(configuration -> {
                DSLContext transactionDsl = DSL.using(configuration);
                transactionDsl.deleteFrom(Tables.PRICE_TABLE_KEY_OID)
                        .where(Tables.PRICE_TABLE_KEY_OID.ID.in(oidsToRemove))
                        .execute();
            });
        } catch (DataAccessException e) {
            throw new DbException("Error deleting Oid Mappings", e);
        }
    }

    /**
     * Save new oid with PriceTableKey details in price_table_key_oid.
     *
     * @param dsl                        DB access context.
     * @param identityMatchingAttributes identifier attributes.
     * @param newOid                     new OID generated by IdentityProvider
     * @throws IdentityStoreException if unable to save row in DB.
     */
    private void saveNewOid(@Nonnull DSLContext dsl,
                            @Nonnull final IdentityMatchingAttributes identityMatchingAttributes,
                            @Nonnull final Long newOid) throws IdentityStoreException {
        try {
            dsl.insertInto(Tables.PRICE_TABLE_KEY_OID)
                    .set(Tables.PRICE_TABLE_KEY_OID.ID, newOid)
                    .set(Tables.PRICE_TABLE_KEY_OID.PRICE_TABLE_KEY,
                            identityMatchingAttributes
                                    .getMatchingAttribute(PRICE_TABLE_KEY_IDENTIFIERS)
                                    .getAttributeValue())
                    .onDuplicateKeyIgnore()
                    .execute();
        } catch (DataAccessException e) {
            throw new IdentityStoreException(
                    MessageFormat.format("Exception while updating oid {0} with attributes: {1}",
                            newOid, identityMatchingAttributes));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void collectDiags(@Nonnull DiagnosticsAppender appender)
            throws DiagnosticsException {
        final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();
        try {
            for (Entry<IdentityMatchingAttributes, Long> entry: fetchAllOidMappings().entrySet()) {
                try {
                    final PriceTableKeyOid priceTableKeyOid =
                            new PriceTableKeyOid(entry.getKey(), entry.getValue());
                    appender.appendString(gson.toJson(priceTableKeyOid, PriceTableKeyOid.class));
                } catch (NumberFormatException | IdentityStoreException e) {
                    logger.error("Could not collect diags for {}.",
                            entry.getValue(), e);
                }
            }
        } catch (DbException e) {
            throw new DiagnosticsException(String.format("Retrieving workflow identifiers from database "
                    + "failed. %s", e));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags,
                             @Nullable Void restoreContext) throws DiagnosticsException {
        final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();
        try {
            dsl.transaction(configuration -> {
                DSLContext context = DSL.using(configuration);
                removeAllOids();
                collectedDiags.forEach(diag -> {
                    PriceTableKeyOid priceTableKeyOid = null;
                    try {
                        priceTableKeyOid = gson.fromJson(diag, PriceTableKeyOid.class);
                        final IdentityMatchingAttributes attrs = PriceTableKeyExtractor
                                .getMatchingAttributes(priceTableKeyOid.priceTableKeyIdentifiers);
                        saveNewOid(context, attrs, priceTableKeyOid.id);
                    } catch (JsonParseException e) {
                        logger.error("Could not convert diag JSON : {} to priceTableKeyOid.", diag, e);
                    } catch (IdentityStoreException e) {
                        logger.error("Could not restore priceTableKeyOid {}.",
                                priceTableKeyOid.id, e);
                    }
                });
            });
        } catch (DataAccessException e) {
            throw new DiagnosticsException(String.format("restoring priceTableKeys to database "
                    + "failed. %s", e));
        }
    }

    @Nonnull
    @Override
    public String getFileName() {
        return getClass().getSimpleName();
    }

    private void removeAllOids() {
        dsl.transaction(configuration -> {
            DSLContext context = DSL.using(configuration);
            context.delete(Tables.PRICE_TABLE_KEY_OID).execute();
        });
    }

    /**
     * Assigns a new OID for a given PriceTableKey; if not found in {@param currentPriceTableKeys}.
     *
     * @param context          DSL context.
     * @param newPriceTableKey    new priceTableKey which is being looked up.
     * @param currentPriceTableKeys Map of current PriceTableKeys and their OIDs.
     * @return OID for {@param priceTableKey}.
     * @throws IdentityStoreException if IdentityStore was unable generate a new OID.
     */
    @Nonnull
    public Entry<IdentityMatchingAttributes, Long> assignPriceTableKeyOid(
            @Nonnull final DSLContext context,
            @Nonnull final PriceTableKey newPriceTableKey,
            @Nonnull final Map<IdentityMatchingAttributes, Long> currentPriceTableKeys)
            throws IdentityStoreException {
        final PriceTableKeyExtractor priceTableKeyExtractor = new PriceTableKeyExtractor();
        final IdentityMatchingAttributes identityMatchingAttributes =
                priceTableKeyExtractor.extractAttributes(newPriceTableKey);
        if (currentPriceTableKeys.containsKey(identityMatchingAttributes)) {
            //use old oid.
            logger.debug("matching priceTableKey found.");
            return new SimpleImmutableEntry<>(identityMatchingAttributes,
                    currentPriceTableKeys.get(identityMatchingAttributes));
        } else {
            //generate new oid..
            logger.debug("matching priceTableKey not found. Generating a new OID.");
            long newOid;
            final Set<Long> currentPriceTableKeyValues =
                    new HashSet<>(currentPriceTableKeys.values());
            newOid = generateNewOid(currentPriceTableKeyValues);
            saveNewOid(context, identityMatchingAttributes, newOid);
            return new SimpleImmutableEntry<>(identityMatchingAttributes, newOid);
        }
    }

    /**
     * Class used to collect and restore from priceTableKeyOid table.
     */
    static class PriceTableKeyOid {
        /**
         * price table key oid.
         */
        private final long id;

        /**
         * IdentityMatchingAttributes used in {@link PriceTableKey}.
         */
        final String priceTableKeyIdentifiers;

        private PriceTableKeyOid(@Nonnull final IdentityMatchingAttributes attrs, final long id)
                throws NumberFormatException, IdentityStoreException {
            this.id = id;
            this.priceTableKeyIdentifiers = Objects.requireNonNull(attrs.getMatchingAttribute(PRICE_TABLE_KEY_IDENTIFIERS)
                    .getAttributeValue());
        }
    }
}
