package com.vmturbo.cost.component.identity;

import static com.vmturbo.cost.component.identity.PriceTableKeyExtractor.getMatchingAttributes;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Type;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.jooq.DSLContext;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableKey;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstancePriceTable;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.identity.PriceTableKeyIdentityStore.PriceTableKeyOid;
import com.vmturbo.cost.component.pricing.PriceTableMerge.PriceTableMergeFactory;
import com.vmturbo.cost.component.pricing.PriceTableStore.PriceTables;
import com.vmturbo.cost.component.pricing.SQLPriceTableStore;
import com.vmturbo.identity.attributes.IdentityMatchingAttribute;
import com.vmturbo.identity.attributes.IdentityMatchingAttributes;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.platform.sdk.common.PricingDTO.ReservedInstancePrice;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;
import com.vmturbo.sql.utils.DbException;

/**
 * Context Configuration for this test class.
 */
public class PriceTableKeyIdentityStoreTest {
    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Cost.COST);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    private final Type type = new TypeToken<Map<String, String>>() {
    }.getType();

    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();
    private MutableFixedClock clock = new MutableFixedClock(Instant.ofEpochMilli(1_000_000_000), ZoneId.systemDefault());

    private DSLContext dsl = dbConfig.getDslContext();

    private PriceTableKeyIdentityStore testIdentityStore = new PriceTableKeyIdentityStore(dsl,
                new IdentityProvider(0));

    private final Long awsServiceProviderOid = 123456L;
    private final Long azureServiceProviderOid = 9876543L;

    /**
     * Set up identity generator.
     */
    @BeforeClass
    public static void setupClass() {
        IdentityGenerator.initPrefix(0L);
    }

    /**
     * Test Empty set of oids.
     *
     * @throws DbException An IdentityStoreException.
     */
    @Test
    public void testEmptySetOfOids() throws DbException {
        Map<IdentityMatchingAttributes, Long> test = testIdentityStore.fetchAllOidMappings();
        assertThat(test.isEmpty(), is(true));
    }

    /**
     * Testing Oid Identifiers.
     *
     * @throws IdentityStoreException if unable to assign new ID.
     * @throws DbException if unable to fetch DB rows.
     */
    @Test
    public void testOidWithIdentifiers() throws IdentityStoreException, DbException {
        PriceTableKey priceTableKey = mockPriceTableKey(awsServiceProviderOid);

        testIdentityStore.fetchOrAssignOid(priceTableKey);
        Map<IdentityMatchingAttributes, Long> matchingAttributesLongMap = testIdentityStore
                .fetchAllOidMappings();
        assertThat(matchingAttributesLongMap.size(), is(1));

        Set<IdentityMatchingAttributes> identityMatchingAttributes = matchingAttributesLongMap.keySet();

        IdentityMatchingAttribute identityMatchingAttribute = identityMatchingAttributes.iterator()
                .next().getMatchingAttribute(PriceTableKeyExtractor.PRICE_TABLE_KEY_IDENTIFIERS);

        Map<String, String> identityMatchingAttributeMap = GSON.fromJson(
                identityMatchingAttribute.getAttributeValue(), type);

        assertThat(identityMatchingAttributeMap.get("enrollmentId"), is("123"));
        assertThat(identityMatchingAttributeMap.get("offerId"), is("456"));
        assertThat(identityMatchingAttributeMap.get("service_provider_oid"), is("123456"));
    }


    /**
     * Testing oids with the same identifier.
     *
     * @throws IdentityStoreException if unable to assign new ID.
     * @throws DbException if unable to fetch DB rows.
     */
    @Test
    public void testOidWithSameIdentifier() throws IdentityStoreException, DbException {
        PriceTableKey priceTableKey = mockPriceTableKey(awsServiceProviderOid);
        testIdentityStore.fetchOrAssignOid(priceTableKey);
        testIdentityStore.fetchOrAssignOid(priceTableKey);
        Map<IdentityMatchingAttributes, Long> matchingAttributesLongMap = testIdentityStore
                .fetchAllOidMappings();
        assertThat(matchingAttributesLongMap.size(), is(1));
    }


    /**
     * Testing oids with unique identifiers.
     *
     * @throws IdentityStoreException if unable to assign new ID.
     */
    @Test
    public void testOidWithUniqueIdentifiers() throws IdentityStoreException {
        PriceTableKey priceTableKey = mockPriceTableKey(azureServiceProviderOid);
        testIdentityStore.fetchOrAssignOid(priceTableKey);

        priceTableKey = priceTableKey.newBuilderForType().setServiceProviderId(awsServiceProviderOid).build();
        testIdentityStore.fetchOrAssignOid(priceTableKey);

        Map<IdentityMatchingAttributes, Long> matchingAttributesLongMap = testIdentityStore.fetchAllOidMappings(dsl);
        assertThat(matchingAttributesLongMap.size(), is(2));
    }

    /**
     * Testing deletion of price table keys.
     *
     * @throws IdentityStoreException if unable to assign new ID.
     * @throws DbException if unable to fetch DB rows.
     */
    @Test
    public void testCascadeDeleteOnPriceTableKeyOidDelete() throws IdentityStoreException, DbException {
        PriceTableKey priceTableKey = mockPriceTableKey(awsServiceProviderOid);
        final long oid = testIdentityStore.fetchOrAssignOid(priceTableKey);

        final PriceTableMergeFactory mergeFactory = mock(PriceTableMergeFactory.class);

        SQLPriceTableStore sqlPriceTableStore = new SQLPriceTableStore(clock, dsl,
                new PriceTableKeyIdentityStore(dsl,
                new IdentityProvider(0)), mergeFactory);

        final ReservedInstancePriceTable fooPriceTable = ReservedInstancePriceTable.newBuilder()
                .putRiPricesBySpecId(1L, ReservedInstancePrice.getDefaultInstance())
                .build();
        sqlPriceTableStore.putProbePriceTables(ImmutableMap.of(priceTableKey,
                new PriceTables(PriceTable.getDefaultInstance(), fooPriceTable, 111L)));


        assertThat(getRowCount(), is(1));

        assertThat(testIdentityStore.fetchAllOidMappings().size(), is(1));

        testIdentityStore.removeOidMappings(Collections.singleton(oid));

        //verify price table oid is empty too.
        assertThat(testIdentityStore.fetchAllOidMappings().isEmpty(), is(true));
        assertThat(getRowCount(), is(0));

    }


    /**
     * Test querying of price tables by oids.
     *
     * @throws DbException Exception when unable to access DB.
     */
    @Test
    public void testGetOidToPriceTableMapping() throws IdentityStoreException {
        final PriceTable priceTable1 = mockPriceTable(2L);
        final PriceTable priceTable2 = mockPriceTable(2L);

        final PriceTableKey priceTableKey1 = mockPriceTableKey(awsServiceProviderOid);
        final PriceTableKey priceTableKey2 = mockPriceTableKey(azureServiceProviderOid);

        final long oid1 = testIdentityStore.fetchOrAssignOid(priceTableKey1);
        final long oid2 = testIdentityStore.fetchOrAssignOid(priceTableKey2);

        final PriceTableMergeFactory mergeFactory = mock(PriceTableMergeFactory.class);
        SQLPriceTableStore sqlPriceTableStore = new SQLPriceTableStore(clock, dsl, new PriceTableKeyIdentityStore(dsl,
                new IdentityProvider(0)), mergeFactory);

        sqlPriceTableStore.putProbePriceTables(ImmutableMap.of(priceTableKey1, new PriceTables(priceTable1,
                ReservedInstancePriceTable.getDefaultInstance(), 111L)));
        sqlPriceTableStore.putProbePriceTables(ImmutableMap.of(priceTableKey2, new PriceTables(priceTable2,
                ReservedInstancePriceTable.getDefaultInstance(), 222L)));

        Map<Long, PriceTable> oidToPriceTable = sqlPriceTableStore.getPriceTables(Arrays.asList(oid1, oid2));

        assertThat(oidToPriceTable.isEmpty(), is(false));

        assertThat(oidToPriceTable.size(), is(2));

        assertThat(oidToPriceTable.get(oid1), is(priceTable1));

        assertThat(oidToPriceTable.get(oid2), is(priceTable2));
    }

    /**
     * Testing collect diags ability for Table: price table key oid.
     *
     * @throws DiagnosticsException   if exception occurs while collecting diags.
     * @throws DbException if there is an error fetching the IDs from the testIdentityStore
     */
    @Test
    public void testCollectDiags() throws DiagnosticsException, DbException {
        insertAzurePriceTable();
        //collecting diags
        final DiagnosticsAppender appender = Mockito.mock(DiagnosticsAppender.class);
        testIdentityStore.collectDiags(appender);
        final ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(appender).appendString(captor.capture());
        final PriceTableKeyOid priceTableKeyOid =
                GSON.fromJson(captor.getValue(), PriceTableKeyOid.class);
        final IdentityMatchingAttributes attrs = getMatchingAttributes(priceTableKeyOid.priceTableKeyIdentifiers);
        Map<IdentityMatchingAttributes, Long> identityMatchingAttributesLongMap = testIdentityStore
                .fetchAllOidMappings();
        assertThat(identityMatchingAttributesLongMap.size(), is(1));
        final IdentityMatchingAttributes attributes =
                identityMatchingAttributesLongMap.entrySet().iterator().next().getKey();
        assertThat(attributes, equalTo(attrs));
    }

    /**
     * Testing restoring diags to {@link Tables#PRICE_TABLE_KEY_OID}.
     *
     * @throws DbException Exception when unable to access DB.
     * @throws DiagnosticsException   if exception occurs while collecting diags.
     */
    @Test
    public void testRestoreDiags() throws DiagnosticsException, DbException {
        insertAzurePriceTable();
        //collecting diags
        final DiagnosticsAppender appender = Mockito.mock(DiagnosticsAppender.class);
        testIdentityStore.collectDiags(appender);
        final ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(appender).appendString(captor.capture());
        final Map<IdentityMatchingAttributes, Long> controlMatchingAttributesLongMap =
                testIdentityStore.fetchAllOidMappings();

        dsl.delete(Tables.PRICE_TABLE_KEY_OID).execute();
        assertThat(testIdentityStore.fetchAllOidMappings().isEmpty(), is(true));

        testIdentityStore.restoreDiags(captor.getAllValues());
        assertThat(controlMatchingAttributesLongMap,
                equalTo(testIdentityStore.fetchAllOidMappings()));
    }

    /**
     * Test collect and restore diags using a newer store.
     *
     * @throws DbException          Exception when unable to access DB.
     * @throws DiagnosticsException   exception during collecting or restoring diags.
     */
    @Test
    public void testCollectAndRestoreDiags() throws DbException, DiagnosticsException {
        insertAzurePriceTable();

        Map<IdentityMatchingAttributes, Long> originalMap = testIdentityStore
                .fetchAllOidMappings();
        final DiagnosticsAppender appender = Mockito.mock(DiagnosticsAppender.class);
        testIdentityStore.collectDiags(appender);
        final ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(appender).appendString(captor.capture());

        PriceTableKeyIdentityStore newPriceTableKeyIdentityStore = new PriceTableKeyIdentityStore(dsl,
                new IdentityProvider(0));
        newPriceTableKeyIdentityStore.restoreDiags(captor.getAllValues());

        Map<IdentityMatchingAttributes, Long> newMap = newPriceTableKeyIdentityStore.fetchAllOidMappings();
        assertThat(originalMap, equalTo(newMap));
    }

    private void insertAzurePriceTable() {
        PriceTableKey priceTableKey = mockPriceTableKey(azureServiceProviderOid);
        final PriceTableMergeFactory mergeFactory = mock(PriceTableMergeFactory.class);
        //add new price Table.
        SQLPriceTableStore sqlPriceTableStore = new SQLPriceTableStore(clock, dsl, new PriceTableKeyIdentityStore(dsl,
                new IdentityProvider(0)), mergeFactory);

        sqlPriceTableStore.putProbePriceTables(ImmutableMap.of(priceTableKey,
                new PriceTables(PriceTable.getDefaultInstance(),
                        ReservedInstancePriceTable.getDefaultInstance(), 111L)));
        final PriceTable priceTable = mockPriceTable(2L);
        sqlPriceTableStore.putProbePriceTables(ImmutableMap.of(priceTableKey, new PriceTables(priceTable,
                ReservedInstancePriceTable.getDefaultInstance(), 222L)));

    }

    private PriceTableKey mockPriceTableKey(final Long serviceProviderId) {
        return PriceTableKey.newBuilder()
                .setServiceProviderId(serviceProviderId)
                .putProbeKeyMaterial("enrollmentId", "123")
                .putProbeKeyMaterial("offerId", "456")
                .build();
    }

    /**
     * Get the row count from the price Table.
     *
     * @return the row Count
     */
    private int getRowCount() {
        return dsl.fetchCount(dsl.select(Tables.PRICE_TABLE.PRICE_TABLE_DATA).from(Tables.PRICE_TABLE));
    }

    @Nonnull
    private static PriceTable mockPriceTable(final long key) {
        return PriceTable.newBuilder()
                .putOnDemandPriceByRegionId(key, OnDemandPriceTable.getDefaultInstance())
                .build();
    }

}
