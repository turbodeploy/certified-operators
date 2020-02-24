package com.vmturbo.cost.component.pricing;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Sets;

import org.apache.commons.collections4.CollectionUtils;
import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.vmturbo.common.protobuf.cost.Pricing.BusinessAccountPriceTableKey;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableKey;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.identity.IdentityProvider;
import com.vmturbo.cost.component.identity.PriceTableKeyIdentityStore;
import com.vmturbo.identity.attributes.IdentityMatchingAttributes;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.sql.utils.DbException;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

/**
 * Context Configuration for this test class.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        loader = AnnotationConfigContextLoader.class,
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=cost"})
public class BusinessAccountPriceTableKeyStoreTest {

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    private Flyway flyway;
    private DSLContext dsl;
    private BusinessAccountPriceTableKeyStore businessAccountPriceTableKeyStore;
    private PriceTableKeyIdentityStore priceTableKeyIdentityStore;

    /**
     * Set up identity generator.
     */
    @BeforeClass
    public static void setupClass() {
        IdentityGenerator.initPrefix(0L);
    }

    /**
     * Setup for this test class.
     */
    @Before
    public void setup() {
        flyway = dbConfig.flyway();
        dsl = dbConfig.dsl();
        priceTableKeyIdentityStore = new PriceTableKeyIdentityStore(dsl,
                new IdentityProvider(0));
        businessAccountPriceTableKeyStore = new BusinessAccountPriceTableKeyStore(dsl, priceTableKeyIdentityStore);

        // Clean the database and bring it up to the production configuration before running test.
        // flyway.clean();
        flyway.migrate();
    }

    /**
     * Teardown after test finishes.
     */
    @After
    public void teardown() {
        flyway.clean();
    }

    /**
     * Test fetch basic functionality.
     */
    @Test
    public void testEmptySetOfOids() {
        Map<Long, Long> priceTableKeyMap = businessAccountPriceTableKeyStore
                .fetchPriceTableKeyOidsByBusinessAccount(Collections.emptySet());
        assertThat(priceTableKeyMap.isEmpty(), is(true));
    }

    /**
     * Test fetching using a BA OID present in DB.
     */
    @Test
    public void testFetchByValidBusinessAccountOID() {
        PriceTableKey priceTableKey = mockPriceTable("aws");
        BusinessAccountPriceTableKey businessAccountPriceTableKey = businessPriceTableKeyGenerator(priceTableKey);
        businessAccountPriceTableKeyStore.uploadBusinessAccount(businessAccountPriceTableKey);
        Map<Long, Long> priceTableKeyMap = businessAccountPriceTableKeyStore
                .fetchPriceTableKeyOidsByBusinessAccount(Collections.singleton(123L));
        assertThat(priceTableKeyMap.size(), is(1));
        assertThat(priceTableKeyMap.keySet().iterator().next(), is(123L));
    }

    /**
     * Test fetching using a BA OID which is not present in DB. Should return empty result set.
     */
    @Test
    public void testFetchByInvalidBusinessAccountOID() {
        PriceTableKey priceTableKey = mockPriceTable("aws");
        BusinessAccountPriceTableKey businessAccountPriceTableKey = businessPriceTableKeyGenerator(priceTableKey);
        businessAccountPriceTableKeyStore.uploadBusinessAccount(businessAccountPriceTableKey);
        Map<Long, Long> priceTableKeyMap = businessAccountPriceTableKeyStore
                .fetchPriceTableKeyOidsByBusinessAccount(Collections.singleton(0L));
        assertThat(priceTableKeyMap.isEmpty(), is(true));
    }

    /**
     * Test fetching without specifying any oids in the argument.
     *
     * @throws IdentityStoreException if exception from priceTableKeyIdentityStore.
     */
    @Test
    public void testAccurateBAToPriceTableMapping() throws IdentityStoreException {
        PriceTableKey priceTableKey = mockPriceTable("aws");
        BusinessAccountPriceTableKey businessAccountPriceTableKey = businessPriceTableKeyGenerator(priceTableKey);
        businessAccountPriceTableKeyStore.uploadBusinessAccount(businessAccountPriceTableKey);
        Map<Long, Long> priceTableKeyMap = businessAccountPriceTableKeyStore.fetchPriceTableKeyOidsByBusinessAccount(Collections.emptySet());
        assertThat(priceTableKeyMap.size(), is(1));

        assertThat(priceTableKeyMap.entrySet().size(), is(1));

        Entry<Long, Long> longPriceTableKeyEntry = priceTableKeyMap.entrySet().iterator().next();

        assertThat(longPriceTableKeyEntry.getValue(),
                is(priceTableKeyIdentityStore.fetchOrAssignOid(priceTableKey)));
        assertThat(longPriceTableKeyEntry.getKey(), is(123L));
    }

    /**
     * Test overwriting an already created BA oid to price table key mapping.
     *
     * @throws IdentityStoreException if exception from priceTableKeyIdentityStore.
     */
    @Test
    public void testOverwriteBAToPriceTableMapping() throws IdentityStoreException {
        PriceTableKey fooPriceTableKey = mockPriceTable("aws");
        PriceTableKey barPriceTableKey = mockPriceTable("azure");

        //first insert
        BusinessAccountPriceTableKey businessAccountPriceTableKey = businessPriceTableKeyGenerator(fooPriceTableKey);
        businessAccountPriceTableKeyStore.uploadBusinessAccount(businessAccountPriceTableKey);
        Map<Long, Long> priceTableKeyMap = businessAccountPriceTableKeyStore.fetchPriceTableKeyOidsByBusinessAccount(Collections.emptySet());
        assertThat(priceTableKeyMap.size(), is(1));

        //update the same business account with new priceTableKey.
        businessAccountPriceTableKey = businessPriceTableKeyGenerator(barPriceTableKey);
        businessAccountPriceTableKeyStore.uploadBusinessAccount(businessAccountPriceTableKey);
        priceTableKeyMap = businessAccountPriceTableKeyStore.fetchPriceTableKeyOidsByBusinessAccount(Collections.emptySet());
        assertThat(priceTableKeyMap.entrySet().size(), is(1));

        Entry<Long, Long> longPriceTableKeyEntry = priceTableKeyMap.entrySet().iterator().next();
        assertThat(longPriceTableKeyEntry.getValue(),
                is(priceTableKeyIdentityStore.fetchOrAssignOid(barPriceTableKey)));
        assertThat(longPriceTableKeyEntry.getValue(),
                not(priceTableKeyIdentityStore.fetchOrAssignOid(fooPriceTableKey)));
        assertThat(longPriceTableKeyEntry.getKey(), is(123L));
    }


    /**
     * Test diags collection and restoring functionality.
     *
     * @throws DiagnosticsException   when fails to either restore or collect diags.
     * @throws IdentityStoreException if exception from priceTableKeyIdentityStore.
     */
    @Test
    public void testCollectAndRestoreDiagsFunctionality() throws DiagnosticsException, IdentityStoreException {
        PriceTableKey fooPriceTableKey = mockPriceTable("aws");
        PriceTableKey barPriceTableKey = mockPriceTable("azure");

        //first insert
        BusinessAccountPriceTableKey businessAccountPriceTableKey = BusinessAccountPriceTableKey.newBuilder()
                .putBusinessAccountPriceTableKey(123L, fooPriceTableKey)
                .putBusinessAccountPriceTableKey(456L, barPriceTableKey)
                .build();
        businessAccountPriceTableKeyStore.uploadBusinessAccount(businessAccountPriceTableKey);
        final DiagnosticsAppender appender = Mockito.mock(DiagnosticsAppender.class);
        businessAccountPriceTableKeyStore.collectDiags(appender);
        final ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(appender, Mockito.atLeastOnce()).appendString(captor.capture());

        dsl.truncate(Tables.BUSINESS_ACCOUNT_PRICE_TABLE_KEY).execute();
        businessAccountPriceTableKeyStore.restoreDiags(captor.getAllValues());
        Map<Long, Long> priceTableKeyMap = businessAccountPriceTableKeyStore.fetchPriceTableKeyOidsByBusinessAccount(Collections.emptySet());

        assertThat(priceTableKeyMap.size(), is(2));
        HashSet<Long> expectedBA_OIDs = Sets.newHashSet(123L, 456L);
        HashSet<Long> expectedPriceTableKeyOids = Sets.newHashSet(priceTableKeyIdentityStore.fetchOrAssignOid(fooPriceTableKey),
                priceTableKeyIdentityStore.fetchOrAssignOid(barPriceTableKey));

        assertThat(CollectionUtils.isEqualCollection(priceTableKeyMap.keySet(), expectedBA_OIDs), is(true));
        assertThat(CollectionUtils.isEqualCollection(priceTableKeyMap.values(), expectedPriceTableKeyOids), is(true));
    }

    /**
     * Tests deleting valid business account oid and its priceTable oid.
     *
     * @throws DbException if fetching fails.
     */
    @Test
    public void testDeleteValidBAOid() throws DbException {
        PriceTableKey priceTableKey = mockPriceTable("aws");
        BusinessAccountPriceTableKey businessAccountPriceTableKey = businessPriceTableKeyGenerator(priceTableKey);
        businessAccountPriceTableKeyStore.uploadBusinessAccount(businessAccountPriceTableKey);

        Map<Long, Long> resultBusinessAccountToPriceTableKey = businessAccountPriceTableKeyStore
                .fetchPriceTableKeyOidsByBusinessAccount(Collections.emptySet());
        assertThat(resultBusinessAccountToPriceTableKey.size(), is(1));
        Map<IdentityMatchingAttributes, Long> listOfPriceTableKey = priceTableKeyIdentityStore.fetchAllOidMappings();
        assertThat(listOfPriceTableKey.size(), is(1));
        Entry<Long, Long> businessAccountToPriceTableKeyEntry = resultBusinessAccountToPriceTableKey.entrySet().iterator().next();
        Entry<IdentityMatchingAttributes, Long> priceTableKeyEntry = listOfPriceTableKey.entrySet().iterator().next();
        assertThat(priceTableKeyEntry.getValue(), is(businessAccountToPriceTableKeyEntry.getValue()));


        businessAccountPriceTableKeyStore.removeBusinessAccountAndPriceTableKeyOid(Collections
                .singleton(businessAccountToPriceTableKeyEntry.getKey()));
        assertThat(priceTableKeyIdentityStore.fetchAllOidMappings().size(), is(0));
        assertThat(businessAccountPriceTableKeyStore
                .fetchPriceTableKeyOidsByBusinessAccount(Collections.emptySet()).size(), is(0));
    }

    /**
     * Test to check if invalid BA oids are handled without any exceptions.
     *
     * @throws DbException if fetching or removal fails.
     */
    @Test
    public void testDeleteinvalidBAOid() throws DbException {
        businessAccountPriceTableKeyStore.removeBusinessAccountAndPriceTableKeyOid(Collections.singleton(11L));
    }

    /**
     * Tests that we can not delete a row which contains a valid priceTable key. Tests FK constrain.
     *
     * @throws DbException if fetching or removal fails.
     */
    @Test(expected = DataIntegrityViolationException.class)
    public void testDeleteBAOidWithNonExistentPriceTablekeyOid() throws DbException {
        PriceTableKey priceTableKey = mockPriceTable("aws");
        BusinessAccountPriceTableKey businessAccountPriceTableKey = businessPriceTableKeyGenerator(priceTableKey);
        businessAccountPriceTableKeyStore.uploadBusinessAccount(businessAccountPriceTableKey);
        Map<IdentityMatchingAttributes, Long> priceTableKeyOids = priceTableKeyIdentityStore.fetchAllOidMappings();
        assertThat(priceTableKeyOids.size(), is(1));
        Long priceTableOid = priceTableKeyOids.entrySet().iterator().next().getValue();
        priceTableKeyIdentityStore.removeOidMappings(Collections.singleton(priceTableOid));
        fail("Should have reported DataIntegrityViolationException.");
    }

    /**
     * Tests that priceTableKeyOid is not deleted if it is getting used by another businessAccount OID
     * which is not deleted.
     *
     * @throws DbException if fetching or removal fails.
     */
    @Test
    public void testDeleteBAWithReusedPriceTableKey() throws DbException {
        PriceTableKey fooPriceTableKey = mockPriceTable("aws");
        BusinessAccountPriceTableKey businessAccountPriceTableKey = businessPriceTableKeyGenerator(fooPriceTableKey);
        businessAccountPriceTableKey = businessAccountPriceTableKey.toBuilder().putBusinessAccountPriceTableKey(456L, fooPriceTableKey).build();
        businessAccountPriceTableKeyStore.uploadBusinessAccount(businessAccountPriceTableKey);
        Map<Long, Long> businessAccountPriceTableRows = businessAccountPriceTableKeyStore.fetchPriceTableKeyOidsByBusinessAccount(Collections.emptySet());
        final Long priceTableKeyOid = businessAccountPriceTableRows.entrySet().iterator().next().getValue();
        assertThat(businessAccountPriceTableRows.size(), is(2));

        businessAccountPriceTableKeyStore.removeBusinessAccountAndPriceTableKeyOid(Collections.singleton(456L));
        Map<Long, Long> listAfterDelete = businessAccountPriceTableKeyStore.fetchPriceTableKeyOidsByBusinessAccount(Collections.emptySet());
        assertThat(listAfterDelete.size(), is(1));
        Map<IdentityMatchingAttributes, Long> priceTableKeyOids = priceTableKeyIdentityStore.fetchAllOidMappings();
        assertThat(priceTableKeyOids.size(), is(1));
        assertThat(priceTableKeyOids.values().contains(priceTableKeyOid), is(true));
    }

    private PriceTableKey mockPriceTable(final String pricingGroup) {
        return PriceTableKey.newBuilder()
                .setPricingGroup(pricingGroup)
                .putProbeKeyMaterial("enrollmentId", "123")
                .putProbeKeyMaterial("offerId", "456")
                .build();
    }

    private BusinessAccountPriceTableKey businessPriceTableKeyGenerator(PriceTableKey fooPriceTableKey) {
        return BusinessAccountPriceTableKey.newBuilder()
                .putBusinessAccountPriceTableKey(123L, fooPriceTableKey).build();
    }
}
