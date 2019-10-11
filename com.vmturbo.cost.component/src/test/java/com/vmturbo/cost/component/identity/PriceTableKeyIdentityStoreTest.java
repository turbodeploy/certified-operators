package com.vmturbo.cost.component.identity;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Type;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.ecwid.consul.json.GsonFactory;
import com.google.common.collect.ImmutableMap;
import com.google.gson.reflect.TypeToken;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.vmturbo.common.protobuf.cost.Pricing.PriceTableKey;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstancePriceTable;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.pricing.PriceTableMerge.PriceTableMergeFactory;
import com.vmturbo.cost.component.pricing.PriceTableStore.PriceTables;
import com.vmturbo.cost.component.pricing.SQLPriceTableStore;
import com.vmturbo.identity.attributes.IdentityMatchingAttribute;
import com.vmturbo.identity.attributes.IdentityMatchingAttributes;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.platform.sdk.common.PricingDTO.ReservedInstancePrice;
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
public class PriceTableKeyIdentityStoreTest {

    private final Type type = new TypeToken<Map<String, String>>() {
    }.getType();
    private MutableFixedClock clock = new MutableFixedClock(Instant.ofEpochMilli(1_000_000_000), ZoneId.systemDefault());

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    private Flyway flyway;
    private DSLContext dsl;
    private PriceTableKeyIdentityStore testIdentityStore;

    /**
     * Setup for this test class.
     */
    @Before
    public void setup() {

        IdentityGenerator.initPrefix(0);
        flyway = dbConfig.flyway();
        dsl = dbConfig.dsl();
        testIdentityStore = new PriceTableKeyIdentityStore(dsl,
                new IdentityProvider(0));
        // Clean the database and bring it up to the production configuration before running test
        flyway.clean();
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
     * Test Empty set of oids.
     *
     * @throws IdentityStoreException An IdentityStoreException.
     */
    @Test
    public void testEmptySetOfOids() throws IdentityStoreException {
        Map<IdentityMatchingAttributes, Long> test = testIdentityStore.fetchAllOidMappings();
        assertThat(test.isEmpty(), is(true));
    }

    /**
     * Testing Oid Identifiers.
     *
     * @throws IdentityStoreException An IdentityStore Exception.
     */
    @Test
    public void testOidWithIdentifiers() throws IdentityStoreException {
        PriceTableKey priceTableKey = mockPriceTable("aws");

        testIdentityStore.fetchOrAssignOid(priceTableKey);
        Map<IdentityMatchingAttributes, Long> matchingAttributesLongMap = testIdentityStore.fetchAllOidMappings();
        assertThat(matchingAttributesLongMap.size(), is(1));

        Set<IdentityMatchingAttributes> identityMatchingAttributes = matchingAttributesLongMap.keySet();

        IdentityMatchingAttribute identityMatchingAttribute = identityMatchingAttributes.iterator()
                .next().getMatchingAttribute(PriceTableKeyIdentityStore.PRICE_TABLE_KEY_IDENTIFIERS);
        Map<String, String> identityMatchingAttributeMap = GsonFactory.getGson().fromJson(identityMatchingAttribute.getAttributeValue(), type);

        assertThat(identityMatchingAttributeMap.get("enrollmentId"), is("123"));
        assertThat(identityMatchingAttributeMap.get("offerId"), is("456"));
        assertThat(identityMatchingAttributeMap.get("probe_type"), is("aws"));
    }


    /**
     * Testing oids with the same identifier.
     *
     * @throws IdentityStoreException An IdentityStore Exception.
     */
    @Test
    public void testOidWithSameIdentifier() throws IdentityStoreException {
        PriceTableKey priceTableKey = mockPriceTable("aws");
        testIdentityStore.fetchOrAssignOid(priceTableKey);
        testIdentityStore.fetchOrAssignOid(priceTableKey);
        Map<IdentityMatchingAttributes, Long> matchingAttributesLongMap = testIdentityStore.fetchAllOidMappings();
        assertThat(matchingAttributesLongMap.size(), is(1));
    }


    /**
     * Testing oids with unique identifiers.
     *
     * @throws IdentityStoreException An IdentityStore Exception.
     */
    @Test
    public void testOidWithUniqueIdentifiers() throws IdentityStoreException {
        PriceTableKey priceTableKey = mockPriceTable("azure");
        testIdentityStore.fetchOrAssignOid(priceTableKey);

        priceTableKey = priceTableKey.newBuilderForType().setProbeType("aws").build();
        testIdentityStore.fetchOrAssignOid(priceTableKey);

        Map<IdentityMatchingAttributes, Long> matchingAttributesLongMap = testIdentityStore.fetchAllOidMappings();
        assertThat(matchingAttributesLongMap.size(), is(2));
    }


    /**
     * Testing deletion of price table keys.
     *
     * @throws IdentityStoreException An IdentityStore Exception.
     */
    @Test
    public void testCascadeDeleteOnPriceTableKeyOidDelete() throws IdentityStoreException {
        PriceTableKey priceTableKey = mockPriceTable("azure");
        final long oid = testIdentityStore.fetchOrAssignOid(priceTableKey);

        final PriceTableMergeFactory mergeFactory = mock(PriceTableMergeFactory.class);

        SQLPriceTableStore sqlPriceTableStore = new SQLPriceTableStore(clock, dsl, new PriceTableKeyIdentityStore(dsl,
                new IdentityProvider(0)), mergeFactory);

        final ReservedInstancePriceTable fooPriceTable = ReservedInstancePriceTable.newBuilder()
                .putRiPricesBySpecId(1L, ReservedInstancePrice.getDefaultInstance())
                .build();
        sqlPriceTableStore.putProbePriceTables(ImmutableMap.of(priceTableKey, new PriceTables(null, fooPriceTable, 111L)));


        assertThat(getRowCount(), is(1));

        assertThat(testIdentityStore.fetchAllOidMappings().size(), is(1));

        testIdentityStore.removeOidMappings(Collections.singleton(oid));

        //verify price table oid is empty too.
        assertThat(testIdentityStore.fetchAllOidMappings().isEmpty(), is(true));
        assertThat(getRowCount(), is(0));

    }

    private PriceTableKey mockPriceTable(final String probeType) {
        return PriceTableKey.newBuilder().setProbeType(probeType)
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
}
