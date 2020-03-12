package com.vmturbo.cost.component.pricing;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.web.bind.annotation.ExceptionHandler;

import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableKey;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstancePriceTable;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.identity.IdentityProvider;
import com.vmturbo.cost.component.identity.PriceTableKeyIdentityStore;
import com.vmturbo.cost.component.pricing.PriceTableMerge.PriceTableMergeFactory;
import com.vmturbo.cost.component.pricing.PriceTableStore.PriceTables;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.platform.sdk.common.PricingDTO.ReservedInstancePrice;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

/**
 * Unit tests for {@link SQLPriceTableStore}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=cost"})
public class SQLPriceTableStoreTest {

    private static final PriceTable MERGED_PRICE_TABLE = mockPriceTable(2L);

    private static final ReservedInstancePriceTable MERGED_RI_PRICE_TABLE = mockRiPriceTable(22L);

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    private Flyway flyway;

    private SQLPriceTableStore store;

    private DSLContext dsl;

    /**
     * The clock can't start at too small of a number because TIMESTAMP starts in 1970, but
     * epoch millis starts in 1969.
     */
    private MutableFixedClock clock = new MutableFixedClock(Instant.ofEpochMilli(1_000_000_000), ZoneId.systemDefault());

    /**
     * We use a common price table merge results for all tests.
     * To look at the price tables actually retrieved from the SQL table
     * during {@link SQLPriceTableStore#getMergedPriceTable()} use the
     * argument captor ({@link SQLPriceTableStoreTest#readTablesCaptor})
     * on {@link PriceTableMerge#merge(Collection)}.
     */
    private PriceTableMerge merge = mock(PriceTableMerge.class);

    @Captor
    private ArgumentCaptor<Set<PriceTable>> readTablesCaptor;

    @Captor
    private ArgumentCaptor<Set<ReservedInstancePriceTable>> readRiTablesCaptor;

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        flyway = dbConfig.flyway();
        dsl = dbConfig.dsl();
        flyway.clean();
        flyway.migrate();

        final PriceTableMergeFactory mergeFactory = mock(PriceTableMergeFactory.class);

        store = new SQLPriceTableStore(clock, dsl, new PriceTableKeyIdentityStore(dsl,
                new IdentityProvider(0)), mergeFactory);

        when(merge.merge(any())).thenReturn(MERGED_PRICE_TABLE);
        when(merge.mergeRi(any())).thenReturn(MERGED_RI_PRICE_TABLE);
        when(mergeFactory.newMerge()).thenReturn(merge);
    }

    @After
    public void teardown() {
        flyway.clean();
    }

    @Test
    public void testUpdateAndRetrievePrice() {
        final PriceTable priceTable = mockPriceTable(1L);
        final PriceTableKey priceTableKey = mockPriceTableKey(1L);
        store.putProbePriceTables(ImmutableMap.of(priceTableKey, new PriceTables(priceTable, null, 111L)));

        final PriceTable resultPriceTable = store.getMergedPriceTable();
        assertThat(resultPriceTable, is(MERGED_PRICE_TABLE));

        verify(merge).merge(readTablesCaptor.capture());
        final Set<PriceTable> readTables = readTablesCaptor.getValue();
        assertThat(readTables.size(), is(1));
        assertThat(readTables.contains(priceTable), is(true));
    }

    @Test
    public void testUpdateAndRetrieveRiPrice() {
        final ReservedInstancePriceTable riPriceTable = mockRiPriceTable(1L);
        final PriceTableKey priceTableKey = mockPriceTableKey(1L);
        store.putProbePriceTables(ImmutableMap.of(priceTableKey, new PriceTables(null, riPriceTable, 111L)));

        final ReservedInstancePriceTable resultRiPriceTable = store.getMergedRiPriceTable();
        assertThat(resultRiPriceTable, is(MERGED_RI_PRICE_TABLE));

        verify(merge).mergeRi(readRiTablesCaptor.capture());
        final Set<ReservedInstancePriceTable> readRiTables = readRiTablesCaptor.getValue();
        assertThat(readRiTables.size(), is(1));
        assertThat(readRiTables.contains(riPriceTable), is(true));
    }

    @Test
    public void testUpdateOverwritesPrevious() {
        final PriceTable fooPriceTable = mockPriceTable(1L);
        final PriceTable barPriceTable = mockPriceTable(2L);
        final PriceTableKey priceTableKey = mockPriceTableKey(1L);
        store.putProbePriceTables(ImmutableMap.of(priceTableKey, new PriceTables(fooPriceTable, null, 111L)));
        PriceTable resultPriceTable = store.getMergedPriceTable();
        assertThat(resultPriceTable, is(MERGED_PRICE_TABLE));
        verify(merge).merge(readTablesCaptor.capture());
        Set<PriceTable> readTables = readTablesCaptor.getValue();
        assertThat(readTables.size(), is(1));
        assertThat(readTables.contains(fooPriceTable), is(true));

        //update the same price_table_key.
        store.putProbePriceTables(ImmutableMap.of(priceTableKey, new PriceTables(barPriceTable, null, 222L)));
        resultPriceTable = store.getMergedPriceTable();
        assertThat(resultPriceTable, is(MERGED_PRICE_TABLE));
        verify(merge, times(2)).merge(readTablesCaptor.capture());
        readTables = readTablesCaptor.getValue();
        assertThat(readTables.size(), is(1));
        assertThat(readTables.contains(fooPriceTable), is(false));
        assertThat(readTables.contains(barPriceTable), is(true));
        store.putProbePriceTables(ImmutableMap.of(priceTableKey, new PriceTables(barPriceTable, null, 222L)));
    }

    @Test
    public void testUpdateAndAddRIPriceTable() {
        final ReservedInstancePriceTable fooPriceTable = mockRiPriceTable(1L);
        final ReservedInstancePriceTable barPriceTable = mockRiPriceTable(2L);
        final PriceTableKey priceTableKey = mockPriceTableKey(1L);
        final PriceTableKey priceTableKey2 = mockPriceTableKey(2L);
        store.putProbePriceTables(ImmutableMap.of(priceTableKey, new PriceTables(null, fooPriceTable, 111L)));
        store.putProbePriceTables(ImmutableMap.of(priceTableKey2, new PriceTables(null, barPriceTable, 222L)));

        final ReservedInstancePriceTable resultPriceTable = store.getMergedRiPriceTable();
        assertThat(resultPriceTable, is(MERGED_RI_PRICE_TABLE));

        verify(merge).mergeRi(readRiTablesCaptor.capture());
        final Set<ReservedInstancePriceTable> readRiTables = readRiTablesCaptor.getValue();
        assertThat(readRiTables.size(), is(2));
        assertThat(readRiTables.contains(barPriceTable), is(true));
    }

    @Test
    public void testAddNewPriceTable() {
        final PriceTable fooPriceTable = mockPriceTable(1L);
        final PriceTable barPriceTable = mockPriceTable(2L);
        final PriceTableKey fooPriceTableKey = mockPriceTableKey(1L);
        final PriceTableKey barPriceTableKey = mockPriceTableKey(2L);
        store.putProbePriceTables(ImmutableMap.of(fooPriceTableKey, new PriceTables(fooPriceTable, null, 111L)));
        // IMPORTANT - Note that the probe type of the second update is different.
        // This is different from the update test, where the probe type of the second
        // update is the same. We still expect the "foo" price table to be removed.
        store.putProbePriceTables(ImmutableMap.of(barPriceTableKey, new PriceTables(barPriceTable, null, 222L)));

        final PriceTable resultPriceTable = store.getMergedPriceTable();
        assertThat(resultPriceTable, is(MERGED_PRICE_TABLE));
        assertThat(dsl.fetchCount(dsl.select(Tables.PRICE_TABLE.PRICE_TABLE_DATA).from(Tables.PRICE_TABLE)),
                is(2));

        // We should still only be merging one price table, because we should have deleted
        // the "foo" price table when processing the bar update.
        verify(merge).merge(readTablesCaptor.capture());
        final Set<PriceTable> readTables = readTablesCaptor.getValue();
        assertThat(readTables.size(), is(2));
        assertThat(readTables.contains(fooPriceTable), is(true));
    }

    @Test
    public void testUpdateRiRemovesOld() {
        final ReservedInstancePriceTable fooPriceTable = mockRiPriceTable(1L);
        final ReservedInstancePriceTable barPriceTable = mockRiPriceTable(2L);
        final PriceTableKey priceTableKey = mockPriceTableKey(1L);
        store.putProbePriceTables(ImmutableMap.of(priceTableKey, new PriceTables(null, fooPriceTable, 111L)));
        store.putProbePriceTables(ImmutableMap.of(priceTableKey, new PriceTables(null, barPriceTable, 222L)));

        final ReservedInstancePriceTable resultPriceTable = store.getMergedRiPriceTable();
        assertThat(resultPriceTable, is(MERGED_RI_PRICE_TABLE));

        // We should still only be merging one price table, because we should have deleted
        // the "foo" price table when processing the bar update.
        verify(merge).mergeRi(readRiTablesCaptor.capture());
        final Set<ReservedInstancePriceTable> readTables = readRiTablesCaptor.getValue();
        assertThat(readTables.size(), is(1));
        assertThat(readTables.contains(barPriceTable), is(true));
        assertThat(readTables.contains(fooPriceTable), is(false));

    }

    @Test
    public void testRetrieveEmpty() {
        final PriceTable resultPriceTable = store.getMergedPriceTable();
        // The merge always outputs the same thing.
        assertThat(resultPriceTable, is(MERGED_PRICE_TABLE));

        verify(merge).merge(readTablesCaptor.capture());
        final Set<PriceTable> readTables = readTablesCaptor.getValue();
        assertThat(readTables.isEmpty(), is(true));
    }

    /**
     * Test checksum by PriceTable key.
     */
    @Test
    public void updateAndGetCheckSumById() {
        final PriceTable fooPriceTable = mockPriceTable(1L);
        final PriceTable barPriceTable = mockPriceTable(2L);
        final PriceTableKey fooPriceTableKey = mockPriceTableKey(1L);
        final PriceTableKey barPriceTableKey = mockPriceTableKey(2L);
        store.putProbePriceTables(ImmutableMap.of(fooPriceTableKey, new PriceTables(fooPriceTable, null, 111L)));
        store.putProbePriceTables(ImmutableMap.of(barPriceTableKey, new PriceTables(barPriceTable, null, 222L)));
        Map<PriceTableKey, Long> map = store.getChecksumByPriceTableKeys(Collections.singletonList(mockPriceTableKey(1L)));
        assertThat(map.containsKey(fooPriceTableKey), is(true));
        assertThat(map.get(fooPriceTableKey), is(111L));
        assertThat(map.size(), is(1));
    }

    /**
     * Test getting all checksums.
     */
    @Test
    public void testGetAllCheckSums() {
        final PriceTable fooPriceTable = mockPriceTable(1L);
        final PriceTable barPriceTable = mockPriceTable(2L);
        final PriceTableKey fooPriceTableKey = mockPriceTableKey(1L);
        final PriceTableKey barPriceTableKey = mockPriceTableKey(2L);
        store.putProbePriceTables(ImmutableMap.of(fooPriceTableKey, new PriceTables(fooPriceTable, null, 111L)));
        store.putProbePriceTables(ImmutableMap.of(barPriceTableKey, new PriceTables(barPriceTable, null, 222L)));
        Map<PriceTableKey, Long> priceTableKeyLongMap = store.getChecksumByPriceTableKeys(Collections.emptyList());
        assertThat(priceTableKeyLongMap.size(), is(2));
    }

    /**
     * Test to check IdentityStoreException is thrown when trying to upload PriceTable.
     *
     * @throws IdentityStoreException intended exception.
     */
    @ExceptionHandler(IdentityStoreException.class)
    @Test
    public void testExceptionHandlingWhenUploadingPriceTable() throws IdentityStoreException {
        final PriceTable fooPriceTable = mockPriceTable(1L);
        final PriceTableKey fooPriceTableKey = mockPriceTableKey(1L);
        PriceTableKeyIdentityStore priceTableKeyIdentityStore = mock(PriceTableKeyIdentityStore.class);
        PriceTableMergeFactory mergeFactory = mock(PriceTableMergeFactory.class);
        store = new SQLPriceTableStore(clock, dsl, priceTableKeyIdentityStore, mergeFactory);
        when(priceTableKeyIdentityStore.fetchOrAssignOid(any()))
                .thenThrow(new IdentityStoreException("Intended exception."));
        store.putProbePriceTables(ImmutableMap.of(fooPriceTableKey, new PriceTables(fooPriceTable,
                null, 111L)));
    }

    @Nonnull
    private static PriceTable mockPriceTable(final long key) {
        return PriceTable.newBuilder()
                .putOnDemandPriceByRegionId(key, OnDemandPriceTable.getDefaultInstance())
                .build();
    }

    @Nonnull
    private static PriceTableKey mockPriceTableKey(final long key) {
        Map<String, String> probeKeyMaterial = new HashMap<>();
        probeKeyMaterial.put("enrollmentId", String.valueOf(key).concat("eId"));
        probeKeyMaterial.put("offerid", String.valueOf(key).concat("oId"));
        return PriceTableKey.newBuilder()
                .putAllProbeKeyMaterial(probeKeyMaterial)
                .setServiceProviderId(key)
                .build();
    }

    @Nonnull
    private static ReservedInstancePriceTable mockRiPriceTable(final long key) {
        return ReservedInstancePriceTable.newBuilder()
                .putRiPricesBySpecId(key, ReservedInstancePrice.getDefaultInstance())
                .build();
    }

}
