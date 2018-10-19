package com.vmturbo.cost.component.pricing;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Set;

import javax.annotation.Nonnull;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
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

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.ProbePriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstancePriceTable;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.cost.component.pricing.PriceTableMerge.PriceTableMergeFactory;
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

        store = new SQLPriceTableStore(clock, dsl, mergeFactory);

        when(merge.merge(any())).thenReturn(MERGED_PRICE_TABLE);
        when(merge.mergeRi(any())).thenReturn(MERGED_RI_PRICE_TABLE);
        when(mergeFactory.newMerge()).thenReturn(merge);
    }

    @Test
    public void testUpdateAndRetrievePrice() {
        final PriceTable priceTable = mockPriceTable(1L);
        final ProbePriceTable probePriceTable = ProbePriceTable.newBuilder()
                .setPriceTable(priceTable)
                .build();
        store.putProbePriceTables(ImmutableMap.of("foo", probePriceTable));

        final PriceTable resultPriceTable = store.getMergedPriceTable();
        assertThat(resultPriceTable, is(MERGED_PRICE_TABLE));

        verify(merge).merge(readTablesCaptor.capture());
        final Set<PriceTable> readTables = readTablesCaptor.getValue();
        assertThat(readTables.size(), is(1));
        assertThat(readTables.iterator().next(), is(priceTable));
    }

    @Test
    public void testUpdateAndRetrieveRiPrice() {
        final ReservedInstancePriceTable riPriceTable = mockRiPriceTable(1L);
        final ProbePriceTable probePriceTable = ProbePriceTable.newBuilder()
                .setRiPriceTable(riPriceTable)
                .build();
        store.putProbePriceTables(ImmutableMap.of("foo", probePriceTable));

        final ReservedInstancePriceTable resultRiPriceTable = store.getMergedRiPriceTable();
        assertThat(resultRiPriceTable, is(MERGED_RI_PRICE_TABLE));

        verify(merge).mergeRi(readRiTablesCaptor.capture());
        final Set<ReservedInstancePriceTable> readRiTables = readRiTablesCaptor.getValue();
        assertThat(readRiTables.size(), is(1));
        assertThat(readRiTables.iterator().next(), is(riPriceTable));
    }

    @Test
    public void testUpdateOverwritesPrevious() {
        final PriceTable fooPriceTable = mockPriceTable(1L);
        final PriceTable barPriceTable = mockPriceTable(2L);
        store.putProbePriceTables(ImmutableMap.of("foo", ProbePriceTable.newBuilder()
                .setPriceTable(fooPriceTable)
                .build()));
        store.putProbePriceTables(ImmutableMap.of("foo", ProbePriceTable.newBuilder()
                .setPriceTable(barPriceTable)
                .build()));

        final PriceTable resultPriceTable = store.getMergedPriceTable();
        assertThat(resultPriceTable, is(MERGED_PRICE_TABLE));

        verify(merge).merge(readTablesCaptor.capture());
        final Set<PriceTable> readTables = readTablesCaptor.getValue();
        assertThat(readTables.size(), is(1));
        assertThat(readTables.iterator().next(), is(barPriceTable));
    }

    @Test
    public void testUpdateRiOverwritesPrevious() {
        final ReservedInstancePriceTable fooPriceTable = mockRiPriceTable(1L);
        final ReservedInstancePriceTable barPriceTable = mockRiPriceTable(2L);
        store.putProbePriceTables(ImmutableMap.of("foo", ProbePriceTable.newBuilder()
                .setRiPriceTable(fooPriceTable)
                .build()));
        store.putProbePriceTables(ImmutableMap.of("foo", ProbePriceTable.newBuilder()
                .setRiPriceTable(barPriceTable)
                .build()));

        final ReservedInstancePriceTable resultPriceTable = store.getMergedRiPriceTable();
        assertThat(resultPriceTable, is(MERGED_RI_PRICE_TABLE));

        verify(merge).mergeRi(readRiTablesCaptor.capture());
        final Set<ReservedInstancePriceTable> readRiTables = readRiTablesCaptor.getValue();
        assertThat(readRiTables.size(), is(1));
        assertThat(readRiTables.iterator().next(), is(barPriceTable));
    }

    @Test
    public void testUpdateRemovesOld() {
        final PriceTable fooPriceTable = mockPriceTable(1L);
        final PriceTable barPriceTable = mockPriceTable(2L);
        store.putProbePriceTables(ImmutableMap.of("foo", ProbePriceTable.newBuilder()
                .setPriceTable(fooPriceTable)
                .build()));
        // IMPORTANT - Note that the probe type of the second update is different.
        // This is different from the update test, where the probe type of the second
        // update is the same. We still expect the "foo" price table to be removed.
        store.putProbePriceTables(ImmutableMap.of("bar", ProbePriceTable.newBuilder()
                .setPriceTable(barPriceTable)
                .build()));

        final PriceTable resultPriceTable = store.getMergedPriceTable();
        assertThat(resultPriceTable, is(MERGED_PRICE_TABLE));

        // We should still only be merging one price table, because we should have deleted
        // the "foo" price table when processing the bar update.
        verify(merge).merge(readTablesCaptor.capture());
        final Set<PriceTable> readTables = readTablesCaptor.getValue();
        assertThat(readTables.size(), is(1));
        assertThat(readTables.iterator().next(), is(barPriceTable));
    }

    @Test
    public void testUpdateRiRemovesOld() {
        final ReservedInstancePriceTable fooPriceTable = mockRiPriceTable(1L);
        final ReservedInstancePriceTable barPriceTable = mockRiPriceTable(2L);
        store.putProbePriceTables(ImmutableMap.of("foo", ProbePriceTable.newBuilder()
                .setRiPriceTable(fooPriceTable)
                .build()));
        // IMPORTANT - Note that the probe type of the second update is different.
        // This is different from the update test, where the probe type of the second
        // update is the same. We still expect the "foo" price table to be removed.
        store.putProbePriceTables(ImmutableMap.of("bar", ProbePriceTable.newBuilder()
                .setRiPriceTable(barPriceTable)
                .build()));

        final ReservedInstancePriceTable resultPriceTable = store.getMergedRiPriceTable();
        assertThat(resultPriceTable, is(MERGED_RI_PRICE_TABLE));

        // We should still only be merging one price table, because we should have deleted
        // the "foo" price table when processing the bar update.
        verify(merge).mergeRi(readRiTablesCaptor.capture());
        final Set<ReservedInstancePriceTable> readTables = readRiTablesCaptor.getValue();
        assertThat(readTables.size(), is(1));
        assertThat(readTables.iterator().next(), is(barPriceTable));
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

    @Nonnull
    private static PriceTable mockPriceTable(final long key) {
        return PriceTable.newBuilder()
                .putOnDemandPriceByRegionId(key, OnDemandPriceTable.getDefaultInstance())
                .build();
    }

    @Nonnull
    private static ReservedInstancePriceTable mockRiPriceTable(final long key) {
        return ReservedInstancePriceTable.newBuilder()
                .putRiPricesBySpecId(key, ReservedInstancePrice.getDefaultInstance())
                .build();
    }
}
