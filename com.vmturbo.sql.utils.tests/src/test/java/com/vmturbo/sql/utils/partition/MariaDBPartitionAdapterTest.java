package com.vmturbo.sql.utils.partition;

import static com.vmturbo.sql.utils.partition.PartitionMatchers.coversInstant;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;
import com.vmturbo.sql.utils.db.SqlUtils;
import com.vmturbo.sql.utils.db.Tables;
import com.vmturbo.sql.utils.jooq.JooqUtil;
import com.vmturbo.sql.utils.tests.SqlUtilsDbEndpointConfig;

/**
 * Live-DB tests of the {@link MariaDBPartitionAdapter} class.
 */
@RunWith(Parameterized.class)
public class MariaDBPartitionAdapterTest extends MultiDbTestBase {

    /**
     * Parameters for test executions.
     *
     * @return list of param lists
     */
    @Parameters
    public static Object[][] parameters() {
        return new Object[][]{
                // this test is speicfic to MariaDB
                MultiDbTestBase.DBENDPOINT_MARIADB_PARAMS
        };
    }

    private final DSLContext dsl;

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect               DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public MariaDBPartitionAdapterTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(SqlUtils.SQL_UTILS, configurableDbDialect, dialect, "sql_utils",
                SqlUtilsDbEndpointConfig.config::sqlUtilsEndpoint);
        dsl = super.getDslContext();
    }

    private MariaDBPartitionAdapter adapter = null;
    private String mappedSchemaName = null;

    /**
     * Complete setup for tests.
     */
    @Before
    public void before() {
        this.adapter = new MariaDBPartitionAdapter(dsl);
        this.mappedSchemaName = JooqUtil.getMappedSchemaName(SqlUtils.SQL_UTILS.getName(), dsl);
    }

    /**
     * Drop any partitions we may have created. The automatic table cleanup performed by our test
     * rule only truncates tables that started out empty; it does not pay attention to partitions.
     *
     * @throws PartitionProcessingException if there's an error
     */
    @After
    public void after() throws PartitionProcessingException {
        for (Entry<String, List<Partition<Instant>>> entry
                : adapter.getSchemaPartitions(mappedSchemaName).entrySet()) {
            List<Partition<Instant>> parts = entry.getValue();
            for (Partition<Instant> part : parts) {
                adapter.dropPartition(part);
            }
        }
    }

    /**
     * Make sure that we can create partitions correclty.
     *
     * @throws PartitionProcessingException if there's a problem
     */
    @Test
    public void testThatParitionCreationWorks() throws PartitionProcessingException {
        List<Partition<Instant>> tableParts = getTablePartitions();
        assertThat(tableParts, is(empty()));
        // create a new partition covering [10000, 20000). Make sure 15000 is not covered prior
        // to creating this partition but is afterward
        assertThat(tableParts, is(not(coversInstant(Instant.ofEpochMilli(15__000L)))));
        adapter.createPartition(mappedSchemaName, Tables.PART_TEST.getName(),
                Instant.ofEpochMilli(10_000L), Instant.ofEpochMilli(20000));
        tableParts = getTablePartitions();
        assertThat(tableParts, is(coversInstant(Instant.ofEpochMilli(15_000L))));
        // same thing for a partition covering [30000, 40000) and testing 35000
        // to creating this partition but is afterward
        assertThat(tableParts, is(not(coversInstant(Instant.ofEpochMilli(35_000L)))));
        adapter.createPartition(mappedSchemaName, Tables.PART_TEST.getName(),
                Instant.ofEpochMilli(30_000L), Instant.ofEpochMilli(40000));
        tableParts = getTablePartitions();
        assertThat(tableParts, is(coversInstant(Instant.ofEpochMilli(35_000L))));
        // now do some checks of edge-case instants. Note that we include some instants
        // that are outside our partition bounds as covered, becasue MariaDB does not support
        // explicit lower partition bounds. Any inter-partition gaps in our specified partition
        // bounds are fully covered by the following partition.
        assertThat(tableParts, coversInstant(Instant.ofEpochMilli(0L))); // implicitly covered
        assertThat(tableParts, coversInstant(Instant.ofEpochMilli(5_000L))); // implicitly covered
        assertThat(tableParts, coversInstant(Instant.ofEpochMilli(10_000L)));
        assertThat(tableParts, coversInstant(Instant.ofEpochMilli(19999L)));
        assertThat(tableParts, coversInstant(Instant.ofEpochMilli(20_000L))); // implicitly covered
        assertThat(tableParts, coversInstant(Instant.ofEpochMilli(29999L))); // implicitly covered
        assertThat(tableParts, coversInstant(Instant.ofEpochMilli(30_000L)));
        assertThat(tableParts, coversInstant(Instant.ofEpochMilli(39999L)));
        assertThat(tableParts, not(coversInstant(Instant.ofEpochMilli(40_000L))));
        assertThat(tableParts, not(coversInstant(Instant.ofEpochMilli(45_000L))));
    }

    /**
     * Make sure that we can drop partitions properly.
     *
     * @throws PartitionProcessingException if there's a problem
     */
    @Test
    public void testThatPartitionDroppingWorks() throws PartitionProcessingException {
        List<Partition<Instant>> tablePartsCopy = getTablePartitions();
        assertThat(tablePartsCopy, is(empty()));
        // create a couple of of partitions
        for (long upperBound = 10_000L; upperBound <= 100_000L; upperBound += 10_000L) {
            adapter.createPartition(mappedSchemaName, Tables.PART_TEST.getName(),
                    Instant.ofEpochMilli(upperBound - 10_000L), Instant.ofEpochMilli(upperBound));
        }
        List<Partition<Instant>> tableParts = getTablePartitions();
        assertThat(tableParts.size(), is(10));
        // get all our partitions; use a copy of the retrieved list so it won't be affected by
        // the drops we perform. This just makes the logic here a bit easier to follow (IMO).
        tablePartsCopy = new ArrayList<>(tableParts);
        // drop a few partitions and check that instants at edge caes are covered or not
        // correctly, taking into account MariaDB's implicit lower bounds.
        adapter.dropPartition(tablePartsCopy.get(2)); // [20000, 30000)
        adapter.dropPartition(tablePartsCopy.get(6)); // [60000, 70000)
        adapter.dropPartition(tablePartsCopy.get(7)); // [70000, 80000)
        adapter.dropPartition(tablePartsCopy.get(9)); // [90000, 100000] - final partition
        tableParts = getTablePartitions();
        assertThat(tableParts, coversInstant(Instant.ofEpochMilli(19999L)));
        assertThat(tableParts, coversInstant(Instant.ofEpochMilli(20_000L))); // ipmlicit
        assertThat(tableParts, coversInstant(Instant.ofEpochMilli(29999L))); // ipmlicit
        assertThat(tableParts, coversInstant(Instant.ofEpochMilli(59999L)));
        assertThat(tableParts, coversInstant(Instant.ofEpochMilli(60_000L))); // ipmlicit
        assertThat(tableParts, coversInstant(Instant.ofEpochMilli(69999L))); // ipmlicit
        assertThat(tableParts, coversInstant(Instant.ofEpochMilli(70_000L))); // ipmlicit
        assertThat(tableParts, coversInstant(Instant.ofEpochMilli(79999L))); // ipmlicit
        assertThat(tableParts, coversInstant(Instant.ofEpochMilli(89999L)));
        assertThat(tableParts, not(coversInstant(Instant.ofEpochMilli(90_000L))));
        assertThat(tableParts, not(coversInstant(Instant.ofEpochMilli(99999L))));
        assertThat(tableParts, not(coversInstant(Instant.ofEpochMilli(100_000L))));
    }

    private List<Partition<Instant>> getTablePartitions() throws PartitionProcessingException {
        return adapter.getSchemaPartitions(mappedSchemaName, Tables.PART_TEST.getName());
    }
}