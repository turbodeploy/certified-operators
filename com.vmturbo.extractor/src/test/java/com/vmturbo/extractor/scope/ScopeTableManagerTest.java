package com.vmturbo.extractor.scope;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.ResultQuery;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.extractor.ExtractorDbConfig;
import com.vmturbo.extractor.schema.ExtractorDbBaseConfig;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.DbEndpointTestRule;

/**
 * Test class for {@link ScopeTableManager}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ExtractorDbConfig.class, ExtractorDbBaseConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_CLASS)
@TestPropertySource(properties = {"enableReporting=true", "sqlDialect=POSTGRES"})
public class ScopeTableManagerTest {
    private DSLContext dsl;
    private DbEndpoint endpoint;
    private Connection connection;
    private ScopeTableManager scopeTableManager;

    private static final String TEST_DATA_CSV = "scope/scope.csv";

    @Autowired
    private ExtractorDbConfig dbConfig;

    /**
     * Manage the live DB endpoint we're using for our tests.
     */
    @Rule
    @ClassRule
    public static DbEndpointTestRule endpointRule = new DbEndpointTestRule("extractor");

    /**
     * Set up for tests.
     *
     * @throws UnsupportedDialectException if the endpoint is mis-configured
     * @throws SQLException                if there's a problem
     * @throws InterruptedException        if interrupted
     */
    @Before
    public void before() throws UnsupportedDialectException, SQLException, InterruptedException {
        endpoint = dbConfig.ingesterEndpoint();
        endpointRule.addEndpoints(endpoint);
        this.scopeTableManager = new ScopeTableManager(endpoint);
        this.dsl = endpoint.dslContext();
        this.connection = endpoint.datasource().getConnection();
    }

    /**
     * Test the logic for processing chunks of scope table for compression.
     *
     * @throws Exception any exception
     */
    @Test
    public void testExecute() throws Exception {
        // Create the hypertable
        scopeTableManager.createScopeHypertable(dsl);

        // Load test data from a CSV file into hypertable.
        loadDataIntoScopeTable(TEST_DATA_CSV);

        // Get the original row count
        final int initialRowCount = getRowCount();

        // Check that there are three chunks
        List<Record> chunks = getChunks();
        assertEquals(3, chunks.size());
        Record firstChunk = chunks.get(0);
        Timestamp rangeStart = firstChunk.get("range_start", Timestamp.class);
        Timestamp rangeEnd = firstChunk.get("range_end", Timestamp.class);

        // Keep a record of the records that need to be split.
        List<Record> recordsThatNeedSplit = getRecordsThatNeedSplit(rangeStart, rangeEnd);
        final int numOfRowsThatNeedSplit = recordsThatNeedSplit.size();

        scopeTableManager.execute();

        // Check that there are three chunks
        chunks = getChunks();
        assertEquals(3, chunks.size());

        // the first (older) chunk is compressed.
        assertTrue(chunks.get(0).get("is_compressed", Boolean.class));
        assertFalse(chunks.get(1).get("is_compressed", Boolean.class));

        // Get row count after the processing
        int finalRowCount = getRowCount();

        // original row count + Number of rows that need to split = current row count
        assertEquals(initialRowCount + numOfRowsThatNeedSplit, finalRowCount);

        // Verify each of the split records have the correct start and finish timestamps.
        verifySplitRecords(recordsThatNeedSplit, chunks.get(1).get("range_start", Timestamp.class));

        // Start and finish dates in the compressed chunk are within the chunk boundaries.
        // Number of rows that need to be split = 0 means we don't have records with finish date
        // beyond the range_end timestamp.
        final int numOfRowsThatNeedSplitAfterExecution = getNumOfRowsThatNeedSplit(rangeStart, rangeEnd);
        assertEquals(0, numOfRowsThatNeedSplitAfterExecution);

        connection.close();
    }

    private int getRowCount() {
        String rowCountSql = "SELECT count(*) FROM scope";
        ResultQuery<Record> query = dsl.resultQuery(rowCountSql);
        Record record = query.fetchAny();
        int num = 0;
        if (record != null) {
            return record.getValue(0, Integer.class);
        }
        return num;
    }

    private List<Record> getRecordsThatNeedSplit(Timestamp rangeStart, Timestamp rangeEnd) {
        String sql = String.format("SELECT * FROM scope WHERE start BETWEEN '%1$s' AND '%2$s' AND finish > '%2$s'",
                rangeStart, rangeEnd);

        ResultQuery<Record> query = dsl.resultQuery(sql);
        List<Record> result = new ArrayList<>();
        result.addAll(query.fetch());
        return result;
    }

    private int getNumOfRowsThatNeedSplit(Timestamp rangeStart, Timestamp rangeEnd) {
        String sql = String.format("SELECT count(*) FROM scope WHERE start >= '%1$s' AND start < '%2$s' AND finish > '%2$s'",
                rangeStart, rangeEnd);

        ResultQuery<Record> query = dsl.resultQuery(sql);
        Record record = query.fetchAny();
        int num = 0;
        if (record != null) {
            return record.getValue(0, Integer.class);
        }
        return num;
    }

    private void verifySplitRecords(List<Record> recordsThatNeedSplit, Timestamp nextChunkRangeStart) {
        for (Record record : recordsThatNeedSplit) {
            long seedOid = record.get("seed_oid", Long.class);
            long scopedOid = record.get("scoped_oid", Long.class);
            Timestamp start = record.get("start", Timestamp.class);
            Timestamp finish = record.get("finish", Timestamp.class);
            assertTrue(isScopeRecordExists(seedOid, scopedOid, start, nextChunkRangeStart));
            assertTrue(isScopeRecordExists(seedOid, scopedOid, nextChunkRangeStart, finish));
        }
    }

    private boolean isScopeRecordExists(long seedOid, long scopeOid, Timestamp start, Timestamp finish) {
        String sql = String.format("SELECT FROM scope where seed_oid=%d AND scoped_oid=%d AND start='%s' and finish = '%s'",
                seedOid, scopeOid, start, finish);
        ResultQuery<Record> query = dsl.resultQuery(sql);
        return query.iterator().hasNext();
    }

    private List<Record> getChunks() {
        String sql = String.format("SELECT c.chunk_name, c.range_start, c.range_end, c.is_compressed, pgc.reltuples AS rows "
                + "FROM timescaledb_information.chunks AS c, pg_class AS pgc "
                + "WHERE c.hypertable_schema = '%s' AND c.hypertable_name = 'scope' AND pgc.relname = c.chunk_name",
                endpoint.getConfig().getSchemaName());
        ResultQuery<Record> query = dsl.resultQuery(sql);
        List<Record> result = new ArrayList<>();
        result.addAll(query.fetch());
        return result;
    }

    private void loadDataIntoScopeTable(String csvFilePath) throws IOException, SQLException, URISyntaxException,
            UnsupportedDialectException, InterruptedException {
        URL scopeCsv = getClass().getClassLoader().getResource(csvFilePath);
        if (scopeCsv == null) {
            return;
        }

        BaseConnection pgcon = (BaseConnection)connection;
        CopyManager copyManager = new CopyManager(pgcon);
        File pathToCsv = Paths.get(scopeCsv.toURI()).toFile();
        Reader reader = new BufferedReader(new FileReader(pathToCsv));
        String sql = "COPY scope FROM stdin CSV DELIMITER ','";
        copyManager.copyIn(sql, reader);
    }
}
