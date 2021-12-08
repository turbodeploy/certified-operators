package com.vmturbo.extractor.topology.fetcher;

import static com.vmturbo.extractor.util.RecordTestUtil.captureSink;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsRecord;
import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsRecord.SavingsRecord;
import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsType;
import com.vmturbo.common.protobuf.cost.CostMoles.CostServiceMole;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.extractor.ExtractorDbConfig;
import com.vmturbo.extractor.models.DslRecordSink;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.schema.ExtractorDbBaseConfig;
import com.vmturbo.extractor.schema.enums.SavingsType;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * For cloud savings related tests.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ExtractorDbConfig.class, ExtractorDbBaseConfig.class})
@TestPropertySource(properties = {"sqlDialect=POSTGRES"})
public class CloudSavingsFetcherTest {
    @Autowired
    private ExtractorDbConfig dbConfig;

    private List<Record> recordsCapture;

    private WriterConfig writerConfig = mock(WriterConfig.class);

    private final CostServiceMole costService = spy(CostServiceMole.class);

    private DslRecordSink sink;

    private CloudSavingsFetcher savingsFetcher;

    /**
     * Rule to initialize FeatureFlags.POSTGRES_PRIMARY_DB store.
     **/
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule(FeatureFlags.POSTGRES_PRIMARY_DB);

    /**
     * GRPC cost service test server.
     */
    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(costService);

    private static final long vmId1 = 1001L;
    private static final long vmId2 = 1002L;

    private static final long timestamp1 = System.currentTimeMillis() - 60000;
    private static final long timestamp2 = timestamp1 + 60000;

    private static final float realizedSavings = 101.491f;
    private static final float missedInvestments = 3901.71f;
    private static final float missedSavings = 0.00281f;

    private final List<SavingsTestData> inputDataset = ImmutableList.of(
            new SavingsTestData(vmId1, timestamp1, EntitySavingsStatsType.REALIZED_SAVINGS,
                    realizedSavings),
            new SavingsTestData(vmId1, timestamp1, EntitySavingsStatsType.MISSED_INVESTMENTS,
                    missedInvestments),
            new SavingsTestData(vmId2, timestamp2, EntitySavingsStatsType.MISSED_SAVINGS,
                    missedSavings));

    /**
     * Test setup.
     *
     * @throws UnsupportedDialectException Thrown on bad DB dialect.
     * @throws SQLException Thrown on general DB error.
     * @throws DataAccessException Thrown on DB access errors.
     * @throws InterruptedException Thrown on interrupt during DB operation.
     */
    @Before
    public void before() throws UnsupportedDialectException, SQLException, InterruptedException,
            DataAccessException {
        final DbEndpoint endpoint = spy(dbConfig.ingesterEndpoint());
        doReturn(mock(DSLContext.class)).when(endpoint).dslContext();
        this.sink = mock(DslRecordSink.class);
        this.recordsCapture = captureSink(sink, false);
        final CostServiceBlockingStub costService = CostServiceGrpc.newBlockingStub(server.getChannel());
        this.savingsFetcher = new CloudSavingsFetcher(endpoint, costService,
                Executors.newSingleThreadScheduledExecutor(), writerConfig);
    }

    /**
     * Convenience class to temporarily store savings related data so that it could be fed as
     * test input and verified as test output.
     */
    private static class SavingsTestData {
        public long entityId;
        public long timestamp;
        public EntitySavingsStatsType statsType;
        public float statsValue;

        SavingsTestData(long vmId, long statsTime, EntitySavingsStatsType type, float value) {
            this.entityId = vmId;
            this.timestamp = statsTime;
            this.statsType = type;
            this.statsValue = value;
        }

        SavingsTestData(Record record) {
            Map<String, Object> values = record.asMap();
            this.entityId = (long)values.get("entity_oid");
            this.timestamp = ((Timestamp)values.get("time")).getTime();
            SavingsType savingsType = (SavingsType)values.get("savings_type");
            this.statsType = EntitySavingsStatsType.valueOf(savingsType.getLiteral());
            this.statsValue = (float)values.get("stats_value");
        }

        EntitySavingsStatsRecord getStatsRecord() {
            return EntitySavingsStatsRecord.newBuilder()
                    .setEntityOid(this.entityId)
                    .setSnapshotDate(this.timestamp)
                    .addStatRecords(SavingsRecord.newBuilder()
                            .setName(statsType.name())
                            .setValue(statsValue)
                            .build())
                    .build();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SavingsTestData that = (SavingsTestData)o;
            return entityId == that.entityId && timestamp == that.timestamp
                    && statsType == that.statsType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(entityId, timestamp, statsType);
        }
    }

    /**
     * Verifies that cloud savings data to a fake sink is successful, output data is verified.
     *
     * @throws InterruptedException when interrupted
     * @throws SQLException should not happen
     */
    @Test
    public void writeCloudSavingsData() throws SQLException, InterruptedException {
        // Make up stats records based on the input data set.
        final List<EntitySavingsStatsRecord> statsRecords = inputDataset
                .stream()
                .map(SavingsTestData::getStatsRecord)
                .collect(Collectors.toList());

        // Write records to sink.
        this.savingsFetcher.writeCloudSavingsData(statsRecords.iterator(), this.sink);
        assertFalse(this.recordsCapture.isEmpty());

        // Convert output records captured into test data objects for verification.
        final List<SavingsTestData> outputDataset = this.recordsCapture.stream()
                .map(SavingsTestData::new)
                .collect(Collectors.toList());
        assertFalse(outputDataset.isEmpty());
        assertEquals(inputDataset.size(), outputDataset.size());

        // Make sure all inputs are converted, verify dataset is empty after all input is removed.
        inputDataset.forEach(outputDataset::remove);
        assertTrue(outputDataset.isEmpty());
    }
}
