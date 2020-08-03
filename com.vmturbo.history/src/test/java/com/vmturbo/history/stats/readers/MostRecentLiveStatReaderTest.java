package com.vmturbo.history.stats.readers;

import static com.vmturbo.common.protobuf.utils.StringConstants.SNAPSHOT_TIME;
import static com.vmturbo.common.protobuf.utils.StringConstants.UUID;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import io.grpc.stub.ServerCallStreamObserver;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.Record2;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockDataProvider;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.stats.Stats.GetMostRecentStatResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatHistoricalEpoch;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.history.db.BasedbIO;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;

/**
 * Unit tests for MostRecentLiveStatReader.
 */
public class MostRecentLiveStatReaderTest {

    private MostRecentLiveStatReader reader;
    private HistorydbIO historydbIO;
    private DSLContext dslContext;
    private ServerCallStreamObserver streamObserver;

    private static final String VOLUME_COMMODITY_KEY = "vol-1111";
    private static final long RELATED_ENTITY_OID = 7777777L;
    private static final long SNAPSHOT_TIME_VALUE = 1582065722040L;

    /**
     * Initialize test resources.
     */
    @Before
    public void setUp() {
        historydbIO = mock(HistorydbIO.class);
        HistorydbIO.setSharedInstance(historydbIO);
        reader = new MostRecentLiveStatReader(historydbIO);
        final MockDataProvider mockDataProvider = mock(MockDataProvider.class);
        final MockConnection mockConnection = new MockConnection(mockDataProvider);
        dslContext = DSL.using(mockConnection, SQLDialect.MARIADB);
        when(HistorydbIO.getJooqBuilder()).thenReturn(dslContext);
        streamObserver = mock(ServerCallStreamObserver.class);
        when(streamObserver.isCancelled()).thenReturn(false);
    }

    /**
     * Test that exception in retrieval results in empty response.
     *
     * @throws VmtDbException if getMostRecentStat throws VmtDbException.
     */
    @Test
    public void testExceptionGetMostRecentLiveStat() throws VmtDbException {
        // given
        doThrow(VmtDbException.class)
                .when(historydbIO)
                .execute(any(BasedbIO.Style.class), any(Query.class));

        // when
        final Optional<GetMostRecentStatResponse.Builder> result =
                reader.getMostRecentStat(StringConstants.VIRTUAL_MACHINE,
                        StringConstants.STORAGE_AMOUNT, VOLUME_COMMODITY_KEY, streamObserver);

        // then
        Assert.assertFalse(result.isPresent());
    }

    /**
     * Test record returned from daily table.
     *
     * @throws VmtDbException if getMostRecentStat throws VmtDbException.
     */
    @Test
    public void testGetRecentLiveStatFromDailyTable() throws VmtDbException {
        // given
        doReturn(createResult())
                .when(historydbIO)
                .execute(any(BasedbIO.Style.class), any(Query.class));

        // when
        final Optional<GetMostRecentStatResponse.Builder> result =
                reader.getMostRecentStat(StringConstants.VIRTUAL_MACHINE,
                        StringConstants.STORAGE_AMOUNT, VOLUME_COMMODITY_KEY, streamObserver);

        // then
        Assert.assertTrue(result.isPresent());
        final GetMostRecentStatResponse.Builder response = result.get();
        Assert.assertEquals(StatHistoricalEpoch.DAY, response.getEpoch());
        Assert.assertEquals(RELATED_ENTITY_OID, response.getEntityUuid());
    }

    /**
     * Test record returned from monthly table.
     *
     * @throws VmtDbException if getMostRecentStat throws VmtDbException.
     */
    @Test
    public void testGetRecentLiveStatFromMonthlyTable() throws VmtDbException {
        // given
        doReturn(createEmptyResult())
                .doReturn(createResult())
                .when(historydbIO)
                .execute(any(BasedbIO.Style.class), any(Query.class));

        // when
        final Optional<GetMostRecentStatResponse.Builder> result =
                reader.getMostRecentStat(StringConstants.VIRTUAL_MACHINE,
                        StringConstants.STORAGE_AMOUNT, VOLUME_COMMODITY_KEY, streamObserver);

        // then
        Assert.assertTrue(result.isPresent());
        final GetMostRecentStatResponse.Builder response = result.get();
        Assert.assertEquals(StatHistoricalEpoch.MONTH, response.getEpoch());
        Assert.assertEquals(RELATED_ENTITY_OID, response.getEntityUuid());
    }

    /**
     * Test no record returned from hourly, daily or monthly table.
     *
     * @throws VmtDbException if getMostRecentStat throws VmtDbException.
     */
    @Test
    public void testNoRecordReturned() throws VmtDbException {
        // given
        doReturn(createEmptyResult())
                .doReturn(createEmptyResult())
                .doReturn(createEmptyResult())
                .when(historydbIO).execute(any(BasedbIO.Style.class), any(Query.class));

        // when
        final Optional<GetMostRecentStatResponse.Builder> result =
                reader.getMostRecentStat(StringConstants.VIRTUAL_MACHINE,
                        StringConstants.STORAGE_AMOUNT, VOLUME_COMMODITY_KEY, streamObserver);

        // then
        Assert.assertFalse(result.isPresent());
    }

    /**
     * Test that the snapshot time corresponds to the most recent entry from the table.
     *
     * @throws VmtDbException if getMostRecentStat throws VmtDbException.
     */
    @Test
    public void testGetRecentLiveStatMostRecentStatReturned() throws VmtDbException {
        // given
        final Result<? extends Record> queryResult = createResult();
        doReturn(queryResult)
                .when(historydbIO)
                .execute(any(BasedbIO.Style.class), any(Query.class));

        // when
        final Optional<GetMostRecentStatResponse.Builder> result =
                reader.getMostRecentStat(StringConstants.VIRTUAL_MACHINE,
                        StringConstants.STORAGE_AMOUNT, VOLUME_COMMODITY_KEY, streamObserver);

        // then
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals(SNAPSHOT_TIME_VALUE, result.get().getSnapshotDate());
    }

    /**
     * Test that when VmtDbException is encountered, for example when there is a SQL timeout, then
     * getMostRecentStat returns an empty optional.
     *
     * @throws VmtDbException if historyDbio::execute throws VmtDbException
     */
    @Test
    public void testGetRecentLiveStatVmtDbException() throws VmtDbException {
        // given
        doThrow(VmtDbException.class)
            .when(historydbIO)
            .execute(any(BasedbIO.Style.class), any(Query.class));

        // when
        final Optional<GetMostRecentStatResponse.Builder> result =
            reader.getMostRecentStat(StringConstants.VIRTUAL_MACHINE,
                StringConstants.STORAGE_AMOUNT, VOLUME_COMMODITY_KEY, streamObserver);

        // then
        Assert.assertFalse(result.isPresent());
    }

    /**
     * Test that getMostRecentStat returns early if StreamObserver is cancelled.
     *
     * @throws VmtDbException if getMostRecentStat throws VmtDbException.
     */
    @Test
    public void testGetRecentLiveStatCancelledObserver() throws VmtDbException {
        // given
        doReturn(createResult())
            .when(historydbIO)
            .execute(any(BasedbIO.Style.class), any(Query.class));
        when(streamObserver.isCancelled()).thenReturn(true);

        // when
        final Optional<GetMostRecentStatResponse.Builder> result =
            reader.getMostRecentStat(StringConstants.VIRTUAL_MACHINE,
                StringConstants.STORAGE_AMOUNT, VOLUME_COMMODITY_KEY, streamObserver);

        // then
        Assert.assertFalse(result.isPresent());
    }

    private Result<? extends Record> createEmptyResult() {
        return dslContext.newResult();
    }

    private Result<? extends Record> createResult() {
        final Field<Timestamp> timeStamp = field(name(SNAPSHOT_TIME), SQLDataType.TIMESTAMP);
        final Field<Long> oid = field(name(UUID), SQLDataType.BIGINT);
        final Result<Record2<Timestamp, Long>> result = dslContext.newResult(timeStamp, oid);
        final Timestamp timestampValue = new Timestamp(SNAPSHOT_TIME_VALUE);
        // newer entry
        result.add(dslContext.newRecord(timeStamp, oid)
                .values(timestampValue, RELATED_ENTITY_OID));
        // older entry
        result.add(dslContext.newRecord(timeStamp, oid)
                .values(new Timestamp(SNAPSHOT_TIME_VALUE - TimeUnit.DAYS.toMillis(1)),
                        RELATED_ENTITY_OID));
        return result;
    }

    /**
     * Delete test resources.
     */
    @After
    public void cleanUp() {
        HistorydbIO.setSharedInstance(null);
    }
}
