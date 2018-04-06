package com.vmturbo.history.stats;

import static com.vmturbo.history.schema.abstraction.Tables.PM_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.tables.PmStatsLatest.PM_STATS_LATEST;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.contains;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.assertj.core.util.Lists;
import org.jooq.Condition;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Select;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.stats.Stats;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.history.db.BasedbIO;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.abstraction.tables.records.PmStatsLatestRecord;

public class LiveStatsReaderTest {

    private static long LATEST_TABLE_TIME_WINDOW_MIN = 15;
    private LiveStatsReader liveStatsReader;
    private List<Long> entities;
    private Timestamp latestFromDb;
    private Result queryResultsMock;
    private HistorydbIO mockHistorydbIO;

    @Before
    public void setup() throws VmtDbException {
        mockHistorydbIO = Mockito.mock(HistorydbIO.class);

        // set up the return value for entity id -> entity type lookup
        entities = Lists.newArrayList(1L);
        List<String> entityUuids = Lists.newArrayList("1");
        String entityType = "PhysicalMachine";
        Map<String, String> entityTypeMap = ImmutableMap.of("1", entityType);
        when(mockHistorydbIO.getTypesForEntities(entityUuids)).thenReturn(entityTypeMap);
        // set up for the "timestamp for latest DB record" query
        latestFromDb = Timestamp.from(Instant.ofEpochSecond(1000));
        when(mockHistorydbIO.getMostRecentTimestamp()).thenReturn(Optional.of(latestFromDb));
        // set up for the stats query - return two "normal" stats values, "A" and "B"
        Select dbSelectMock = Mockito.mock(Select.class);
        when(mockHistorydbIO.getStatsSelect(anyObject(), anyList(), anyList(), anyObject()))
                .thenReturn(dbSelectMock);
        queryResultsMock = Mockito.mock(Result.class);
        // create two stats values to be returned as the "normal" part of the stats query
        final PmStatsLatestRecord testRecord1 = createStatsRecord(latestFromDb, "A", "SubA");
        final PmStatsLatestRecord testRecord2 = createStatsRecord(latestFromDb, "B", "SubB");
        Record[] resultList = {
                testRecord1,
                testRecord2
        };
        when(queryResultsMock.toArray()).thenReturn(resultList);
        when(mockHistorydbIO.execute(BasedbIO.Style.FORCED, dbSelectMock))
                .thenReturn(queryResultsMock);
    }

    /**
     * Test that the DB select statement includes the select statements to implement the
     * CommodityRequests given.
     */
    @Test
    public void testGetStatsCommoditySelect() throws Exception {
        // arrange
        LiveStatsReader liveStatsReader = new LiveStatsReader(mockHistorydbIO, 0, 0, 0,
                LATEST_TABLE_TIME_WINDOW_MIN);
        List<CommodityRequest> commodityRequests = Lists.newArrayList(
                CommodityRequest.newBuilder()
                        .setCommodityName("X")
                        .addPropertyValueFilter(
                                Stats.StatsFilter.PropertyValueFilter.newBuilder()
                                        .setProperty("relation")
                                        .setValue("bought")
                                        .build())
                        .build(),
                CommodityRequest.newBuilder()
                        .setCommodityName("Y")
                        .build()
        );
        final String X_TEST = PM_STATS_BY_MONTH.PROPERTY_TYPE.eq("X")
                .and(PM_STATS_BY_MONTH.RELATION.eq(RelationType.COMMODITIESBOUGHT)).toString();
        final String Y_TEST = PM_STATS_BY_MONTH.PROPERTY_TYPE.eq("Y").toString();

        // act
        liveStatsReader.getStatsRecords(
                entities.stream()
                        .map(id -> Long.toString(id))
                        .collect(Collectors.toList()),
                null, null, commodityRequests);

        // assert
        ArgumentCaptor<List> whereCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockHistorydbIO).getStatsSelect(anyObject(), anyList(), whereCaptor.capture(),
                anyObject());
        final List<List> whereClauses = whereCaptor.getAllValues();
        assertThat(whereClauses.size(), equalTo(1));
        List<Condition> innerWhereClauses = whereClauses.get(0);
        // conditions: snapshot_time, uuid, properties
        assertThat(innerWhereClauses.size(), equalTo(3));

        // convert the properties where clause to string
        String propertiesConditionsString = innerWhereClauses.get(2).toString();
        assertThat(propertiesConditionsString, containsString(X_TEST));
        assertThat(propertiesConditionsString, containsString(Y_TEST));
    }

    /**
     * Test that the timestamp of the counted-values match the timestamp returned from the DB.
     * The count values are given by four simple SE counts, e.g. NUM_HOSTS, listed in
     * HistoryStatsUtils.countSEsMetrics, and four SE's "per" SE-type, e.g. NUM_VMS_PER_HOST,
     * given by HistoryStatsUtils.countPerSEsMetrics.
     *
     * @throws Exception should never happen
     */
    @Test
    public void testGetStatsRecordsTimestamps() throws Exception {
        // arrange
        LiveStatsReader liveStatsReader = new LiveStatsReader(mockHistorydbIO, 0, 0, 0,
                LATEST_TABLE_TIME_WINDOW_MIN);

        // act
        List<Record> records = liveStatsReader.getStatsRecords(
                entities.stream()
                        .map(id -> Long.toString(id))
                        .collect(Collectors.toList()),
                null, null, Lists.newArrayList());

        // assert
        // There are 8 count records always generated + 2 stat records we created for this test
        assertThat(records.size(), equalTo(10));
        // check the timestamps all match
        records.forEach(r -> assertThat(r.getValue(PM_STATS_LATEST.SNAPSHOT_TIME),
                equalTo(latestFromDb)));
    }

    /**
     * Create a DB record for the PmStatsLatest table with the given timestamp, propertyType, and
     * propertySubtype. The value fields are not set, as the test above does not require it.
     *
     * @param timestamp the timestamp for when this record was created
     * @param propertyType the type string for this property, e.g. "VStorage"
     * @param propertySubtype the subtype string for this property, e.g. "used"
     * @return a DB record initialzied with the given values
     */
    public PmStatsLatestRecord createStatsRecord(Timestamp timestamp, String propertyType, String propertySubtype) {
        final PmStatsLatestRecord testRecord = new PmStatsLatestRecord();
        testRecord.setPropertyType(propertyType);
        testRecord.setPropertySubtype(propertySubtype);
        testRecord.setSnapshotTime(timestamp);
        return testRecord;
    }

}