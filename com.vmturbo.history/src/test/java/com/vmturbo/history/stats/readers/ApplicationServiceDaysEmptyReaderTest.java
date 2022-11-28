package com.vmturbo.history.stats.readers;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.jooq.DSLContext;
import org.jooq.Record3;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockDataProvider;
import org.jooq.tools.jdbc.MockExecuteContext;
import org.jooq.tools.jdbc.MockResult;
import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.history.component.api.HistoryComponentNotifications.ApplicationServiceHistoryNotification.DaysEmptyInfo;
import com.vmturbo.history.schema.abstraction.tables.ApplicationServiceDaysEmpty;

/**
 * Unit tests for VolumeAttachmentHistoryReader.
 */
public class ApplicationServiceDaysEmptyReaderTest {

    private static final ApplicationServiceDaysEmpty ASDE = ApplicationServiceDaysEmpty.APPLICATION_SERVICE_DAYS_EMPTY;

    private final DSLContext dsl = DSL.using(SQLDialect.MARIADB);

    /**
     * Test that empty List is returned when Db returns empty result set.
     *
     * @throws DataAccessException on DB error
     */
    @Test
    public void testNoRecordsFound() throws DataAccessException {
        DSLContext mockDsl = mockDsl(new DaysEmptyDataProvider(Collections.emptyList()));
        ApplicationServiceDaysEmptyReader reader = new ApplicationServiceDaysEmptyReader(mockDsl);
        Assert.assertTrue(reader.getApplicationServiceDaysEmptyInfo().isEmpty());
    }

    /**
     * Test that volumeOids with single element which returns single attached record is correctly
     * retrieved.
     *
     * @throws DataAccessException on DB error
     */
    @Test
    public void testGetVolumeAttachmentHistorySingleVolumeFoundAttachedRecord()
            throws DataAccessException {
        Instant now = Instant.now();
        final Record3<Long, String, Timestamp> record1 = createRecord(1L, "appSvc1", Timestamp.from(now));
        final Record3<Long, String, Timestamp> record2 = createRecord(2L, "appSvc2", Timestamp.from(now.minus(1, ChronoUnit.DAYS)));
        final Record3<Long, String, Timestamp> record3 = createRecord(3L, "appSvc3", Timestamp.from(now.minus(2, ChronoUnit.DAYS)));
        List<Record3<Long, String, Timestamp>> recordsIn = Arrays.asList(record1, record2, record3);

        DSLContext mockDsl = mockDsl(new DaysEmptyDataProvider(recordsIn));
        ApplicationServiceDaysEmptyReader reader = new ApplicationServiceDaysEmptyReader(mockDsl);

        final List<DaysEmptyInfo> daysEmptyInfos = reader.getApplicationServiceDaysEmptyInfo();
        Map<Long, DaysEmptyInfo> daysEmptyByAppSvc = daysEmptyInfos.stream()
                .collect(Collectors.toMap(DaysEmptyInfo::getAppSvcOid, Function.identity()));
        assertEquals(daysEmptyByAppSvc.size(), recordsIn.size());
        for (Record3<Long, String, Timestamp> record : recordsIn) {
            DaysEmptyInfo daysEmptyInfo = daysEmptyByAppSvc.get(record.component1());
            assertNotNull(daysEmptyInfo);
            assertThat(daysEmptyInfo.getAppSvcName(), equalTo(record.component2()));
            assertThat(daysEmptyInfo.getFirstDiscoveredTime(), equalTo(record.component3().getTime()));
        }
    }

    private DSLContext mockDsl(MockDataProvider mockDataProvider) {
        return DSL.using(new MockConnection(mockDataProvider));
    }

    /**
     * DaysEmptyDataProvider mock data provider.
     */
    private class DaysEmptyDataProvider implements MockDataProvider {
        final Record3<Long, String, Timestamp>[] records;

        DaysEmptyDataProvider(List<Record3<Long, String, Timestamp>> recordsIn) {
            this.records = recordsIn.toArray(new Record3[0]);
        }

        @Override
        public MockResult[] execute(MockExecuteContext ctx) throws SQLException {
            Result<Record3<Long, String, Timestamp>> result = dsl.newResult(ASDE.ID, ASDE.NAME, ASDE.FIRST_DISCOVERED);
            for (Record3<Long, String, Timestamp> r : records) {
                result.add(r);
            }
            return new MockResult[]{new MockResult(1, result)};
        }
    }

    private Record3<Long, String, Timestamp> createRecord(final long oid,
            final String name,
            final Timestamp firstDiscovered) {
        return dsl.newRecord(ASDE.ID, ASDE.NAME, ASDE.FIRST_DISCOVERED)
                .values(oid, name, firstDiscovered);
    }
}