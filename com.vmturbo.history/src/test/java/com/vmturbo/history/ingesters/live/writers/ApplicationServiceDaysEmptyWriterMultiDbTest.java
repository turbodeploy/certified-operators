package com.vmturbo.history.ingesters.live.writers;

import static com.vmturbo.history.ingesters.live.writers.ApplicationServiceDaysEmptyWriterTest.createNewTopologyInfo;
import static com.vmturbo.history.ingesters.live.writers.ApplicationServiceDaysEmptyWriterTest.createVmSpecEntityDto;
import static com.vmturbo.history.ingesters.live.writers.ApplicationServiceDaysEmptyWriterTest.processEntities;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ApplicationServiceInfo;
import com.vmturbo.history.db.TestHistoryDbEndpointConfig;
import com.vmturbo.history.db.bulk.BulkInserterConfig;
import com.vmturbo.history.db.bulk.ImmutableBulkInserterConfig;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.listeners.PartmanHelper;
import com.vmturbo.history.notifications.ApplicationServiceHistoryNotificationSender;
import com.vmturbo.history.schema.abstraction.Vmtdb;
import com.vmturbo.history.schema.abstraction.tables.ApplicationServiceDaysEmpty;
import com.vmturbo.history.schema.abstraction.tables.records.ApplicationServiceDaysEmptyRecord;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Unit tests for ApplicationServiceDaysEmptyWriter.
 */
@RunWith(Parameterized.class)
public class ApplicationServiceDaysEmptyWriterMultiDbTest extends MultiDbTestBase {

    private final DSLContext dsl;
    private SimpleBulkLoaderFactory loaders;

    private ApplicationServiceDaysEmptyDbHelper dbHelper;

    /**
     * Provide test parameter values.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return getParameters();
    }

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect               DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public ApplicationServiceDaysEmptyWriterMultiDbTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Vmtdb.VMTDB, configurableDbDialect, dialect, "history",
                TestHistoryDbEndpointConfig::historyEndpoint);
        dsl = super.getDslContext();
        dbHelper = new ApplicationServiceDaysEmptyDbHelper(dsl);
        BulkInserterConfig config = ImmutableBulkInserterConfig.builder()
                .batchSize(10)
                .maxBatchRetries(1)
                .maxRetryBackoffMsec(1000)
                .maxPendingBatches(1)
                .flushTimeoutSecs(10)
                .build();
        loaders = new SimpleBulkLoaderFactory(dsl, config, mock(PartmanHelper.class),
                Executors::newSingleThreadExecutor);
    }

    /**
     * Does test cleanup.
     * @throws InterruptedException an exception that might be thrown.
     */
    @After
    public void after() throws InterruptedException {
        loaders.close(null);
    }

    /**
     * This test ensures that the updates to the app service days empty table work correctly.
     * It tests 3 rounds of updates to ensure that records are properly updated based on app services
     * moving from having apps to having no apps and vice versa.  It ensures all fields are properly
     * updated or not updated, and that in cases where apps go from 0 apps to > 0 apps that records
     * are deleted.
     *
     * @throws DataAccessException data access exception.
     */
    @Test
    public void testUpdatesWorkCorrectly() throws Exception {
        final TopologyEntityDTO app1 = createVmSpecEntityDto(1L, "app1", 1);
        final TopologyEntityDTO app2 = createVmSpecEntityDto(2L, "app2", 2);
        final TopologyEntityDTO app3 = createVmSpecEntityDto(3L, "app3", 3);
        final TopologyEntityDTO app4 = createVmSpecEntityDto(4L, "app4", 0);
        final TopologyEntityDTO app5 = createVmSpecEntityDto(5L, "app5", 0);
        List<TopologyEntityDTO> entities = Arrays.asList(
                app1,
                app2,
                app3,
                app4,
                app5
        );
        // ROUND 1
        LocalDateTime dateTime = LocalDateTime.of(2022, 1, 1, 00, 00);
        Instant time = dateTime.toInstant(ZoneOffset.UTC);
        processEntities(createWriter(time), entities);
        Map<Long, ApplicationServiceDaysEmptyRecord> recordsRound1 = getDbRecordsByAppSvcId();
        verifyOnlyEmptyEntitiesAreStored(recordsRound1, app4, app5);
        assertFirstDiscoveredEqualsLastDiscovered(recordsRound1, app4, app5);
        // ROUND 2
        // Remove apps from one of our app svc entities and reprocess
        TopologyEntityDTO app1v2 = updateEntity(app1, 0);
        entities = Arrays.asList(
                app1v2,
                app2,
                app3,
                app4,
                app5
        );

        processEntities(createWriter(time.plus(1, ChronoUnit.HOURS)), entities);
        Map<Long, ApplicationServiceDaysEmptyRecord> recordsRound2 = getDbRecordsByAppSvcId();
        verifyOnlyEmptyEntitiesAreStored(recordsRound2, app1, app4, app5);
        // The first_discovered for app4 & app5 should be the same as round 1.
        assertFirstDiscoveredIsNotUpdated(recordsRound1, recordsRound2, app4, app5);
        // The last_discovered should now be newer than first_discovered for apps 4 & 5.
        assertLastDiscoveredIsAfterFirstDiscovered(recordsRound2, app4, app5);
        // The last_discovered ts should be updated for all
        assertLastDiscoveredIsUpdated(recordsRound1, recordsRound2, app4, app5);
        // The first & last should be the same for app1
        assertFirstDiscoveredEqualsLastDiscovered(recordsRound2, app1);
        // first_discovered for app1 should be newer than first_discovered for app4 or app5
        assertFirstDiscoveredIsNewer(recordsRound2, app1, app4);
        assertFirstDiscoveredIsNewer(recordsRound2, app1, app5);

        // ROUND 3
        // Add apps to one of our empty app svc entities and reprocess
        TopologyEntityDTO app4v2 = updateEntity(app4, 1);
        entities = Arrays.asList(
                app1v2,
                app2,
                app3,
                app4v2,
                app5
        );
        processEntities(createWriter(time.plus(2, ChronoUnit.HOURS)), entities);
        Map<Long, ApplicationServiceDaysEmptyRecord> recordsRound3 = getDbRecordsByAppSvcId();
        verifyOnlyEmptyEntitiesAreStored(recordsRound3, app1, app5);
        // The first_discovered for app1 & app5 should be the same as round 1.
        assertFirstDiscoveredIsNotUpdated(recordsRound2, recordsRound3, app1, app5);
        // The last_discovered should be newer than first_discovered for apps 1 & 5.
        assertLastDiscoveredIsAfterFirstDiscovered(recordsRound3, app1, app5);
        // The last_discovered should be updated for all
        assertLastDiscoveredIsUpdated(recordsRound2, recordsRound3, app1, app5);
    }

    private ApplicationServiceDaysEmptyWriter createWriter(Instant topologyCreationTime) {
        return new ApplicationServiceDaysEmptyWriter(
                createNewTopologyInfo(topologyCreationTime.toEpochMilli()),
                loaders,
                mock(ApplicationServiceHistoryNotificationSender.class),
                dsl);
    }

    private void assertLastDiscoveredIsUpdated(
            Map<Long, ApplicationServiceDaysEmptyRecord> daysEmptyRecordsPrevious,
            Map<Long, ApplicationServiceDaysEmptyRecord> daysEmptyRecordsCurrent,
            TopologyEntityDTO... entities) {
        for (TopologyEntityDTO entity : entities) {
            assertTrue("Expected daysEmptyRecordsPrevious to contain " + entity.getDisplayName(),
                    daysEmptyRecordsPrevious.containsKey(entity.getOid()));
            assertTrue("Expected daysEmptyRecordsCurrent to contain " + entity.getDisplayName(),
                    daysEmptyRecordsCurrent.containsKey(entity.getOid()));
            Timestamp ldPrevious = daysEmptyRecordsPrevious.get(entity.getOid()).getLastDiscovered();
            Timestamp ldCurrent = daysEmptyRecordsCurrent.get(entity.getOid()).getLastDiscovered();
            assertTrue("Expected last discovered to be updated for " + entity.getDisplayName(),
                    ldCurrent.toLocalDateTime().isAfter(ldPrevious.toLocalDateTime()));
        }
    }

    private void assertFirstDiscoveredIsNotUpdated(
            Map<Long, ApplicationServiceDaysEmptyRecord> daysEmptyRecordsPrevious,
            Map<Long, ApplicationServiceDaysEmptyRecord> daysEmptyRecordsCurrent,
            TopologyEntityDTO... entities) {
        for (TopologyEntityDTO entity : entities) {
            assertTrue("Expected daysEmptyRecordsPrevious to contain " + entity.getDisplayName(),
                    daysEmptyRecordsPrevious.containsKey(entity.getOid()));
            assertTrue("Expected daysEmptyRecordsCurrent to contain " + entity.getDisplayName(),
                    daysEmptyRecordsCurrent.containsKey(entity.getOid()));
            Timestamp fdPrevious = daysEmptyRecordsPrevious.get(entity.getOid()).getFirstDiscovered();
            Timestamp fdCurrent = daysEmptyRecordsCurrent.get(entity.getOid()).getFirstDiscovered();
            assertTrue("Expected first discovered not to be updated for " + entity.getDisplayName(),
                    fdCurrent.toLocalDateTime().isEqual(fdPrevious.toLocalDateTime()));
        }
    }

    private void assertFirstDiscoveredEqualsLastDiscovered(
            Map<Long, ApplicationServiceDaysEmptyRecord> daysEmptyRecords,
            TopologyEntityDTO... entities) {
        for (TopologyEntityDTO entity : entities) {
            assertTrue("Expected daysEmptyRecords to contain " + entity.getDisplayName(),
                    daysEmptyRecords.containsKey(entity.getOid()));
            Timestamp fd = daysEmptyRecords.get(entity.getOid()).getFirstDiscovered();
            Timestamp ld = daysEmptyRecords.get(entity.getOid()).getLastDiscovered();
            assertTrue("Expected first discovered to be equal to last discovered for " + entity.getDisplayName(),
                    fd.toLocalDateTime().isEqual(ld.toLocalDateTime()));
        }
    }

    private void assertLastDiscoveredIsAfterFirstDiscovered(
            Map<Long, ApplicationServiceDaysEmptyRecord> daysEmptyRecords,
            TopologyEntityDTO... entities) {
        for (TopologyEntityDTO entity : entities) {
            assertTrue("Expected daysEmptyRecords to contain " + entity.getDisplayName(),
                    daysEmptyRecords.containsKey(entity.getOid()));
            Timestamp fd = daysEmptyRecords.get(entity.getOid()).getFirstDiscovered();
            Timestamp ld = daysEmptyRecords.get(entity.getOid()).getLastDiscovered();
            assertTrue("Expected last discovered to be after to first discovered for " + entity.getDisplayName(),
                    ld.toLocalDateTime().isAfter(fd.toLocalDateTime()));
        }
    }

    private void assertFirstDiscoveredIsNewer(
            Map<Long, ApplicationServiceDaysEmptyRecord> daysEmptyRecords,
            TopologyEntityDTO newTopologyEntityDto,
            TopologyEntityDTO oldTopologyEntityDto) {
            assertTrue("Expected daysEmptyRecords to contain " + oldTopologyEntityDto.getDisplayName(),
                    daysEmptyRecords.containsKey(oldTopologyEntityDto.getOid()));
            assertTrue("Expected daysEmptyRecords to contain " + newTopologyEntityDto.getDisplayName(),
                    daysEmptyRecords.containsKey(newTopologyEntityDto.getOid()));
            Timestamp fdOld = daysEmptyRecords.get(oldTopologyEntityDto.getOid()).getFirstDiscovered();
            Timestamp fdNew = daysEmptyRecords.get(newTopologyEntityDto.getOid()).getFirstDiscovered();
            assertTrue("Expected first discovered for " + newTopologyEntityDto.getDisplayName()
                    + "to be newwer than first discovered for " + oldTopologyEntityDto.getDisplayName(),
                    fdNew.toLocalDateTime().isAfter(fdOld.toLocalDateTime()));
    }

    private Map<Long, ApplicationServiceDaysEmptyRecord> getDbRecordsByAppSvcId() {
        return dbHelper.selectAllDaysEmptyRecords().stream().collect(
                        Collectors.toMap(ApplicationServiceDaysEmptyRecord::getId,
                                Function.identity()));
    }

    private void verifyOnlyEmptyEntitiesAreStored(
            Map<Long, ApplicationServiceDaysEmptyRecord> dbRecordsByAppSvcId,
            TopologyEntityDTO... emptyEntities) {
        // only app services without apps should be stored therefor these should be equal:
        assertThat(dbRecordsByAppSvcId.size(), equalTo(emptyEntities.length));
        // verify IDs & name
        List<TopologyEntityDTO> emptyAppSvcs = Arrays.asList(emptyEntities);
        for (TopologyEntityDTO emptyAppSvc : emptyAppSvcs) {
            ApplicationServiceDaysEmptyRecord record = dbRecordsByAppSvcId.get(emptyAppSvc.getOid());
            assertNotNull(record);
            assertThat(record.getName(), equalTo(emptyAppSvc.getDisplayName()));
        }
    }

    private TopologyEntityDTO updateEntity(TopologyEntityDTO entity, int appCount) {
        ApplicationServiceInfo appSvcInfo = entity.getTypeSpecificInfo()
                .getApplicationService()
                .toBuilder()
                .setAppCount(appCount)
                .build();
        return entity.toBuilder().setTypeSpecificInfo(
                entity.getTypeSpecificInfo()
                        .toBuilder()
                        .setApplicationService(appSvcInfo)
                        .build()).build();
    }

    /**
     * Class which provides helper functions for ApplicationServiceDaysEmpty test consumers.
     */
    public static class ApplicationServiceDaysEmptyDbHelper {

        private static final ApplicationServiceDaysEmpty ASDE = ApplicationServiceDaysEmpty.APPLICATION_SERVICE_DAYS_EMPTY;

        private final DSLContext dsl;

        /**
         * Creates a new ApplicationServiceDaysEmptyDbHelper.
         * @param dsl the DSL context.
         */
        public ApplicationServiceDaysEmptyDbHelper(DSLContext dsl) {
            this.dsl = dsl;
        }

        /**
         * Creates a new ApplicationServiceDaysEmptyRecord.
         * @param id the ID.
         * @param name The name.
         * @param lastDiscovered The lastDiscovered.
         * @return the record.
         */
        public ApplicationServiceDaysEmptyRecord newAppSvcDaysEmptyRecord(final long id,
                final String name,
                final Instant lastDiscovered) {
            ApplicationServiceDaysEmptyRecord record = dsl.newRecord(ASDE);
            record.setId(id);
            record.setName(name);
            record.setFirstDiscovered(Timestamp.from(lastDiscovered));
            record.setLastDiscovered(Timestamp.from(lastDiscovered));
            return record;
        }

        /**
         * Selects all records for the given IDs.
         *
         * @param ids The ids.
         * @return a list of records.
         * @throws DataAccessException a data access exception.
         */
        public List<ApplicationServiceDaysEmptyRecord> selectDaysEmptyRecords(Long... ids)
                throws DataAccessException {
            return dsl.selectFrom(ASDE)
                    .where(ASDE.ID.in(ids))
                    .fetch().stream().collect(Collectors.toList());
        }

        /**
         * Selects all records.
         *
         * @return all records.
         * @throws DataAccessException data access exception
         */
        public List<ApplicationServiceDaysEmptyRecord> selectAllDaysEmptyRecords()
                throws DataAccessException {
            return dsl.selectFrom(ASDE).fetch().stream().collect(Collectors.toList());
        }
    }

}
