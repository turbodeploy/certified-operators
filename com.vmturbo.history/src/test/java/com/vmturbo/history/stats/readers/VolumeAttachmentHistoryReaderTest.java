package com.vmturbo.history.stats.readers;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.junit.Assert.assertEquals;

import java.sql.Date;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record3;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockDataProvider;
import org.jooq.tools.jdbc.MockExecuteContext;
import org.jooq.tools.jdbc.MockResult;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for VolumeAttachmentHistoryReader.
 */
public class VolumeAttachmentHistoryReaderTest {

    private static final long VOLUME_1_OID = 11111L;
    private static final long VOLUME_2_OID = 22222L;
    private static final long VM_1_OID = 444444L;
    private static final long VM_2_OID = 555555L;
    private static final long INSTANT = 1605755159394L;
    private static final Date DATE_1 = new Date(INSTANT);
    private static final Date DATE_2 = new Date(INSTANT + TimeUnit.DAYS.toMillis(1));
    private static final Date DATE_3 = new Date(INSTANT + TimeUnit.DAYS.toMillis(2));
    private static final Field<Date> lastAttachedDateField = field(name("last_attached_date"),
            SQLDataType.DATE);
    private static final Field<Long> volumeOidField = field(name("volume_oid"), SQLDataType.BIGINT);
    private static final Field<Long> vmOidField = field(name("vm_oid"), SQLDataType.BIGINT);

    private final DSLContext dsl = DSL.using(SQLDialect.MARIADB);

    /**
     * Test that empty List is returned when Db returns empty result set.
     *
     * @throws DataAccessException on DB error
     */
    @Test
    public void testGetVolumeAttachmentHistoryEmptyList() throws DataAccessException {
        final VolumeAttachmentHistoryReader reader = new VolumeAttachmentHistoryReader(
                recordProviderDsl(ctx -> new Record[]{dsl.newRecord()}));
        Assert.assertTrue(reader.getVolumeAttachmentHistory(Collections.singletonList(VOLUME_1_OID))
                .isEmpty());
    }

    /**
     * Test that empty List is returned when input volumeOids is empty.
     *
     * @throws DataAccessException if there are DB problems
     */
    @Test
    public void testGetVolumeAttachmentHistoryEmptyVolumeOids() throws DataAccessException {
        final VolumeAttachmentHistoryReader reader = new VolumeAttachmentHistoryReader(
                DSL.using(new MockConnection(ctx -> null)));
        Assert.assertTrue(reader.getVolumeAttachmentHistory(Collections.emptyList())
                .isEmpty());
    }

    /**
     * Test that empty List is returned when Db returns null result set.
     *
     * @throws DataAccessException on DB error
     */
    @Test
    public void testGetVolumeAttachmentHistorySingleVolumeNotFound() throws DataAccessException {
        final VolumeAttachmentHistoryReader reader = new VolumeAttachmentHistoryReader(
                DSL.using(new MockConnection(ctx -> null)));
        Assert.assertTrue(reader.getVolumeAttachmentHistory(Collections.singletonList(VOLUME_1_OID))
                .isEmpty());
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
        final Record3<Long, Long, Date> record = createRecord(VOLUME_1_OID, VM_1_OID, DATE_1);
        final VolumeAttachmentHistoryReader reader = new VolumeAttachmentHistoryReader(
                recordProviderDsl(ctx -> new Record[]{record}));
        final List<Record3<Long, Long, Date>> records =
                reader.getVolumeAttachmentHistory(Collections.singletonList(VOLUME_1_OID));
        assertEquals(Collections.singleton(record), new HashSet<>(records));
    }

    /**
     * Test that volumeOids with single element which returns single unattached record is correctly
     * retrieved.
     *
     * @throws DataAccessException on DB error
     */
    @Test
    public void testGetVolumeAttachmentHistorySingleVolumeFoundUnattachedRecord()
            throws DataAccessException {
        final Record3<Long, Long, Date> record = createRecord(VOLUME_1_OID, 0, DATE_1);
        final VolumeAttachmentHistoryReader reader = new VolumeAttachmentHistoryReader(
                recordProviderDsl(ctx -> new Record[]{record}));
        final List<Record3<Long, Long, Date>> records =
                reader.getVolumeAttachmentHistory(Collections.singletonList(VOLUME_1_OID));
        assertEquals(Collections.singleton(record), new HashSet<>(records));
    }

    /**
     * Test that volumeOids with single element which returns two records, one attached and one
     * attached (attached date earlier than unattached date) is correctly retrieved.
     *
     * @throws DataAccessException on DB error
     */
    @Test
    public void testGetVolumeAttachmentHistorySingleVolumeTwoRecords() throws DataAccessException {
        final Record3<Long, Long, Date> record1 = createRecord(VOLUME_1_OID, VM_1_OID, DATE_1);
        final Record3<Long, Long, Date> record2 = createRecord(VOLUME_1_OID, 0, DATE_2);
        final VolumeAttachmentHistoryReader reader = new VolumeAttachmentHistoryReader(
                recordProviderDsl(ctx -> new Record[]{record1, record2}));
        final List<Record3<Long, Long, Date>> records = reader.getVolumeAttachmentHistory(
                Collections.singletonList(VOLUME_1_OID));
        assertEquals(Collections.singleton(record1), new HashSet<>(records));
    }

    /**
     * Test that volumeOids with single element which returns two records, one attached and one
     * attached (unattached date earlier than attached date) is correctly retrieved.
     *
     * @throws DataAccessException on DB error
     */
    @Test
    public void testGetVolumeAttachmentHistorySingleVolumeTwoRecordsReverseOrder()
            throws DataAccessException {
        final Record3<Long, Long, Date> record1 = createRecord(VOLUME_1_OID, VM_1_OID, DATE_2);
        final Record3<Long, Long, Date> record2 = createRecord(VOLUME_1_OID, 0, DATE_1);
        final VolumeAttachmentHistoryReader reader = new VolumeAttachmentHistoryReader(
                recordProviderDsl(ctx -> new Record[]{record1, record2}));
        final List<Record3<Long, Long, Date>> records = reader.getVolumeAttachmentHistory(
                Collections.singletonList(VOLUME_1_OID));
        assertEquals(Collections.singleton(record1), new HashSet<>(records));
    }

    /**
     * Test that volumeOids with single element which returns two records, both attahced is
     * correctly retrieved.
     *
     * @throws DataAccessException on DB error
     */
    @Test
    public void testGetVolumeAttachmentHistorySingleVolumeTwoRecordsBothAttached()
            throws DataAccessException {
        final Record3<Long, Long, Date> record1 = createRecord(VOLUME_1_OID, VM_1_OID, DATE_1);
        final Record3<Long, Long, Date> record2 = createRecord(VOLUME_1_OID, VM_2_OID, DATE_2);
        final VolumeAttachmentHistoryReader reader = new VolumeAttachmentHistoryReader(
                recordProviderDsl(ctx -> new Record[]{record1, record2}));
        final List<Record3<Long, Long, Date>> records = reader.getVolumeAttachmentHistory(
                Collections.singletonList(VOLUME_1_OID));
        assertEquals(Collections.singleton(record2), new HashSet<>(records));
    }

    /**
     * Test that volumeOids with 2 elements which returns 1 attached record each is correctly
     * retrieved.
     *
     * @throws DataAccessException on DB error
     */
    @Test
    public void testGetVolumeAttachmentHistoryTwoVolumesOneRecord() throws DataAccessException {
        final Record3<Long, Long, Date> record1 = createRecord(VOLUME_1_OID, VM_1_OID, DATE_1);
        final Record3<Long, Long, Date> record2 = createRecord(VOLUME_2_OID, VM_2_OID, DATE_1);
        final VolumeAttachmentHistoryReader reader = new VolumeAttachmentHistoryReader(
                recordProviderDsl(ctx -> new Record[]{record1, record2}));
        final List<Record3<Long, Long, Date>> records = reader.getVolumeAttachmentHistory(
                Stream.of(VOLUME_1_OID, VOLUME_2_OID).collect(Collectors.toList()));
        assertEquals(Stream.of(record1, record2).collect(Collectors.toSet()),
                new HashSet<>(records));
    }

    /**
     * Test that volumeOids with 2 elements which return 2 records each, one attached one
     * unattached, is correctly retrieved.
     *
     * @throws DataAccessException on DB error
     */
    @Test
    public void testGetVolumeAttachmentHistoryTwoVolumeTwoRecords() throws DataAccessException {
        final Record3<Long, Long, Date> record1Vol1 = createRecord(VOLUME_1_OID, VM_1_OID, DATE_1);
        final Record3<Long, Long, Date> record2Vol1 = createRecord(VOLUME_1_OID, 0, DATE_2);
        final Record3<Long, Long, Date> record1Vol2 = createRecord(VOLUME_2_OID, VM_2_OID, DATE_1);
        final Record3<Long, Long, Date> record2Vol2 = createRecord(VOLUME_2_OID, 0, DATE_2);
        final VolumeAttachmentHistoryReader reader = new VolumeAttachmentHistoryReader(
                recordProviderDsl(
                        ctx -> new Record[]{record1Vol1, record2Vol1, record1Vol2, record2Vol2}));
        final List<Record3<Long, Long, Date>> records = reader.getVolumeAttachmentHistory(
                Stream.of(VOLUME_1_OID, VOLUME_2_OID).collect(Collectors.toList()));
        Assert.assertEquals(Stream.of(record1Vol1, record1Vol2).collect(Collectors.toSet()),
                new HashSet<>(records));
    }

    /**
     * Test that volumeOids with 1 element which returns 3 records where one is unattached,
     * returns the most recent attached record.
     *
     * @throws DataAccessException on DB error
     */
    @Test
    public void testGetVolumeAttachmentHistorySingleVolumeThreeRecords() throws DataAccessException {
        final Record3<Long, Long, Date> record1 = createRecord(VOLUME_1_OID, VM_1_OID, DATE_1);
        final Record3<Long, Long, Date> record2 = createRecord(VOLUME_1_OID, VM_2_OID, DATE_2);
        final Record3<Long, Long, Date> record3 = createRecord(VOLUME_1_OID, 0, DATE_3);
        final VolumeAttachmentHistoryReader reader = new VolumeAttachmentHistoryReader(
                recordProviderDsl(ctx -> new Record[]{record1, record2, record3}));
        final List<Record3<Long, Long, Date>> records = reader.getVolumeAttachmentHistory(
                Stream.of(VOLUME_1_OID).collect(Collectors.toList()));
        Assert.assertEquals(Stream.of(record2).collect(Collectors.toSet()),
                new HashSet<>(records));
    }

    private DSLContext mockDsl(MockDataProvider mockDataProvider) {
        return DSL.using(new MockConnection(mockDataProvider));
    }

    private DSLContext recordProviderDsl(Function<MockExecuteContext, Record[]> provider) {
        return mockDsl(ctx -> {
            Record[] recs = provider.apply(ctx);
            Result<Record> result = dsl.newResult(recs[0].fields());
            result.addAll(Arrays.asList(recs));
            return new MockResult[]{new MockResult(recs.length, result)};
        });
    }

    private Record3<Long, Long, Date> createRecord(final long volumeOid, final long vmOid,
            final Date lastAttachedDate) {
        return dsl.newRecord(volumeOidField, vmOidField, lastAttachedDateField)
                .values(volumeOid, vmOid, lastAttachedDate);
    }
}