package com.vmturbo.extractor.topology;

import static com.vmturbo.extractor.models.ModelDefinitions.ATTRS;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_HASH_AS_HASH;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_OID_AS_OID;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_STATE;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_TABLE;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_TYPE_AS_TYPE;
import static com.vmturbo.extractor.models.ModelDefinitions.ENVIRONMENT_TYPE;
import static com.vmturbo.extractor.models.ModelDefinitions.FIRST_SEEN;
import static com.vmturbo.extractor.models.ModelDefinitions.LAST_SEEN;
import static com.vmturbo.extractor.models.ModelDefinitions.SCOPED_OIDS;
import static com.vmturbo.extractor.util.ExtractorTestUtil.config;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.api.enums.EntityState;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.extractor.models.Column.JsonString;
import com.vmturbo.extractor.models.DslRecordSink;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.models.Table.TableWriter;
import com.vmturbo.extractor.topology.EntityHashManager.SnapshotManager;
import com.vmturbo.extractor.util.RecordTestUtil;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Tests for the {@link EntityHashManager}.
 */
public class EntityHashManagerTest {

    private EntityHashManager entityHashManager;
    private Record baseEntity;
    private List<Record> sinkCapture;
    private DslRecordSink sink;

    /**
     * Set up for tests, by creating an entities record that will be used in the tests, and a table
     * writer based on a mock sink.
     */
    @Before
    public void before() {
        entityHashManager = new EntityHashManager(config);
        baseEntity = new Record(ENTITY_TABLE);
        baseEntity.set(ENTITY_OID_AS_OID, 1L);
        baseEntity.set(ENTITY_TYPE_AS_TYPE, EntityType.VIRTUAL_MACHINE.name());
        baseEntity.set(ENVIRONMENT_TYPE, EnvironmentType.ON_PREM.name());
        baseEntity.set(ENTITY_STATE, EntityState.ACTIVE.name());
        baseEntity.set(ATTRS, new JsonString("{}"));
        baseEntity.set(SCOPED_OIDS, new Long[]{});
        this.sink = mock(DslRecordSink.class);
        this.sinkCapture = RecordTestUtil.captureSink(sink, false);
    }

    /**
     * Test that the entities hash manager properly tracks entity state through a series of changes,
     * including reverting back to a previously seen state.
     *
     * @throws UnsupportedDialectException if endpoint is mis-configured
     * @throws SQLException                if there's a DB error
     */
    @Test
    public void testEntityHashChangesAreNoticed() throws UnsupportedDialectException, SQLException {
        final Long oid = baseEntity.get(ENTITY_OID_AS_OID);
        long topologyTime = 0L;

        // entity initially seen at time 1; new entity => needs to be recorded
        Long hash1;
        try (SnapshotManager snapshotManager = entityHashManager.open(++topologyTime);
             TableWriter entitiesWriter = ENTITY_TABLE.open(sink)) {
            assertThat(snapshotManager.updateEntityHash(baseEntity), notNullValue());
            snapshotManager.processChanges(entitiesWriter);
            hash1 = entityHashManager.getEntityHash(oid);
            assertThat(entityHashManager.getHashLastSeen(hash1), is(topologyTime));
        }

        // same entity seen at time 2, no change so no need to record
        try (SnapshotManager snapshotManager = entityHashManager.open(++topologyTime);
             TableWriter entitiesWriter = ENTITY_TABLE.open(sink)) {
            assertThat(snapshotManager.updateEntityHash(baseEntity), nullValue());
            snapshotManager.processChanges(entitiesWriter);
            assertThat(entityHashManager.getEntityHash(oid), is(hash1));
            assertThat(entityHashManager.getHashLastSeen(hash1), is(topologyTime));
        }

        // change entity type (unrealistic, but...); change => need to record
        Long hash2;
        try (SnapshotManager snapshotManager = entityHashManager.open(++topologyTime);
             TableWriter entitiesWriter = ENTITY_TABLE.open(sink)) {
            baseEntity.set(ENTITY_TYPE_AS_TYPE, EntityType.PHYSICAL_MACHINE.name());
            assertThat(snapshotManager.updateEntityHash(baseEntity), notNullValue());
            snapshotManager.processChanges(entitiesWriter);
            hash2 = entityHashManager.getEntityHash(oid);
            assertThat(entityHashManager.getEntityHash(oid), is(hash2));
            assertThat(entityHashManager.getHashLastSeen(hash2), is(topologyTime));
            // hash should be different, and original hash should no longer be tracked
            assertThat(hash2, not(hash1));
            assertThat(entityHashManager.getHashLastSeen(hash1), nullValue());
            // undo change
            baseEntity.set(ENTITY_TYPE_AS_TYPE, EntityType.VIRTUAL_MACHINE.name());
        }
        // change environment... similar checks as above
        Long hash3;
        try (SnapshotManager snapshotManager = entityHashManager.open(++topologyTime);
             TableWriter entitiesWriter = ENTITY_TABLE.open(sink)) {
            baseEntity.set(ENVIRONMENT_TYPE, EnvironmentType.CLOUD.name());
            assertThat(snapshotManager.updateEntityHash(baseEntity), notNullValue());
            hash3 = entityHashManager.getEntityHash(oid);
            snapshotManager.processChanges(entitiesWriter);
            assertThat(entityHashManager.getEntityHash(oid), is(hash3));
            assertThat(entityHashManager.getHashLastSeen(hash3), is(topologyTime));
            assertThat(hash3, not(hash1));
            assertThat(hash3, not(hash2));
            assertThat(entityHashManager.getHashLastSeen(hash2), nullValue());
            baseEntity.set(ENVIRONMENT_TYPE, EnvironmentType.ON_PREM.name());
        }

        // same deal, change entity state
        Long hash4;
        try (SnapshotManager snapshotManager = entityHashManager.open(++topologyTime);
             TableWriter entitiesWriter = ENTITY_TABLE.open(sink)) {
            baseEntity.set(ENTITY_STATE, EntityState.MAINTENANCE.name());
            assertThat(snapshotManager.updateEntityHash(baseEntity), notNullValue());
            hash4 = entityHashManager.getEntityHash(oid);
            snapshotManager.processChanges(entitiesWriter);
            assertThat(entityHashManager.getEntityHash(oid), is(hash4));
            assertThat(entityHashManager.getHashLastSeen(hash4), is(topologyTime));
            assertThat(hash4, not(hash1));
            assertThat(hash4, not(hash3));
            assertThat(entityHashManager.getHashLastSeen(hash3), nullValue());
            baseEntity.set(ENTITY_STATE, EntityState.ACTIVE.name());
        }

        // change attrs
        Long hash5;
        try (SnapshotManager snapshotManager = entityHashManager.open(++topologyTime);
             TableWriter entitiesWriter = ENTITY_TABLE.open(sink)) {
            baseEntity.set(ATTRS, new JsonString("{\"x\": 1}"));
            assertThat(snapshotManager.updateEntityHash(baseEntity), notNullValue());
            hash5 = entityHashManager.getEntityHash(oid);
            snapshotManager.processChanges(entitiesWriter);
            assertThat(entityHashManager.getEntityHash(oid), is(hash5));
            assertThat(entityHashManager.getHashLastSeen(hash5), is(topologyTime));
            assertThat(hash5, not(hash1));
            assertThat(hash5, not(hash4));
            assertThat(entityHashManager.getHashLastSeen(hash4), nullValue());
            baseEntity.set(ATTRS, new JsonString("{}"));
        }

        // change scope
        Long hash6;
        try (SnapshotManager snapshotManager = entityHashManager.open(++topologyTime);
             TableWriter entitiesWriter = ENTITY_TABLE.open(sink)) {
            baseEntity.set(SCOPED_OIDS, new Long[]{1234L});
            assertThat(snapshotManager.updateEntityHash(baseEntity), notNullValue());
            hash6 = entityHashManager.getEntityHash(oid);
            snapshotManager.processChanges(entitiesWriter);
            assertThat(entityHashManager.getEntityHash(oid), is(hash6));
            assertThat(entityHashManager.getHashLastSeen(hash6), is(topologyTime));
            assertThat(hash6, not(hash1));
            assertThat(hash6, not(hash5));
            assertThat(entityHashManager.getHashLastSeen(hash5), nullValue());
            baseEntity.set(SCOPED_OIDS, new Long[]{});
        }

        // entity is back to original state... make sure we see first hash again
        Long hash7;
        try (SnapshotManager snapshotManager = entityHashManager.open(++topologyTime);
             TableWriter entitiesWriter = ENTITY_TABLE.open(sink)) {
            assertThat(snapshotManager.updateEntityHash(baseEntity), notNullValue());
            hash7 = entityHashManager.getEntityHash(oid);
            snapshotManager.processChanges(entitiesWriter);
            assertThat(entityHashManager.getEntityHash(oid), is(hash7));
            assertThat(entityHashManager.getHashLastSeen(hash7), is(topologyTime));
            assertThat(hash7, is(hash1));
            assertThat(hash7, not(hash6));
        }
    }

    /**
     * Test that an attempt to open one snapshot manager when another is active fails.
     */
    @Test(expected = IllegalStateException.class)
    public void testCantOpenTwoSnapshotManagersAtOnce() {
        try (SnapshotManager sm1 = entityHashManager.open(1L)) {
            try (SnapshotManager m2 = entityHashManager.open(2L)) {
            }
        }
    }

    /**
     * Check that snapshot managers can be opened in sequence.
     */
    @Test
    public void testCanOpenTwoSnapshotManagersInSequence() {
        try (SnapshotManager sm = entityHashManager.open(1L)) {
        }
        try (SnapshotManager sm = entityHashManager.open(2L)) {
        }
    }

    /**
     * Test that a snapshot manager may not specify sime time as the previous one.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSnapshotTimesCannotRemainUnchanged() {
        try (SnapshotManager sm = entityHashManager.open(1L)) {
        }
        try (SnapshotManager sm = entityHashManager.open(1L)) {
        }
    }

    /**
     * Test that a snapshot manager may not specify a time prior to the previous one.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSnapshotTimesCannotDecrease() {
        try (SnapshotManager sm = entityHashManager.open(2L)) {
        }
        try (SnapshotManager sm = entityHashManager.open(1L)) {
        }
    }

    /**
     * When an entity previously in the topology drops out, we should stop tracking the hash
     * associated with that OID.
     *
     * @throws SQLException if there's a db error
     */
    @Test
    public void testThatOrphanedEntitiesAreRemoved() throws SQLException {
        long baseOid = baseEntity.get(ENTITY_OID_AS_OID);
        try (SnapshotManager sm = entityHashManager.open(1L);
             TableWriter entitiesWriter = ENTITY_TABLE.open(sink)) {
            sm.updateEntityHash(baseEntity);
            sm.processChanges(entitiesWriter);
            assertThat(entityHashManager.getEntityHash(baseOid), notNullValue());
        }
        try (SnapshotManager sm = entityHashManager.open(2L);
             TableWriter entitiesWriter = ENTITY_TABLE.open(sink)) {
            baseEntity.set(ENTITY_OID_AS_OID, baseEntity.get(ENTITY_OID_AS_OID) + 1);
            sm.updateEntityHash(baseEntity);
            sm.processChanges(entitiesWriter);
            assertThat(entityHashManager.getEntityHash(baseOid), is(nullValue()));
        }
    }

    /**
     * Test that first-seen and last-seen values added to current-topology entity recores are
     * correct.
     *
     * @throws SQLException if there's a db issue
     */
    @Test
    public void testThatFirstAndLastSeenValuesAreCorrect() throws SQLException {
        long updateInterval = TimeUnit.MINUTES.toMillis(config.lastSeenUpdateIntervalMinutes());
        long updateFuzz = TimeUnit.MINUTES.toMillis(config.lastSeenAdditionalFuzzMinutes());
        try (SnapshotManager sm = entityHashManager.open(0L);
             TableWriter entitiesWriter = ENTITY_TABLE.open(sink)) {
            final Record r = new Record(ENTITY_TABLE);
            sm.setEntityTimes(r);
            sm.processChanges(entitiesWriter);
            assertThat(r.get(FIRST_SEEN).getTime(), is(0L));
            assertThat(r.get(LAST_SEEN).getTime(), is(updateInterval + updateFuzz));
        }
        try (SnapshotManager sm = entityHashManager.open(updateInterval / 2);
             TableWriter entitiesWriter = ENTITY_TABLE.open(sink)) {
            final Record r = new Record(ENTITY_TABLE);
            sm.setEntityTimes(r);
            sm.processChanges(entitiesWriter);
            assertThat(r.get(FIRST_SEEN).getTime(), is(updateInterval / 2));
            assertThat(r.get(LAST_SEEN).getTime(), is(updateInterval + updateFuzz));
        }
        try (SnapshotManager sm = entityHashManager.open(updateInterval - 1);
             TableWriter entitiesWriter = ENTITY_TABLE.open(sink)) {
            final Record r = new Record(ENTITY_TABLE);
            sm.setEntityTimes(r);
            sm.processChanges(entitiesWriter);
            assertThat(r.get(FIRST_SEEN).getTime(), is(updateInterval - 1));
            assertThat(r.get(LAST_SEEN).getTime(), is(updateInterval + updateFuzz));
        }
        try (SnapshotManager sm = entityHashManager.open(updateInterval);
             TableWriter entitiesWriter = ENTITY_TABLE.open(sink)) {
            final Record r = new Record(ENTITY_TABLE);
            sm.setEntityTimes(r);
            sm.processChanges(entitiesWriter);
            assertThat(r.get(FIRST_SEEN).getTime(), is(updateInterval));
            assertThat(r.get(LAST_SEEN).getTime(), is(2 * updateInterval + updateFuzz));
        }
    }

    /**
     * Test that the {@link SnapshotManager#setEntityTimes(Record)} method correctly sets first and
     * last times in the record it's given.
     */
    @Test
    public void testFirstLastSeenTimesSetCorrectly() {
        try (SnapshotManager sm = entityHashManager.open(0L)) {
            Record r = new Record(ENTITY_TABLE);
            sm.setEntityTimes(r);
            // every record goes in with current snapshot time as "first_seen". That will only
            // make it into the DB if the OID has never been recorded before.
            assertThat(r.get(FIRST_SEEN).getTime(), is(0L));
            // last-seen will should be time of next expected last-seen update, plus some extra
            // fuzz. In this case we haven't done a last-seen update yet, so next one should be]
            // current time + interval + fuzz
            assertThat(r.get(LAST_SEEN).getTime(), is(
                    TimeUnit.MINUTES.toMillis(config.lastSeenUpdateIntervalMinutes())
                            + TimeUnit.MINUTES.toMillis(config.lastSeenAdditionalFuzzMinutes())));
        }
    }

    /**
     * Test that when entities drop out of the topology, the records sent to update their last-seen
     * values are correct.
     *
     * @throws SQLException if there's a DB error
     */
    @Test
    public void testCorrectedUpdateRecordsForDroppedHashes() throws SQLException {
        final Long baseOid = baseEntity.get(ENTITY_OID_AS_OID);
        try (SnapshotManager sm = entityHashManager.open(1L);
             TableWriter entitiesWriter = ENTITY_TABLE.open(sink)) {
            sm.updateEntityHash(baseEntity);
            sm.setEntityTimes(baseEntity);
            sm.processChanges(entitiesWriter);
        }
        assertThat(sinkCapture, is(empty()));
        long hash = entityHashManager.getEntityHash(baseOid);
        try (SnapshotManager sm = entityHashManager.open(2L);
             TableWriter entitiesWriter = ENTITY_TABLE.open(sink)) {
            baseEntity.set(ENTITY_OID_AS_OID, baseOid + 1);
            sm.updateEntityHash(baseEntity);
            sm.setEntityTimes(baseEntity);
            sm.processChanges(entitiesWriter);
        }
        assertThat(sinkCapture.size(), is(1));
        Record r = sinkCapture.get(0);
        assertThat(r.get(ENTITY_HASH_AS_HASH), is(hash));
        assertThat(r.get(LAST_SEEN).getTime(), is(1L));
    }

    /**
     * Test that a last-seen update kicks off, any hash that was already present in the topology and
     * remains so is included in the update.
     *
     * @throws SQLException if there's a db error
     */
    @Test
    public void testCorrectUpdateRecordsForPeriodicUpdate() throws SQLException {
        final long updatePeriod = TimeUnit.MINUTES.toMillis(config.lastSeenUpdateIntervalMinutes());
        final long updateFuzz = TimeUnit.MINUTES.toMillis(config.lastSeenAdditionalFuzzMinutes());
        final long baseOid = baseEntity.get(ENTITY_OID_AS_OID);
        try (SnapshotManager sm = entityHashManager.open(0L);
             TableWriter entitiesWriter = ENTITY_TABLE.open(sink)) {
            sm.updateEntityHash(baseEntity);
            sm.setEntityTimes(baseEntity);
            sm.processChanges(entitiesWriter);
        }
        assertThat(sinkCapture, is(empty()));
        long hash = entityHashManager.getEntityHash(baseOid);
        try (SnapshotManager sm = entityHashManager.open(updatePeriod);
             TableWriter entitiesWriter = ENTITY_TABLE.open(sink)) {
            sm.updateEntityHash(baseEntity);
            sm.setEntityTimes(baseEntity);
            sm.processChanges(entitiesWriter);
        }
        assertThat(sinkCapture.size(), is(1));
        Record r = sinkCapture.get(0);
        assertThat(r.get(ENTITY_HASH_AS_HASH), is(hash));
        assertThat(r.get(LAST_SEEN).getTime(), is(2 * updatePeriod + updateFuzz));
    }
}
