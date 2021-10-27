package com.vmturbo.extractor.topology;

import static com.vmturbo.extractor.models.ModelDefinitions.ATTRS;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_OID_AS_OID;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_TABLE;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_TYPE_AS_TYPE_ENUM;
import static com.vmturbo.extractor.models.ModelDefinitions.ENVIRONMENT_TYPE_ENUM;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.UpdateConditionStep;
import org.jooq.UpdateSetFirstStep;
import org.jooq.UpdateSetMoreStep;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.utils.DataPacks.LongDataPack;
import com.vmturbo.extractor.models.Column.JsonString;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.enums.EnvironmentType;

/**
 * Tests for the {@link EntityHashManager}.
 */
public class EntityHashManagerTest {
    private static final Logger logger = LogManager.getLogger();
    private EntityHashManager entityHashManager;
    private Record baseEntity;
    private LongDataPack oidPack;
    private int baseEntityIId;
    private final DSLContext dsl = getMockDsl();
    private final int dbFetchSize = 2;

    /**
     * Set up for tests, by creating an entities record that will be used in the tests, and a table
     * writer based on a mock sink.
     */
    @Before
    public void before() {
        oidPack = new LongDataPack();
        entityHashManager = new EntityHashManager(oidPack, dbFetchSize);
        entityHashManager.injectPriorTopology();
        baseEntity = new Record(ENTITY_TABLE);
        baseEntity.set(ENTITY_OID_AS_OID, 1L);
        baseEntity.set(ENTITY_TYPE_AS_TYPE_ENUM, EntityType.VIRTUAL_MACHINE);
        baseEntity.set(ENVIRONMENT_TYPE_ENUM, EnvironmentType.ON_PREM);
        baseEntity.set(ATTRS, new JsonString("{}"));
        baseEntityIId = oidPack.toIndex(baseEntity.get(ENTITY_OID_AS_OID));
    }

    /**
     * Test that the entities hash manager properly tracks entity state through a series of changes,
     * including reverting back to a previously seen state.
     *
     */
    @Test
    public void testEntityHashChangesAreNoticed() {
        final Long oid = baseEntity.get(ENTITY_OID_AS_OID);

        // entity initially seen at time 1; new entity => needs to be recorded
        TopologyInfo topologyInfo = getTopoInfo(1000L);
        Long hash1;
        entityHashManager.open(topologyInfo, dsl);
        assertThat(entityHashManager.processEntity(baseEntity), is(true));
        hash1 = entityHashManager.getEntityHash(oid);
        entityHashManager.close();
        assertThat(entityHashManager.getPriorHashes(), is(aMapWithSize(1)));
        assertThat(entityHashManager.getPriorHashes().get(baseEntityIId), is(hash1));

        // same entity seen at time 2, no change so no need to record
        topologyInfo = getTopoInfo(2000);
        entityHashManager.open(topologyInfo, dsl);
        assertThat(entityHashManager.processEntity(baseEntity), is(false));
        assertThat(entityHashManager.getEntityHash(oid), is(hash1));
        entityHashManager.close();
        assertThat(entityHashManager.getPriorHashes(), is(aMapWithSize(1)));
        assertThat(entityHashManager.getPriorHashes().get(baseEntityIId), is(hash1));

        // change entity type (unrealistic, but...); change => need to record
        topologyInfo = getTopoInfo(3000);
        entityHashManager.open(topologyInfo, null);
        Long hash2;
        baseEntity.set(ENTITY_TYPE_AS_TYPE_ENUM, EntityType.PHYSICAL_MACHINE);
        assertThat(entityHashManager.processEntity(baseEntity), is(true));
        hash2 = entityHashManager.getEntityHash(oid);
        //hash should be different
        assertThat(hash2, not(hash1));
        entityHashManager.close();
        assertThat(entityHashManager.getPriorHashes(), is(aMapWithSize(1)));
        assertThat(entityHashManager.getPriorHashes().get(baseEntityIId), is(hash2));
        // undo change
        baseEntity.set(ENTITY_TYPE_AS_TYPE_ENUM, EntityType.VIRTUAL_MACHINE);

        // and change entity type... similar checks to above
        topologyInfo = getTopoInfo(4000);
        entityHashManager.open(topologyInfo, dsl);
        Long hash3;
        baseEntity.set(ENVIRONMENT_TYPE_ENUM, EnvironmentType.CLOUD);
        assertThat(entityHashManager.processEntity(baseEntity), is(true));
        hash3 = entityHashManager.getEntityHash(oid);
        assertThat(hash3, not(hash2));
        assertThat(hash3, not(hash1));
        entityHashManager.close();
        assertThat(entityHashManager.getPriorHashes(), is(aMapWithSize(1)));
        assertThat(entityHashManager.getPriorHashes().get(baseEntityIId), is(hash3));
        baseEntity.set(ENVIRONMENT_TYPE_ENUM, EnvironmentType.ON_PREM);

        // same deal, change attrs
        topologyInfo = getTopoInfo(5000);
        entityHashManager.open(topologyInfo, dsl);
        Long hash4;
        baseEntity.set(ATTRS, new JsonString("{\"x\": 1}"));
        assertThat(entityHashManager.processEntity(baseEntity), is(true));
        hash4 = entityHashManager.getEntityHash(oid);
        assertThat(hash4, not(hash1));
        assertThat(hash4, not(hash3));
        entityHashManager.close();
        assertThat(entityHashManager.getPriorHashes(), is(aMapWithSize(1)));
        assertThat(entityHashManager.getPriorHashes().get(baseEntityIId), is(hash4));
        baseEntity.set(ATTRS, new JsonString("{}"));

        // entity is back to original state... make sure we see first hash again
        topologyInfo = getTopoInfo(6000);
        entityHashManager.open(topologyInfo, dsl);
        Long hash5;
        assertThat(entityHashManager.processEntity(baseEntity), is(true));
        hash5 = entityHashManager.getEntityHash(oid);
        assertThat(hash5, is(hash1));
        entityHashManager.close();
        assertThat(entityHashManager.getPriorHashes(), is(aMapWithSize(1)));
        assertThat(entityHashManager.getPriorHashes().get(baseEntityIId), is(hash5));
    }


    /**
     * Test that an entity that is present in the first topology but not the next is dropped.
     */
    @Test
    public void testDisappearingEntitiesAreDropped() {
        entityHashManager.open(getTopoInfo(1000L), dsl);
        entityHashManager.processEntity(baseEntity);
        entityHashManager.close();
        entityHashManager.open(getTopoInfo(2000L), dsl);
        assertThat(entityHashManager.getDroppedIids(), contains(baseEntityIId));
        entityHashManager.close();
        assertThat(entityHashManager.getPriorHashes().containsKey(baseEntityIId), is(false));
    }

    /**
     * Test that on first topology after a restart, entities that were present in the prior
     * topology are re-upserted even if they have not changed, and entities that were present but
     * no longer are are dropped.
     */
    @Test
    public void testAllEntitiesUpsertedAfterRestart() {
        entityHashManager.open(getTopoInfo(1000), dsl);
        entityHashManager.processEntity(baseEntity);
        final long hash = entityHashManager.getEntityHash(baseEntity.get(ENTITY_OID_AS_OID));
        entityHashManager.close();
        EntityHashManager restartHashManager = new EntityHashManager(new LongDataPack(), dbFetchSize);
        restartHashManager.injectPriorTopology(baseEntity.get(ENTITY_OID_AS_OID), 1000L, 2000L);
        restartHashManager.open(getTopoInfo(2000), dsl);
        assertThat(restartHashManager.getEntityHash(baseEntity.get(ENTITY_OID_AS_OID)), is(0L));
        assertThat(restartHashManager.processEntity(baseEntity), is(true));
        assertThat(restartHashManager.getEntityHash(baseEntity.get(ENTITY_OID_AS_OID)), is(hash));
        assertThat(restartHashManager.getDroppedIids(), containsInAnyOrder(
                oidPack.toIndex(1000L), oidPack.toIndex(2000L)));
        restartHashManager.close();
        assertThat(restartHashManager.getPriorHashes().containsKey(oidPack.toIndex(1000L)), is(false));
        assertThat(restartHashManager.getPriorHashes().containsKey(oidPack.toIndex(2000L)), is(false));
    }

    /**
     * Test that an attempt to open one topology when another is active fails.
     */
    @Test(expected = IllegalStateException.class)
    public void testCantOpenTwoTopologiesAtOnce() {
        entityHashManager.open(getTopoInfo(1000), dsl);
        entityHashManager.open(getTopoInfo(2000), dsl);
    }


    /**
     * Test that we cannot open a topology with the same timestamp as a prior one.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testTopologyTimesCannotRemainUnchanged() {
        TopologyInfo topologyInfo = getTopoInfo(1000);
        entityHashManager.open(topologyInfo, dsl);
        entityHashManager.close();
        entityHashManager.open(topologyInfo, dsl);
    }

    /**
     * Test that we cannot open a topology with a time prior to the previous one.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testTopologyTimesCannotDecrease() {
        TopologyInfo topologyInfo = getTopoInfo(2000);
        entityHashManager.open(topologyInfo, dsl);
        entityHashManager.close();
        topologyInfo = getTopoInfo(1000);
        entityHashManager.open(topologyInfo, dsl);
    }

    /**
     * Test that we cannot close the current topology without first opening it.
     */
    @Test(expected = IllegalStateException.class)
    public void testCannotCloseWithoutOpenTopology() {
        entityHashManager.close();
    }

    private TopologyInfo getTopoInfo(long time) {
        return TopologyInfo.newBuilder().setCreationTime(time).build();
    }

    private DSLContext getMockDsl() {
        final DSLContext mock = mock(DSLContext.class);
        final UpdateSetFirstStep<?> updateSetFirstStep = mock(UpdateSetFirstStep.class);
        when(mock.update(any(Table.class))).thenReturn(updateSetFirstStep);
        final UpdateSetMoreStep<?> updateSetMoreStep = mock(UpdateSetMoreStep.class);
        when(updateSetFirstStep.set(any(Field.class), any(OffsetDateTime.class))).thenReturn(updateSetMoreStep);
        final UpdateConditionStep updateConditionStep = mock(UpdateConditionStep.class);
        when(updateSetMoreStep.where(any(Condition.class))).thenReturn(updateConditionStep);
        when(updateConditionStep.execute()).thenReturn(0);
        return mock;
    }
}
