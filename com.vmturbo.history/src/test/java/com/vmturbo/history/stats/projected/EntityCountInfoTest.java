package com.vmturbo.history.stats.projected;

import static com.vmturbo.history.stats.projected.ProjectedStatsTestConstants.COMMODITY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Collections;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.common.stats.StatsAccumulator;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class EntityCountInfoTest {

    /**
     * Test that entity of all states are considered in the projected stats.
     */
    @Test
    public void testEntityCountBuilderIncludingAllStates() {
        EntityCountInfo.Builder builder = EntityCountInfo.newBuilder();
        for (EntityState state : EntityState.values()) {
            // add a new entity for each EntityState
            builder.addEntity(TopologyEntityDTO.newBuilder()
                    // the oid doesn't really matter here, just use the state integer value as oid
                    .setOid(state.getNumber())
                    .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                    .setEntityState(state)
                    .build());
        }
        EntityCountInfo countInfo = builder.build();
        assertEquals(EntityState.values().length, countInfo.entityCount(EntityType.PHYSICAL_MACHINE));
    }

    @Test
    public void testEntityCountStatRecord() {
        final float value = 10;
        final StatRecord record = EntityCountInfo.constructPostProcessRecord(COMMODITY, value);

        final StatValue expectedValue = StatValue.newBuilder()
                .setMin(value)
                .setMax(value)
                .setAvg(value)
                .setTotal(value)
                .build();
        final StatRecord expectedRecord = StatRecord.newBuilder()
                .setName(COMMODITY)
                .setRelation(RelationType.METRICS.getLiteral())
                .setCurrentValue(value)
                .setCapacity(StatsAccumulator.singleStatValue(value))
                .setUsed(expectedValue)
                .setPeak(expectedValue)
                .setValues(expectedValue)
                .build();

        assertEquals(expectedRecord, record);
    }

    @Test
    public void testEntityCountVM() {
        EntityCountInfo info = new EntityCountInfo(
                ImmutableMap.of(EntityType.VIRTUAL_MACHINE.getNumber(), 10));
        final StatRecord record = info.getCountRecord(StringConstants.NUM_VMS).get();
        assertEquals(10, record.getCurrentValue(), 0);
    }

    @Test
    public void testEntityCountVMZero() {
        EntityCountInfo info = new EntityCountInfo(Collections.emptyMap());
        final StatRecord record = info.getCountRecord(StringConstants.NUM_VMS).get();
        assertEquals(0, record.getCurrentValue(), 0);
    }

    @Test
    public void testEntityCountPM() {
        EntityCountInfo info = new EntityCountInfo(
                ImmutableMap.of(EntityType.PHYSICAL_MACHINE.getNumber(), 10));
        final StatRecord record = info.getCountRecord(StringConstants.NUM_HOSTS).get();
        assertEquals(10, record.getCurrentValue(), 0);
    }

    @Test
    public void testEntityCountPMZero() {
        EntityCountInfo info = new EntityCountInfo(Collections.emptyMap());
        final StatRecord record = info.getCountRecord(StringConstants.NUM_HOSTS).get();
        assertEquals(0, record.getCurrentValue(), 0);
    }

    @Test
    public void testEntityCountContainer() {
        EntityCountInfo info = new EntityCountInfo(
                ImmutableMap.of(EntityType.CONTAINER.getNumber(), 10));
        final StatRecord record = info.getCountRecord(StringConstants.NUM_CONTAINERS).get();
        assertEquals(10, record.getCurrentValue(), 0);
    }

    @Test
    public void testEntityCountContainerZero() {
        EntityCountInfo info = new EntityCountInfo(Collections.emptyMap());
        final StatRecord record = info.getCountRecord(StringConstants.NUM_CONTAINERS).get();
        assertEquals(0, record.getCurrentValue(), 0);
    }

    @Test
    public void testEntityCountStorage() {
        EntityCountInfo info = new EntityCountInfo(
                ImmutableMap.of(EntityType.STORAGE.getNumber(), 10));
        final StatRecord record = info.getCountRecord(StringConstants.NUM_STORAGES).get();
        assertEquals(10, record.getCurrentValue(), 0);
    }

    @Test
    public void testEntityCountStorageZero() {
        EntityCountInfo info = new EntityCountInfo(Collections.emptyMap());
        final StatRecord record = info.getCountRecord(StringConstants.NUM_STORAGES).get();
        assertEquals(0, record.getCurrentValue(), 0);
    }

    @Test
    public void testEntityCountVMperPM() {
        EntityCountInfo info = new EntityCountInfo(ImmutableMap.of(
                EntityType.VIRTUAL_MACHINE.getNumber(), 10,
                EntityType.PHYSICAL_MACHINE.getNumber(), 5));
        final StatRecord record = info.getCountRecord(StringConstants.NUM_VMS_PER_HOST).get();
        assertEquals(2, record.getCurrentValue(), 0);
    }

    @Test
    public void testEntityCountVMperPMNoHosts() {
        EntityCountInfo info = new EntityCountInfo(ImmutableMap.of(
                EntityType.VIRTUAL_MACHINE.getNumber(), 10));
        final StatRecord record = info.getCountRecord(StringConstants.NUM_VMS_PER_HOST).get();
        assertEquals(0, record.getCurrentValue(), 0);
    }

    @Test
    public void testEntityCountVMperStorage() {
        EntityCountInfo info = new EntityCountInfo(ImmutableMap.of(
                EntityType.VIRTUAL_MACHINE.getNumber(), 10,
                EntityType.STORAGE.getNumber(), 5));
        final StatRecord record = info.getCountRecord(StringConstants.NUM_VMS_PER_STORAGE).get();
        assertEquals(2, record.getCurrentValue(), 0);
    }

    @Test
    public void testEntityCountVMperStorageNoStorages() {
        EntityCountInfo info = new EntityCountInfo(ImmutableMap.of(
                EntityType.VIRTUAL_MACHINE.getNumber(), 10));
        final StatRecord record = info.getCountRecord(StringConstants.NUM_VMS_PER_STORAGE).get();
        assertEquals(0, record.getCurrentValue(), 0);
    }

    @Test
    public void testEntityCountContainerPerPM() {
        EntityCountInfo info = new EntityCountInfo(ImmutableMap.of(
                EntityType.CONTAINER.getNumber(), 10,
                EntityType.PHYSICAL_MACHINE.getNumber(), 5));
        final StatRecord record = info.getCountRecord(StringConstants.NUM_CNT_PER_HOST).get();
        assertEquals(2, record.getCurrentValue(), 0);
    }

    @Test
    public void testEntityCountContainerPerPMNoHosts() {
        EntityCountInfo info = new EntityCountInfo(ImmutableMap.of(
                EntityType.CONTAINER.getNumber(), 10));
        final StatRecord record = info.getCountRecord(StringConstants.NUM_CNT_PER_HOST).get();
        assertEquals(0, record.getCurrentValue(), 0);
    }

    @Test
    public void testEntityCountContainerPerStorage() {
        EntityCountInfo info = new EntityCountInfo(ImmutableMap.of(
                EntityType.CONTAINER.getNumber(), 10,
                EntityType.STORAGE.getNumber(), 5));
        final StatRecord record = info.getCountRecord(StringConstants.NUM_CNT_PER_STORAGE).get();
        assertEquals(2, record.getCurrentValue(), 0);
    }

    @Test
    public void testEntityCountContainerPerStorageNoStorages() {
        EntityCountInfo info = new EntityCountInfo(ImmutableMap.of(
                EntityType.CONTAINER.getNumber(), 10));
        final StatRecord record = info.getCountRecord(StringConstants.NUM_CNT_PER_STORAGE).get();
        assertEquals(0, record.getCurrentValue(), 0);
    }

    @Test
    public void testEntityCountInvalidStat() {
        EntityCountInfo info = new EntityCountInfo(Collections.emptyMap());
        assertFalse(info.getCountRecord("beer").isPresent());
    }
}
