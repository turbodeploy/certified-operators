package com.vmturbo.history.stats.projected;

import static com.vmturbo.history.stats.projected.ProjectedStatsTestConstants.COMMODITY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Collections;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.StringConstants;
import com.vmturbo.history.stats.StatsAccumulator;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class EntityCountInfoTest {

    @Test
    public void testEntityCountBuilderOnlyPoweredOn() {
        final TopologyEntityDTO entity = TopologyEntityDTO.newBuilder()
                .setOid(1)
                .setEntityType(EntityType.PHYSICAL_MACHINE.getNumber())
                .build();
        EntityCountInfo.Builder builder = EntityCountInfo.newBuilder();
        for (EntityState state : EntityState.values()) {
            builder.addEntity(TopologyEntityDTO.newBuilder(entity).setEntityState(state).build());
        }
        EntityCountInfo countInfo = builder.build();
        assertEquals(1L, countInfo.entityCount(EntityType.PHYSICAL_MACHINE));
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
                ImmutableMap.of(EntityType.VIRTUAL_MACHINE.getNumber(), 10L));
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
                ImmutableMap.of(EntityType.PHYSICAL_MACHINE.getNumber(), 10L));
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
                ImmutableMap.of(EntityType.CONTAINER.getNumber(), 10L));
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
                ImmutableMap.of(EntityType.STORAGE.getNumber(), 10L));
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
                EntityType.VIRTUAL_MACHINE.getNumber(), 10L,
                EntityType.PHYSICAL_MACHINE.getNumber(), 5L));
        final StatRecord record = info.getCountRecord(StringConstants.NUM_VMS_PER_HOST).get();
        assertEquals(2, record.getCurrentValue(), 0);
    }

    @Test
    public void testEntityCountVMperPMNoHosts() {
        EntityCountInfo info = new EntityCountInfo(ImmutableMap.of(
                EntityType.VIRTUAL_MACHINE.getNumber(), 10L));
        final StatRecord record = info.getCountRecord(StringConstants.NUM_VMS_PER_HOST).get();
        assertEquals(0, record.getCurrentValue(), 0);
    }

    @Test
    public void testEntityCountVMperStorage() {
        EntityCountInfo info = new EntityCountInfo(ImmutableMap.of(
                EntityType.VIRTUAL_MACHINE.getNumber(), 10L,
                EntityType.STORAGE.getNumber(), 5L));
        final StatRecord record = info.getCountRecord(StringConstants.NUM_VMS_PER_STORAGE).get();
        assertEquals(2, record.getCurrentValue(), 0);
    }

    @Test
    public void testEntityCountVMperStorageNoStorages() {
        EntityCountInfo info = new EntityCountInfo(ImmutableMap.of(
                EntityType.VIRTUAL_MACHINE.getNumber(), 10L));
        final StatRecord record = info.getCountRecord(StringConstants.NUM_VMS_PER_STORAGE).get();
        assertEquals(0, record.getCurrentValue(), 0);
    }

    @Test
    public void testEntityCountContainerPerPM() {
        EntityCountInfo info = new EntityCountInfo(ImmutableMap.of(
                EntityType.CONTAINER.getNumber(), 10L,
                EntityType.PHYSICAL_MACHINE.getNumber(), 5L));
        final StatRecord record = info.getCountRecord(StringConstants.NUM_CNT_PER_HOST).get();
        assertEquals(2, record.getCurrentValue(), 0);
    }

    @Test
    public void testEntityCountContainerPerPMNoHosts() {
        EntityCountInfo info = new EntityCountInfo(ImmutableMap.of(
                EntityType.CONTAINER.getNumber(), 10L));
        final StatRecord record = info.getCountRecord(StringConstants.NUM_CNT_PER_HOST).get();
        assertEquals(0, record.getCurrentValue(), 0);
    }

    @Test
    public void testEntityCountContainerPerStorage() {
        EntityCountInfo info = new EntityCountInfo(ImmutableMap.of(
                EntityType.CONTAINER.getNumber(), 10L,
                EntityType.STORAGE.getNumber(), 5L));
        final StatRecord record = info.getCountRecord(StringConstants.NUM_CNT_PER_STORAGE).get();
        assertEquals(2, record.getCurrentValue(), 0);
    }

    @Test
    public void testEntityCountContainerPerStorageNoStorages() {
        EntityCountInfo info = new EntityCountInfo(ImmutableMap.of(
                EntityType.CONTAINER.getNumber(), 10L));
        final StatRecord record = info.getCountRecord(StringConstants.NUM_CNT_PER_STORAGE).get();
        assertEquals(0, record.getCurrentValue(), 0);
    }

    @Test
    public void testEntityCountInvalidStat() {
        EntityCountInfo info = new EntityCountInfo(Collections.emptyMap());
        assertFalse(info.getCountRecord("beer").isPresent());
    }
}
