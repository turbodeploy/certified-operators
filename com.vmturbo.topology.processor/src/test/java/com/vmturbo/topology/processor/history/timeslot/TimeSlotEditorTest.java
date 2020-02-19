package com.vmturbo.topology.processor.history.timeslot;

import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import com.google.common.collect.ImmutableList;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;
import com.vmturbo.topology.processor.history.BaseGraphRelatedTest;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryAggregationContext;
import com.vmturbo.topology.processor.history.HistoryCalculationException;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;

/**
 * Unit tests for TimeSlotEditor.
 */
public class TimeSlotEditorTest extends BaseGraphRelatedTest {
    private static final TimeslotHistoricalEditorConfig CONFIG =
                    new TimeslotHistoricalEditorConfig(1, 1, 1, 1, 1, 1, Clock.systemUTC());
    private static final long OID1 = 12;
    private static final long OID2 = 15;
    private static final long OID3 = 16;
    private static final long PERIOD1 = 100;
    private static final long PERIOD2 = 200;
    private static final CommodityType CT = CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.POOL_CPU_VALUE).build();

    /**
     * Test the preparation tasks creation.
     * That tasks are created for uninitialized commodities for different observation windows.
     *
     * @throws InterruptedException when interrupted
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testCreatePreparationTasks() throws HistoryCalculationException, InterruptedException {
        Map<Long, Builder> topologyBuilderMap = new HashMap<>();
        Map<Long, EntitySettings> entitySettings = new HashMap<>();

        addEntityWithSetting(OID1,
                             EntityType.BUSINESS_USER_VALUE,
                             EntitySettingSpecs.MaxObservationPeriodDesktopPool,
                             PERIOD1,
                             topologyBuilderMap, entitySettings);
        addEntityWithSetting(OID2,
                             EntityType.BUSINESS_USER_VALUE,
                             EntitySettingSpecs.MaxObservationPeriodDesktopPool,
                             PERIOD1,
                             topologyBuilderMap, entitySettings);
        addEntityWithSetting(OID3,
                             EntityType.BUSINESS_USER_VALUE,
                             EntitySettingSpecs.MaxObservationPeriodDesktopPool,
                             PERIOD2,
                             topologyBuilderMap, entitySettings);
        GraphWithSettings graphWithSettings = new GraphWithSettings(TopologyEntityTopologyGraphCreator
                                                  .newGraph(topologyBuilderMap),
                                  entitySettings,
                                  Collections.emptyMap());

        EntityCommodityReference ref1 = new EntityCommodityReference(OID1, CT, null);
        EntityCommodityReference ref2 = new EntityCommodityReference(OID2, CT, null);
        EntityCommodityReference ref3 = new EntityCommodityReference(OID3, CT, null);

        TimeslotEditorCacheAccess editor = new TimeslotEditorCacheAccess(CONFIG, null);
        List<EntityCommodityReference> comms = ImmutableList.of(ref1, ref2, ref3);
        HistoryAggregationContext context = new HistoryAggregationContext(graphWithSettings, false);
        editor.initContext(context, comms);
        // as if already got data for oid2
        editor.getCache().put(new EntityCommodityFieldReference(ref2, CommodityField.USED),
                              new TimeSlotCommodityData());

        List<? extends Callable<List<EntityCommodityFieldReference>>> tasks = editor
                        .createPreparationTasks(context, comms);

        // should have 2 tasks - for ref1 and ref3
        Assert.assertEquals(2, tasks.size());
    }

    /**
     * Access to the editor cached data.
     */
    private static class TimeslotEditorCacheAccess extends TimeSlotEditor {
        /**
         * Construct the instance.
         *
         * @param config configuration parameters
         * @param statsHistoryClient remote persistence
         */
        TimeslotEditorCacheAccess(TimeslotHistoricalEditorConfig config,
                StatsHistoryServiceBlockingStub statsHistoryClient) {
            super(config, statsHistoryClient);
        }

        @Override
        public Map<EntityCommodityFieldReference, TimeSlotCommodityData> getCache() {
            return super.getCache();
        }
    }

}
