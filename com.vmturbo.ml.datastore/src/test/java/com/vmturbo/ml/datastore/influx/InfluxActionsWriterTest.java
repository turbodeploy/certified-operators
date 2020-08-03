package com.vmturbo.ml.datastore.influx;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class InfluxActionsWriterTest {
    private final MetricsStoreWhitelist metricStoreWhitelist = mock(MetricsStoreWhitelist.class);
    private final InfluxDB influx = mock(InfluxDB.class);

    private InfluxActionsWriter metricsWriter;

    private static final long TOPOLOGY_TIME = 999999L;
    private static final long PROVIDER_ID = 12345L;
    private static final long ENTITY_ID = 11111L;

    private final ActionNotificationDTO.ActionSuccess actionSuccess = ActionNotificationDTO.ActionSuccess.newBuilder()
            .setActionId(123L).build();

    private final ActionDTO.Action move = ActionDTO.Action.newBuilder()
            .setInfo(ActionDTO.ActionInfo.newBuilder().setMove(
                    ActionDTO.Move.newBuilder()
                            .setTarget(ActionDTO.ActionEntity.newBuilder()
                                    .setId(5L)
                                    .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                                    .build())
                            .addChanges(ActionDTO.ChangeProvider.newBuilder()
                                    .setSource(ActionDTO.ActionEntity.newBuilder()
                                            .setId(1L)
                                            .setType(EntityType.PHYSICAL_MACHINE_VALUE)
                                            .build())
                                    .setDestination(ActionDTO.ActionEntity.newBuilder()
                                            .setId(2L)
                                            .setType(EntityType.PHYSICAL_MACHINE_VALUE)
                                            .build()))
                            .addChanges(ActionDTO.ChangeProvider.newBuilder()
                                    .setSource(ActionDTO.ActionEntity.newBuilder()
                                            .setId(3L)
                                            .setType(EntityType.STORAGE_VALUE)
                                            .build())
                                    .setDestination(ActionDTO.ActionEntity.newBuilder()
                                            .setId(4L)
                                            .setType(EntityType.STORAGE_VALUE)
                                            .build()))
                            .build())
                    .build())
            .setId(121L)
            .setDeprecatedImportance(0)
            .setExplanation(ActionDTO.Explanation.newBuilder().build())
            .build();

    private final ActionDTO.Action deactivate = ActionDTO.Action.newBuilder()
            .setInfo(ActionDTO.ActionInfo.newBuilder().setDeactivate(
                    ActionDTO.Deactivate.newBuilder()
                            .setTarget(ActionDTO.ActionEntity.newBuilder()
                                    .setId(1L)
                                    .setType(EntityType.PHYSICAL_MACHINE_VALUE)
                                    .build())
                            .build())
                    .build())
            .setId(122L)
            .setDeprecatedImportance(0)
            .setExplanation(ActionDTO.Explanation.newBuilder().build())
            .build();

    private final ActionDTO.ActionPlan actionPlan = ActionDTO.ActionPlan.newBuilder()
        .addAction(move)
        .addAction(deactivate)
        .setInfo(ActionPlanInfo.newBuilder()
            .setMarket(MarketActionPlanInfo.newBuilder()
                .setSourceTopologyInfo(TopologyInfo.newBuilder()
                    .setTopologyId(1)
                    .setTopologyContextId(777777)
                    .setTopologyType(TopologyType.REALTIME))))
        .setId(111L)
        .build();

    private final ActionNotificationDTO.ActionSuccess success = ActionNotificationDTO.ActionSuccess.newBuilder()
            .setActionId(111L).build();

    private static final String DATABASE = "database";
    private static final String RETENTION_POLICY = "rp";

    @Captor
    private ArgumentCaptor<Point> pointCaptor;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        metricsWriter = new InfluxActionsWriter(influx, DATABASE, RETENTION_POLICY,
                                    metricStoreWhitelist);
    }

    /**
     * Test that non-whitelisted actions are not written.
     */
    @Test
    public void testWriteRecommendedActionsOnActionTypeWhitelist() {
        when(metricStoreWhitelist.getWhitelist(MetricsStoreWhitelist.ACTION_TYPE))
                .thenReturn(ImmutableSet.of(MLDatastore.ActionTypeWhitelist.ActionType.MOVE));
        when(metricStoreWhitelist.getWhitelist(MetricsStoreWhitelist.ACTION_STATE))
                .thenReturn(Collections.singleton(MLDatastore.ActionStateWhitelist.ActionState.RECOMMENDED_ACTIONS));

        metricsWriter.writeMetrics(actionPlan, new HashMap<>(), MLDatastore.ActionStateWhitelist.ActionState.RECOMMENDED_ACTIONS);

        verify(influx).write(eq(DATABASE), eq(RETENTION_POLICY), pointCaptor.capture());
        final Point point = pointCaptor.getValue();
        // assert presence of move action
        assertThat(point.toString(), containsString("destination_0"));
        assertThat(point.toString(), containsString("move"));
        // assert absence of deactivate action
        assertThat(point.toString(), not(containsString("deactivate")));
    }

    /**
     * Test that whitelisted actions can be written in the expected schema.
     */
    @Test
    public void testWriteRecommendedActions() {
        when(metricStoreWhitelist.getWhitelist(MetricsStoreWhitelist.ACTION_TYPE))
            .thenReturn(ImmutableSet.of(MLDatastore.ActionTypeWhitelist.ActionType.MOVE, MLDatastore.ActionTypeWhitelist.ActionType.DEACTIVATE));
        when(metricStoreWhitelist.getWhitelist(MetricsStoreWhitelist.ACTION_STATE))
            .thenReturn(Collections.singleton(MLDatastore.ActionStateWhitelist.ActionState.RECOMMENDED_ACTIONS));

        metricsWriter.writeMetrics(actionPlan, new HashMap<>(), MLDatastore.ActionStateWhitelist.ActionState.RECOMMENDED_ACTIONS);

        // Both move and deactivate action stats should be written.
        verify(influx, Mockito.times(2)).write(eq(DATABASE), eq(RETENTION_POLICY), pointCaptor.capture());
        final List<Point> points = pointCaptor.getAllValues();
        assertThat(points.get(0).toString(), containsString("move"));
        assertThat(points.get(1).toString(), containsString("deactivate"));
    }

    /**
     * Test that completed actions are the only ones written to influx.
     */
    @Test
    public void testWriteWhitelistedCompletedActions() {
        when(metricStoreWhitelist.getWhitelist(MetricsStoreWhitelist.ACTION_TYPE))
                .thenReturn(ImmutableSet.of(MLDatastore.ActionTypeWhitelist.ActionType.MOVE, MLDatastore.ActionTypeWhitelist.ActionType.DEACTIVATE));
        when(metricStoreWhitelist.getWhitelist(MetricsStoreWhitelist.ACTION_STATE))
                .thenReturn(Collections.singleton(MLDatastore.ActionStateWhitelist.ActionState.COMPLETED_ACTIONS));

        metricsWriter.writeMetrics(actionPlan, new HashMap<>(), MLDatastore.ActionStateWhitelist.ActionState.RECOMMENDED_ACTIONS);
        metricsWriter.writeCompletedActionMetrics(actionSuccess);

        // Both move and deactivate action stats should be written.
        verify(influx, Mockito.times(1)).write(eq(DATABASE), eq(RETENTION_POLICY), pointCaptor.capture());
        final Point point = pointCaptor.getValue();
        assertThat(point.toString(), not(containsString("move")));
        assertThat(point.toString(), not(containsString("deactivate")));
        assertThat(point.toString(), containsString("action_id"));
    }

    /**
     * Test that flushing the metrics writer flushes the underlying influx connection.
     */
    @Test
    public void testFlush() {
        metricsWriter.flush();
        verify(influx).flush();
    }

    /**
     * Test that closing the metrics writer closes the underlying influx connection.
     */
    @Test
    public void testClose() throws Exception {
        metricsWriter.close();
        verify(influx).close();
    }
}