package com.vmturbo.ml.datastore.topology;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.ml.datastore.influx.*;
import com.vmturbo.ml.datastore.influx.InfluxMetricsWriterFactory.InfluxUnavailableException;
import com.vmturbo.ml.datastore.influx.Obfuscator.HashingObfuscator;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;

import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class ActionsListenerTest {

    private final InfluxMetricsWriterFactory connectionFactory = mock(InfluxMetricsWriterFactory.class);
    private final MetricsStoreWhitelist whitelist = mock(MetricsStoreWhitelist.class);
    private final InfluxActionsWriter metricsWriter = mock(InfluxActionsWriter.class);

    private final MLDatastore.ActionStateWhitelist.ActionState recommended = MLDatastore.ActionStateWhitelist.ActionState.RECOMMENDED_ACTIONS;
    private final ActionMetricsListener listener = new ActionMetricsListener(connectionFactory,
                                                            whitelist);
    private static final long TIME = 99999L;

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
            .setImportance(0)
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
            .setImportance(0)
            .setExplanation(ActionDTO.Explanation.newBuilder().build())
            .build();

    private final ActionDTO.ActionPlan actionPlan = ActionDTO.ActionPlan.newBuilder()
            .addAction(move)
            .addAction(deactivate)
            .setTopologyContextId(777777)
            .setId(111L)
            .build();

    @Before
    @SuppressWarnings("unchecked")
    public void setup() throws Exception {
        when(connectionFactory.getDatabase()).thenReturn("db");
        when(connectionFactory.getRetentionPolicyName()).thenReturn("rp");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCompletedActionsMetricsWritten() {
        when(connectionFactory.createActionMetricsWriter(eq(whitelist)))
            .thenReturn(metricsWriter);

        listener.onActionSuccess(actionSuccess);
        verify(metricsWriter).writeCompletedActionMetrics(actionSuccess);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRecommendedActionsMetricsWritten() {
        when(connectionFactory.createActionMetricsWriter(eq(whitelist)))
                .thenReturn(metricsWriter);

        listener.onActionsReceived(actionPlan);
        verify(metricsWriter).writeMetrics(eq(actionPlan),
                anyMap(),
                eq(recommended));
    }

    @Test
    public void testWritesFlushed() {
        when(connectionFactory.createActionMetricsWriter(eq(whitelist)))
            .thenReturn(metricsWriter);

        listener.onActionsReceived(actionPlan);
        verify(metricsWriter).flush();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testInfluxConnectionRecreatedOnError() {
        // First make influx unavailable
        doThrow(InfluxUnavailableException.class)
            .when(connectionFactory)
            .createActionMetricsWriter(eq(whitelist));
        listener.onActionsReceived(actionPlan);

        // Then influx is available.
        when(connectionFactory.createActionMetricsWriter(eq(whitelist)))
            .thenReturn(metricsWriter);
        listener.onActionsReceived(actionPlan);
        verify(metricsWriter).writeMetrics(eq(actionPlan), anyMap(), eq(recommended));
    }
}