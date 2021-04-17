package com.vmturbo.cost.component.savings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import com.google.gson.Gson;
import com.google.protobuf.InvalidProtocolBufferException;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.EntityStateChangeDetails;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.TopologyEventInfo;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.TopologyEventType;
import com.vmturbo.cost.component.savings.EntityEventsJournal.ActionEvent;
import com.vmturbo.cost.component.savings.EntityEventsJournal.ActionEvent.ActionEventType;
import com.vmturbo.cost.component.savings.EntityEventsJournal.SavingsEvent;
import com.vmturbo.cost.component.savings.SqlAuditLogWriter.AuditLogEntry;

/**
 * Tests for audit log writer.
 */
public class SqlAuditLogWriterTest {

    private static final long ACTION_EXPIRATION_TIME = TimeUnit.HOURS.toMillis(1L);

    /**
     * Check if the topology audit event is being translated correctly.
     *
     * @throws InvalidProtocolBufferException Thrown on protobuf parse error.
     */
    @Test
    public void auditTopologyEvent() throws InvalidProtocolBufferException {
        long vmId = 101L;
        long vendorId = 2001L;
        long timestamp = System.currentTimeMillis();
        long powerChangeTime = timestamp - 60 * 60 * 1000;
        TopologyEventType eventType = TopologyEventType.STATE_CHANGE;

        final TopologyDTO.EntityState sourceState = TopologyDTO.EntityState.POWERED_OFF;
        final TopologyDTO.EntityState destinationState = TopologyDTO.EntityState.POWERED_ON;
        final SavingsEvent savingsEvent = new SavingsEvent.Builder()
                .entityId(vmId)
                .timestamp(timestamp)
                .topologyEvent(TopologyEvent.newBuilder()
                        .setEventTimestamp(powerChangeTime)
                        .setType(eventType)
                        .setEventInfo(TopologyEventInfo.newBuilder()
                                .setVendorEventId(String.valueOf(vendorId))
                                .setStateChange(EntityStateChangeDetails.newBuilder()
                                        .setSourceState(sourceState)
                                        .setDestinationState(destinationState)
                                        .build())
                                .build())
                        .build())
                .build();

        final Gson gson = SqlAuditLogWriter.createGson();
        final AuditLogEntry logEntry = new AuditLogEntry(savingsEvent, gson);

        assertNotNull(logEntry);
        assertEquals(vmId, logEntry.getEntityOid());
        assertEquals(timestamp, logEntry.getEventTime());
        assertEquals(eventType.getNumber(), logEntry.getEventType());
        assertEquals(String.valueOf(vendorId), logEntry.getEventId());

        final String eventInfo = logEntry.getEventInfo();
        assertNotNull(eventInfo);
        assertTrue(eventInfo.contains(String.format("\"sourceState\":\"%s\"",
                sourceState.name())));
        assertTrue(eventInfo.contains(String.format("\"destinationState\":\"%s\"",
                destinationState.name())));
        assertTrue(eventInfo.contains(String.format("\"eventTimestamp\":\"%d\"",
                powerChangeTime)));
    }

    /**
     * Check if action event is being translated correctly.
     *
     * @throws InvalidProtocolBufferException Thrown on parse error (not for action events).
     */
    @Test
    public void auditActionEvent() throws InvalidProtocolBufferException {
        long vmId = 101L;
        long actionId = 2001L;
        long timestamp = System.currentTimeMillis();
        ActionEventType eventType = ActionEventType.SCALE_EXECUTION_SUCCESS;
        final double preActionCost = 10.01d;
        final double postActionCost = 20.15d;

        final SavingsEvent savingsEvent = new SavingsEvent.Builder()
                .entityId(vmId)
                .timestamp(timestamp)
                // .expirationTime(timestamp + ACTION_EXPIRATION_TIME)
                .entityPriceChange(new EntityPriceChange.Builder()
                        .sourceOid(501L)
                        .sourceCost(preActionCost)
                        .destinationOid(601L)
                        .destinationCost(postActionCost)
                        .build())
                .actionEvent(new ActionEvent.Builder()
                        .eventType(eventType)
                        .actionId(actionId)
                        .build())
                .build();

        final Gson gson = SqlAuditLogWriter.createGson();
        final AuditLogEntry logEntry = new AuditLogEntry(savingsEvent, gson);

        assertNotNull(logEntry);
        assertEquals(vmId, logEntry.getEntityOid());
        assertEquals(timestamp, logEntry.getEventTime());
        assertEquals(eventType.getTypeCode(), logEntry.getEventType());
        assertEquals(String.valueOf(actionId), logEntry.getEventId());

        final String eventInfo = logEntry.getEventInfo();
        assertNotNull(eventInfo);
        assertTrue(eventInfo.contains(String.format("\"sourceCost\":%s", preActionCost)));
        assertTrue(eventInfo.contains(String.format("\"destinationCost\":%s", postActionCost)));
    }
}
