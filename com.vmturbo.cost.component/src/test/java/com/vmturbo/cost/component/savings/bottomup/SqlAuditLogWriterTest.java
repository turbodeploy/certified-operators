package com.vmturbo.cost.component.savings.bottomup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.gson.Gson;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.cost.component.savings.bottomup.ActionEvent.ActionEventType;
import com.vmturbo.cost.component.savings.bottomup.SqlAuditLogWriter.AuditLogEntry;
import com.vmturbo.cost.component.savings.bottomup.TopologyEvent.EventType;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;

/**
 * Tests for audit log writer.
 */
public class SqlAuditLogWriterTest {

    /**
     * Check if the topology audit event is being translated correctly.
     *
     * @throws InvalidProtocolBufferException Thrown on protobuf parse error.
     */
    @Test
    public void auditTopologyEvent() throws InvalidProtocolBufferException {
        long vmId = 101L;
        long timestamp = System.currentTimeMillis();
        long powerChangeTime = timestamp - 60 * 60 * 1000;
        EventType eventType = EventType.STATE_CHANGE;
        final boolean destinationState = true;
        final SavingsEvent savingsEvent = new SavingsEvent.Builder()
                .entityId(vmId)
                .timestamp(timestamp)
                .topologyEvent(new TopologyEvent.Builder()
                        .eventType(eventType.getValue())
                        .timestamp(powerChangeTime)
                        .entityOid(vmId)
                        .entityType(EntityType.VIRTUAL_MACHINE.getValue())
                        .poweredOn(true)
                        .build())
                .build();

        final Gson gson = SqlAuditLogWriter.createGson();
        final AuditLogEntry logEntry = new AuditLogEntry(savingsEvent, gson);
        final long vendorId = logEntry.getEventInfo().hashCode() & 0xfffffff;

        assertNotNull(logEntry);
        assertEquals(vmId, logEntry.getEntityOid());
        assertEquals(timestamp, logEntry.getEventTime());
        assertEquals(eventType.getValue(), logEntry.getEventType());
        assertEquals(String.valueOf(vendorId), logEntry.getEventId());

        final String eventInfo = logEntry.getEventInfo();
        assertNotNull(eventInfo);
        assertTrue(eventInfo.contains(String.format("\"ds\":%d", 1)));
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
                .entityPriceChange(new EntityPriceChange.Builder()
                        .sourceOid(501L)
                        .sourceCost(preActionCost)
                        .destinationOid(601L)
                        .destinationCost(postActionCost)
                        .build())
                .actionEvent(new ActionEvent.Builder()
                        .eventType(eventType)
                        .actionId(actionId)
                        .description(StringUtils.EMPTY)
                        .entityType(EntityType.VIRTUAL_MACHINE.getValue())
                        .actionType(ActionType.SCALE_VALUE)
                        .actionCategory(ActionCategory.EFFICIENCY_IMPROVEMENT_VALUE)
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
        assertTrue(eventInfo.contains(String.format("\"sc\":%s", preActionCost)));
        assertTrue(eventInfo.contains(String.format("\"dc\":%s", postActionCost)));
    }
}
