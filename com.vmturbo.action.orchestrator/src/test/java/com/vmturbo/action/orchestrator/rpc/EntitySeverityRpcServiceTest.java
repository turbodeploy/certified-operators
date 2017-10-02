package com.vmturbo.action.orchestrator.rpc;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.EntitySeverityCache;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.MultiEntityRequest;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceBlockingStub;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Tests for Entity Severity service.
 */
public class EntitySeverityRpcServiceTest {
    private GrpcTestServer grpcServer;
    private EntitySeverityServiceBlockingStub severityServiceClient;

    private final ActionStorehouse actionStorehouse = Mockito.mock(ActionStorehouse.class);

    private final EntitySeverityCache severityCache = Mockito.mock(EntitySeverityCache.class);
    private final long topologyContextId = 3;

    @Before
    public void setup() throws IOException {
        IdentityGenerator.initPrefix(0);

        EntitySeverityRpcService entitySeverityRpcService = new EntitySeverityRpcService(actionStorehouse);

        grpcServer = GrpcTestServer.withServices(entitySeverityRpcService);
        severityServiceClient = EntitySeverityServiceGrpc.newBlockingStub(grpcServer.getChannel());
    }

    @After
    public void teardown() {
        grpcServer.close();
    }

    @Test
    public void testGetEntitySeverityPresent() throws Exception {
        when(severityCache.getSeverity(1234L)).thenReturn(Optional.of(Severity.CRITICAL));
        MultiEntityRequest severityContext = MultiEntityRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .addEntityIds(1234L)
            .build();

        when(actionStorehouse.getSeverityCache(topologyContextId)).thenReturn(Optional.of(severityCache));
        EntitySeverity severity = severityServiceClient.getEntitySeverities(severityContext).next();

        assertTrue(severity.hasSeverity());
        assertEquals(Severity.CRITICAL, severity.getSeverity());
        assertEquals(1234L, severity.getEntityId());
    }

    @Test
    public void testGetEntitySeverityAbsent() throws Exception {
        when(severityCache.getSeverity(1234L)).thenReturn(Optional.empty());
        MultiEntityRequest severityContext = MultiEntityRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .addEntityIds(1234L)
            .build();

        when(actionStorehouse.getSeverityCache(topologyContextId)).thenReturn(Optional.of(severityCache));
        EntitySeverity severity = severityServiceClient.getEntitySeverities(severityContext).next();

        assertFalse(severity.hasSeverity());
        assertEquals(1234L, severity.getEntityId());
    }

    @Test
    public void testGetEntitySeveritiesMixed() throws Exception {
        when(severityCache.getSeverity(1234L)).thenReturn(Optional.of(Severity.MINOR));
        when(severityCache.getSeverity(5678L)).thenReturn(Optional.empty());

        MultiEntityRequest severityContext = MultiEntityRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .addEntityIds(1234L)
            .addEntityIds(5678L)
            .build();

        when(actionStorehouse.getSeverityCache(topologyContextId)).thenReturn(Optional.of(severityCache));
        Iterable<EntitySeverity> severitiesIter = () -> severityServiceClient.getEntitySeverities(severityContext);
        Map<Long, Optional<Severity>> severities = StreamSupport.stream(severitiesIter.spliterator(), false)
            .collect(Collectors.toMap(
                EntitySeverity::getEntityId,
                severity -> severity.hasSeverity() ? Optional.of(severity.getSeverity()) : Optional.empty()));


        assertEquals(Optional.of(Severity.MINOR), severities.get(1234L));
        assertEquals(Optional.empty(), severities.get(5678L));
    }

    @Test
    public void testGetEntitySeveritiesUnknownTopologyContext() {
        MultiEntityRequest severityContext = MultiEntityRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .addEntityIds(1234L)
            .addEntityIds(5678L)
            .build();

        when(actionStorehouse.getSeverityCache(topologyContextId)).thenReturn(Optional.empty());
        assertFalse(severityServiceClient.getEntitySeverities(severityContext).hasNext());
    }
}