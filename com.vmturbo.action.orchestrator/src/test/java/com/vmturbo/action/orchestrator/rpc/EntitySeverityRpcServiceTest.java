package com.vmturbo.action.orchestrator.rpc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.EntitySeverityCache;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.MultiEntityRequest;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.SeverityCount;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.SeverityCountsResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceBlockingStub;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Tests for Entity Severity service.
 */
public class EntitySeverityRpcServiceTest {
    private EntitySeverityServiceBlockingStub severityServiceClient;

    private final ActionStorehouse actionStorehouse = Mockito.mock(ActionStorehouse.class);

    private final EntitySeverityCache severityCache = Mockito.mock(EntitySeverityCache.class);
    private final long topologyContextId = 3;

    private final EntitySeverityRpcService entitySeverityRpcService =
            new EntitySeverityRpcService(actionStorehouse);

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(entitySeverityRpcService);

    @Before
    public void setup() throws IOException {
        IdentityGenerator.initPrefix(0);

        severityServiceClient = EntitySeverityServiceGrpc.newBlockingStub(grpcServer.getChannel());
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
        Iterable<EntitySeverity> severitiesIter = () -> severityServiceClient.getEntitySeverities(severityContext);
        Map<Long, Optional<Severity>> severities = StreamSupport.stream(severitiesIter.spliterator(), false)
            .collect(Collectors.toMap(
                EntitySeverity::getEntityId,
                severity -> severity.hasSeverity() ? Optional.of(severity.getSeverity()) : Optional.empty()));

        // All should be missing severity.
        assertFalse(severities.values().stream().anyMatch(Optional::isPresent));
    }

    @Test
    public void testGetSeverityCountsUnknownTopologyContext() {
        MultiEntityRequest severityRequest = MultiEntityRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .addEntityIds(1234L)
            .addEntityIds(5678L)
            .build();
        when(actionStorehouse.getSeverityCache(topologyContextId)).thenReturn(Optional.empty());
        final SeverityCountsResponse response = severityServiceClient.getSeverityCounts(severityRequest);

        assertEquals(2, response.getUnknownEntityCount());
        assertEquals(0, response.getCountsCount());
    }

    @Test
    public void testGetSeverityCounts() {
        when(severityCache.getSeverityCounts(anyList())).thenReturn(
            ImmutableMap.of(
                Optional.of(Severity.MINOR), 1L,
                Optional.of(Severity.CRITICAL), 1L,
                Optional.empty(), 1L
            ));

        MultiEntityRequest severityContext = MultiEntityRequest.newBuilder()
                .setTopologyContextId(topologyContextId)
                .addEntityIds(1234L)
                .addEntityIds(5678L)
                .addEntityIds(9999L)
                .build();

        when(actionStorehouse.getSeverityCache(topologyContextId)).thenReturn(Optional.of(severityCache));
        final SeverityCountsResponse response = severityServiceClient.getSeverityCounts(severityContext);

        assertEquals(1, response.getUnknownEntityCount());
        assertEquals(2, response.getCountsCount());
        assertThat(response.getCountsList(), hasItem(
            SeverityCount.newBuilder().setEntityCount(1L).setSeverity(Severity.MINOR).build()));
        assertThat(response.getCountsList(), hasItem(
            SeverityCount.newBuilder().setEntityCount(1L).setSeverity(Severity.CRITICAL).build()));

    }
}