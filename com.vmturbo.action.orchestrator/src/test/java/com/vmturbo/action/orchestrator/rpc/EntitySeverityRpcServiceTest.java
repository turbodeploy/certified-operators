package com.vmturbo.action.orchestrator.rpc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.EntitySeverityCache;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.MultiEntityRequest;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.SeverityCount;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.SeverityCountsResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceBlockingStub;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.SearchOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
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
            new EntitySeverityRpcService(actionStorehouse,
                    100, 500);

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
        EntitySeverity severity =
                severityServiceClient.getEntitySeverities(severityContext).getEntitySeverityList().get(0);

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
        EntitySeverity severity =
                severityServiceClient.getEntitySeverities(severityContext).getEntitySeverityList().get(0);

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
        List<EntitySeverity> severitiesList =
                severityServiceClient.getEntitySeverities(severityContext).getEntitySeverityList();
        Map<Long, Optional<Severity>> severities = StreamSupport.stream(severitiesList.spliterator(), false)
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
        List<EntitySeverity> severitiesList =
                severityServiceClient.getEntitySeverities(severityContext).getEntitySeverityList();
        Map<Long, Optional<Severity>> severities = StreamSupport.stream(severitiesList.spliterator(), false)
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

    @Test
    public void testPaginateEntitiesBySeverity() {
        final List<Long> oids = Lists.newArrayList(1L, 2L, 3L);
        final PaginationParameters paginationParams = PaginationParameters.newBuilder()
                .setLimit(2)
                .setOrderBy(OrderBy.newBuilder().setSearch(SearchOrderBy.ENTITY_SEVERITY))
                .setAscending(false)
                .build();

        MultiEntityRequest request = MultiEntityRequest.newBuilder()
                .setTopologyContextId(topologyContextId)
                .addAllEntityIds(oids)
                .setPaginationParams(paginationParams)
                .build();
        when(actionStorehouse.getSeverityCache(topologyContextId)).thenReturn(Optional.of(severityCache));

        when(severityCache.getSeverity(1L)).thenReturn(Optional.of(Severity.NORMAL));
        when(severityCache.getSeverity(2L)).thenReturn(Optional.of(Severity.CRITICAL));
        when(severityCache.getSeverity(3L)).thenReturn(Optional.of(Severity.CRITICAL));
        EntitySeveritiesResponse response = severityServiceClient.getEntitySeverities(request);
        assertEquals(2, response.getEntitySeverityCount());
        Set<Long> responseOids = response.getEntitySeverityList().stream()
                .map(EntitySeverity::getEntityId)
                .collect(Collectors.toSet());
        assertTrue(responseOids.contains(2L));
        assertTrue(responseOids.contains(3L));
        assertEquals("2", response.getPaginationResponse().getNextCursor());

        final PaginationParameters paginationParamsLastPage = PaginationParameters.newBuilder()
                .setLimit(3)
                .setOrderBy(OrderBy.newBuilder().setSearch(SearchOrderBy.ENTITY_SEVERITY))
                .setAscending(false)
                .build();

        MultiEntityRequest requestLastPage = MultiEntityRequest.newBuilder()
                .setTopologyContextId(topologyContextId)
                .addAllEntityIds(oids)
                .setPaginationParams(paginationParamsLastPage)
                .build();
        response = severityServiceClient.getEntitySeverities(requestLastPage);
        assertEquals(3, response.getEntitySeverityCount());
        responseOids = response.getEntitySeverityList().stream()
                .map(EntitySeverity::getEntityId)
                .collect(Collectors.toSet());
        assertTrue(responseOids.contains(2L));
        assertTrue(responseOids.contains(3L));
        assertFalse(response.getPaginationResponse().hasNextCursor());
    }
}