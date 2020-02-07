package com.vmturbo.action.orchestrator.rpc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
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
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesResponse.TypeCase;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.MultiEntityRequest;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.SeverityCount;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.SeverityCountsResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceBlockingStub;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.SearchOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse.Builder;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Tests for Entity Severity service.
 */
public class EntitySeverityRpcServiceTest {
    private EntitySeverityServiceBlockingStub severityServiceClient;
    private final SupplyChainServiceMole supplyChainServiceMole = spy(new SupplyChainServiceMole());
    private final RepositoryServiceMole repositoryServiceMole = spy(new RepositoryServiceMole());

    private EntitySeverityCache severityCache;

    private final ActionStorehouse actionStorehouse = Mockito.mock(ActionStorehouse.class);

    private final long topologyContextId = 3;

    private final EntitySeverityRpcService entitySeverityRpcService =
            new EntitySeverityRpcService(actionStorehouse,
                    100, 500, 5000);

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(
        entitySeverityRpcService,
        supplyChainServiceMole,
        repositoryServiceMole);

    @Before
    public void setup() throws IOException {
        IdentityGenerator.initPrefix(0);

        severityServiceClient = EntitySeverityServiceGrpc.newBlockingStub(grpcServer.getChannel());
        severityCache = Mockito.spy(new EntitySeverityCache(
            SupplyChainServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            RepositoryServiceGrpc.newBlockingStub(grpcServer.getChannel())));
    }

    @Test
    public void testGetEntitySeverityPresent() throws Exception {
        when(severityCache.getSeverity(1234L)).thenReturn(Optional.of(Severity.CRITICAL));
        MultiEntityRequest severityContext = MultiEntityRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .addEntityIds(1234L)
            .build();

        when(actionStorehouse.getSeverityCache(topologyContextId)).thenReturn(Optional.of(severityCache));
        Iterable<EntitySeveritiesResponse> response =
            () -> severityServiceClient.getEntitySeverities(severityContext);
        EntitySeverity severity = processEntitySeverityStream(response).get(0);
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
        Iterable<EntitySeveritiesResponse> response =
            () -> severityServiceClient.getEntitySeverities(severityContext);
        EntitySeverity severity = processEntitySeverityStream(response).get(0);
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
        Iterable<EntitySeveritiesResponse> response =
                () -> severityServiceClient.getEntitySeverities(severityContext);
        List<EntitySeverity> severitiesList = processEntitySeverityStream(response);
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
        Iterable<EntitySeveritiesResponse> response =
            () -> severityServiceClient.getEntitySeverities(severityContext);
        List<EntitySeverity> severitiesList = processEntitySeverityStream(response);
        Map<Long, Optional<Severity>> severities = StreamSupport.stream(severitiesList.spliterator(), false)
            .collect(Collectors.toMap(
                EntitySeverity::getEntityId,
                severity -> severity.hasSeverity() ? Optional.of(severity.getSeverity()) : Optional.empty()));

        // All should be missing severity.
        assertFalse(severities.values().stream().anyMatch(Optional::isPresent));

        // without an action store, all EntitySeverities should have empty severity breakdown map
        assertEquals(0, severitiesList.stream()
            .filter(entitySeverity -> !entitySeverity.getSeverityBreakdownMap().isEmpty())
            .count());
    }

    /**
     * GetEntitySeverities should take the severity counts from the cache and places them in the severity
     * breakdown field.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testGetEntitySeveritiesWithSeverityBreakdown() throws Exception {
        EntitySeverityCache.SeverityCount severityCount = new EntitySeverityCache.SeverityCount();
        severityCount.addSeverity(Severity.CRITICAL);
        severityCount.addSeverity(Severity.CRITICAL);
        severityCount.addSeverity(Severity.MAJOR);
        when(severityCache.getSeverityBreakdown(1234L)).thenReturn(Optional.of(severityCount));

        MultiEntityRequest severityContext = MultiEntityRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .addEntityIds(1234L)
            .build();

        when(actionStorehouse.getSeverityCache(topologyContextId)).thenReturn(Optional.of(severityCache));
        Iterable<EntitySeveritiesResponse> response =
            () -> severityServiceClient.getEntitySeverities(severityContext);
        EntitySeverity severity = processEntitySeverityStream(response).get(0);
        assertFalse(severity.hasSeverity());
        assertEquals(1234L, severity.getEntityId());
        assertEquals(2, severity.getSeverityBreakdownMap().size());
        assertEquals(2L, severity.getSeverityBreakdownMap().get(Severity.CRITICAL.getNumber()).longValue());
        assertEquals(1L, severity.getSeverityBreakdownMap().get(Severity.MAJOR.getNumber()).longValue());
    }

    /**
     * GetEntitySeverities should have an empty severity breakdown map when the cache returns Optional.empty().
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testGetEntitySeveritiesWithEmptySeverityBreakdown() throws Exception {
        when(severityCache.getSeverityBreakdown(1234L)).thenReturn(Optional.empty());

        MultiEntityRequest severityContext = MultiEntityRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .addEntityIds(1234L)
            .build();

        when(actionStorehouse.getSeverityCache(topologyContextId)).thenReturn(Optional.of(severityCache));
        Iterable<EntitySeveritiesResponse> response =
            () -> severityServiceClient.getEntitySeverities(severityContext);
        EntitySeverity severity = processEntitySeverityStream(response).get(0);
        assertFalse(severity.hasSeverity());
        assertEquals(1234L, severity.getEntityId());
        assertTrue(severity.getSeverityBreakdownMap().isEmpty());
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
        final List<Long> oids = Lists.newArrayList(1L, 3L, 2L);
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
        Iterable<EntitySeveritiesResponse> response =
            () -> severityServiceClient.getEntitySeverities(request);
        List<EntitySeverity> entitySeverityList = new ArrayList<>();
        Builder paginationResponseBuilder = PaginationResponse.newBuilder();
        StreamSupport.stream(response.spliterator(), false)
            .forEach(chunk -> {
                if (chunk.getTypeCase() == TypeCase.ENTITY_SEVERITY) {
                    entitySeverityList.addAll(chunk.getEntitySeverity()
                        .getEntitySeverityList());
                } else {
                    paginationResponseBuilder.setNextCursor(chunk.getPaginationResponse().getNextCursor());
                }
            });
        assertEquals(2, entitySeverityList.size());
        Set<Long> responseOids = entitySeverityList.stream()
                .map(EntitySeverity::getEntityId)
                .collect(Collectors.toSet());
        assertTrue(responseOids.contains(2L));
        assertTrue(responseOids.contains(3L));
        assertEquals("2", paginationResponseBuilder.getNextCursor());

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
        response = () -> severityServiceClient.getEntitySeverities(requestLastPage);
        Builder lastPaginationResponseBuilder = PaginationResponse.newBuilder();
        List<EntitySeverity> lastPageEntitySeverityList = new ArrayList<>();
        StreamSupport.stream(response.spliterator(), false)
            .forEach(chunk -> {
                if (chunk.getTypeCase() == TypeCase.ENTITY_SEVERITY) {
                    lastPageEntitySeverityList.addAll(chunk.getEntitySeverity()
                        .getEntitySeverityList());
                } else {
                    if (chunk.getPaginationResponse().hasNextCursor()) {
                        lastPaginationResponseBuilder.setNextCursor(chunk.getPaginationResponse().getNextCursor());
                    }
                }
            });

        assertEquals(3, lastPageEntitySeverityList.size());
        responseOids = lastPageEntitySeverityList.stream()
                .map(EntitySeverity::getEntityId)
                .collect(Collectors.toSet());
        assertTrue(responseOids.contains(2L));
        assertTrue(responseOids.contains(3L));
        assertFalse(lastPaginationResponseBuilder.hasNextCursor());
    }

    private List<EntitySeverity> processEntitySeverityStream(Iterable<EntitySeveritiesResponse> response) {
        List<EntitySeverity> entitySeverityList = new ArrayList<>();
        StreamSupport.stream(response.spliterator(), false)
            .forEach(chunk -> {
                if (chunk.getTypeCase() == TypeCase.ENTITY_SEVERITY) {
                    entitySeverityList.addAll(chunk.getEntitySeverity()
                        .getEntitySeverityList());
                }
            });
        return entitySeverityList;
    }
}
