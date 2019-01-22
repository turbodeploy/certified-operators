package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.grpc.stub.StreamObserver;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.action.ActionScopesApiInputDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.common.protobuf.action.ActionDTO.AcceptActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter.InvolvedEntities;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByEntityRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByEntityResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByEntityResponse.ActionCountsByEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.TypeCount;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;


/**
 * Test the service methods for the /actions endpoint.
 */
public class ActionsServiceTest {

    public static final String[] ALL_ACTION_MODES = {
            "RECOMMEND", "DISABLED", "MANUAL", "AUTOMATIC"
    };
    /**
     * The backend the API forwards calls to (i.e. the part that's in the plan orchestrator).
     */
    private final ActionsServiceMole actionsServiceBackend =
            Mockito.spy(new ActionsServiceMole());

    private ActionsService actionsServiceUnderTest;

    private RepositoryApi repositoryApi = Mockito.mock(RepositoryApi.class);

    private GroupExpander groupExpander = Mockito.mock(GroupExpander.class);

    ActionsServiceGrpc.ActionsServiceBlockingStub actionsRpcService;

    private ActionSpecMapper actionSpecMapper;

    private final long REALTIME_TOPOLOGY_ID = 777777L;

    private final String UUID = "12345";

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(actionsServiceBackend);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() throws IOException {

        // set up a mock Actions RPC server
        actionSpecMapper = Mockito.mock(ActionSpecMapper.class);
        actionsRpcService = ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel());

        // set up the ActionsService to test
        actionsServiceUnderTest = new ActionsService(actionsRpcService, actionSpecMapper,
            repositoryApi, REALTIME_TOPOLOGY_ID, groupExpander);
    }

    /**
     * Test the return of available action modes for a given actionType and seType.
     * Note that the actionType and seType are currently ignored in XL, pending implementation
     * of user permissions, etc.
     */
    @Test
    public void testGetAvailActionModes() throws Exception {
        // Arrange

        // note that the getAvailableActions()
        String actionType = "ignored";
        String seType = "ignored";
        // Act
        List<String> modes = actionsServiceUnderTest.getAvailActionModes(actionType, seType);
        // Assert
        assertThat(modes, containsInAnyOrder(ALL_ACTION_MODES));

    }

    /**
     * Test execute action throw correct exception when RPC call return errors
     * @throws Exception
     */
    @Test
    public void testExecuteActionThrowException() throws Exception {
        expectedException.expect(UnknownObjectException.class);
        when(actionsServiceBackend.acceptAction(any())).thenReturn(acceptanceError("Action error"));
        actionsServiceUnderTest.executeAction(UUID, true);
    }

    @Test
    public void testGetActionTypeCountStatsByUuidsQuery() throws Exception {
        final Set<Long> entityIds = Sets.newHashSet(1L, 2L);
        final ServiceEntityApiDTO serviceEntityOne = new ServiceEntityApiDTO();
        serviceEntityOne.setDisplayName("First Entity");
        serviceEntityOne.setClassName("VM #1");
        serviceEntityOne.setUuid("1");
        final ServiceEntityApiDTO serviceEntityTwo = new ServiceEntityApiDTO();
        serviceEntityTwo.setDisplayName("Second Entity");
        serviceEntityTwo.setClassName("PM");
        serviceEntityTwo.setUuid("2");
        final Map<Long, Optional<ServiceEntityApiDTO>> serviceEntityMap =
            ImmutableMap.of(1L, Optional.of(serviceEntityOne),
                2L, Optional.of(serviceEntityTwo));
        final ActionScopesApiInputDTO actionScopesApiInputDTO = new ActionScopesApiInputDTO();
        final ActionApiInputDTO actionApiInputDTO = new ActionApiInputDTO();
        actionApiInputDTO.setGroupBy(Lists.newArrayList("actionTypes"));
        actionScopesApiInputDTO.setScopes(Lists.newArrayList("1", "2"));
        actionScopesApiInputDTO.setActionInput(actionApiInputDTO);

        final ActionQueryFilter filter = ActionQueryFilter.newBuilder()
            .setVisible(true)
            .setInvolvedEntities(InvolvedEntities.newBuilder()
                .addOids(1L)
                .addOids(2L))
            .build();

        when(actionsServiceBackend.getActionCountsByEntity(any()))
            .thenReturn(GetActionCountsByEntityResponse.newBuilder()
                .addActionCountsByEntity(ActionCountsByEntity.newBuilder()
                    .setEntityId(1L)
                    .addCountsByType(TypeCount.newBuilder()
                            .setCount(2)
                            .setType(ActionType.MOVE))
                    .addCountsByType(TypeCount.newBuilder()
                            .setCount(1)
                            .setType(ActionType.DEACTIVATE)))
                .build());

        when(repositoryApi.getServiceEntitiesById(any()))
            .thenReturn(serviceEntityMap);

        when(actionSpecMapper.createActionFilter(actionApiInputDTO, Optional.of(entityIds)))
            .thenReturn(filter);

        when(groupExpander.expandUuid("1")).thenReturn(Sets.newHashSet(1L));
        when(groupExpander.expandUuid("2")).thenReturn(Sets.newHashSet(2L));

        final List<EntityStatsApiDTO> entityStatsApiDTOS =
            actionsServiceUnderTest.getActionStatsByUuidsQuery(actionScopesApiInputDTO);

        assertEquals(2, entityStatsApiDTOS.size());
        assertEquals(1, entityStatsApiDTOS.get(0).getStats().size());
        assertTrue(entityStatsApiDTOS.get(1).getStats().isEmpty());
        final List<StatSnapshotApiDTO> statSnapshotApiDTOS = entityStatsApiDTOS.get(0).getStats();
        assertEquals(2, statSnapshotApiDTOS.get(0).getStatistics().size());
        final List<StatApiDTO> statApiDTOS = statSnapshotApiDTOS.get(0).getStatistics();
        assertEquals("MOVE", statApiDTOS.get(0).getFilters().get(0).getValue());
        assertEquals(2.0f, statApiDTOS.get(0).getValue(), 0.000000001);
        assertEquals("SUSPEND", statApiDTOS.get(1).getFilters().get(0).getValue());
        assertEquals(1.0f, statApiDTOS.get(1).getValue(), 0.000000001);

    }

    @Test
    public void testGetActionTypeCountStatsByUuidsQueryWithGroupUuid() throws Exception {
        final ServiceEntityApiDTO serviceEntityOne = new ServiceEntityApiDTO();
        serviceEntityOne.setDisplayName("First Entity");
        serviceEntityOne.setClassName("VM #1");
        serviceEntityOne.setUuid("1");
        final ServiceEntityApiDTO serviceEntityTwo = new ServiceEntityApiDTO();
        serviceEntityTwo.setDisplayName("Second Entity");
        serviceEntityTwo.setClassName("PM #1");
        serviceEntityTwo.setUuid("2");
        final Map<Long, Optional<ServiceEntityApiDTO>> serviceEntityMap =
            ImmutableMap.of(1L, Optional.of(serviceEntityOne));
        final ActionScopesApiInputDTO actionScopesApiInputDTO = new ActionScopesApiInputDTO();
        final ActionApiInputDTO actionApiInputDTO = new ActionApiInputDTO();
        actionApiInputDTO.setGroupBy(Lists.newArrayList("actionTypes"));
        // Set scope with one uuid of VM and other of cluster
        // (VM has uuid "1" and cluster has uuid "3")
        actionScopesApiInputDTO.setScopes(Lists.newArrayList("1", "3"));
        actionScopesApiInputDTO.setActionInput(actionApiInputDTO);

        // For VM : Set MOVE, DEACTIVATE to be returned by service.
        when(actionsServiceBackend.getActionCountsByEntity(any()))
            .thenReturn(GetActionCountsByEntityResponse.newBuilder()
                .addActionCountsByEntity(ActionCountsByEntity.newBuilder()
                    .setEntityId(1L)
                    .addCountsByType(TypeCount.newBuilder()
                            .setCount(2)
                            .setType(ActionType.MOVE))
                    .addCountsByType(TypeCount.newBuilder()
                            .setCount(1)
                            .setType(ActionType.DEACTIVATE)))
                .build());

     // For Cluster's member PM : Set PROVISION, RECONFIGURE to be returned by service.
        when(actionsServiceBackend.getActionCounts(any()))
        .thenReturn(GetActionCountsResponse.newBuilder()
            .addCountsByType(TypeCount.newBuilder()
                        .setCount(1)
                        .setType(ActionType.PROVISION))
                .addCountsByType(TypeCount.newBuilder()
                        .setCount(1)
                        .setType(ActionType.RECONFIGURE))
                .build());

        when(repositoryApi.getServiceEntitiesById(any()))
            .thenReturn(serviceEntityMap);

        // Create a filter object to be by for actionSpecMapper.
        final ActionQueryFilter filter = ActionQueryFilter.newBuilder()
            .setVisible(true)
            .setInvolvedEntities(InvolvedEntities.newBuilder()
                .addOids(1L))
            .build();
        when(actionSpecMapper.createActionFilter(actionApiInputDTO, Optional.of(Sets.newHashSet(1L))))
            .thenReturn(filter);
        when(actionSpecMapper.createActionFilter(actionApiInputDTO, Optional.of(Sets.newHashSet(2L))))
        .thenReturn(filter);

        // Make VM (uuid : 1) return its own uuid via group expander.
        when(groupExpander.expandUuid("1")).thenReturn(Sets.newHashSet(1L));
        // Make Cluster (uuid : 2) return its member's uuid(PMs uuid : 2) via group expander.
        when(groupExpander.expandUuid("3")).thenReturn(Sets.newHashSet(2L));

        final List<EntityStatsApiDTO> entityStatsApiDTOS =
            actionsServiceUnderTest.getActionStatsByUuidsQuery(actionScopesApiInputDTO);

        assertEquals(2, entityStatsApiDTOS.size());
        assertEquals(1, entityStatsApiDTOS.get(0).getStats().size());
        assertEquals(1, entityStatsApiDTOS.get(1).getStats().size());

        // Analyze response for Cluster (uuid : 3) stats.
        // It will contain stats for its member PM (uuid : 3)
        assertEquals("3", entityStatsApiDTOS.get(0).getUuid());
        final List<StatSnapshotApiDTO> firstStatSnapshotApiDTOS = entityStatsApiDTOS.get(0).getStats();
        assertEquals(2, firstStatSnapshotApiDTOS.get(0).getStatistics().size());
        final List<StatApiDTO> secondStatApiDTOS = firstStatSnapshotApiDTOS.get(0).getStatistics();
        assertEquals("PROVISION", secondStatApiDTOS.get(0).getFilters().get(0).getValue());
        assertEquals(1.0f, secondStatApiDTOS.get(0).getValue(), 0.000000001);
        assertEquals("RECONFIGURE", secondStatApiDTOS.get(1).getFilters().get(0).getValue());
        assertEquals(1.0f, secondStatApiDTOS.get(1).getValue(), 0.000000001);

        // Analyze response for VM (uuid : 1) stats.
        // It should contain stats for itself.
        assertEquals("1", entityStatsApiDTOS.get(1).getUuid());
        final List<StatSnapshotApiDTO> secondStatSnapshotApiDTOS = entityStatsApiDTOS.get(1).getStats();
        assertEquals(2, secondStatSnapshotApiDTOS.get(0).getStatistics().size());
        final List<StatApiDTO> statApiDTOS = secondStatSnapshotApiDTOS.get(0).getStatistics();
        assertEquals("MOVE", statApiDTOS.get(0).getFilters().get(0).getValue());
        assertEquals(2.0f, statApiDTOS.get(0).getValue(), 0.000000001);
        assertEquals("SUSPEND", statApiDTOS.get(1).getFilters().get(0).getValue());
        assertEquals(1.0f, statApiDTOS.get(1).getValue(), 0.000000001);
    }

    private static AcceptActionResponse acceptanceError(@Nonnull final String error) {
        return AcceptActionResponse.newBuilder()
                .setError(error)
                .build();
    }
}
