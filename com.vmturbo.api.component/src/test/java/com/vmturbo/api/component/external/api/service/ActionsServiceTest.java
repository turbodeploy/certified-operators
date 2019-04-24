package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.ServiceEntitiesRequest;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.UIEntityType;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor.ActionStatsQuery;
import com.vmturbo.api.component.external.api.util.action.ImmutableActionStatsQuery;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.action.ActionScopesApiInputDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.common.protobuf.action.ActionDTO.AcceptActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter.InvolvedEntities;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByEntityResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByEntityResponse.ActionCountsByEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.TypeCount;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;


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

    private ActionStatsQueryExecutor actionStatsQueryExecutor = mock(ActionStatsQueryExecutor.class);

    private UuidMapper uuidMapper = mock(UuidMapper.class);

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
            repositoryApi, REALTIME_TOPOLOGY_ID,
            actionStatsQueryExecutor, uuidMapper);
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
        actionsServiceUnderTest.executeAction(UUID, true, false);
    }

    @Test
    public void testGetActionTypeCountStatsByUuidsQuery() throws Exception {
        final ServiceEntityApiDTO serviceEntityOne = new ServiceEntityApiDTO();
        serviceEntityOne.setDisplayName("First Entity");
        serviceEntityOne.setClassName("VM #1");
        serviceEntityOne.setUuid("1");
        final Map<Long, Optional<ServiceEntityApiDTO>> serviceEntityMap =
            ImmutableMap.of(1L, Optional.of(serviceEntityOne));

        when(repositoryApi.getServiceEntitiesById(any()))
            .thenReturn(serviceEntityMap);

        final ActionScopesApiInputDTO actionScopesApiInputDTO = new ActionScopesApiInputDTO();
        final ActionApiInputDTO actionApiInputDTO = new ActionApiInputDTO();
        actionScopesApiInputDTO.setScopes(Lists.newArrayList("1", "3"));

        final ApiId scope1 = ApiTestUtils.mockEntityId("1", uuidMapper);
        final ApiId scope2 = ApiTestUtils.mockGroupId("3", uuidMapper);
        actionScopesApiInputDTO.setActionInput(actionApiInputDTO);
        actionScopesApiInputDTO.setRelatedType(UIEntityType.PHYSICAL_MACHINE.getValue());

        final ActionStatsQuery expectedQuery = ImmutableActionStatsQuery.builder()
                .entityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .actionInput(actionApiInputDTO)
                .addScopes(scope1)
                .addScopes(scope2)
                .build();

        final List<StatSnapshotApiDTO> scope1Snapshots = Arrays.asList(new StatSnapshotApiDTO());
        final List<StatSnapshotApiDTO> scope2Snapshots = Arrays.asList(new StatSnapshotApiDTO(), new StatSnapshotApiDTO());

        when(actionStatsQueryExecutor.retrieveActionStats(any()))
            .thenReturn(ImmutableMap.of(scope1, scope1Snapshots, scope2, scope2Snapshots));

        final Map<String, EntityStatsApiDTO> statsByUuid =
            actionsServiceUnderTest.getActionStatsByUuidsQuery(actionScopesApiInputDTO).stream()
                .collect(Collectors.toMap(EntityStatsApiDTO::getUuid, Function.identity()));

        verify(actionStatsQueryExecutor).retrieveActionStats(expectedQuery);
        verify(repositoryApi).getServiceEntitiesById(
                ServiceEntitiesRequest.newBuilder(Collections.singleton(1L))
                    .build());

        assertThat(statsByUuid.keySet(), containsInAnyOrder("1", "3"));
        assertThat(statsByUuid.get("1").getStats(), is(scope1Snapshots));
        assertThat(statsByUuid.get("1").getDisplayName(), is(serviceEntityOne.getDisplayName()));
        assertThat(statsByUuid.get("1").getClassName(), is(serviceEntityOne.getClassName()));

        assertThat(statsByUuid.get("3").getStats(), is(scope2Snapshots));
    }

    private static AcceptActionResponse acceptanceError(@Nonnull final String error) {
        return AcceptActionResponse.newBuilder()
                .setError(error)
                .build();
    }
}
