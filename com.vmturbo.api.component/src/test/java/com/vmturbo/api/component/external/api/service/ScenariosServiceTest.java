package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.grpc.Status;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.api.component.external.api.mapper.ScenarioMapper;
import com.vmturbo.api.dto.scenario.ScenarioApiDTO;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.common.protobuf.plan.ScenarioMoles.ScenarioServiceMole;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.DeleteScenarioResponse;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.GetScenariosOptions;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.Scenario;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioId;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.UpdateScenarioRequest;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.UpdateScenarioResponse;
import com.vmturbo.common.protobuf.plan.ScenarioServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Unit tests for {@link ScenariosService}.
 */
public class ScenariosServiceTest {

    private static final Scenario SCENARIO_RESPONSE = Scenario.newBuilder()
            .setId(7)
            .setScenarioInfo(ScenarioInfo.newBuilder()
                .setName("test"))
            .build();

    private final ScenarioServiceMole scenarioServiceMole =
            Mockito.spy(new ScenarioServiceMole());

    private ScenariosService scenariosService;

    private ScenarioMapper scenarioMapper = mock(ScenarioMapper.class);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(scenarioServiceMole);

    @Before
    public void setup() throws IOException {
        scenariosService = new ScenariosService(
                ScenarioServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                    scenarioMapper);
    }

    @Test
    public void testGetScenarios() throws Exception {
        final ScenarioApiDTO apiDTO = new ScenarioApiDTO();
        when(scenarioServiceMole.getScenarios(GetScenariosOptions.getDefaultInstance()))
                .thenReturn(Collections.singletonList(SCENARIO_RESPONSE));
        when(scenarioMapper.toScenarioApiDTO(SCENARIO_RESPONSE))
                .thenReturn(apiDTO);

        final List<ScenarioApiDTO> result = scenariosService.getScenarios(true);
        assertThat(result, containsInAnyOrder(apiDTO));
    }

    @Test
    public void testGetScenarioByName() throws Exception {
        when(scenarioServiceMole.getScenario(ScenarioId.newBuilder()
                        .setScenarioId(SCENARIO_RESPONSE.getId())
                        .build()))
            .thenReturn(SCENARIO_RESPONSE);
        final ScenarioApiDTO apiDTO = new ScenarioApiDTO();
        when(scenarioMapper.toScenarioApiDTO(SCENARIO_RESPONSE))
                .thenReturn(apiDTO);

        ScenarioApiDTO result = scenariosService.getScenario(SCENARIO_RESPONSE.getId());
        assertThat(result, is(apiDTO));
    }

    @Test
    public void testGetScenarioByNameNotExists() throws Exception {
        when(scenarioServiceMole.getScenarioError(any()))
            .thenReturn(Optional.of(Status.NOT_FOUND.asException()));
        expectedException.expect(UnknownObjectException.class);
        scenariosService.getScenario(0L);
    }

    @Test
    public void testCreateScenario() throws Exception {
        final ScenarioApiDTO scenarioApiDTO = new ScenarioApiDTO();
        scenarioApiDTO.setDisplayName(SCENARIO_RESPONSE.getScenarioInfo().getName());

        when(scenarioMapper.toScenarioInfo(scenarioApiDTO.getDisplayName(), scenarioApiDTO))
                .thenReturn(ScenarioInfo.getDefaultInstance());
    }

    @Test
    public void testConfigureScenario() throws Exception {
        when(scenarioServiceMole.updateScenario(UpdateScenarioRequest.newBuilder()
                .setScenarioId(SCENARIO_RESPONSE.getId())
                .setNewInfo(SCENARIO_RESPONSE.getScenarioInfo())
                .build()))
            .thenReturn(UpdateScenarioResponse.newBuilder()
                    .setScenario(SCENARIO_RESPONSE)
                    .build());

        final ScenarioApiDTO apiDTO = new ScenarioApiDTO();
        when(scenarioMapper.toScenarioInfo(SCENARIO_RESPONSE.getScenarioInfo().getName(), apiDTO))
                .thenReturn(SCENARIO_RESPONSE.getScenarioInfo());

        when(scenarioMapper.toScenarioApiDTO(SCENARIO_RESPONSE))
                .thenReturn(apiDTO);

        ScenarioApiDTO result = scenariosService.configureScenario(
                SCENARIO_RESPONSE.getId(), SCENARIO_RESPONSE.getScenarioInfo().getName(), null,
                null, null, null, null, null, null, null, null, null, null, null,
                apiDTO);

        assertThat(result, is(apiDTO));
    }

    @Test
    public void testConfigureScenarioNotExists() throws Exception {
        when(scenarioServiceMole.updateScenarioError(any()))
            .thenReturn(Optional.of(Status.NOT_FOUND.asException()));
        when(scenarioMapper.toScenarioInfo(anyString(), any()))
            .thenReturn(ScenarioInfo.getDefaultInstance());
        expectedException.expect(UnknownObjectException.class);
        scenariosService.configureScenario(
                0L, "newName", null,
                null, null, null, null, null, null, null, null, null, null, null,
                new ScenarioApiDTO());
    }

    @Test
    public void testDeleteScenario() throws Exception {
        when(scenarioServiceMole.deleteScenario(ScenarioId.newBuilder()
                .setScenarioId(SCENARIO_RESPONSE.getId())
                .build()))
            .thenReturn(DeleteScenarioResponse.newBuilder()
                    .build());

        Assert.assertTrue(scenariosService.deleteScenario(SCENARIO_RESPONSE.getId()));
    }

    @Test
    public void testDeleteScenarioNotExisting() throws Exception {
        when(scenarioServiceMole.deleteScenarioError(any()))
                .thenReturn(Optional.of(Status.NOT_FOUND.asException()));
        expectedException.expect(UnknownObjectException.class);
        scenariosService.deleteScenario(0L);
    }
}
