package com.vmturbo.api.component.external.api.service;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ScenarioMapper;
import com.vmturbo.api.dto.scenario.ScenarioApiDTO;
import com.vmturbo.api.dto.scenario.ScenarioApiDTO;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.GetScenariosOptions;
import com.vmturbo.common.protobuf.plan.PlanDTO.Scenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioId;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.PlanDTO.UpdateScenarioRequest;
import com.vmturbo.common.protobuf.plan.ScenarioServiceGrpc.ScenarioServiceImplBase;
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

    /**
     * Jackson mapper to serialize API DTO objects to JSON strings.
     */
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * The JSON-serialized version of the response API DTO.
     */
    private String apiDTOJson;

    /**
     * The backend the API forwards calls to (i.e. the part that's in the plan orchestrator).
     */
    private final TestScenarioRpcService scenarioServiceBackend =
            Mockito.spy(new TestScenarioRpcService());

    private ScenariosService scenariosService;

    private ScenarioMapper scenarioMapper;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(scenarioServiceBackend);

    @Before
    public void setup() throws IOException {

        final RepositoryApi repositoryApi = Mockito.mock(RepositoryApi.class);
        Mockito.when(repositoryApi.getServiceEntitiesById(Mockito.any()))
               .thenReturn(Collections.emptyMap());
        scenarioMapper = new ScenarioMapper(repositoryApi);

        scenariosService = new ScenariosService(grpcServer.getChannel(), scenarioMapper);

        apiDTOJson = objectMapper.writeValueAsString(scenarioMapper.toScenarioApiDTO(SCENARIO_RESPONSE));
    }

    @Test
    public void testGetScenarios() throws Exception {
        List<ScenarioApiDTO> result = scenariosService.getScenarios(true);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(apiDTOJson, objectMapper.writeValueAsString(result.get(0)));

        Mockito.verify(scenarioServiceBackend).getScenarios(
                Mockito.eq(GetScenariosOptions.getDefaultInstance()),
                Mockito.any());
    }

    @Test
    public void testGetScenarioByName() throws Exception {
        ScenarioApiDTO result = scenariosService.getScenario(SCENARIO_RESPONSE.getId());
        Assert.assertEquals(apiDTOJson, objectMapper.writeValueAsString(result));

        Mockito.verify(scenarioServiceBackend).getScenario(
                Mockito.eq(ScenarioId.newBuilder()
                    .setScenarioId(SCENARIO_RESPONSE.getId())
                    .build()),
                Mockito.any());
    }

    @Test
    public void testGetScenarioByNameNotExists() throws Exception {
        expectedException.expect(UnknownObjectException.class);
        scenariosService.getScenario(0L);
    }

    @Test
    public void testCreateScenario() throws Exception {
        final ScenarioApiDTO scenarioApiDTO = new ScenarioApiDTO();
        scenarioApiDTO.setDisplayName(SCENARIO_RESPONSE.getScenarioInfo().getName());
        ScenarioApiDTO result = scenariosService.createScenario(scenarioApiDTO);
        Assert.assertEquals(apiDTOJson, objectMapper.writeValueAsString(result));

        Mockito.verify(scenarioServiceBackend).createScenario(
                Mockito.eq(SCENARIO_RESPONSE.getScenarioInfo()),
                Mockito.any());
    }

    @Test
    public void testConfigureScenario() throws Exception {
        ScenarioApiDTO result = scenariosService.configureScenario(
                SCENARIO_RESPONSE.getId(), SCENARIO_RESPONSE.getScenarioInfo().getName(), null,
                null, null, null, null, null, null, null, null, null, null, null,
                new ScenarioApiDTO());

        Assert.assertEquals(apiDTOJson, objectMapper.writeValueAsString(result));
        Mockito.verify(scenarioServiceBackend).updateScenario(
                Mockito.eq(UpdateScenarioRequest.newBuilder()
                        .setScenarioId(SCENARIO_RESPONSE.getId())
                        .setNewInfo(SCENARIO_RESPONSE.getScenarioInfo())
                        .build()),
                Mockito.any());
    }

    @Test
    public void testConfigureScenarioNotExists() throws Exception {
        expectedException.expect(UnknownObjectException.class);
        scenariosService.configureScenario(
                0L, "newName", null,
                null, null, null, null, null, null, null, null, null, null, null,
                new ScenarioApiDTO());
    }

    @Test
    public void testDeleteScenario() throws Exception {
        Assert.assertTrue(scenariosService.deleteScenario(SCENARIO_RESPONSE.getId()));
    }

    @Test
    public void testDeleteScenarioNotExisting() throws Exception {
        expectedException.expect(UnknownObjectException.class);
        scenariosService.deleteScenario(0L);
    }

    /**
     * A test implementation of scenario service that returns predictable responses.
     */
    private class TestScenarioRpcService extends ScenarioServiceImplBase {
        public void createScenario(ScenarioInfo request,
                                   StreamObserver<Scenario> responseObserver) {
            responseObserver.onNext(Scenario.newBuilder()
                    .setId(SCENARIO_RESPONSE.getId())
                    .setScenarioInfo(request)
                    .build());
            responseObserver.onCompleted();
        }

        public void getScenario(PlanDTO.ScenarioId request,
                                StreamObserver<PlanDTO.Scenario> responseObserver) {
            if (request.getScenarioId() == SCENARIO_RESPONSE.getId()) {
                responseObserver.onNext(SCENARIO_RESPONSE);
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(Status.NOT_FOUND.asException());
            }
        }

        public void getScenarios(PlanDTO.GetScenariosOptions request,
                                 StreamObserver<PlanDTO.Scenario> responseObserver) {
            responseObserver.onNext(SCENARIO_RESPONSE);
            responseObserver.onCompleted();
        }

        public void updateScenario(UpdateScenarioRequest request,
                                   StreamObserver<PlanDTO.UpdateScenarioResponse> responseObserver) {
            if (request.getScenarioId() == SCENARIO_RESPONSE.getId()) {
                responseObserver.onNext(PlanDTO.UpdateScenarioResponse.newBuilder()
                        .setScenario(PlanDTO.Scenario.newBuilder()
                                .setId(request.getScenarioId())
                                .setScenarioInfo(request.getNewInfo()))
                        .build());
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(Status.NOT_FOUND.asException());
            }
        }

        public void deleteScenario(PlanDTO.ScenarioId request,
                                   StreamObserver<PlanDTO.DeleteScenarioResponse> responseObserver) {
            if (request.getScenarioId() == SCENARIO_RESPONSE.getId()) {
                responseObserver.onNext(PlanDTO.DeleteScenarioResponse.newBuilder()
                        .setScenario(SCENARIO_RESPONSE)
                        .build());
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(Status.NOT_FOUND.asException());
            }
        }

    }
}
