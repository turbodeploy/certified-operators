package com.vmturbo.plan.orchestrator.scenario;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.google.common.collect.ImmutableList;

import io.grpc.Status.Code;

import com.vmturbo.common.protobuf.plan.PlanDTO.DeleteScenarioResponse;
import com.vmturbo.common.protobuf.plan.PlanDTO.GetScenariosOptions;
import com.vmturbo.common.protobuf.plan.PlanDTO.Scenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioId;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.PlanDTO.UpdateScenarioRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.UpdateScenarioResponse;
import com.vmturbo.common.protobuf.plan.ScenarioServiceGrpc;
import com.vmturbo.common.protobuf.plan.ScenarioServiceGrpc.ScenarioServiceBlockingStub;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.components.api.test.GrpcExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
    loader = AnnotationConfigContextLoader.class,
    classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=plan"})
public class ScenarioRpcServiceTest {

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    private Flyway flyway;

    private ScenarioRpcService scenarioRpcService;

    private GrpcTestServer grpcServer;

    private ScenarioServiceBlockingStub scenarioServiceClient;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() throws Exception {
        IdentityGenerator.initPrefix(0);
        prepareDatabase();
        prepareGrpc();
    }

    private void prepareDatabase() throws Exception {
        flyway = dbConfig.flyway();
        final DSLContext dsl = dbConfig.dsl();
        scenarioRpcService = new ScenarioRpcService(dsl, new IdentityInitializer(0));

        // Clean the database and bring it up to the production configuration before running test
        flyway.clean();
        flyway.migrate();
    }

    private void prepareGrpc() throws Exception {
        grpcServer = GrpcTestServer.withServices(scenarioRpcService);
        scenarioServiceClient = ScenarioServiceGrpc.newBlockingStub(grpcServer.getChannel());
    }

    @After
    public void teardown() {
        grpcServer.close();
        flyway.clean();
    }

    @Test
    public void testCreateScenario() throws Exception {
        ScenarioInfo toCreate = ScenarioInfo.newBuilder()
            .setName("Scenario")
            .addChanges(topologyAdditionChange())
            .build();

        // Test that when you get the scenarios there are none.
        Iterator<Scenario> scenariosIter = scenarioServiceClient.getScenarios(GetScenariosOptions.getDefaultInstance());
        assertFalse(scenariosIter.hasNext());

        Scenario createdScenario = scenarioServiceClient.createScenario(toCreate);
        assertEquals(toCreate, createdScenario.getScenarioInfo());

        // Test that there are now scenarios.
        scenariosIter = scenarioServiceClient.getScenarios(GetScenariosOptions.getDefaultInstance());
        assertTrue(scenariosIter.hasNext());
    }

    @Test
    public void testUpdateScenario() throws Exception {
        ScenarioInfo toCreate = ScenarioInfo.newBuilder()
                .setName("Scenario")
                .addChanges(topologyAdditionChange())
                .build();
        Scenario createdScenario = scenarioServiceClient.createScenario(toCreate);

        ScenarioInfo newInfo = ScenarioInfo.newBuilder()
                .setName("New Scenario")
                .build();
        UpdateScenarioResponse response = scenarioServiceClient.updateScenario(
                UpdateScenarioRequest.newBuilder()
                    .setScenarioId(createdScenario.getId())
                    .setNewInfo(newInfo)
                    .build());
        Assert.assertTrue(response.hasScenario());
        Assert.assertEquals(newInfo, response.getScenario().getScenarioInfo());
    }

    @Test
    public void testUpdateNonExistingScenario() throws Exception {
        expectedException.expect(GrpcExceptionMatcher.code(Code.NOT_FOUND)
                .descriptionContains("1"));
        scenarioServiceClient.updateScenario(
                UpdateScenarioRequest.newBuilder()
                        .setScenarioId(1L)
                        .setNewInfo(ScenarioInfo.newBuilder()
                            .setName("Edited")
                            .build())
                        .build());
    }

    @Test
    public void testDeleteScenario() throws Exception {
        ScenarioInfo newInfo = ScenarioInfo.newBuilder()
                .setName("New Scenario")
                .build();
        Scenario createdScenario = scenarioServiceClient.createScenario(newInfo);

        DeleteScenarioResponse response = scenarioServiceClient.deleteScenario(
            ScenarioId.newBuilder()
                .setScenarioId(createdScenario.getId())
                .build());
        Assert.assertTrue(response.hasScenario());
        Assert.assertEquals(newInfo, response.getScenario().getScenarioInfo());
    }

    @Test
    public void testDeleteNonExistingScenario() throws Exception {
        expectedException.expect(GrpcExceptionMatcher.code(Code.NOT_FOUND)
                .descriptionContains("1"));
        scenarioServiceClient.deleteScenario(
                ScenarioId.newBuilder()
                        .setScenarioId(1)
                        .build());
    }

    @Test
    public void testGetScenario() throws Exception {
        // Create a scenario and get it.
        ScenarioInfo toCreate = ScenarioInfo.newBuilder()
            .setName("Scenario")
            .addChanges(topologyAdditionChange())
            .build();

        Scenario createdScenario = scenarioServiceClient.createScenario(toCreate);
        Scenario retrievedScenario = scenarioServiceClient.getScenario(
            ScenarioId.newBuilder()
                .setScenarioId(createdScenario.getId())
                .build());

        assertEquals(retrievedScenario, createdScenario);
    }

    @Test
    public void testGetNonExistingScenario() throws Exception {
        expectedException.expect(GrpcExceptionMatcher.code(Code.NOT_FOUND)
                .descriptionContains("1234"));
        scenarioServiceClient.getScenario(
            ScenarioId.newBuilder()
                .setScenarioId(1234L)
                .build());
    }

    @Test
    public void testGetScenarios() throws Exception {
        // Create two scenarios and get them both.
        scenarioServiceClient.createScenario(ScenarioInfo.newBuilder()
            .setName("FirstScenario")
            .addChanges(topologyAdditionChange())
            .build());

        scenarioServiceClient.createScenario(ScenarioInfo.newBuilder()
            .setName("SecondScenario")
            .addChanges(topologyAdditionChange())
            .build());

        List<Scenario> scenarios = ImmutableList.copyOf(
            scenarioServiceClient.getScenarios(GetScenariosOptions.getDefaultInstance()));

        assertEquals(2, scenarios.size());
        assertThat(
            scenarios.stream()
                .map(Scenario::getScenarioInfo)
                .map(ScenarioInfo::getName)
                .collect(Collectors.toList()),
            containsInAnyOrder(Arrays.asList("FirstScenario", "SecondScenario").toArray())
        );
    }

    private static ScenarioChange topologyAdditionChange() {
        return ScenarioChange.newBuilder()
            .setTopologyAddition(TopologyAddition.newBuilder()
                .setAdditionCount(2)
                .setEntityId(1234L))
            .build();
    }
}