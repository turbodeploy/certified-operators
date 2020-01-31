package com.vmturbo.plan.orchestrator.scenario;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import io.grpc.Status.Code;

import org.flywaydb.core.Flyway;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.DeleteScenarioResponse;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.GetScenariosOptions;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.Scenario;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioId;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.UpdateScenarioRequest;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.UpdateScenarioResponse;
import com.vmturbo.common.protobuf.plan.ScenarioServiceGrpc;
import com.vmturbo.common.protobuf.plan.ScenarioServiceGrpc.ScenarioServiceBlockingStub;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
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

    private ScenarioServiceBlockingStub scenarioServiceClient;

    private ScenarioDao scenarioDao;

    private GroupServiceMole groupServiceMole = spy(GroupServiceMole.class);

    private GroupServiceBlockingStub groupServiceClient;

    private SearchServiceBlockingStub searchServiceClient;

    private GrpcTestServer groupGrpcServer;

    private final UserSessionContext userSessionContext = mock(UserSessionContext.class);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Not a rule, because we need the DB config for the service that goes
     * into the server.
     */
    private GrpcTestServer grpcServer;

    @Before
    public void setup() throws Exception {
        IdentityGenerator.initPrefix(0);
        prepareDatabase();
        prepareGrpc();
    }

    private void prepareDatabase() throws Exception {
        flyway = dbConfig.flyway();

        // Clean the database and bring it up to the production configuration before running test
        flyway.clean();
        flyway.migrate();
    }

    private void prepareGrpc() throws Exception {
        // test grpc server for the group service -- it's separate to avoid a circular dependency
        groupGrpcServer = GrpcTestServer.newServer(groupServiceMole);
        groupGrpcServer.start();
        groupServiceClient = GroupServiceGrpc.newBlockingStub(groupGrpcServer.getChannel());

        scenarioDao = new ScenarioDao(dbConfig.dsl());
        scenarioRpcService = new ScenarioRpcService(scenarioDao, new IdentityInitializer(0),
                userSessionContext, groupServiceClient, searchServiceClient);
        grpcServer = GrpcTestServer.newServer(scenarioRpcService);
        grpcServer.start();
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
        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.NOT_FOUND)
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
        // on successful delete, the response object should be valid.
        Assert.assertNotNull(response);
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
        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.NOT_FOUND)
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

    @Test
    public void testCollectDiags() throws Exception {
        scenarioServiceClient.createScenario(ScenarioInfo.newBuilder()
            .setName("FirstScenario")
            .addChanges(topologyAdditionChange())
            .build());

        scenarioServiceClient.createScenario(ScenarioInfo.newBuilder()
            .setName("SecondScenario")
            .addChanges(topologyAdditionChange())
            .build());

        final List<String> scenarios = scenarioDao.getScenarios().stream()
            .map(scenario -> ScenarioDao.GSON.toJson(scenario,
                com.vmturbo.plan.orchestrator.db.tables.pojos.Scenario.class))
            .collect(Collectors.toList());

        final DiagnosticsAppender appender = Mockito.mock(DiagnosticsAppender.class);
        scenarioDao.collectDiags(appender);
        final ArgumentCaptor<String> diags = ArgumentCaptor.forClass(String.class);
        Mockito.verify(appender, Mockito.atLeastOnce()).appendString(diags.capture());

        assertEquals(scenarios, diags.getAllValues());
    }

    @Test
    public void testRestoreFromDiags() throws Exception {

        scenarioServiceClient.createScenario(ScenarioInfo.newBuilder()
            .setName("preexisting").addChanges(topologyAdditionChange()).build());

        final com.vmturbo.plan.orchestrator.db.tables.pojos.Scenario preexisting =
            scenarioDao.getScenarios().get(0);

        final List<String> serialized = Arrays.asList(
            "{\"id\":\"1997624970512\",\"createTime\":{\"date\":{\"year\":2018,\"month\":3," +
                "\"day\":12},\"time\":{\"hour\":12,\"minute\":29,\"second\":5,\"nano\":0}}," +
                "\"updateTime\":{\"date\":{\"year\":2018,\"month\":3,\"day\":12},\"time\":" +
                "{\"hour\":12,\"minute\":29,\"second\":5,\"nano\":0}},\"scenarioInfo\":" +
                "{\"name\":\"FirstScenario\",\"changes\":[{\"topologyAddition\":" +
                "{\"additionCount\":2,\"entityId\":\"1234\"}}]}}",
            "{\"id\":\"1997624975312\",\"createTime\":{\"date\":{\"year\":2018,\"month\":3," +
                "\"day\":12},\"time\":{\"hour\":12,\"minute\":29,\"second\":6,\"nano\":0}}," +
                "\"updateTime\":{\"date\":{\"year\":2018,\"month\":3,\"day\":12},\"time\":" +
                "{\"hour\":12,\"minute\":29,\"second\":6,\"nano\":0}},\"scenarioInfo\":" +
                "{\"name\":\"SecondScenario\",\"changes\":[{\"topologyAddition\":" +
                "{\"additionCount\":2,\"entityId\":\"1234\"}}]}}"
        );

        try {
            scenarioDao.restoreDiags(serialized);
            fail();
        } catch (DiagnosticsException e) {
            assertTrue(e.hasErrors());
            assertEquals(1, e.getErrors().size());
            assertTrue(e.getErrors().get(0).contains("preexisting scenarios"));
        }

        final List<com.vmturbo.plan.orchestrator.db.tables.pojos.Scenario> result =
            scenarioDao.getScenarios();

        assertEquals(2, result.size());
        assertFalse(result.contains(preexisting));

        serialized.stream()
            .map(str -> ScenarioDao.GSON.fromJson(str,
                com.vmturbo.plan.orchestrator.db.tables.pojos.Scenario.class))
            .forEach(scenario -> assertTrue(scenarioDao.getScenario(scenario.getId()).isPresent()));

    }

}
