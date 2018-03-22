package com.vmturbo.plan.orchestrator.deployment.profile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfile;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.common.diagnostics.Diagnosable.DiagnosticsException;
import com.vmturbo.plan.orchestrator.plan.DiscoveredNotSupportedOperationException;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
    classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=plan"})
public class DeploymentProfileDaoImplTest {

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    private Flyway flyway;

    private DeploymentProfileDaoImpl deploymentProfileDao;

    @Before
    public void setup() throws Exception {
        IdentityGenerator.initPrefix(0);
        prepareDatabase();
    }

    private void prepareDatabase() throws Exception {
        flyway = dbConfig.flyway();
        final DSLContext dsl = dbConfig.dsl();
        deploymentProfileDao = new DeploymentProfileDaoImpl(dsl);
        flyway.clean();
        flyway.migrate();
    }

    @After
    public void teardown() {
        flyway.clean();
    }

    @Test
    public void testCreateDeploymentProfile() {
        DeploymentProfileInfo deploymentProfileInfo = DeploymentProfileInfo.newBuilder()
            .setName("test-deployment-profile")
            .build();
        DeploymentProfile result = deploymentProfileDao.createDeploymentProfile(deploymentProfileInfo);
        assertEquals(result.getDeployInfo(), deploymentProfileInfo);
    }

    @Test
    public void testGetDeploymentProfile() {
        DeploymentProfileInfo deploymentProfileInfo = DeploymentProfileInfo.newBuilder()
            .setName("test-deployment-profile")
            .build();
        DeploymentProfile createdDeploymentProfile = deploymentProfileDao.createDeploymentProfile(deploymentProfileInfo);
        Optional<DeploymentProfile> retrievedDeploymentProfile = deploymentProfileDao.getDeploymentProfile(createdDeploymentProfile.getId());

        assertEquals(retrievedDeploymentProfile.get(), createdDeploymentProfile);
    }

    @Test
    public void testGetAllDeploymentProfile() {
        DeploymentProfileInfo firstDeploymentProfile = DeploymentProfileInfo.newBuilder()
            .setName("first-deployment-profile")
            .build();
        DeploymentProfileInfo secondDeploymentProfile = DeploymentProfileInfo.newBuilder()
            .setName("second-deployment-profile")
            .build();

        DeploymentProfile createdFirst = deploymentProfileDao.createDeploymentProfile(firstDeploymentProfile);
        DeploymentProfile createdSecond = deploymentProfileDao.createDeploymentProfile(secondDeploymentProfile);
        Set<DeploymentProfile> retrievedDeployments = deploymentProfileDao.getAllDeploymentProfiles();
        assertTrue(retrievedDeployments.size() == 2);
        assertTrue(retrievedDeployments.stream()
            .anyMatch(deploymentProfile -> deploymentProfile.getId() == createdFirst.getId()));
        assertTrue(retrievedDeployments.stream()
            .anyMatch(deploymentProfile -> deploymentProfile.getId() == createdSecond.getId()));
    }

    @Test
    public void testEditTemplate() throws NoSuchObjectException, DiscoveredNotSupportedOperationException {
        DeploymentProfileInfo firstDeploymentProfile = DeploymentProfileInfo.newBuilder()
            .setName("first-deployment-profile")
            .build();
        DeploymentProfileInfo secondDeploymentProfile = DeploymentProfileInfo.newBuilder()
            .setName("second-deployment-profile")
            .build();
        DeploymentProfile createdFirst = deploymentProfileDao.createDeploymentProfile(firstDeploymentProfile);
        DeploymentProfile newDeploymentProfile = deploymentProfileDao.editDeploymentProfile(createdFirst.getId(),
            secondDeploymentProfile);
        Optional<DeploymentProfile> retrievedDeploymentProfile = deploymentProfileDao.getDeploymentProfile(createdFirst.getId());
        assertEquals(retrievedDeploymentProfile.get(), newDeploymentProfile);
        assertEquals(retrievedDeploymentProfile.get().getDeployInfo(), secondDeploymentProfile);
    }

    @Test
    public void testDeleteDeploymentProfile() throws NoSuchObjectException, DiscoveredNotSupportedOperationException {
        DeploymentProfileInfo deploymentProfileInfo = DeploymentProfileInfo.newBuilder()
            .setName("test-deployment-profile")
            .build();
        DeploymentProfile createdDeploymentProfile = deploymentProfileDao.createDeploymentProfile(deploymentProfileInfo);
        DeploymentProfile deletedDeploymentProfile = deploymentProfileDao.deleteDeploymentProfile(createdDeploymentProfile.getId());
        assertEquals(deletedDeploymentProfile, createdDeploymentProfile);
    }

    @Test
    public void testCollectDiags() throws Exception {
        DeploymentProfileInfo firstInfo = DeploymentProfileInfo.newBuilder()
            .setName("first-deployment-profile")
            .build();
        DeploymentProfileInfo secondInfo = DeploymentProfileInfo.newBuilder()
            .setName("second-deployment-profile")
            .build();
        DeploymentProfile first = deploymentProfileDao.createDeploymentProfile(firstInfo);
        DeploymentProfile second = deploymentProfileDao.createDeploymentProfile(secondInfo);

        final List<String> result = deploymentProfileDao.collectDiags();
        final List<String> expected = Stream.of(first, second)
            .map(profile -> DeploymentProfileDaoImpl.GSON.toJson(profile, DeploymentProfile.class))
            .collect(Collectors.toList());

       assertTrue(result.containsAll(expected));
       assertTrue(expected.containsAll(result));
    }

    @Test
    public void testRestoreFromDiags() throws Exception {

        final DeploymentProfile preexisting = deploymentProfileDao.createDeploymentProfile(
            DeploymentProfileInfo.newBuilder().setName("preexisting").build());

        final List<String> diags = Arrays.asList(
            "{\"id\":\"1997875644768\",\"deployInfo\":{\"name\":\"first-deployment-profile\"}}",
            "{\"id\":\"1997875644928\",\"deployInfo\":{\"name\":\"second-deployment-profile\"}}"
        );

        try {
            deploymentProfileDao.restoreDiags(diags);
            fail();
        } catch (DiagnosticsException e) {
            assertTrue(e.hasErrors());
            assertEquals(1, e.getErrors().size());
            assertTrue(e.getErrors().get(0).contains("preexisting deployment profiles"));
        }

        final Set<DeploymentProfile> result = deploymentProfileDao.getAllDeploymentProfiles();

        assertEquals(2, result.size());
        assertFalse(result.contains(preexisting));
        final List<DeploymentProfile> expected = diags.stream()
            .map(serial -> DeploymentProfileDaoImpl.GSON.fromJson(serial, DeploymentProfile.class))
            .collect(Collectors.toList());
        assertTrue(expected.containsAll(result));
        assertTrue(result.containsAll(expected));


    }
}
