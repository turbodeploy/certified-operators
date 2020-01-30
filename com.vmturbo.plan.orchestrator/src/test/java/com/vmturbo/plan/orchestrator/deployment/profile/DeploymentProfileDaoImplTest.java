package com.vmturbo.plan.orchestrator.deployment.profile;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Sets;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfile;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.plan.orchestrator.db.Tables;
import com.vmturbo.plan.orchestrator.db.tables.records.TemplateRecord;
import com.vmturbo.plan.orchestrator.db.tables.records.TemplateToDeploymentProfileRecord;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

/**
 * Unit tests for {@link DeploymentProfileDaoImpl}.
 */
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

    /**
     * Common setup code, mainly to prepare the database.
     *
     * @throws Exception If anything goes wrong.
     */
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

    /**
     * Clean up the database after the test runs.
     */
    @After
    public void teardown() {
        flyway.clean();
    }

    /**
     * Test the creation of a deployment profile.
     */
    @Test
    public void testCreateDeploymentProfile() {
        DeploymentProfileInfo deploymentProfileInfo = DeploymentProfileInfo.newBuilder()
            .setName("test-deployment-profile")
            .build();
        DeploymentProfile result = deploymentProfileDao.createDeploymentProfile(deploymentProfileInfo);
        assertEquals(result.getDeployInfo(), deploymentProfileInfo);
    }

    /**
     * Test getting a created deployment profile by ID.
     */
    @Test
    public void testGetDeploymentProfile() {
        DeploymentProfileInfo deploymentProfileInfo = DeploymentProfileInfo.newBuilder()
            .setName("test-deployment-profile")
            .build();
        DeploymentProfile createdDeploymentProfile = deploymentProfileDao.createDeploymentProfile(deploymentProfileInfo);
        Optional<DeploymentProfile> retrievedDeploymentProfile = deploymentProfileDao.getDeploymentProfile(createdDeploymentProfile.getId());

        assertEquals(retrievedDeploymentProfile.get(), createdDeploymentProfile);
    }

    /**
     * Test getting a created deployment profile by the associated templates ID.
     */
    @Test
    public void testGetDeploymentProfilesForTemplates() {
        final DeploymentProfileInfo info1 = DeploymentProfileInfo.newBuilder()
            .setName("test1")
            .build();
        final DeploymentProfileInfo info2 = DeploymentProfileInfo.newBuilder()
            .setName("test2")
            .build();
        final DeploymentProfileInfo info3 = DeploymentProfileInfo.newBuilder()
            .setName("test3")
            .build();
        final DeploymentProfile profile1 = deploymentProfileDao.createDeploymentProfile(info1);
        final DeploymentProfile profile2 = deploymentProfileDao.createDeploymentProfile(info2);
        final DeploymentProfile profile3 = deploymentProfileDao.createDeploymentProfile(info3);

        final long t1Id = 1;
        final long t2Id = 2;

        // Create the template records to satisfy foreign key constraints.
        createTemplateRecord(t1Id).store();
        createTemplateRecord(t2Id).store();

        createTemplateToDepProfRecord(t1Id, profile1.getId()).store();
        createTemplateToDepProfRecord(t1Id, profile2.getId()).store();
        createTemplateToDepProfRecord(t2Id, profile3.getId()).store();

        final Map<Long, Set<DeploymentProfile>> profilesByTemplateId =
            deploymentProfileDao.getDeploymentProfilesForTemplates(Sets.newHashSet(t2Id, t1Id));
        assertThat(profilesByTemplateId.keySet(), containsInAnyOrder(t1Id, t2Id));
        assertThat(profilesByTemplateId.get(t1Id), containsInAnyOrder(profile1, profile2));
        assertThat(profilesByTemplateId.get(t2Id), containsInAnyOrder(profile3));
    }

    /**
     * Test getting all created deployment profiles.
     */
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

    /**
     * Test editing a previously created deployment profile.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testEditProfile() throws Exception {
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

    /**
     * Test deleting a previously created deployment profile.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testDeleteDeploymentProfile() throws Exception {
        DeploymentProfileInfo deploymentProfileInfo = DeploymentProfileInfo.newBuilder()
            .setName("test-deployment-profile")
            .build();
        DeploymentProfile createdDeploymentProfile = deploymentProfileDao.createDeploymentProfile(deploymentProfileInfo);
        DeploymentProfile deletedDeploymentProfile = deploymentProfileDao.deleteDeploymentProfile(createdDeploymentProfile.getId());
        assertEquals(deletedDeploymentProfile, createdDeploymentProfile);
    }

    /**
     * Test that all created deployment profiles appear in collected diags.
     *
     * @throws Exception If anything goes wrong.
     */
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

        final DiagnosticsAppender appender = Mockito.mock(DiagnosticsAppender.class);
        deploymentProfileDao.collectDiags(appender);
        final ArgumentCaptor<String> diags = ArgumentCaptor.forClass(String.class);
        Mockito.verify(appender, Mockito.atLeastOnce()).appendString(diags.capture());
        final List<String> expected = Stream.of(first, second)
            .map(profile -> DeploymentProfileDaoImpl.GSON.toJson(profile, DeploymentProfile.class))
            .collect(Collectors.toList());

       assertTrue(diags.getAllValues().containsAll(expected));
       assertTrue(expected.containsAll(diags.getAllValues()));
    }

    /**
     * Test that restoring from diags correctly creates deployment profiles in the database.
     *
     * @throws Exception If anything goes wrong.
     */
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

    private TemplateRecord createTemplateRecord(final long templateId) {
        final TemplateRecord t1Record = dbConfig.dsl().newRecord(Tables.TEMPLATE);
        t1Record.setId(templateId);
        t1Record.setName("foo");
        t1Record.setTemplateInfo(TemplateInfo.getDefaultInstance());
        t1Record.setEntityType(1);
        t1Record.setType("blah");
        return t1Record;
    }

    private TemplateToDeploymentProfileRecord createTemplateToDepProfRecord(final long templateId, final long profileId) {
        final TemplateToDeploymentProfileRecord t1Record1 =
            dbConfig.dsl().newRecord(Tables.TEMPLATE_TO_DEPLOYMENT_PROFILE);
        t1Record1.setTemplateId(templateId);
        t1Record1.setDeploymentProfileId(profileId);
        return t1Record1;
    }
}
