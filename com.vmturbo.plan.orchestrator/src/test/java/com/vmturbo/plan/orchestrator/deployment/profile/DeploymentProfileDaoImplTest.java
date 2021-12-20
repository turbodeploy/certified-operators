package com.vmturbo.plan.orchestrator.deployment.profile;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Sets;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfile;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.plan.orchestrator.PlanOrchestratorDBEndpointConfig;
import com.vmturbo.plan.orchestrator.db.Plan;
import com.vmturbo.plan.orchestrator.db.Tables;
import com.vmturbo.plan.orchestrator.db.tables.records.TemplateRecord;
import com.vmturbo.plan.orchestrator.db.tables.records.TemplateToDeploymentProfileRecord;
import com.vmturbo.plan.orchestrator.deployment.profile.DeploymentProfileDaoImplTest.TestPlanOrchestratorDBEndpointConfig;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointCompleter;
import com.vmturbo.sql.utils.DbEndpointTestRule;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * Unit tests for {@link DeploymentProfileDaoImpl}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {TestPlanOrchestratorDBEndpointConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_CLASS)
@TestPropertySource(properties = {"sqlDialect=MARIADB"})
public class DeploymentProfileDaoImplTest {

    @Autowired(required = false)
    private TestPlanOrchestratorDBEndpointConfig dbEndpointConfig;

    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Plan.PLAN);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    /**
     * Test rule to use {@link DbEndpoint}s in test.
     */
    @Rule
    public DbEndpointTestRule dbEndpointTestRule = new DbEndpointTestRule("plan-orchestrator");

    /**
     * Rule to manage feature flag enablement to make sure FeatureFlagManager store is set up.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule().testAllCombos(
            FeatureFlags.POSTGRES_PRIMARY_DB);

    private DeploymentProfileDaoImpl deploymentProfileDao;

    private DSLContext dsl;

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
        if (FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled()) {
            dbEndpointTestRule.addEndpoints(dbEndpointConfig.planEndpoint());
            dsl = dbEndpointConfig.planEndpoint().dslContext();
        } else {
            dsl = dbConfig.getDslContext();
        }

        deploymentProfileDao = new DeploymentProfileDaoImpl(dsl);
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
            deploymentProfileDao.restoreDiags(diags, null);
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
        final TemplateRecord t1Record = dsl.newRecord(Tables.TEMPLATE);
        t1Record.setId(templateId);
        t1Record.setName("foo");
        t1Record.setTemplateInfo(TemplateInfo.getDefaultInstance());
        t1Record.setEntityType(1);
        t1Record.setType("blah");
        return t1Record;
    }

    private TemplateToDeploymentProfileRecord createTemplateToDepProfRecord(final long templateId, final long profileId) {
        final TemplateToDeploymentProfileRecord t1Record1 =
            dsl.newRecord(Tables.TEMPLATE_TO_DEPLOYMENT_PROFILE);
        t1Record1.setTemplateId(templateId);
        t1Record1.setDeploymentProfileId(profileId);
        return t1Record1;
    }

    /**
     * Workaround for {@link PlanOrchestratorDBEndpointConfig} (remove conditional annotation), since
     * it's conditionally initialized based on {@link FeatureFlags#POSTGRES_PRIMARY_DB}. When we
     * test all combinations of it using {@link FeatureFlagTestRule}, first it's false, so
     * {@link PlanOrchestratorDBEndpointConfig} is not created; then second it's true,
     * {@link PlanOrchestratorDBEndpointConfig} is created, but the endpoint inside is also eagerly
     * initialized due to the same FF, which results in several issues like: it doesn't go through
     * DbEndpointTestRule, making call to auth to get root password, etc.
     */
    @Configuration
    public static class TestPlanOrchestratorDBEndpointConfig
            extends PlanOrchestratorDBEndpointConfig {
        @Override
        public DbEndpointCompleter endpointCompleter() {
            // prevent actual completion of the DbEndpoint
            DbEndpointCompleter dbEndpointCompleter = spy(super.endpointCompleter());
            doNothing().when(dbEndpointCompleter).setEnvironment(any());
            return dbEndpointCompleter;
        }
    }
}
