package com.vmturbo.plan.orchestrator.templates;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.assertj.core.util.Lists;
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
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.plan.orchestrator.deployment.profile.DeploymentProfileDaoImpl;
import com.vmturbo.plan.orchestrator.templates.DiscoveredTemplateDeploymentProfileDaoImpl.TemplateInfoToDeploymentProfileMap;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
    classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=plan"})
public class DiscoveredTemplateDeploymentProfileDaoImplTest {

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    private Flyway flyway;

    private DiscoveredTemplateDeploymentProfileDaoImpl discoveredTemplateDeploymentProfileDao;

    private TemplatesDaoImpl templatesDao;

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

        discoveredTemplateDeploymentProfileDao = new DiscoveredTemplateDeploymentProfileDaoImpl(dsl);
        templatesDao = new TemplatesDaoImpl(dsl, "emptyDefaultTemplates.json");
    }

    @After
    public void teardown() {
        flyway.clean();
    }

    /**
     * Test the first time upload discovered templates and deployment profiles, and also test following
     * upload to replace old data.
     */
    @Test
    public void testFirstTimeUploadAndFollowReplace() {
        final long targetId = 123;
        final Map<Long, TemplateInfoToDeploymentProfileMap> uploadMap = new HashMap<>();
        final Map<Long, List<DeploymentProfileInfo>> noReferenceMap = new HashMap<>();

        TemplateInfoToDeploymentProfileMap testMap = new TemplateInfoToDeploymentProfileMap();

        TemplateInfo firstTemplateInfo = TemplateInfo.newBuilder()
            .setProbeTemplateId("probe-template-1")
            .setName("first-template")
            .build();
        TemplateInfo secondTemplateInfo = TemplateInfo.newBuilder()
            .setProbeTemplateId("probe-template-2")
            .setName("second-template")
            .build();
        TemplateInfo thirdTemplateInfo = TemplateInfo.newBuilder()
            .setProbeTemplateId("probe-template-3")
            .setName("third-template")
            .build();

        DeploymentProfileInfo firstDeploymentProfile = DeploymentProfileInfo.newBuilder()
            .setName("first-deployment-profile")
            .setProbeDeploymentProfileId("probe-dp-1")
            .build();
        DeploymentProfileInfo secondDeploymentProfile = DeploymentProfileInfo.newBuilder()
            .setName("second-deployment-profile")
            .setProbeDeploymentProfileId("probe-dp-2")
            .build();

        testMap.put(firstTemplateInfo, Lists.newArrayList(firstDeploymentProfile));
        testMap.put(secondTemplateInfo, Lists.newArrayList(secondDeploymentProfile));
        testMap.put(thirdTemplateInfo, Lists.emptyList());

        uploadMap.put(targetId, testMap);
        noReferenceMap.put(targetId, new ArrayList<>());

        discoveredTemplateDeploymentProfileDao.setDiscoveredTemplateDeploymentProfile(uploadMap, noReferenceMap);

        Set<Template> allTemplates = templatesDao.getAllTemplates();
        Set<DeploymentProfile> allDeploymentProfiles = deploymentProfileDao.getAllDeploymentProfiles();

        assertTrue(allTemplates.size() == 3);
        assertTrue(allTemplates.stream().anyMatch(template ->
            template.getTemplateInfo().getName().equals("first-template")));
        assertTrue(allTemplates.stream().anyMatch(template ->
            template.getTemplateInfo().getName().equals("second-template")));
        assertTrue(allTemplates.stream().anyMatch(template ->
            template.getTemplateInfo().getName().equals("third-template")));

        assertTrue(allDeploymentProfiles.size() == 2);
        assertTrue(allDeploymentProfiles.stream().anyMatch(deploymentProfile ->
            deploymentProfile.getDeployInfo().getName().equals("first-deployment-profile")));
        assertTrue(allDeploymentProfiles.stream().anyMatch(deploymentProfile ->
            deploymentProfile.getDeployInfo().getName().equals("second-deployment-profile")));

        // Following upload to replace discovered template and deployment profile
        TemplateInfo needToReplaceTemplateInfo = TemplateInfo.newBuilder()
            .setProbeTemplateId("probe-template-new")
            .setName("new-template")
            .build();
        DeploymentProfileInfo noReferenceDeploymentProfile = DeploymentProfileInfo.newBuilder()
            .setName("no-reference-deployment-profile")
            .setProbeDeploymentProfileId("probe-dp-3")
            .build();
        testMap.clear();
        uploadMap.clear();
        noReferenceMap.clear();
        testMap.put(needToReplaceTemplateInfo, Lists.newArrayList(firstDeploymentProfile, secondDeploymentProfile));
        testMap.put(secondTemplateInfo, Lists.newArrayList(firstDeploymentProfile));
        testMap.put(thirdTemplateInfo, Lists.emptyList());
        uploadMap.put(targetId, testMap);
        noReferenceMap.put(targetId, Lists.newArrayList(noReferenceDeploymentProfile));

        discoveredTemplateDeploymentProfileDao.setDiscoveredTemplateDeploymentProfile(uploadMap, noReferenceMap);

        Set<Template> allNewTemplates = templatesDao.getAllTemplates();
        Set<DeploymentProfile> allNewDeploymentProfiles = deploymentProfileDao.getAllDeploymentProfiles();
        assertTrue(allTemplates.size() == 3);
        assertTrue(allNewTemplates.stream().anyMatch(template ->
            template.getTemplateInfo().getName().equals("second-template")));
        assertTrue(allNewTemplates.stream().anyMatch(template ->
            template.getTemplateInfo().getName().equals("new-template")));
        assertTrue(allNewTemplates.stream().anyMatch(template ->
            template.getTemplateInfo().getName().equals("third-template")));

        assertTrue(allNewDeploymentProfiles.size() == 3);
        assertTrue(allNewDeploymentProfiles.stream().anyMatch(deploymentProfile ->
            deploymentProfile.getDeployInfo().getName().equals("first-deployment-profile")));
        assertTrue(allNewDeploymentProfiles.stream().anyMatch(deploymentProfile ->
            deploymentProfile.getDeployInfo().getName().equals("second-deployment-profile")));
        assertTrue(allNewDeploymentProfiles.stream().anyMatch(deploymentProfile ->
            deploymentProfile.getDeployInfo().getName().equals("no-reference-deployment-profile")));
    }

    /**
     * Test upload discovered templates and deployment profile to table which also have user created
     * templates.
     */
    @Test
    public void testWithExistingUserCreatedData() {
        final long targetId = 123;
        final Map<Long, TemplateInfoToDeploymentProfileMap> uploadMap = new HashMap<>();
        final Map<Long, List<DeploymentProfileInfo>> noReferenceMap = new HashMap<>();
        TemplateInfo userCreatedTemplateFirst = TemplateInfo.newBuilder()
            .setName("user-template-1")
            .build();

        TemplateInfo userCreatedTemplateSecond = TemplateInfo.newBuilder()
            .setName("user-template-2")
            .build();

        templatesDao.createTemplate(userCreatedTemplateFirst);
        templatesDao.createTemplate(userCreatedTemplateSecond);

        TemplateInfoToDeploymentProfileMap testMap = new TemplateInfoToDeploymentProfileMap();

        TemplateInfo firstTemplateInfo = TemplateInfo.newBuilder()
            .setProbeTemplateId("probe-template-1")
            .setName("first-template")
            .build();
        TemplateInfo secondTemplateInfo = TemplateInfo.newBuilder()
            .setProbeTemplateId("probe-template-2")
            .setName("second-template")
            .build();

        DeploymentProfileInfo firstDeploymentProfile = DeploymentProfileInfo.newBuilder()
            .setName("first-deployment-profile")
            .setProbeDeploymentProfileId("probe-dp-1")
            .build();
        DeploymentProfileInfo secondDeploymentProfile = DeploymentProfileInfo.newBuilder()
            .setName("second-deployment-profile")
            .setProbeDeploymentProfileId("probe-dp-2")
            .build();

        DeploymentProfileInfo noReferenceOldDeploymentProfile = DeploymentProfileInfo.newBuilder()
            .setName("no-reference-old-deployment-profile")
            .setProbeDeploymentProfileId("probe-dp-3")
            .build();

        testMap.put(firstTemplateInfo, Lists.newArrayList(firstDeploymentProfile));
        testMap.put(secondTemplateInfo, Lists.newArrayList(secondDeploymentProfile));
        uploadMap.put(targetId, testMap);
        noReferenceMap.put(targetId, Lists.newArrayList(noReferenceOldDeploymentProfile));

        discoveredTemplateDeploymentProfileDao.setDiscoveredTemplateDeploymentProfile(uploadMap, noReferenceMap);

        Set<Template> allTemplates = templatesDao.getAllTemplates();
        Set<DeploymentProfile> allDeploymentProfiles = deploymentProfileDao.getAllDeploymentProfiles();

        assertTrue(allTemplates.size() == 4);
        assertTrue(allTemplates.stream().anyMatch(template ->
            template.getTemplateInfo().getName().equals("first-template")));
        assertTrue(allTemplates.stream().anyMatch(template ->
            template.getTemplateInfo().getName().equals("second-template")));
        assertTrue(allTemplates.stream().anyMatch(template ->
            template.getTemplateInfo().getName().equals("user-template-1")));
        assertTrue(allTemplates.stream().anyMatch(template ->
            template.getTemplateInfo().getName().equals("user-template-2")));

        assertTrue(allDeploymentProfiles.size() == 3);
        assertTrue(allDeploymentProfiles.stream().anyMatch(deploymentProfile ->
            deploymentProfile.getDeployInfo().getName().equals("first-deployment-profile")));
        assertTrue(allDeploymentProfiles.stream().anyMatch(deploymentProfile ->
            deploymentProfile.getDeployInfo().getName().equals("second-deployment-profile")));
        assertTrue(allDeploymentProfiles.stream().anyMatch(deploymentProfile ->
            deploymentProfile.getDeployInfo().getName().equals("no-reference-old-deployment-profile")));

        DeploymentProfileInfo updateDeploymentProfile = DeploymentProfileInfo.newBuilder()
            .setName("update-deployment-profile")
            .setProbeDeploymentProfileId("probe-dp-1")
            .build();
        DeploymentProfileInfo newDeploymentProfile = DeploymentProfileInfo.newBuilder()
            .setName("new-deployment-profile")
            .setProbeDeploymentProfileId("probe-dp-new")
            .build();
        DeploymentProfileInfo noReferenceDeploymentProfile = DeploymentProfileInfo.newBuilder()
            .setName("no-reference-deployment-profile")
            .setProbeDeploymentProfileId("probe-dp-3")
            .build();

        testMap.clear();
        uploadMap.clear();
        noReferenceMap.clear();
        testMap.put(firstTemplateInfo, Lists.newArrayList(updateDeploymentProfile, newDeploymentProfile));
        testMap.put(secondTemplateInfo, Lists.newArrayList(updateDeploymentProfile));
        uploadMap.put(targetId, testMap);
        noReferenceMap.put(targetId, Lists.newArrayList(noReferenceDeploymentProfile));

        discoveredTemplateDeploymentProfileDao.setDiscoveredTemplateDeploymentProfile(uploadMap, noReferenceMap);

        Set<Template> allNewTemplates = templatesDao.getAllTemplates();
        Set<DeploymentProfile> allNewDeploymentProfiles = deploymentProfileDao.getAllDeploymentProfiles();

        assertTrue(allNewTemplates.size() == 4);
        assertTrue(allNewTemplates.stream().anyMatch(template ->
            template.getTemplateInfo().getName().equals("first-template")));
        assertTrue(allNewTemplates.stream().anyMatch(template ->
            template.getTemplateInfo().getName().equals("second-template")));
        assertTrue(allNewTemplates.stream().anyMatch(template ->
            template.getTemplateInfo().getName().equals("user-template-1")));
        assertTrue(allNewTemplates.stream().anyMatch(template ->
            template.getTemplateInfo().getName().equals("user-template-2")));

        assertTrue(allNewDeploymentProfiles.size() == 3);
        assertTrue(allNewDeploymentProfiles.stream().anyMatch(deploymentProfile ->
            deploymentProfile.getDeployInfo().getName().equals("update-deployment-profile")));
        assertTrue(allNewDeploymentProfiles.stream().anyMatch(deploymentProfile ->
            deploymentProfile.getDeployInfo().getName().equals("new-deployment-profile")));
        assertTrue(allNewDeploymentProfiles.stream().anyMatch(deploymentProfile ->
            deploymentProfile.getDeployInfo().getName().equals("no-reference-deployment-profile")));
    }

    @Test
    public void testOnlyUploadDiscoveredTemplates() {
        final long targetId = 123;
        final Map<Long, TemplateInfoToDeploymentProfileMap> uploadMap = new HashMap<>();
        final Map<Long, List<DeploymentProfileInfo>> noReferenceMap = new HashMap<>();

        TemplateInfoToDeploymentProfileMap testMap = new TemplateInfoToDeploymentProfileMap();

        TemplateInfo firstTemplateInfo = TemplateInfo.newBuilder()
            .setProbeTemplateId("probe-template-1")
            .setName("first-template")
            .build();
        TemplateInfo secondTemplateInfo = TemplateInfo.newBuilder()
            .setProbeTemplateId("probe-template-2")
            .setName("second-template")
            .build();

        testMap.put(firstTemplateInfo, Lists.emptyList());
        testMap.put(secondTemplateInfo, Lists.emptyList());

        uploadMap.put(targetId, testMap);
        noReferenceMap.put(targetId, new ArrayList<>());

        discoveredTemplateDeploymentProfileDao.setDiscoveredTemplateDeploymentProfile(uploadMap, noReferenceMap);

        Set<Template> allTemplates = templatesDao.getAllTemplates();

        assertTrue(allTemplates.size() == 2);
        assertTrue(allTemplates.stream().anyMatch(template ->
            template.getTemplateInfo().getName().equals("first-template")));
        assertTrue(allTemplates.stream().anyMatch(template ->
            template.getTemplateInfo().getName().equals("second-template")));
    }

    @Test
    public void testMultipleTarget() {
        final long firstTarget = 123;
        final long secondTarget = 456;
        final Map<Long, TemplateInfoToDeploymentProfileMap> uploadMap = new HashMap<>();
        final Map<Long, List<DeploymentProfileInfo>> noReferenceMap = new HashMap<>();

        TemplateInfoToDeploymentProfileMap firstTargetMap = new TemplateInfoToDeploymentProfileMap();
        TemplateInfoToDeploymentProfileMap secondTargetMap = new TemplateInfoToDeploymentProfileMap();

        TemplateInfo firstTargetTemplateInfo1 = TemplateInfo.newBuilder()
            .setProbeTemplateId("probe-template-1")
            .setName("first-target-template-1")
            .build();
        TemplateInfo secondTargetTemplateInfo2 = TemplateInfo.newBuilder()
            .setProbeTemplateId("probe-template-2")
            .setName("first-target-template-2")
            .build();
        TemplateInfo thirdTemplateInfo = TemplateInfo.newBuilder()
            .setProbeTemplateId("probe-template-3")
            .setName("second-target-template-1")
            .build();

        DeploymentProfileInfo firstDeploymentProfile = DeploymentProfileInfo.newBuilder()
            .setName("first-target-deployment-profile")
            .setProbeDeploymentProfileId("probe-dp-1")
            .build();
        DeploymentProfileInfo secondDeploymentProfile = DeploymentProfileInfo.newBuilder()
            .setName("second-target-deployment-profile")
            .setProbeDeploymentProfileId("probe-dp-2")
            .build();

        firstTargetMap.put(firstTargetTemplateInfo1, Lists.newArrayList(firstDeploymentProfile));
        firstTargetMap.put(secondTargetTemplateInfo2, Lists.newArrayList(firstDeploymentProfile));
        secondTargetMap.put(thirdTemplateInfo, Lists.newArrayList(secondDeploymentProfile));

        uploadMap.put(firstTarget, firstTargetMap);
        uploadMap.put(secondTarget, secondTargetMap);
        noReferenceMap.put(firstTarget, new ArrayList<>());
        noReferenceMap.put(secondTarget, new ArrayList<>());

        discoveredTemplateDeploymentProfileDao.setDiscoveredTemplateDeploymentProfile(uploadMap, noReferenceMap);

        Set<Template> allTemplates = templatesDao.getAllTemplates();
        Set<DeploymentProfile> allDeploymentProfiles = deploymentProfileDao.getAllDeploymentProfiles();

        assertTrue(allTemplates.size() == 3);
        assertTrue(allTemplates.stream().anyMatch(template ->
            template.getTemplateInfo().getName().equals("first-target-template-1")));
        assertTrue(allTemplates.stream().anyMatch(template ->
            template.getTemplateInfo().getName().equals("first-target-template-2")));
        assertTrue(allTemplates.stream().anyMatch(template ->
            template.getTemplateInfo().getName().equals("second-target-template-1")));

        assertTrue(allDeploymentProfiles.size() == 2);
        assertTrue(allDeploymentProfiles.stream().anyMatch(deploymentProfile ->
            deploymentProfile.getDeployInfo().getName().equals("first-target-deployment-profile")));
        assertTrue(allDeploymentProfiles.stream().anyMatch(deploymentProfile ->
            deploymentProfile.getDeployInfo().getName().equals("second-target-deployment-profile")));

        uploadMap.clear();
        noReferenceMap.clear();
        // remove first target and only upload second target
        uploadMap.put(secondTarget, secondTargetMap);
        noReferenceMap.put(secondTarget, new ArrayList<>());

        discoveredTemplateDeploymentProfileDao.setDiscoveredTemplateDeploymentProfile(uploadMap, noReferenceMap);

        Set<Template> allNewTemplates = templatesDao.getAllTemplates();
        Set<DeploymentProfile> allNewDeploymentProfiles = deploymentProfileDao.getAllDeploymentProfiles();

        assertTrue(allNewTemplates.size() == 1);
        assertTrue(allNewTemplates.stream().anyMatch(template ->
            template.getTemplateInfo().getName().equals("second-target-template-1")));

        assertTrue(allNewDeploymentProfiles.size() == 1);
        assertTrue(allNewDeploymentProfiles.stream().anyMatch(deploymentProfile ->
            deploymentProfile.getDeployInfo().getName().equals("second-target-deployment-profile")));
    }
}
