package com.vmturbo.plan.orchestrator.templates;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.*;
import java.util.stream.Collectors;

import com.vmturbo.common.protobuf.plan.ReservationDTO;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass;
import com.vmturbo.plan.orchestrator.reservation.ReservationDaoImpl;
import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfile;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.UpdateDiscoveredTemplateDeploymentProfileResponse;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.UpdateDiscoveredTemplateDeploymentProfileResponse.TargetProfileIdentities;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplatesFilter;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.plan.orchestrator.deployment.profile.DeploymentProfileDaoImpl;
import com.vmturbo.plan.orchestrator.templates.DiscoveredTemplateDeploymentProfileDaoImpl.TemplateInfoToDeploymentProfileMap;
import com.vmturbo.plan.orchestrator.templates.exceptions.DuplicateTemplateException;
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

    private ReservationDaoImpl reservationDao;

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
        templatesDao = new TemplatesDaoImpl(dsl, "emptyDefaultTemplates.json",
                new IdentityInitializer(0));
        reservationDao = new ReservationDaoImpl(dsl);
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
        final String templateId1 = "probe-template-1";
        final String templateId2 = "probe-template-2";
        final String templateId3 = "probe-template-3";
        final String depId1 = "probe-dp-1";
        final String depId2 = "probe-dp-2";

        TemplateInfoToDeploymentProfileMap testMap = new TemplateInfoToDeploymentProfileMap();

        TemplateInfo firstTemplateInfo = TemplateInfo.newBuilder()
            .setProbeTemplateId(templateId1)
            .setName("first-template")
            .build();
        TemplateInfo secondTemplateInfo = TemplateInfo.newBuilder()
            .setProbeTemplateId(templateId2)
            .setName("second-template")
            .build();
        TemplateInfo thirdTemplateInfo = TemplateInfo.newBuilder()
            .setProbeTemplateId(templateId3)
            .setName("third-template")
            .build();

        DeploymentProfileInfo firstDeploymentProfile = DeploymentProfileInfo.newBuilder()
            .setName("first-deployment-profile")
            .setProbeDeploymentProfileId(depId1)
            .build();
        DeploymentProfileInfo secondDeploymentProfile = DeploymentProfileInfo.newBuilder()
            .setName("second-deployment-profile")
            .setProbeDeploymentProfileId(depId2)
            .build();

        testMap.put(firstTemplateInfo, Collections.singletonList(firstDeploymentProfile));
        testMap.put(secondTemplateInfo, Collections.singletonList(secondDeploymentProfile));
        testMap.put(thirdTemplateInfo, Collections.emptyList());

        uploadMap.put(targetId, testMap);
        noReferenceMap.put(targetId, new ArrayList<>());

        final UpdateDiscoveredTemplateDeploymentProfileResponse response =
               discoveredTemplateDeploymentProfileDao
                               .setDiscoveredTemplateDeploymentProfile(uploadMap,
                                                                       noReferenceMap);

        final Set<Template> allTemplates =
            templatesDao.getFilteredTemplates(TemplatesFilter.getDefaultInstance());
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

        Assert.assertNotNull(response);
        Assert.assertEquals(1, response.getTargetProfileIdentitiesCount());
        TargetProfileIdentities identities = response.getTargetProfileIdentities(0);
        Assert.assertEquals(targetId, identities.getTargetOid());
        Assert.assertEquals(3, identities.getProfileIdToOidCount());
        Assert.assertTrue(identities.containsProfileIdToOid(templateId1));
        Assert.assertTrue(identities.containsProfileIdToOid(templateId2));
        Assert.assertTrue(identities.containsProfileIdToOid(templateId3));
        Assert.assertEquals(2, identities.getDeploymentProfileIdToOidCount());
        Assert.assertTrue(identities.containsDeploymentProfileIdToOid(depId1));
        Assert.assertTrue(identities.containsDeploymentProfileIdToOid(depId2));

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
        testMap.put(thirdTemplateInfo, Collections.emptyList());
        uploadMap.put(targetId, testMap);
        noReferenceMap.put(targetId, Lists.newArrayList(noReferenceDeploymentProfile));

        discoveredTemplateDeploymentProfileDao.setDiscoveredTemplateDeploymentProfile(uploadMap, noReferenceMap);

        final Set<Template> allNewTemplates =
            templatesDao.getFilteredTemplates(TemplatesFilter.getDefaultInstance());
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
    public void testWithExistingUserCreatedData() throws DuplicateTemplateException {
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

        Set<Template> allTemplates = templatesDao.getFilteredTemplates(TemplatesFilter.getDefaultInstance());
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

        Set<Template> allNewTemplates = templatesDao.getFilteredTemplates(TemplatesFilter.getDefaultInstance());
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

        testMap.put(firstTemplateInfo, Collections.emptyList());
        testMap.put(secondTemplateInfo, Collections.emptyList());

        uploadMap.put(targetId, testMap);
        noReferenceMap.put(targetId, new ArrayList<>());

        discoveredTemplateDeploymentProfileDao.setDiscoveredTemplateDeploymentProfile(uploadMap, noReferenceMap);

        Set<Template> allTemplates = templatesDao.getFilteredTemplates(TemplatesFilter.getDefaultInstance());

        assertTrue(allTemplates.size() == 2);
        assertTrue(allTemplates.stream().anyMatch(template ->
            template.getTemplateInfo().getName().equals("first-template")));
        assertTrue(allTemplates.stream().anyMatch(template ->
            template.getTemplateInfo().getName().equals("second-template")));
    }

    /**
     * Test reservations will have "invalid" status, if the depended template is deleted.
     */
    @Test
    public void testUpdateReservationToInvalidWhenDeletingDependedTemplate() {
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

        final Set<Template> allTemplates = templatesDao.getFilteredTemplates(TemplatesFilter.getDefaultInstance());

        uploadMap.clear();
        noReferenceMap.clear();

        uploadMap.put(secondTarget, secondTargetMap);
        noReferenceMap.put(secondTarget, new ArrayList<>());

        // verify init states before removing templates
        final Set<ReservationDTO.Reservation> reservations = allTemplates.stream()
                .map(template -> reservationDao.createReservation(buildReservation(template.getId())))
                .collect(Collectors.toSet());
        assertEquals(3, reservations.size());
        reservations.stream().allMatch(reservation -> reservation.getStatus().equals(ReservationDTO.ReservationStatus.FUTURE));

        // delete system templates
        discoveredTemplateDeploymentProfileDao.setDiscoveredTemplateDeploymentProfile(uploadMap, noReferenceMap);

        // verify all reservations depend on deleted system templates are set to invalid state.
        Set<ReservationDTO.Reservation> allReservations = reservationDao.getAllReservations();
        assertEquals(3, allReservations.size());
        allReservations.stream().allMatch(reservation -> reservation.getStatus().equals(ReservationDTO.ReservationStatus.INVALID));
    }

    /**
     * Test duplicate name filtering.
     */
    @Test
    public void testUploadDiscoveredTemplatesDuplicateName() {
        final long targetId = 123;
        final Map<Long, TemplateInfoToDeploymentProfileMap> uploadMap = new HashMap<>();
        final Map<Long, List<DeploymentProfileInfo>> noReferenceMap = new HashMap<>();

        TemplateInfoToDeploymentProfileMap testMap = new TemplateInfoToDeploymentProfileMap();

        TemplateInfo template1 = TemplateInfo.newBuilder()
            .setProbeTemplateId("probe-template-1")
            .setName("template1")
            .build();
        TemplateInfo template2 = TemplateInfo.newBuilder()
            .setProbeTemplateId("probe-template-1")
            .setName("template2")
            .build();

        TemplateInfo template3DupName = TemplateInfo.newBuilder()
            .setProbeTemplateId("probe-template-2")
            .setName("template1")
            .build();

        testMap.put(template1, Collections.emptyList());
        testMap.put(template2, Collections.emptyList());
        testMap.put(template3DupName, Collections.emptyList());

        uploadMap.put(targetId, testMap);
        noReferenceMap.put(targetId, new ArrayList<>());

        discoveredTemplateDeploymentProfileDao.setDiscoveredTemplateDeploymentProfile(uploadMap, noReferenceMap);

        Set<Template> allTemplates = templatesDao.getFilteredTemplates(TemplatesFilter.getDefaultInstance());

        assertEquals(2, allTemplates.size());
        assertTrue(allTemplates.stream().anyMatch(template ->
            template.getTemplateInfo().getName().equals("template1")));
        assertTrue(allTemplates.stream().anyMatch(template ->
            template.getTemplateInfo().getName().equals("template2")));
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

        Set<Template> allTemplates = templatesDao.getFilteredTemplates(TemplatesFilter.getDefaultInstance());
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

        Set<Template> allNewTemplates = templatesDao.getFilteredTemplates(TemplatesFilter.getDefaultInstance());
        Set<DeploymentProfile> allNewDeploymentProfiles = deploymentProfileDao.getAllDeploymentProfiles();

        assertTrue(allNewTemplates.size() == 1);
        assertTrue(allNewTemplates.stream().anyMatch(template ->
            template.getTemplateInfo().getName().equals("second-target-template-1")));

        assertTrue(allNewDeploymentProfiles.size() == 1);
        assertTrue(allNewDeploymentProfiles.stream().anyMatch(deploymentProfile ->
            deploymentProfile.getDeployInfo().getName().equals("second-target-deployment-profile")));
    }

    private ReservationDTO.Reservation buildReservation(long templateId) {
        return ReservationDTO.Reservation.newBuilder()
                .setName("Test-first-reservation")
                .setStartDate(1543105352845L)
                .setExpirationDate(1553105352845L)
                .setStatus(ReservationDTO.ReservationStatus.FUTURE)
                .setReservationTemplateCollection(ReservationDTO.ReservationTemplateCollection.newBuilder()
                        .addReservationTemplate(ReservationDTO.ReservationTemplateCollection.ReservationTemplate.newBuilder()
                                .setCount(1)
                                .setTemplateId(templateId)
                                .addReservationInstance(ReservationDTO.ReservationTemplateCollection
                                        .ReservationTemplate.ReservationInstance.newBuilder()
                                        .setEntityId(456)
                                        .addPlacementInfo(ReservationDTO.ReservationTemplateCollection
                                                .ReservationTemplate.ReservationInstance.PlacementInfo.newBuilder()
                                                .setProviderId(678)
                                                .setProviderId(14)))))
                .setConstraintInfoCollection(ReservationDTO.ConstraintInfoCollection.newBuilder()
                        .addReservationConstraintInfo(ScenarioOuterClass.ReservationConstraintInfo.newBuilder()
                                .setConstraintId(100)
                                .setType(ScenarioOuterClass.ReservationConstraintInfo.Type.DATA_CENTER)))
                .build();
    }

}
