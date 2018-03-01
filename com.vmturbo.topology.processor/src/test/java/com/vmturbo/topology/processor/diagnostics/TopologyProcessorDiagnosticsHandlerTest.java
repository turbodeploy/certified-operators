package com.vmturbo.topology.processor.diagnostics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;

import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredSettingPolicyInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo.DeploymentProfileContextData;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo.Scope;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo.ScopeAccessType;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter.TraversalFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.Search.SearchFilter.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.DiscoveredGroup.DiscoveredGroupInfo;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.DiagnosticsWriter;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.kvstore.MapKeyValueStore;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.BuyerMetaData;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintInfo;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.SelectionSpec;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.SelectionSpec.ExpressionType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.SelectionSpec.PropertyStringList;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.SelectionSpecList;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.common.dto.ProfileDTO.CommodityProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO.DBProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO.LicenseMapEntry;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO.PMProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO.VMProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO.VMProfileDTO.DedicatedStorageNetworkState;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.diagnostics.TopologyProcessorDiagnosticsHandler.DeploymentProfileWithTemplate;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.entity.IdentifiedEntityDTO;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.plan.DiscoveredTemplateDeploymentProfileUploader;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.scheduling.TargetDiscoverySchedule;
import com.vmturbo.topology.processor.targets.InvalidTargetException;
import com.vmturbo.topology.processor.targets.KVBackedTargetStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetDeserializationException;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Tests for {@link TopologyProcessorDiagnosticsHandler}.
 *
 */
public class TopologyProcessorDiagnosticsHandlerTest {

    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

    private final List<Target> targets = Lists.newArrayList();
    private final TargetStore targetStore = Mockito.mock(TargetStore.class);
    private final Scheduler scheduler = Mockito.mock(Scheduler.class);
    private final EntityStore entityStore = Mockito.mock(EntityStore.class);
    private final DiscoveredGroupUploader groupUploader = Mockito.mock(DiscoveredGroupUploader.class);
    private final DiscoveredTemplateDeploymentProfileUploader templateDeploymentProfileUploader =
        Mockito.mock(DiscoveredTemplateDeploymentProfileUploader.class);
    private final IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);
    private final EntityDTO nwDto =
            EntityDTO.newBuilder().setId("NW-1").setEntityType(EntityType.NETWORK).build();

    private final TargetSpec.Builder targetSpecBuilder = TargetSpec.newBuilder()
            .addAccountValue(AccountValue.newBuilder()
                .setKey("targetIdentifier")
                .setStringValue("fieldValue"));

    // With a mock schedule, the Gson response will always be with zero fields
    private static final String SCHEDULE_JSON =
            GSON.toJson(Mockito.mock(TargetDiscoverySchedule.class), TargetDiscoverySchedule.class);

    private static final String IDENTIFIED_ENTITY =
            "{\"oid\":\"199\",\"entity\":"
            + "{\"entityType\":\"NETWORK\",\"id\":\"NW-1\"}}";

    private final Map<Long, List<DiscoveredGroupInfo>> discoveredGroupMap = new HashMap<>();
    private final Multimap<Long, DiscoveredSettingPolicyInfo> discoveredSettingPolicyMap =
        ArrayListMultimap.create();
    private final Map<Long, Map<DeploymentProfileInfo, Set<EntityProfileDTO>>> discoveredProfileMap
        = new HashMap<>();

    @Before
    public void setup() {
        Mockito.when(targetStore.getAll()).thenReturn(targets);
        Map<Long, EntityDTO> map = ImmutableMap.of(199L, nwDto);
        Mockito.when(entityStore.discoveredByTarget(100001)).thenReturn(map);
        Mockito.when(entityStore.getTargetLastUpdatedTime(100001)).thenReturn(Optional.of(12345L));
        Mockito.doReturn(discoveredGroupMap).when(groupUploader).getDiscoveredGroupInfoByTarget();
        Mockito.doReturn(discoveredSettingPolicyMap).when(groupUploader)
            .getDiscoveredSettingPolicyInfoByTarget();
        Mockito.doReturn(discoveredProfileMap).when(templateDeploymentProfileUploader)
            .getDiscoveredDeploymentProfilesByTarget();
    }

    private ZipInputStream dumpDiags() throws IOException {
        ByteArrayOutputStream zipBytes = new ByteArrayOutputStream();
        ZipOutputStream zos = new ZipOutputStream(zipBytes);
        TopologyProcessorDiagnosticsHandler handler =
                new TopologyProcessorDiagnosticsHandler(targetStore, scheduler, entityStore,
                    groupUploader, templateDeploymentProfileUploader, identityProvider,
                    new DiagnosticsWriter());
        handler.dumpDiags(zos);
        zos.close();
        return new ZipInputStream(new ByteArrayInputStream(zipBytes.toByteArray()));
    }

    /**
     * Test case with no targets (and no schedules).
     * The generated diags should be empty.
     *
     * @throws IOException
     */
    @Test
    public void testNoTargets() throws IOException {
        ZipInputStream zis = dumpDiags();

        ZipEntry ze = zis.getNextEntry();
        assertTrue(ze.getName().equals("Targets.diags"));
        byte[] bytes = new byte[20];
        zis.read(bytes);
        assertEquals(0, bytes[0]); // the entry is empty

        ze = zis.getNextEntry();
        assertTrue(ze.getName().equals("Schedules.diags"));
        bytes = new byte[20];
        zis.read(bytes);
        assertEquals(0, bytes[0]); // the entry is empty

        ze = zis.getNextEntry();
        assertEquals("Identity.diags", ze.getName());

        ze = zis.getNextEntry();
        assertNull(ze);
        zis.close();
    }

    @Test
    public void testTargetSecretFields()
            throws InvalidTargetException, IOException {
        final long targetId = 1;
        final long probeId = 2;
        Mockito.when(entityStore.getTargetLastUpdatedTime(targetId)).thenReturn(Optional.of(12345L));

        // A probe with a secret field.
        final ProbeInfo probeInfo = ProbeInfo.newBuilder()
                .setProbeCategory("cat")
                .setProbeType("type")
                .addTargetIdentifierField("field")
                .addAccountDefinition(AccountDefEntry.newBuilder()
                    .setMandatory(true)
                    .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                        .setName("name")
                        .setDisplayName("displayName")
                        .setDescription("description")
                        .setIsSecret(true)))
                .build();

        // The target spec with an account value for the secret field
        final TargetSpec targetSpec = TargetSpec.newBuilder()
                .setProbeId(probeId)
                .addAccountValue(AccountValue.newBuilder()
                    .setKey("name")
                    .setStringValue("value"))
                .build();

        final ProbeStore probeStore = Mockito.mock(ProbeStore.class);
        Mockito.when(probeStore.getProbe(Mockito.eq(probeId)))
               .thenReturn(Optional.of(probeInfo));
        Mockito.when(scheduler.getDiscoverySchedule(targetId))
                .thenReturn(Optional.empty());

        targets.add(new Target(targetId, probeStore, targetSpec, true));

        final ZipInputStream zis = dumpDiags();

        final ZipEntry ze = zis.getNextEntry();
        assertEquals("Targets.diags", ze.getName());
        byte[] bytes = new byte[1024];
        zis.read(bytes);
        final String targetJson = new String(bytes, 0, 1024).split("\n")[0];
        final TargetInfo savedTargetInfo = GSON.fromJson(targetJson, TargetInfo.class);
        Assert.assertEquals(0, savedTargetInfo.getSpec().getAccountValueCount());
    }

    @Test
    public void testRestoreTargetsInvalidJson()
            throws IOException, TargetDeserializationException, InvalidTargetException {
        final long targetId = 1;
        final TargetInfo validTarget = TargetInfo.newBuilder()
                .setId(targetId)
                .setSpec(targetSpecBuilder.setProbeId(2))
                .build();
        final TopologyProcessorDiagnosticsHandler handler =
                new TopologyProcessorDiagnosticsHandler(targetStore, scheduler, entityStore,
                    groupUploader, templateDeploymentProfileUploader, identityProvider,
                    new DiagnosticsWriter());
        // Valid json, but not a target info
        final String invalidJsonTarget = GSON.toJson(targetSpecBuilder.setProbeId(3));
        // Invalid json
        final String invalidJson = "{osaeintr";
        final String validJsonTarget = GSON.toJson(validTarget);
        // Put the valid JSON target at the end of the target list, to assure
        // that preceding invalid targets don't halt processing.
        handler.restoreTargets(ImmutableList.of(invalidJsonTarget, invalidJson, validJsonTarget));

        // Verify that we only restored one target.
        Mockito.verify(targetStore, Mockito.times(1)).createTarget(
                Mockito.anyLong(),
                any());
        // Verify that the restored target had the right information.
        Mockito.verify(targetStore).createTarget(
                Mockito.eq(targetId),
                Mockito.eq(validTarget.getSpec()));
    }

    /**
     * Test case with some targets, schedules, discovered groups, discovered policies, and
     * discovered settings policies.
     *
     * @throws IOException in case of IO error reading/writing
     */
    @Test
    public void testSomeTargets() throws IOException {
        long[] targetIds = new long[]{100001,200001};
        long[] probeIds = new long[]{101,201};
        String[] entities = new String[]{
            IDENTIFIED_ENTITY,
            null
        };
        TargetInfo[] targetInfos = new TargetInfo[]{
                TargetInfo.newBuilder()
                        .setId(targetIds[0])
                        .setSpec(targetSpecBuilder.setProbeId(probeIds[0]))
                        .build(),
                TargetInfo.newBuilder()
                        .setId(targetIds[1])
                        .setSpec(targetSpecBuilder.setProbeId(probeIds[1]))
                        .build(),
        };
        long[] times = new long[]{12345, 23456};
        TargetDiscoverySchedule[] schedules = new TargetDiscoverySchedule[]{
                null,
                Mockito.mock(TargetDiscoverySchedule.class)};

        DiscoveredGroupInfo[] groups = new DiscoveredGroupInfo[]{
            makeDiscoveredGroupInfo(makeClusterInfo()),
            makeDiscoveredGroupInfo(makeGroupInfo())
        };
        DiscoveredSettingPolicyInfo[] settingPolicies = new DiscoveredSettingPolicyInfo[]{
            makeDiscoveredSettingPolicyInfo("settingPolicy1"),
            makeDiscoveredSettingPolicyInfo("settingPolicy2")
        };
        EntityProfileDTO[] templates = new EntityProfileDTO[]{
            makeDiscoveredTemplate("123456789"),
            makeDiscoveredTemplate("abcdefg")
        };
        DeploymentProfileInfo[] profiles = new DeploymentProfileInfo[]{
            makeDiscoveredProfile("123456789"),
            makeDiscoveredProfile("abcdefg")
        };
        for (int i = 0; i < 2; ++i) {
            targets.add(mockTarget(targetIds[i], targetInfos[i], schedules[i], groups[i],
                settingPolicies[i], times[i], templates[i], profiles[i]));

        }

        final ZipInputStream zis = dumpDiags();

        ZipEntry ze = zis.getNextEntry();
        assertEquals("Targets.diags", ze.getName());
        byte[] bytes = new byte[1024];
        zis.read(bytes);
        String firstTargetJson = new String(bytes, 0, 1024).split("\n")[0];
        assertEquals(targetInfos[0], GSON.fromJson(firstTargetJson, TargetInfo.class));

        ze = zis.getNextEntry();
        assertEquals("Schedules.diags", ze.getName());
        bytes = new byte[1024];
        zis.read(bytes);
        assertEquals(SCHEDULE_JSON, new String(bytes, 0, SCHEDULE_JSON.length()));

        for (int i = 0; i < targetIds.length; i++) {
            final String suffix = "." + targetIds[i] + "-" + times[i] + ".diags";

            ze = zis.getNextEntry();
            assertEquals("Entities" + suffix, ze.getName());
            bytes = new byte[100];
            zis.read(bytes);

            if (entities[i] != null) {
                String json = new String(bytes, 0, entities[i].length());
                assertEquals(entities[i], json);

                IdentifiedEntityDTO dto = IdentifiedEntityDTO.fromJson(json);
                assertEquals(199, dto.getOid());
                assertEquals(nwDto, dto.getEntity());
            } else {
                assertEquals(0, bytes[0]);
            }

            ze = zis.getNextEntry();
            assertEquals("DiscoveredGroupsAndPolicies" + suffix, ze.getName());
            bytes = new byte[2048];
            zis.read(bytes);
            String result = new String(bytes, 0, 2048).split("\n")[0];
            assertEquals(groups[i], GSON.fromJson(result, DiscoveredGroupInfo.class));

            ze = zis.getNextEntry();
            assertEquals("DiscoveredSettingPolicies" + suffix, ze.getName());
            bytes = new byte[2048];
            zis.read(bytes);
            result = new String(bytes, 0, 2048).split("\n")[0];
            assertEquals(settingPolicies[i], GSON.fromJson(result, DiscoveredSettingPolicyInfo.class));

            ze = zis.getNextEntry();
            assertEquals("DiscoveredDeploymentProfilesAndTemplates" + suffix, ze.getName());
            bytes = new byte[2048];
            zis.read(bytes);
            result = new String(bytes, 0, 2048).trim();
            assertEquals(profiles[i], GSON.fromJson(result, DeploymentProfileWithTemplate.class).getProfile());
            assertEquals(templates[i],
                GSON.fromJson(result, DeploymentProfileWithTemplate.class).getTemplates().iterator().next());
        }

        ze = zis.getNextEntry();
        assertEquals("Identity.diags", ze.getName());

        ze = zis.getNextEntry();
        assertNull(ze);
        zis.close();
    }

    @Test
    public void testRestore() throws Exception {
        TargetStore simpleTargetStore =
                new KVBackedTargetStore(
                        new MapKeyValueStore(),
                        Mockito.mock(IdentityProvider.class),
                        Mockito.mock(ProbeStore.class));
        TopologyProcessorDiagnosticsHandler handler =
                new TopologyProcessorDiagnosticsHandler(simpleTargetStore, scheduler, entityStore,
                    groupUploader, templateDeploymentProfileUploader, identityProvider,
                    new DiagnosticsWriter());

        handler.restore(new FileInputStream(new File(fullPath("diags/compressed/diags0.zip"))));
        List<Target> targets = simpleTargetStore.getAll();
        assertTrue(!targets.isEmpty());

        for (Target target : simpleTargetStore.getAll()) {
            Mockito.verify(scheduler, Mockito.times(1))
                .setDiscoverySchedule(target.getId(), 600000, TimeUnit.MILLISECONDS);
            Mockito.verify(entityStore).entitiesRestored(
                    Mockito.eq(target.getId()),
                    Mockito.anyLong(),
                    Mockito.anyMapOf(Long.class, EntityDTO.class));
            Mockito.verify(groupUploader).setTargetDiscoveredGroups(
                Mockito.eq(target.getId()), Mockito.anyListOf(GroupDTO.class));
            Mockito.verify(groupUploader).setTargetDiscoveredSettingPolicies(
                Mockito.eq(target.getId()), Mockito.anyListOf(DiscoveredSettingPolicyInfo.class));
            Mockito.verify(templateDeploymentProfileUploader)
                .setTargetsTemplateDeploymentProfileInfos(Mockito.eq(target.getId()), Mockito.anyMap());
        }
        Mockito.verify(identityProvider).restoreDiags(any());
    }

    private Target mockTarget(long targetId,
                              TargetInfo targetInfo,
                              TargetDiscoverySchedule schedule,
                              DiscoveredGroupInfo groupInfo,
                              DiscoveredSettingPolicyInfo settingPolicyInfo,
                              long time, EntityProfileDTO template, DeploymentProfileInfo profile) {
        Target target = Mockito.mock(Target.class);
        Mockito.when(target.getId()).thenReturn(targetId);
        Mockito.when(target.getNoSecretDto()).thenReturn(targetInfo);
        Mockito.when(scheduler.getDiscoverySchedule(targetId))
               .thenReturn(Optional.ofNullable(schedule));
        discoveredGroupMap.put(targetId, Collections.singletonList(groupInfo));
        discoveredSettingPolicyMap.put(targetId, settingPolicyInfo);
        discoveredProfileMap.put(targetId, ImmutableMap.of(profile, Collections.singleton(template)));
        Mockito.when(entityStore.getTargetLastUpdatedTime(targetId)).thenReturn(Optional.of(time));
        return target;
    }

    /**
     * Converts a relative (to src/test/resources) path to absolute path.
     * @param fileName file name as relative path
     * @return file name with absolute path
     * @throws URISyntaxException if the path name cannot be parsed properly
     * @throws IOException is I/O error occurs
     */
    private String fullPath(String fileName) throws URISyntaxException, IOException {
        URL fileUrl = this.getClass().getClassLoader().getResources(fileName).nextElement();
        return Paths.get(fileUrl.toURI()).toString();
    }


    /**
     * Note: the groups, policies, and settings policies below are created solely for testing and
     * are almost certainly self-contradictory and meaningless. They should not be taken as
     * realistic examples of what such structures may look like. Here they only exist to test
     * that such complex objects can be accurately dumped and restored.
     */

    private DiscoveredSettingPolicyInfo makeDiscoveredSettingPolicyInfo(String name) {
        return DiscoveredSettingPolicyInfo.newBuilder()
            .addDiscoveredGroupNames("discovered group name")
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .setName(name)
            .addSettings(Setting.newBuilder()
                .setSettingSpecName(EntitySettingSpecs.IOPSCapacity.getSettingName())
                .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(250).build()))
            .build();
    }

    private DiscoveredGroupInfo makeDiscoveredGroupInfo(GroupInfo groupInfo) {
        return DiscoveredGroupInfo.newBuilder()
            .setDiscoveredGroup(makeGroupDTO())
            .setInterpretedGroup(groupInfo)
            .build();
    }

    private DiscoveredGroupInfo makeDiscoveredGroupInfo(ClusterInfo clusterInfo) {
        return DiscoveredGroupInfo.newBuilder()
            .setDiscoveredGroup(makeGroupDTO())
            .setInterpretedCluster(clusterInfo)
            .build();
    }

    private GroupDTO makeGroupDTO() {
        return GroupDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE)
            .setDisplayName("displayName")
            .setConstraintInfo(ConstraintInfo.newBuilder()
                .setConstraintType(ConstraintType.CLUSTER)
                .setConstraintId("constraintId")
                .setBuyerMetaData(BuyerMetaData.newBuilder()
                    .setSellerType(EntityType.PHYSICAL_MACHINE))
                .setConstraintName("constraintName")
                .setConstraintDisplayName("constraintDisplayName"))
            .setSelectionSpecList(SelectionSpecList.newBuilder()
                .addSelectionSpec(SelectionSpec.newBuilder()
                    .setProperty("property")
                    .setExpressionType(ExpressionType.CONTAINED_BY)
                    .setPropertyValueStringList(PropertyStringList.newBuilder()
                        .addPropertyValue("propertyValue"))))
            .addEntityProperties(EntityProperty.newBuilder()
                .setNamespace("namespace")
                .setName("name")
                .setValue("value"))
            .build();
    }

    private ClusterInfo makeClusterInfo() {
        return ClusterInfo.newBuilder()
            .setName("name")
            .setClusterType(Type.COMPUTE)
            .setMembers(StaticGroupMembers.newBuilder()
                .addStaticMemberOids(4815162342108L))
            .build();
    }

    private GroupInfo makeGroupInfo() {
        return GroupInfo.newBuilder()
            .setName("name")
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .setSearchParametersCollection(SearchParametersCollection.newBuilder()
                .addSearchParameters(SearchParameters.newBuilder()
                    .setStartingFilter(PropertyFilter.newBuilder()
                        .setPropertyName("propertyName")
                        .setNumericFilter(NumericFilter.newBuilder()
                            .setComparisonOperator(ComparisonOperator.GT)
                            .setValue(123)))
                    .addSearchFilter(SearchFilter.newBuilder()
                        .setTraversalFilter(TraversalFilter.newBuilder()
                            .setTraversalDirection(TraversalDirection.CONSUMES)
                            .setStoppingCondition(StoppingCondition.newBuilder()
                                .setNumberHops(3))))))
            .build();
    }

    private EntityProfileDTO makeDiscoveredTemplate(String id) {
        EntityProfileDTO.Builder result = EntityProfileDTO.newBuilder()
            .setId(id)
            .setDisplayName("displayName" + id)
            .setEntityType(EntityType.PHYSICAL_MACHINE)
            .addCommodityProfile(CommodityProfileDTO.newBuilder()
                .setCommodityType(CommodityType.BALLOONING)
                .setCapacity(1.5f)
                .setConsumedFactor(1.5f)
                .setConsumed(1.5f)
                .setReservation(1.5f)
                .setOverhead(1.5f))
            .setModel("tyra banks")
            .setVendor("vendor")
            .setDescription("description")
            .setEnableProvisionMatch(true)
            .setEnableResizeMatch(false)
            .addEntityProperties(EntityProperty.newBuilder()
                .setName("name").setValue("value").setNamespace("namespace"));
        if (id.startsWith("123")) {
            result.setVmProfileDTO(VMProfileDTO.newBuilder()
                .setNumVCPUs(3)
                .setVCPUSpeed(1.5f)
                .setNumStorageConsumed(3)
                .setDiskType("diskType")
                .setFamily("kennedy")
                .setNumberOfCoupons(3)
                .setDedicatedStorageNetworkState(DedicatedStorageNetworkState.CONFIGURED_DISABLED)
                .addLicense(LicenseMapEntry.newBuilder()
                    .setRegion("region")
                    .addLicenseName("license")
                )
                .setClonedUuid("CT-21-0408")
            );
        } else if (id.startsWith("456")) {
            result.setPmProfileDTO(PMProfileDTO.newBuilder()
                .setNumCores(3)
                .setCpuCoreSpeed(1.5f)
            );
        } else {
            result.setDbProfileDTO(DBProfileDTO.newBuilder()
                .addRegion("region")
                .setDbCode(3)
                .addDbEdition("edition")
                .addDbEngine("thomas the tank engine")
                .addDeploymentOption("option")
                .setNumVCPUs(3)
                .addLicense(LicenseMapEntry.newBuilder()
                    .setRegion("region")
                    .addLicenseName("license")
                )
            );
        }
        return result.build();
    }

    private DeploymentProfileInfo makeDiscoveredProfile(String name) {
        return DeploymentProfileInfo.newBuilder()
            .setName(name)
            .setDiscovered(true)
            .addContextData(DeploymentProfileContextData.newBuilder()
                .setKey("key")
                .setValue("value"))
            .addScopes(Scope.newBuilder()
                .addIds(123456)
                .setScopeAccessType(ScopeAccessType.And))
            .setProbeDeploymentProfileId("something")
            .build();
    }

}
