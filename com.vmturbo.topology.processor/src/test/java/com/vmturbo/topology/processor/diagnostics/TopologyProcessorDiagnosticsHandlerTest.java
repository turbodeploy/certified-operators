package com.vmturbo.topology.processor.diagnostics;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

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
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.DiscoveredGroup.DiscoveredGroupInfo;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.DiagnosticsWriter;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.identity.store.PersistentIdentityStore;
import com.vmturbo.kvstore.MapKeyValueStore;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO.ActionCapability;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO.ActionPolicyElement;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ComputeTierData.DedicatedStorageNetworkState;
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
import com.vmturbo.platform.common.dto.CommonDTO.PropertyHandler;
import com.vmturbo.platform.common.dto.CommonDTO.ServerEntityPropDef;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry.GroupScopeProperty;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry.GroupScopePropertySet;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry.PrimitiveValue;
import com.vmturbo.platform.common.dto.ProfileDTO.CommodityProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO.DBProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO.LicenseMapEntry;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO.PMProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO.VMProfileDTO;
import com.vmturbo.platform.common.dto.SupplyChain.ExternalEntityLink;
import com.vmturbo.platform.common.dto.SupplyChain.ExternalEntityLink.CommodityDef;
import com.vmturbo.platform.common.dto.SupplyChain.ExternalEntityLink.EntityPropertyDef;
import com.vmturbo.platform.common.dto.SupplyChain.Provider;
import com.vmturbo.platform.common.dto.SupplyChain.Provider.ProviderType;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateCommodity;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO.CommBoughtProviderOrSet;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO.CommBoughtProviderProp;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO.ExternalEntityLinkProp;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO.TemplateType;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata.PropertyMetadata;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.TestIdentityStore;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader;
import com.vmturbo.topology.processor.cost.PriceTableUploader;
import com.vmturbo.topology.processor.diagnostics.TopologyProcessorDiagnosticsHandler.DeploymentProfileWithTemplate;
import com.vmturbo.topology.processor.diagnostics.TopologyProcessorDiagnosticsHandler.ProbeInfoWithId;
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
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetSpecAttributeExtractor;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.util.Probes;

/**
 * Tests for {@link TopologyProcessorDiagnosticsHandler}.
 *
 */
public class TopologyProcessorDiagnosticsHandlerTest {

    private static final Logger logger = LogManager.getLogger();

    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

    private final List<Target> targets = Lists.newArrayList();
    private final TargetStore targetStore = mock(TargetStore.class);
    private final Scheduler scheduler = mock(Scheduler.class);
    private final EntityStore entityStore = mock(EntityStore.class);
    private final ProbeStore probeStore = mock(ProbeStore.class);
    private final PersistentIdentityStore targetPersistentIdentityStore =
            mock(PersistentIdentityStore.class);
    private final DiscoveredGroupUploader groupUploader = mock(DiscoveredGroupUploader.class);
    private final DiscoveredTemplateDeploymentProfileUploader templateDeploymentProfileUploader =
        mock(DiscoveredTemplateDeploymentProfileUploader.class);
    private final DiscoveredCloudCostUploader discoveredCloudCostUploader = mock(DiscoveredCloudCostUploader.class);
    private final PriceTableUploader priceTableUploader = mock(PriceTableUploader.class);
    private final IdentityProvider identityProvider = mock(IdentityProvider.class);
    private final EntityDTO nwDto =
            EntityDTO.newBuilder().setId("NW-1").setEntityType(EntityType.NETWORK).build();

    private final TargetSpec.Builder targetSpecBuilder = TargetSpec.newBuilder()
            .addAccountValue(AccountValue.newBuilder()
                .setKey("targetIdentifier")
                .setStringValue("fieldValue"));

    // With a mock schedule, the Gson response will always be with zero fields
    private static final String SCHEDULE_JSON =
            GSON.toJson(mock(TargetDiscoverySchedule.class), TargetDiscoverySchedule.class);

    private static final String IDENTIFIED_ENTITY =
            "{\"oid\":\"199\",\"entity\":"
            + "{\"entityType\":\"NETWORK\",\"id\":\"NW-1\"}}";

    private final Map<Long, List<DiscoveredGroupInfo>> discoveredGroupMap = new HashMap<>();
    private final Multimap<Long, DiscoveredSettingPolicyInfo> discoveredSettingPolicyMap =
        ArrayListMultimap.create();
    private final Map<Long, Map<DeploymentProfileInfo, Set<EntityProfileDTO>>> discoveredProfileMap
        = new HashMap<>();
    private final Map<Long, ProbeInfo> probeMap = new HashMap<>();
    private final List<String> identifierDiags = new ArrayList<>();

    private static final String DISCOVERED_CLOUD_COST = "foo";
    private static final String DISCOVERED_PRICE_TABLE = "bar";

    @Before
    public void setup() throws Exception {
        Map<Long, EntityDTO> map = ImmutableMap.of(199L, nwDto);
        when(entityStore.discoveredByTarget(100001)).thenReturn(map);
        when(entityStore.getTargetLastUpdatedTime(100001)).thenReturn(Optional.of(12345L));

        when(targetStore.getAll()).thenReturn(targets);
        doReturn(discoveredGroupMap).when(groupUploader).getDiscoveredGroupInfoByTarget();
        doReturn(discoveredSettingPolicyMap).when(groupUploader)
            .getDiscoveredSettingPolicyInfoByTarget();
        doReturn(discoveredProfileMap).when(templateDeploymentProfileUploader)
            .getDiscoveredDeploymentProfilesByTarget();
        when(probeStore.getProbes()).thenReturn(probeMap);
        when(targetPersistentIdentityStore.collectDiags()).thenReturn(identifierDiags);
        when(discoveredCloudCostUploader.collectDiags()).thenReturn(Collections.singletonList(DISCOVERED_CLOUD_COST));
        when(priceTableUploader.collectDiags()).thenReturn(Collections.singletonList(DISCOVERED_PRICE_TABLE));
    }

    private ZipInputStream dumpDiags() throws IOException {
        ByteArrayOutputStream zipBytes = new ByteArrayOutputStream();
        ZipOutputStream zos = new ZipOutputStream(zipBytes);
        TopologyProcessorDiagnosticsHandler handler =
                new TopologyProcessorDiagnosticsHandler(targetStore, targetPersistentIdentityStore, scheduler,
                        entityStore, probeStore, groupUploader, templateDeploymentProfileUploader,
                        identityProvider, discoveredCloudCostUploader, priceTableUploader, new DiagnosticsWriter());
        handler.dumpDiags(zos);
        zos.close();
        return new ZipInputStream(new ByteArrayInputStream(zipBytes.toByteArray()));
    }

    /**
     * Test case with no probes and no targets (and no schedules).
     * The generated diags should be empty.
     *
     * @throws IOException if error reading input stream
     */
    @Test
    public void testNoTargets() throws IOException {
        ZipInputStream zis = dumpDiags();

        ZipEntry ze = zis.getNextEntry();
        assertEquals("Probes.diags", ze.getName());
        byte[] bytes = new byte[20];
        assertEquals(-1, zis.read(bytes));
        assertEquals(0, bytes[0]); // the entry is empty

        ze = zis.getNextEntry();
        assertEquals("Target.identifiers.diags", ze.getName());
        bytes = new byte[20];
        assertEquals(-1, zis.read(bytes));
        assertEquals(0, bytes[0]); // the entry is empty

        ze = zis.getNextEntry();
        assertEquals("Targets.diags", ze.getName());
        bytes = new byte[20];
        assertEquals(-1, zis.read(bytes));
        assertEquals(0, bytes[0]); // the entry is empty

        ze = zis.getNextEntry();
        assertEquals("Schedules.diags", ze.getName());
        bytes = new byte[20];
        assertEquals(-1, zis.read(bytes));
        assertEquals(0, bytes[0]); // the entry is empty

        ze = zis.getNextEntry();
        assertEquals("DiscoveredCloudCost.diags", ze.getName());

        ze = zis.getNextEntry();
        assertEquals("PriceTables.diags", ze.getName());

        ze = zis.getNextEntry();
        assertEquals("Identity.diags", ze.getName());

        ze = zis.getNextEntry();
        assertEquals("PrometheusMetrics.diags", ze.getName());

        ze = zis.getNextEntry();
        assertNull(ze);
        zis.close();
    }

    @Test
    public void testTargetSecretFields()
            throws InvalidTargetException, IOException {
        final long targetId = 1;
        final long probeId = 2;

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

        TestTopology withSecretFields = new TestTopology("withSecret")
            .withTargetId(targetId)
            .withProbeId(probeId).withTime(12345).withProbeInfo(probeInfo)
            .setUpTargetDependentMocks()
            .withTarget(new Target(targetId, probeStore, targetSpec, true));

        targets.add(withSecretFields.target);
        identifierDiags.add("test diags");

        final ZipInputStream zis = dumpDiags();

        ZipEntry ze = zis.getNextEntry();
        assertEquals("Probes.diags", ze.getName());
        byte[] bytes = new byte[1024];
        assertNotEquals(-1, zis.read(bytes));
        final String probeJson = new String(bytes, 0, 1024).trim();
        final ProbeInfoWithId result = GSON.fromJson(probeJson, ProbeInfoWithId.class);
        assertEquals(withSecretFields.probeId, result.getProbeId());
        assertEquals(withSecretFields.probeInfo, result.getProbeInfo());

        ze = zis.getNextEntry();
        assertEquals("Target.identifiers.diags", ze.getName());
        assertNotEquals(-1, zis.read(bytes));

        ze = zis.getNextEntry();
        assertEquals("Targets.diags", ze.getName());
        bytes = new byte[1024];
        assertNotEquals(-1, zis.read(bytes));
        final String targetJson = new String(bytes, 0, 1024).trim();
        final TargetInfo savedTargetInfo = GSON.fromJson(targetJson, TargetInfo.class);
        Assert.assertEquals(0, savedTargetInfo.getSpec().getAccountValueCount());
    }

    @Test
    public void testRestoreTargetsInvalidJson()
        throws IOException, TargetDeserializationException, InvalidTargetException, IdentityStoreException, TargetNotFoundException {
        final long targetId = 1;
        final TargetInfo validTarget = TargetInfo.newBuilder()
                .setId(targetId)
                .setSpec(targetSpecBuilder.setProbeId(2))
                .build();
        final TopologyProcessorDiagnosticsHandler handler =
                new TopologyProcessorDiagnosticsHandler(targetStore, targetPersistentIdentityStore, scheduler,
                        entityStore, probeStore, groupUploader, templateDeploymentProfileUploader,
                        identityProvider, discoveredCloudCostUploader, priceTableUploader,new DiagnosticsWriter());
        // Valid json, but not a target info
        final String invalidJsonTarget = GSON.toJson(targetSpecBuilder.setProbeId(3));
        // Invalid json
        final String invalidJson = "{osaeintr";
        final String validJsonTarget = GSON.toJson(validTarget);

        final Target target = mock(Target.class);
        when(target.getId()).thenReturn(targetId);
        when(targetStore.restoreTarget(validTarget.getId(), validTarget.getSpec()))
            .thenReturn(target);
        // Put the valid JSON target at the end of the target list, to assure
        // that preceding invalid targets don't halt processing.
        handler.restoreTargets(ImmutableList.of(invalidJsonTarget, invalidJson, validJsonTarget));

        // Verify that we only restored one target.
        verify(targetStore, times(1)).restoreTarget(
                anyLong(),
                any());
        // Verify that the restored target had the right information.
        verify(targetStore).restoreTarget(
                eq(targetId),
                eq(validTarget.getSpec()));
        // Verify that we set the broadcast interval.
        verify(scheduler).setDiscoverySchedule(targetId, 365, TimeUnit.DAYS);
    }

    /**
     * Test case with some targets, probes, schedules, discovered groups, discovered policies, and
     * discovered settings policies.
     *
     * @throws IOException in case of IO error reading/writing
     */
    @Test
    public void testSomeTargets() throws IOException {

        TestTopology[] testTopologies = new TestTopology[]{
            new TestTopology("123456789").withTargetId(100001).withProbeId(101).withProbeInfo()
                .withTime(12345).withEntity(IDENTIFIED_ENTITY).withDiscoveredClusterGroup()
                .withSettingPolicy().withTemplate().withProfile().withTarget(mock(Target.class))
                .withTargetInfo().setUpMocks(),

            new TestTopology("abcdefghij").withTargetId(200001).withProbeId(201).withProbeInfo()
                .withTargetInfo().withTime(23456).withDiscoveredGroupGroup().withSettingPolicy()
                .withTemplate().withTarget(mock(Target.class)).withProfile()
                .withSchedule(mock(TargetDiscoverySchedule.class))
                .setUpMocks()
        };

        final ZipInputStream zis = dumpDiags();

        ZipEntry ze = zis.getNextEntry();
        assertEquals("Probes.diags", ze.getName());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[3000];
        int bytesRead = zis.read(buffer);
        while (bytesRead != -1) {
            out.write(buffer, 0, bytesRead);
            bytesRead = zis.read(buffer);
        }
        byte[] bytes = out.toByteArray();

        String[] probeJsons = new String(bytes).trim().split("\n");
        for (int i = 0; i < probeJsons.length; i++) {
            final ProbeInfoWithId result = GSON.fromJson(probeJsons[i], ProbeInfoWithId.class);
            assertEquals(testTopologies[i].probeInfo, result.getProbeInfo());
            assertEquals(testTopologies[i].probeId, result.getProbeId());
        }

        ze = zis.getNextEntry();
        assertEquals("Target.identifiers.diags", ze.getName());

        ze = zis.getNextEntry();
        assertEquals("Targets.diags", ze.getName());
        bytes = new byte[1024];
        assertNotEquals(-1, zis.read(bytes));
        String[] targetJsons = new String(bytes, 0, 1024).trim().split("\n");
        for (int i = 0; i < targetJsons.length; i++) {
            assertEquals(testTopologies[i].targetInfo,
                GSON.fromJson(targetJsons[i], TargetInfo.class));
        }

        ze = zis.getNextEntry();
        assertEquals("Schedules.diags", ze.getName());
        bytes = new byte[1024];
        assertNotEquals(-1, zis.read(bytes));
        assertEquals(SCHEDULE_JSON, new String(bytes, 0, SCHEDULE_JSON.length()));

        ze = zis.getNextEntry();
        assertEquals("DiscoveredCloudCost.diags", ze.getName());
        bytes = new byte[1024];
        assertNotEquals(-1, zis.read(bytes));
        assertEquals(DISCOVERED_CLOUD_COST, new String(bytes, 0, DISCOVERED_CLOUD_COST.length()));

        ze = zis.getNextEntry();
        assertEquals("PriceTables.diags", ze.getName());
        bytes = new byte[1024];
        assertNotEquals(-1, zis.read(bytes));
        assertEquals(DISCOVERED_PRICE_TABLE, new String(bytes, 0, DISCOVERED_PRICE_TABLE.length()));

        for (TestTopology testTopology: testTopologies) {
            final String suffix = "." + testTopology.targetId + "-" + testTopology.time + ".diags";

            ze = zis.getNextEntry();
            assertEquals("Entities" + suffix, ze.getName());
            bytes = new byte[100];
            bytesRead = zis.read(bytes);

            if (testTopology.entity != null) {
                assertNotEquals(-1, bytesRead);
                String json = new String(bytes, 0, testTopology.entity.length());
                assertEquals(testTopology.entity, json);

                IdentifiedEntityDTO dto = IdentifiedEntityDTO.fromJson(json);
                assertEquals(199, dto.getOid());
                assertEquals(nwDto, dto.getEntity());
            } else {
                assertEquals(-1, bytesRead);
                assertEquals(0, bytes[0]);
            }

            ze = zis.getNextEntry();
            assertEquals("DiscoveredGroupsAndPolicies" + suffix, ze.getName());
            bytes = new byte[2048];
            assertNotEquals(-1, zis.read(bytes));
            String result = new String(bytes, 0, 2048).trim();
            assertEquals(testTopology.discoveredGroupInfo,
                GSON.fromJson(result, DiscoveredGroupInfo.class));

            ze = zis.getNextEntry();
            assertEquals("DiscoveredSettingPolicies" + suffix, ze.getName());
            bytes = new byte[2048];
            assertNotEquals(-1, zis.read(bytes));
            result = new String(bytes, 0, 2048).trim();
            assertEquals(testTopology.settingPolicyInfo,
                GSON.fromJson(result, DiscoveredSettingPolicyInfo.class));

            ze = zis.getNextEntry();
            assertEquals("DiscoveredDeploymentProfilesAndTemplates" + suffix, ze.getName());
            bytes = new byte[2048];
            assertNotEquals(-1, zis.read(bytes));
            result = new String(bytes, 0, 2048).trim();
            assertEquals(testTopology.profile,
                GSON.fromJson(result, DeploymentProfileWithTemplate.class).getProfile());
            assertEquals(testTopology.template, GSON.fromJson(result,
                DeploymentProfileWithTemplate.class).getTemplates().iterator().next());
        }

        ze = zis.getNextEntry();
        assertEquals("Identity.diags", ze.getName());

        ze = zis.getNextEntry();
        assertEquals("PrometheusMetrics.diags", ze.getName());

        ze = zis.getNextEntry();
        assertNull(ze);
        zis.close();
    }

    @Test
    public void testRestore() throws Exception {
        TargetStore simpleTargetStore = new KVBackedTargetStore(new MapKeyValueStore(), probeStore,
                new TestIdentityStore<>(new TargetSpecAttributeExtractor(probeStore)));
        TopologyProcessorDiagnosticsHandler handler = new TopologyProcessorDiagnosticsHandler(
            simpleTargetStore, targetPersistentIdentityStore, scheduler, entityStore, probeStore,
            groupUploader, templateDeploymentProfileUploader, identityProvider,
            discoveredCloudCostUploader, priceTableUploader, new DiagnosticsWriter());
        when(probeStore.getProbe(71664194068896L)).thenReturn(Optional.of(Probes.defaultProbe));
        when(probeStore.getProbe(71564745273056L)).thenReturn(Optional.of(Probes.defaultProbe));
        handler.restore(new FileInputStream(new File(fullPath("diags/compressed/diags0.zip"))));
        List<Target> targets = simpleTargetStore.getAll();
        assertTrue(!targets.isEmpty());
        for (Target target : simpleTargetStore.getAll()) {
            verify(scheduler, times(1)).setDiscoverySchedule(target.getId(), 600000, TimeUnit.MILLISECONDS);
            verify(entityStore).entitiesRestored(eq(target.getId()), anyLong(), anyMapOf(Long.class, EntityDTO.class));

            ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);

            verify(groupUploader).setTargetDiscoveredGroups(eq(target.getId()), captor.capture());
            List<GroupDTO> groupResult = captor.<List<GroupDTO>>getValue();
            assertEquals(3, groupResult.size());
            assertTrue(groupResult.stream().allMatch(GroupDTO::hasMemberList));

            verify(groupUploader).setTargetDiscoveredSettingPolicies(eq(target.getId()), captor.capture());
            List<DiscoveredSettingPolicyInfo> settingResult = captor.<List<DiscoveredSettingPolicyInfo>>getValue();
            assertEquals(2, settingResult.size());
            assertTrue(settingResult.stream().allMatch(setting -> setting.getEntityType() == 14));

            ArgumentCaptor<Map> mapCaptor = ArgumentCaptor.forClass(Map.class);
            verify(templateDeploymentProfileUploader).setTargetsTemplateDeploymentProfileInfos(eq(target.getId()),
                    mapCaptor.capture());
            Map<DeploymentProfileInfo, Set<EntityProfileDTO>> profileResult = mapCaptor
                    .<Map<DeploymentProfileInfo, Set<EntityProfileDTO>>>getValue();
            assertEquals(2, profileResult.size());
            assertTrue(profileResult.values().stream().allMatch(set -> set.size() == 1));

        }
        verify(identityProvider).restoreDiags(any());

        ArgumentCaptor<Map> mapCaptor = ArgumentCaptor.forClass(Map.class);
        verify(probeStore).overwriteProbeInfo(mapCaptor.capture());
        Map<Long, ProbeInfo> probeResult = mapCaptor.<Map<Long, ProbeInfo>>getValue();
        assertEquals(2, probeResult.size());
        assertTrue(probeResult.values().stream().allMatch(
                probeInfo -> probeInfo.getSupplyChainDefinitionSet(0).getTemplateClass() == EntityType.APPLICATION));
    }

    /**
     * Note: the groups, policies, and settings policies below are created solely for testing and
     * are almost certainly self-contradictory and meaningless. They should not be taken as
     * realistic examples of what such structures may look like. Here they only exist to test
     * that such complex objects can be accurately dumped and restored.
     *
     * This class gathers test objects and sets up the mock relationships between them.
     */
    private class TestTopology {
        private final String testTopologyName;
        private long targetId;
        private Target target;
        private TargetInfo targetInfo;
        private long probeId;
        private ProbeInfo probeInfo;
        private TargetDiscoverySchedule schedule;
        private DiscoveredGroupInfo discoveredGroupInfo;
        private DiscoveredSettingPolicyInfo settingPolicyInfo;
        private DeploymentProfileInfo profile;
        private EntityProfileDTO template;
        private long time;
        private String entity;

        /**
         * Set up the full system of mock relationships between all the fields.
         * @return this with all necessary mock relationships set up.
         */
        TestTopology setUpMocks() {
            probeMap.put(probeId, probeInfo);
            targets.add(target);
            when(target.getId()).thenReturn(targetId);
            when(target.getNoSecretDto()).thenReturn(targetInfo);

            when(entityStore.getTargetLastUpdatedTime(targetId)).thenReturn(Optional.of(time));
            doReturn(Optional.ofNullable(probeInfo)).when(probeStore).getProbe(eq(probeId));
            when(scheduler.getDiscoverySchedule(targetId)).thenReturn(Optional.ofNullable(schedule));

            discoveredGroupMap.put(targetId, Collections.singletonList(discoveredGroupInfo));
            discoveredSettingPolicyMap.put(targetId, settingPolicyInfo);
            discoveredProfileMap.put(targetId, ImmutableMap.of(profile, Collections.singleton(template)));

            return this;
        }

        /**
         * Set up the limited system of mocks that are necessary for testing the dump of a target's
         * secret fields. This omits many of the mock relationships necessary for the full dump.
         * @return this with all necessary mock relationships set up
         */
        TestTopology setUpTargetDependentMocks() {
            probeMap.put(probeId, probeInfo);
            when(entityStore.getTargetLastUpdatedTime(targetId)).thenReturn(Optional.of(time));
            doReturn(Optional.ofNullable(probeInfo)).when(probeStore).getProbe(eq(probeId));
            when(scheduler.getDiscoverySchedule(targetId)).thenReturn(Optional.ofNullable(schedule));
            return this;
        }

        TestTopology(String testTopologyName) {
            this.testTopologyName = testTopologyName;
        }

        TestTopology withEntity(String entity) {
            this.entity = entity;
            return this;
        }

        TestTopology withTarget(Target target) {
            this.target = target;
            return this;
        }

        TestTopology withTargetInfo() {
            this.targetInfo = TargetInfo.newBuilder()
                .setId(targetId)
                .setSpec(targetSpecBuilder.setProbeId(probeId))
                .build();
            return this;
        }

        TestTopology withProbeId(long probeId) {
            this.probeId = probeId;
            return this;
        }

        TestTopology withProbeInfo(ProbeInfo probeInfo) {
            this.probeInfo = probeInfo;
            return this;
        }

        TestTopology withProbeInfo() {
            this.probeInfo = ProbeInfo.newBuilder()
                .setProbeCategory("cat")
                .setProbeType(testTopologyName)
                .addSupplyChainDefinitionSet(TemplateDTO.newBuilder()
                    .setTemplateClass(EntityType.APPLICATION)
                    .setTemplateType(TemplateType.BASE)
                    .setTemplatePriority(3)
                    .addCommoditySold(TemplateCommodity.newBuilder()
                        .setCommodityType(CommodityType.BALLOONING)
                        .setKey("abcdefg")
                        .addChargedBy(CommodityType.BALLOONING)
                    )
                    .addCommodityBought(CommBoughtProviderProp.newBuilder()
                        .setKey(Provider.newBuilder()
                            .setTemplateClass(EntityType.APPLICATION)
                            .setProviderType(ProviderType.HOSTING)
                            .setCardinalityMax(123)
                            .setCardinalityMin(101)
                        )
                        .addValue(TemplateCommodity.newBuilder()
                            .setCommodityType(CommodityType.BALLOONING)
                            .setKey("abcdefg")
                            .addChargedBy(CommodityType.BALLOONING)
                        )
                    )
                    .addExternalLink(ExternalEntityLinkProp.newBuilder()
                        .setKey(EntityType.APPLICATION)
                        .setValue(ExternalEntityLink.newBuilder()
                            .setBuyerRef(EntityType.APPLICATION)
                            .setSellerRef(EntityType.APPLICATION)
                            .setRelationship(ProviderType.HOSTING)
                            .addCommodityDefs(CommodityDef.newBuilder()
                                .setType(CommodityType.BALLOONING)
                            )
                            .setKey("abcdefg")
                            .setHasExternalEntity(true)
                            .addProbeEntityPropertyDef(EntityPropertyDef.newBuilder()
                                .setName("abc")
                                .setDescription("def")
                            )
                            .addExternalEntityPropertyDefs(ServerEntityPropDef.newBuilder()
                                .setEntity(EntityType.APPLICATION)
                                .setAttribute("abcdef")
                                .setUseTopoExt(true)
                                .setPropertyHandler(PropertyHandler.newBuilder()
                                    .setMethodName("qwerty")
                                    .setEntityType(EntityType.APPLICATION)
                                    .setDirectlyApply(true)
                                    .setNextHandler(PropertyHandler.newBuilder()
                                        .setMethodName("zxcvbn")
                                        .setEntityType(EntityType.APPLICATION)
                                        .setDirectlyApply(false)
                                    )
                                )
                            )
                            .addReplacesEntity(EntityType.APPLICATION)
                        )
                    )
                    .addCommBoughtOrSet(CommBoughtProviderOrSet.newBuilder()
                        .addCommBought(CommBoughtProviderProp.newBuilder()
                            .setKey(Provider.newBuilder()
                                .setTemplateClass(EntityType.APPLICATION)
                                .setProviderType(ProviderType.HOSTING)
                                .setCardinalityMax(123)
                                .setCardinalityMin(101)
                            )
                            .addValue(TemplateCommodity.newBuilder()
                                .setCommodityType(CommodityType.BALLOONING)
                                .setKey("abcdefg")
                                .addChargedBy(CommodityType.BALLOONING)
                            )
                        )
                    )
                )
                .addAccountDefinition(AccountDefEntry.newBuilder()
                    .setMandatory(true)
                    .setCustomDefinition((testTopologyName.startsWith("123") ?
                            CustomAccountDefEntry.newBuilder().setPrimitiveValue(PrimitiveValue.BOOLEAN) :
                            CustomAccountDefEntry.newBuilder().setGroupScope(
                                GroupScopePropertySet.newBuilder()
                                    .setEntityType(EntityType.APPLICATION)
                                    .addProperty(GroupScopeProperty.newBuilder().setPropertyName("abc"))
                            )
                        ).setName("name")
                            .setDisplayName("displayName")
                            .setDescription("description")
                            .setIsSecret(true)
                    )
                    .setDefaultValue("abcdefghij")
                )
                .addTargetIdentifierField("field")
                .setFullRediscoveryIntervalSeconds(123)
                .addEntityMetadata(EntityIdentityMetadata.newBuilder()
                    .setEntityType(EntityType.APPLICATION)
                    .addNonVolatileProperties(PropertyMetadata.newBuilder()
                        .setName("12345")
                    )
                    .addVolatileProperties(PropertyMetadata.newBuilder()
                        .setName("jkl;")
                    )
                    .addHeuristicProperties(PropertyMetadata.newBuilder()
                        .setName("asdf")
                    )
                )
                .addActionPolicy(ActionPolicyDTO.newBuilder()
                    .setEntityType(EntityType.APPLICATION)
                    .addPolicyElement(ActionPolicyElement.newBuilder()
                        .setActionType(ActionType.CHANGE)
                        .setActionCapability(ActionCapability.NOT_EXECUTABLE)
                    )
                )
                .setIncrementalRediscoveryIntervalSeconds(123)
                .setPerformanceRediscoveryIntervalSeconds(123)
                .build();
            return this;
        }

        TestTopology withTargetId(long targetId) {
            this.targetId = targetId;
            return this;
        }

        TestTopology withSchedule(TargetDiscoverySchedule schedule) {
            this.schedule = schedule;
            return this;
        }

        TestTopology withDiscoveredGroupGroup() {
            this.discoveredGroupInfo = DiscoveredGroupInfo.newBuilder()
                .setDiscoveredGroup(makeGroupDTO())
                .setInterpretedGroup(GroupInfo.newBuilder()
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
                    .build())
                .build();
            return this;
        }

        TestTopology withDiscoveredClusterGroup() {
            this.discoveredGroupInfo = DiscoveredGroupInfo.newBuilder()
                .setDiscoveredGroup(makeGroupDTO())
                .setInterpretedCluster(ClusterInfo.newBuilder()
                    .setName("name")
                    .setClusterType(Type.COMPUTE)
                    .setMembers(StaticGroupMembers.newBuilder()
                        .addStaticMemberOids(4815162342108L))
                    .build())
                .build();
            return this;
        }

        TestTopology withSettingPolicy() {
            this.settingPolicyInfo = DiscoveredSettingPolicyInfo.newBuilder()
                .addDiscoveredGroupNames("discovered group name")
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .setName(testTopologyName)
                .addSettings(Setting.newBuilder()
                    .setSettingSpecName(EntitySettingSpecs.IOPSCapacity.getSettingName())
                    .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(250).build()))
                .build();
            return this;
        }

        TestTopology withProfile() {
            this.profile = DeploymentProfileInfo.newBuilder()
                .setName(testTopologyName)
                .setDiscovered(true)
                .addContextData(DeploymentProfileContextData.newBuilder()
                    .setKey("key")
                    .setValue("value"))
                .addScopes(Scope.newBuilder()
                    .addIds(123456)
                    .setScopeAccessType(ScopeAccessType.And))
                .setProbeDeploymentProfileId("something")
                .build();
            return this;
        }

        TestTopology withTemplate() {
            EntityProfileDTO.Builder result = EntityProfileDTO.newBuilder()
                .setId(testTopologyName)
                .setDisplayName("displayName " + testTopologyName)
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
            if (testTopologyName.startsWith("123")) {
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
            } else if (testTopologyName.startsWith("456")) {
                result.setPmProfileDTO(PMProfileDTO.newBuilder()
                    .setNumCores(3)
                    .setCpuCoreSpeed(1.5f)
                );
            } else {
                result.setDbProfileDTO(DBProfileDTO.newBuilder()
                    .addDbEdition("edition")
                    .addDbEngine("thomas the tank engine")
                    .setNumVCPUs(3)
                    .addLicense(LicenseMapEntry.newBuilder()
                        .setRegion("region")
                        .addLicenseName("license")
                    )
                );
            }
            this.template = result.build();
            return this;
        }

        TestTopology withTime(long time) {
            this.time = time;
            return this;
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
}
