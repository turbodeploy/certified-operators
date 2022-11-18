package com.vmturbo.topology.processor.diagnostics;

import static com.vmturbo.topology.processor.identity.StaleOidManagerImpl.DIAGS_FILE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
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
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Clock;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import common.HealthCheck.HealthState;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredGroupsPoliciesSettings.UploadedGroup;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredSettingPolicyInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters.EntityFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
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
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealth;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealthSubCategory;
import com.vmturbo.common.protobuf.topology.DiscoveredGroup.DiscoveredGroupInfo;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.test.ResourcePath;
import com.vmturbo.components.common.diagnostics.BinaryDiagsRestorable;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;
import com.vmturbo.components.common.diagnostics.DiagsZipReader;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
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
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
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
import com.vmturbo.topology.processor.diagnostics.TopologyProcessorDiagnosticsHandler.TargetHealthInfo;
import com.vmturbo.topology.processor.discoverydumper.BinaryDiscoveryDumper;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.entity.EntityStore.TargetIncrementalEntities;
import com.vmturbo.topology.processor.entity.IdentifiedEntityDTO;
import com.vmturbo.topology.processor.entity.IncrementalEntityByMessageDTO;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader.TargetDiscoveredData;
import com.vmturbo.topology.processor.group.discovery.InterpretedGroup;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.identity.StaleOidManager;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.rpc.TargetHealthRetriever;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.scheduling.TargetDiscoverySchedule;
import com.vmturbo.topology.processor.scheduling.UnsupportedDiscoveryTypeException;
import com.vmturbo.topology.processor.staledata.StalenessInformationProvider;
import com.vmturbo.topology.processor.targets.CachingTargetStore;
import com.vmturbo.topology.processor.targets.InvalidTargetException;
import com.vmturbo.topology.processor.targets.KvTargetDao;
import com.vmturbo.topology.processor.targets.PersistentTargetSpecIdentityStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetSpecAttributeExtractor;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.targets.status.TargetStatusTracker;
import com.vmturbo.topology.processor.template.DiscoveredTemplateDeploymentProfileUploader;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService;
import com.vmturbo.topology.processor.util.Probes;

/**
 * Tests for {@link TopologyProcessorDiagnosticsHandler}.
 *
 */
public class TopologyProcessorDiagnosticsHandlerTest {

    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();
    public static final String TARGET_STATUSES_DIAGS_NAME = "TargetStatuses";
    private static final String TARGET_HEALTH_DIAGS_FILE = "TargetHealth.diags";
    private static final long LAST_SUCCESSFUL_DISCOVERY_COMPLETION_TIME = 1639045033567L;
    private static final long TIME_OF_FIRST_DISCOVERY_FAILURE = 1639047797788L;

    private final List<Target> targets = Lists.newArrayList();
    private final TargetStore targetStore = mock(TargetStore.class);
    private final BinaryDiscoveryDumper binaryDiscoveryDumper = mock(BinaryDiscoveryDumper.class);
    private final Scheduler scheduler = mock(Scheduler.class);
    private final EntityStore entityStore = mock(EntityStore.class);
    private final ProbeStore probeStore = mock(ProbeStore.class);
    private PersistentIdentityStore targetPersistentIdentityStore;
    private final DiscoveredGroupUploader groupUploader = mock(DiscoveredGroupUploader.class);
    private final DiscoveredTemplateDeploymentProfileUploader templateDeploymentProfileUploader =
        mock(DiscoveredTemplateDeploymentProfileUploader.class);
    private final DiscoveredCloudCostUploader discoveredCloudCostUploader = mock(DiscoveredCloudCostUploader.class);
    private final PriceTableUploader priceTableUploader = mock(PriceTableUploader.class);
    private final TopologyPipelineExecutorService pipelineExecutorService = mock(TopologyPipelineExecutorService.class);
    private IdentityProvider identityProvider;
    private final TargetIncrementalEntities targetIncrementalEntities =
        mock(TargetIncrementalEntities.class);
    private final TargetStatusTracker targetStatusTracker = mock(TargetStatusTracker.class);
    private final StaleOidManager staleOidManager = mock(StaleOidManager.class);
    private final StalenessInformationProvider stalenessProvider = mock(StalenessInformationProvider.class);
    private final TargetHealthRetriever targetHealthRetriever = mock(TargetHealthRetriever.class);
    private final EntityDTO nwDto =
            EntityDTO.newBuilder().setId("NW-1").setEntityType(EntityType.NETWORK).build();

    private final TargetSpec.Builder targetSpecBuilder = TargetSpec.newBuilder()
            .addAccountValue(AccountValue.newBuilder()
                .setKey("targetIdentifier")
                .setStringValue("fieldValue"));

    // With a mock schedule, the Gson response will always be with zero fields
    private static final String SCHEDULE_JSON =
        "{\"targetId\":\"0\",\"scheduleIntervalMillis\":\"0\",\"incrementalIntervalMillis\":\"0\"}\n" +
        "{\"targetId\":\"0\",\"scheduleIntervalMillis\":\"0\",\"incrementalIntervalMillis\":\"0\"}";

    private static final String IDENTIFIED_ENTITY =
            "{\"oid\":\"199\",\"entity\":"
            + "{\"entityType\":\"NETWORK\",\"id\":\"NW-1\"}}";

    private final Map<Long, Map<DeploymentProfileInfo, Set<EntityProfileDTO>>> discoveredProfileMap
        = new HashMap<>();
    private final Map<Long, TargetDiscoveredData> discoveredGroupMap = new HashMap<>();
    private final Map<Long, ProbeInfo> probeMap = new HashMap<>();
    private final LinkedHashMap<Integer, Collection<EntityDTO>>  messageIdToEntityDTO =
        new LinkedHashMap<>();
    private final int messageId = 1;
    private final EntityDTO entityDTO =
        EntityDTO.newBuilder().setId("1").setDisplayName("2").setEntityType(EntityType.PHYSICAL_MACHINE).build();
    private final IncrementalEntityByMessageDTO incrementalEntityByMessageDTO =
        new IncrementalEntityByMessageDTO(messageId, entityDTO);
    private static final String DISCOVERED_CLOUD_COST = "foo";
    private static final String DISCOVERED_PRICE_TABLE = "bar";

    private static final String TARGET_DISPLAY_NAME = "target name";
    private Map<String, BinaryDiagsRestorable> statefulEditors = Collections.emptyMap();

    @Before
    public void setup() throws Exception {
        identityProvider = Mockito.mock(IdentityProvider.class);
        Mockito.when(identityProvider.getFileName())
                .thenReturn(IdentityProviderImpl.ID_DIAGS_FILE_NAME);
        targetPersistentIdentityStore = mock(PersistentIdentityStore.class);
        Mockito.when(targetPersistentIdentityStore.getFileName())
                .thenReturn(PersistentTargetSpecIdentityStore.TARGET_IDENTIFIERS_DIAGS_FILE_NAME);
        Map<Long, EntityDTO> map = ImmutableMap.of(199L, nwDto);
        when(entityStore.discoveredByTarget(100001)).thenReturn(map);
        when(entityStore.getTargetLastUpdatedTime(100001)).thenReturn(Optional.of(12345L));
        messageIdToEntityDTO.put(messageId, Collections.singletonList(entityDTO));
        when(targetIncrementalEntities.getEntitiesByMessageId()).thenReturn(messageIdToEntityDTO);
        when(entityStore.getIncrementalEntities(1)).thenReturn(Optional.of(targetIncrementalEntities));
        when(targetStore.getAll()).thenReturn(targets);
        doReturn(discoveredGroupMap).when(groupUploader).getDataByTarget();
        doReturn(discoveredProfileMap).when(templateDeploymentProfileUploader)
            .getDiscoveredDeploymentProfilesByTarget();
        when(probeStore.getProbes()).thenReturn(probeMap);
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                final DiagnosticsAppender appender =
                        invocation.getArgumentAt(0, DiagnosticsAppender.class);
                appender.appendString(DISCOVERED_CLOUD_COST);
                return null;
            }
        }).when(discoveredCloudCostUploader).collectDiags(Mockito.any());
        Mockito.when(discoveredCloudCostUploader.getFileName())
                .thenReturn(DiscoveredCloudCostUploader.DISCOVERED_CLOUD_COST_NAME);
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                final DiagnosticsAppender appender =
                        invocation.getArgumentAt(0, DiagnosticsAppender.class);
                appender.appendString(DISCOVERED_PRICE_TABLE);
                return null;
            }
        }).when(priceTableUploader).collectDiags(Mockito.any());
        Mockito.when(priceTableUploader.getFileName())
                .thenReturn(PriceTableUploader.PRICE_TABLE_NAME);
        Mockito.when(targetStatusTracker.getFileName()).thenReturn(TARGET_STATUSES_DIAGS_NAME);
        Mockito.when(staleOidManager.getFileName()).thenReturn(DIAGS_FILE_NAME);
        statefulEditors = Collections.emptyMap();
    }

    private ZipInputStream dumpDiags() throws IOException {
        ByteArrayOutputStream zipBytes = new ByteArrayOutputStream();
        ZipOutputStream zos = new ZipOutputStream(zipBytes);
        TopologyProcessorDiagnosticsHandler handler =
                new TopologyProcessorDiagnosticsHandler(targetStore, targetPersistentIdentityStore, scheduler,
                        entityStore, probeStore, groupUploader, templateDeploymentProfileUploader,
                        identityProvider, discoveredCloudCostUploader, priceTableUploader, pipelineExecutorService,
                                statefulEditors, binaryDiscoveryDumper, targetStatusTracker, stalenessProvider, staleOidManager, targetHealthRetriever);
        handler.dump(zos);
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
        String extension = "";

        ZipEntry ze = zis.getNextEntry();
        String filename = "Identity" + getDiagsExtension(IdentityProvider.class);
        assertEquals(filename, ze.getName());

        ze = zis.getNextEntry();
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
        assertEquals(TARGET_HEALTH_DIAGS_FILE, ze.getName());
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
        assertEquals("PrometheusMetrics", ze.getName());

        ze = zis.getNextEntry();
        assertEquals(TARGET_STATUSES_DIAGS_NAME, ze.getName());

        ze = zis.getNextEntry();
        assertEquals(DIAGS_FILE_NAME, ze.getName());

        ze = zis.getNextEntry();
        assertNull(ze);
        zis.close();
    }

    @Test
    public void testTargetSecretFields()
        throws InvalidTargetException, IOException, DiagnosticsException {
        final long targetId = 1;
        final long probeId = 2;

        // A probe with a secret field.
        final ProbeInfo probeInfo = ProbeInfo.newBuilder()
            .setProbeCategory("cat")
            .setUiProbeCategory("uiCat")
            .setProbeType("type")
            .addTargetIdentifierField("name")
            .addAccountDefinition(AccountDefEntry.newBuilder()
                .setMandatory(true)
                .setIsTargetDisplayName(true)
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
            .withTarget(new Target(targetId, probeStore, targetSpec, true, true, Clock.systemUTC()));

        targets.add(withSecretFields.target);

        final ZipInputStream zis = dumpDiags();

        ZipEntry ze = zis.getNextEntry();
        String filename = "Identity" + getDiagsExtension(IdentityProvider.class);
        assertEquals(filename, ze.getName());

        ze = zis.getNextEntry();
        assertEquals("Probes.diags", ze.getName());
        byte[] bytes = new byte[1024];
        assertNotEquals(-1, zis.read(bytes));
        final String probeJson = new String(bytes, 0, 1024).trim();
        final ProbeInfoWithId result = GSON.fromJson(probeJson, ProbeInfoWithId.class);
        assertEquals(withSecretFields.probeId, result.getProbeId());
        assertEquals(withSecretFields.probeInfo, result.getProbeInfo());

        ze = zis.getNextEntry();
        assertEquals("Target.identifiers.diags", ze.getName());
        bytes = new byte[1024];
        assertEquals(-1, zis.read(bytes));

        ze = zis.getNextEntry();
        assertEquals("Targets.diags", ze.getName());
        bytes = new byte[1024];
        assertNotEquals(-1, zis.read(bytes));
        final String targetJson = new String(bytes, 0, 1024).trim();
        final TargetInfo savedTargetInfo = GSON.fromJson(targetJson, TargetInfo.class);
        Assert.assertEquals(0, savedTargetInfo.getSpec().getAccountValueCount());
    }

    @Test
    public void testRestoreTargetsInvalidJson() throws InvalidTargetException,
            TargetNotFoundException, UnsupportedDiscoveryTypeException {
        final long targetId = 1;
        final TargetInfo validTarget = TargetInfo.newBuilder()
                .setId(targetId)
                .setSpec(targetSpecBuilder.setProbeId(2))
                .setDisplayName(TARGET_DISPLAY_NAME)
                .build();
        final TopologyProcessorDiagnosticsHandler handler =
            new TopologyProcessorDiagnosticsHandler(targetStore, targetPersistentIdentityStore, scheduler,
                entityStore, probeStore, groupUploader, templateDeploymentProfileUploader,
                identityProvider, discoveredCloudCostUploader, priceTableUploader, pipelineExecutorService,
                            statefulEditors, binaryDiscoveryDumper, targetStatusTracker, stalenessProvider, null, targetHealthRetriever);
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
        verify(scheduler).setDiscoverySchedule(targetId, DiscoveryType.FULL, 365, TimeUnit.DAYS,
            false);
    }

    /**
     * Test case with some targets, probes, schedules, discovered groups, discovered policies, and
     * discovered settings policies.
     *
     * @throws IOException in case of IO error reading/writing
     */
    @Test
    public void testSomeTargets() throws IOException {

        final int firstTargetId = 100001;
        final int secondTargetId = 200001;
        TestTopology[] testTopologies = new TestTopology[]{
            new TestTopology("123456789").withTargetId(firstTargetId).withProbeId(101).withProbeInfo()
                .withTime(12345).withEntity(IDENTIFIED_ENTITY).withDiscoveredClusterGroup()
                .withSettingPolicy().withTemplate().withProfile().withTarget(mock(Target.class))
                .withTargetInfo().withFullSchedule(mock(TargetDiscoverySchedule.class))
                .withIncrementalSchedule(mock(TargetDiscoverySchedule.class))
                .setUpMocks(),

            new TestTopology("abcdefghij").withTargetId(secondTargetId).withProbeId(201).withProbeInfo()
                .withTargetInfo().withTime(23456).withDiscoveredGroupGroup().withSettingPolicy()
                .withTemplate().withTarget(mock(Target.class)).withProfile()
                .withFullSchedule(mock(TargetDiscoverySchedule.class))
                .withIncrementalSchedule(mock(TargetDiscoverySchedule.class))
                .setUpMocks()
        };
        when(entityStore.getIncrementalEntities(firstTargetId)).thenReturn(Optional.of(targetIncrementalEntities));
        when(entityStore.getIncrementalEntities(secondTargetId)).thenReturn(Optional.of(targetIncrementalEntities));

        //successful discovery
        final TargetHealth normalTargetHealth = TargetHealth.newBuilder()
                .setTargetName(TARGET_DISPLAY_NAME)
                .setSubcategory(TargetHealthSubCategory.DISCOVERY)
                .setHealthState(HealthState.NORMAL)
                .setLastSuccessfulDiscoveryCompletionTime(LAST_SUCCESSFUL_DISCOVERY_COMPLETION_TIME)
                .build();
        Mockito.when(stalenessProvider.getLastKnownTargetHealth(firstTargetId)).thenReturn(normalTargetHealth);

        //discovery failed due to incorrect credentials
        final TargetHealth criticalTargetHealth = TargetHealth.newBuilder()
                .setTargetName(TARGET_DISPLAY_NAME)
                .setSubcategory(TargetHealthSubCategory.VALIDATION)
                .setHealthState(HealthState.CRITICAL)
                .setTimeOfFirstFailure(TIME_OF_FIRST_DISCOVERY_FAILURE)
                .setMessageText(HealthState.CRITICAL
                        + ": Validation failed. Cannot complete login due to an incorrect user name or password.")
                .setLastSuccessfulDiscoveryCompletionTime(LAST_SUCCESSFUL_DISCOVERY_COMPLETION_TIME)
                .build();
        Mockito.when(stalenessProvider.getLastKnownTargetHealth(secondTargetId)).thenReturn(criticalTargetHealth);
        final ZipInputStream zis = dumpDiags();

        ZipEntry ze = zis.getNextEntry();
        String filename = "Identity" + getDiagsExtension(IdentityProvider.class);
        assertEquals(filename, ze.getName());

        ze = zis.getNextEntry();
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
        assertEquals(TARGET_HEALTH_DIAGS_FILE, ze.getName());
        bytes = new byte[1024];
        assertNotEquals(-1, zis.read(bytes));
        final String[] targetHealthJsons = new String(bytes, 0, 1024).trim().split("\n");

        final TargetHealthInfo[] targetHealths = new TargetHealthInfo[]{new TargetHealthInfo(
                firstTargetId, normalTargetHealth), new TargetHealthInfo(secondTargetId,
                criticalTargetHealth)};
        for (int i = 0; i < targetHealthJsons.length; i++) {
            assertEquals(targetHealths[i],
                    GSON.fromJson(targetHealthJsons[i], TargetHealthInfo.class));
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
            assertEquals("IncrementalEntities" + suffix, ze.getName());
            bytes = new byte[100];
            bytesRead = zis.read(bytes);
            assertNotEquals(-1, bytesRead);
            String json = new String(bytes, 0, 100).trim();
            assertEquals(IncrementalEntityByMessageDTO.toJson(incrementalEntityByMessageDTO), json);

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
        assertEquals("PrometheusMetrics", ze.getName());

        ze = zis.getNextEntry();
        assertEquals(TARGET_STATUSES_DIAGS_NAME, ze.getName());

        ze = zis.getNextEntry();
        assertEquals(DIAGS_FILE_NAME, ze.getName());

        ze = zis.getNextEntry();
        assertNull(ze);
        zis.close();
    }

    /**
     * Test the dump diags for a target that has NO discovered groups, policies, settings, templates.
     * If this test is throwing Exception, then there is an issue in how we deal with the fact that those
     * objects are not present.
     *
     * @throws IOException in case of IO error reading/writing
     */
    @Test
    public void testTargetWithNoExtraDataDiscovered() throws IOException {
        final int targetId = 100001;
        TestTopology[] testTopologies = new TestTopology[]{
            new TestTopology("123456789").withTargetId(targetId).withProbeId(101).withProbeInfo()
                .withTime(12345).withEntity(IDENTIFIED_ENTITY).withDiscoveredClusterGroup()
                .withSettingPolicy().withTemplate().withProfile().withTarget(mock(Target.class))
                .withTargetInfo().setUpMocksWithNoExtraDiscoveredData(),
        };
        when(entityStore.getIncrementalEntities(targetId)).thenReturn(Optional.of(targetIncrementalEntities));
        // this method should not throw exception, otherwise we are not dealing correctly with targets
        // missing those extra data
        final ZipInputStream zis = dumpDiags();
    }

    @Test
    public void testRestore() throws Exception {
        TargetStore simpleTargetStore = new CachingTargetStore(
                new KvTargetDao(new MapKeyValueStore(), probeStore, Clock.systemUTC()), probeStore,
                new TestIdentityStore<>(new TargetSpecAttributeExtractor(probeStore)),
                Clock.systemUTC(), binaryDiscoveryDumper);
        TopologyProcessorDiagnosticsHandler handler = new TopologyProcessorDiagnosticsHandler(
            simpleTargetStore, targetPersistentIdentityStore, scheduler, entityStore, probeStore,
            groupUploader, templateDeploymentProfileUploader, identityProvider,
            discoveredCloudCostUploader, priceTableUploader, pipelineExecutorService,
                        statefulEditors, binaryDiscoveryDumper, targetStatusTracker, stalenessProvider, null, targetHealthRetriever);
        when(probeStore.getProbe(71664194068896L)).thenReturn(Optional.of(Probes.defaultProbe));
        when(probeStore.getProbe(71564745273056L)).thenReturn(Optional.of(Probes.defaultProbe));
        handler.restore(new FileInputStream(ResourcePath.getTestResource(getClass(), "diags"
            + "/compressed/diags0.zip").toFile()), null);
        List<Target> targets = simpleTargetStore.getAll();
        assertFalse(targets.isEmpty());
        for (Target target : simpleTargetStore.getAll()) {
            // one from restoring targets, one from restoring schedules
            verify(scheduler, times(2)).setDiscoverySchedule(target.getId(), DiscoveryType.FULL,
                365, TimeUnit.DAYS, false);
            verify(entityStore).entitiesRestored(eq(target.getId()), anyLong(), anyMapOf(Long.class, EntityDTO.class));

            ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);

            verify(groupUploader).setTargetDiscoveredGroups(eq(target.getId()), captor.capture());
            List<GroupDTO> groupResult = captor.getValue();
            assertEquals(3, groupResult.size());
            assertTrue(groupResult.stream().allMatch(GroupDTO::hasMemberList));

            verify(groupUploader).restoreDiscoveredSettingPolicies(eq(target.getId()), captor.capture());
            List<DiscoveredSettingPolicyInfo> settingResult = captor.getValue();
            assertEquals(2, settingResult.size());
            assertTrue(settingResult.stream().allMatch(setting -> setting.getEntityType() == 14));

            ArgumentCaptor<Map> mapCaptor = ArgumentCaptor.forClass(Map.class);
            verify(templateDeploymentProfileUploader).setTargetsTemplateDeploymentProfileInfos(eq(target.getId()),
                    mapCaptor.capture());
            Map<DeploymentProfileInfo, Set<EntityProfileDTO>> profileResult = mapCaptor.getValue();
            assertEquals(2, profileResult.size());
            assertTrue(profileResult.values().stream().allMatch(set -> set.size() == 1));

        }
        verify(identityProvider).restoreStringDiags(anyList(), any());

        ArgumentCaptor<Map> mapCaptor = ArgumentCaptor.forClass(Map.class);
        verify(probeStore).overwriteProbeInfo(mapCaptor.capture());
        Map<Long, ProbeInfo> probeResult = mapCaptor.getValue();
        assertEquals(2, probeResult.size());
        assertTrue(probeResult.values().stream().allMatch(
                probeInfo -> probeInfo.getSupplyChainDefinitionSet(0).getTemplateClass() == EntityType.APPLICATION));
    }

    /**
     * Restore target health data from zip file into TargetHealthRetriever.
     * @throws Exception any exception.
     */
    @Test
    public void testRestoreTargetHealthFromZipFile() throws Exception {
        Map<Long, TargetHealth> targetHealthMap = new HashMap<>();
        Mockito.doAnswer( i-> {
            targetHealthMap.putAll(i.getArgumentAt(0, Map.class));
            return null;
        }).when(targetHealthRetriever).setHealthFromDiags(anyMapOf(Long.class, TargetHealth.class));

        TopologyProcessorDiagnosticsHandler handler = new TopologyProcessorDiagnosticsHandler(
                targetStore, targetPersistentIdentityStore, scheduler, entityStore, probeStore,
                groupUploader, templateDeploymentProfileUploader, identityProvider,
                discoveredCloudCostUploader, priceTableUploader, pipelineExecutorService,
                statefulEditors, binaryDiscoveryDumper, targetStatusTracker, stalenessProvider, null, targetHealthRetriever);

        //This zip file has TargetHealth.diags file
        handler.restore(new FileInputStream(ResourcePath.getTestResource(getClass(), "diags"
                + "/compressed/diags_target_health_only.zip").toFile()), null);

        assertEquals(3, targetHealthMap.size());

    }

    /**
     * Test if there is no target health file in the zip file, we need to make up target health data using target info in target store.
     * @throws Exception any exceptions.
     */
    @Test
    public void testMakeupTargetHealthWithoutFile() throws Exception{
        Map<Long, TargetHealth> targetHealthMap = new HashMap<>();
        Mockito.doAnswer( i-> {
            targetHealthMap.putAll(i.getArgumentAt(0, Map.class));
            return null;
        }).when(targetHealthRetriever).setHealthFromDiags(anyMapOf(Long.class, TargetHealth.class));

        //These targets are in targetStore
        Target target1 = Mockito.mock(Target.class);
        Target target2 = Mockito.mock(Target.class);
        when(target1.getId()).thenReturn(111111L);
        when(target2.getId()).thenReturn(222222L);
        when(target1.getDisplayName()).thenReturn("target1FromStore");
        when(target2.getDisplayName()).thenReturn("target2FromStore");
        targets.clear();
        targets.add(target1);
        targets.add(target2);


        TopologyProcessorDiagnosticsHandler handler = new TopologyProcessorDiagnosticsHandler(
                targetStore, targetPersistentIdentityStore, scheduler, entityStore, probeStore,
                groupUploader, templateDeploymentProfileUploader, identityProvider,
                discoveredCloudCostUploader, priceTableUploader, pipelineExecutorService,
                statefulEditors, binaryDiscoveryDumper, targetStatusTracker, stalenessProvider, null, targetHealthRetriever);

        //This zip file has no TargetHealth.diags file, hence all the populated info are made up using target store, all the targets are healthy.
        handler.restore(new FileInputStream(ResourcePath.getTestResource(getClass(), "diags"
                + "/compressed/diags_nothing.zip").toFile()), null);

        assertEquals(2, targetHealthMap.size());
        for (TargetHealth health: targetHealthMap.values()) {
            assertEquals(HealthState.NORMAL, health.getHealthState());
            assertEquals(TargetHealthSubCategory.DISCOVERY, health.getSubcategory());
        }
    }

    /**
     * Get the file extension depending on the class diags implementation
     *
     * @param c class that extends the Diagnosable
     * @return  extension for the diag file
     */
    private String getDiagsExtension (Class c) {
        String extension = "";
        if (BinaryDiagsRestorable.class.isAssignableFrom(c)) {
            extension = DiagsZipReader.BINARY_DIAGS_SUFFIX;
        } else if (DiagsRestorable.class.isAssignableFrom(c)) {
            extension = DiagsZipReader.TEXT_DIAGS_SUFFIX;
        }
        return extension;
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
        private TargetDiscoverySchedule fullSchedule;
        private TargetDiscoverySchedule incrementalSchedule;
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
            final Map<DiscoveryType, TargetDiscoverySchedule> scheduleMap = new HashMap<>();
            if (fullSchedule != null) {
                scheduleMap.put(DiscoveryType.FULL, fullSchedule);
            }
            if (incrementalSchedule != null) {
                scheduleMap.put(DiscoveryType.INCREMENTAL, incrementalSchedule);
            }
            when(scheduler.getDiscoverySchedule(targetId)).thenReturn(scheduleMap);

            final TargetDiscoveredData targetDiscoveredData = mock(TargetDiscoveredData.class);
            when(targetDiscoveredData.getDiscoveredSettingPolicies()).thenReturn(Stream.of(settingPolicyInfo));
            final InterpretedGroup group = mock(InterpretedGroup.class);
            when(group.createDiscoveredGroupInfo()).thenReturn(discoveredGroupInfo);
            when(targetDiscoveredData.getDiscoveredGroups()).thenReturn(Stream.of(group));
            discoveredGroupMap.put(targetId, targetDiscoveredData);
            discoveredProfileMap.put(targetId, ImmutableMap.of(profile, Collections.singleton(template)));

            return this;
        }

        /**
         * Set up a mocked topology with all the needed fields and relationship between them.
         * This mock is creating a topology with NO discovered groups, settings, policies and templates.
         *
         * @return mocked topology with NO discovered groups, settings, policies and templates.
         */
        TestTopology setUpMocksWithNoExtraDiscoveredData() {
            probeMap.put(probeId, probeInfo);
            targets.add(target);
            when(target.getId()).thenReturn(targetId);
            when(target.getNoSecretDto()).thenReturn(targetInfo);

            when(entityStore.getTargetLastUpdatedTime(targetId)).thenReturn(Optional.of(time));
            doReturn(Optional.ofNullable(probeInfo)).when(probeStore).getProbe(eq(probeId));

            final Map<DiscoveryType, TargetDiscoverySchedule> scheduleMap = new HashMap<>();
            if (fullSchedule != null) {
                scheduleMap.put(DiscoveryType.FULL, fullSchedule);
            }
            if (incrementalSchedule != null) {
                scheduleMap.put(DiscoveryType.INCREMENTAL, incrementalSchedule);
            }
            when(scheduler.getDiscoverySchedule(targetId)).thenReturn(scheduleMap);

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
            final Map<DiscoveryType, TargetDiscoverySchedule> scheduleMap = new HashMap<>();
            if (fullSchedule != null) {
                scheduleMap.put(DiscoveryType.FULL, fullSchedule);
            }
            if (incrementalSchedule != null) {
                scheduleMap.put(DiscoveryType.INCREMENTAL, incrementalSchedule);
            }
            when(scheduler.getDiscoverySchedule(targetId)).thenReturn(scheduleMap);
            discoveredGroupMap.put(targetId, new TargetDiscoveredData());
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
                .setDisplayName(TARGET_DISPLAY_NAME)
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
                .setUiProbeCategory("uiCat")
                .setProbeType(testTopologyName)
                .addSupplyChainDefinitionSet(TemplateDTO.newBuilder()
                    .setTemplateClass(EntityType.APPLICATION_COMPONENT)
                    .setTemplateType(TemplateType.BASE)
                    .setTemplatePriority(3)
                    .addCommoditySold(TemplateCommodity.newBuilder()
                        .setCommodityType(CommodityType.BALLOONING)
                        .setKey("abcdefg")
                        .addChargedBy(CommodityType.BALLOONING)
                    )
                    .addCommodityBought(CommBoughtProviderProp.newBuilder()
                        .setKey(Provider.newBuilder()
                            .setTemplateClass(EntityType.APPLICATION_COMPONENT)
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
                        .setKey(EntityType.APPLICATION_COMPONENT)
                        .setValue(ExternalEntityLink.newBuilder()
                            .setBuyerRef(EntityType.APPLICATION_COMPONENT)
                            .setSellerRef(EntityType.APPLICATION_COMPONENT)
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
                                .setEntity(EntityType.APPLICATION_COMPONENT)
                                .setAttribute("abcdef")
                                .setUseTopoExt(true)
                                .setPropertyHandler(PropertyHandler.newBuilder()
                                    .setMethodName("qwerty")
                                    .setEntityType(EntityType.APPLICATION_COMPONENT)
                                    .setDirectlyApply(true)
                                    .setNextHandler(PropertyHandler.newBuilder()
                                        .setMethodName("zxcvbn")
                                        .setEntityType(EntityType.APPLICATION_COMPONENT)
                                        .setDirectlyApply(false)
                                    )
                                )
                            )
                            .addReplacesEntity(EntityType.APPLICATION_COMPONENT)
                        )
                    )
                    .addCommBoughtOrSet(CommBoughtProviderOrSet.newBuilder()
                        .addCommBought(CommBoughtProviderProp.newBuilder()
                            .setKey(Provider.newBuilder()
                                .setTemplateClass(EntityType.APPLICATION_COMPONENT)
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
                    .setIsTargetDisplayName(true)
                    .setCustomDefinition((testTopologyName.startsWith("123") ?
                            CustomAccountDefEntry.newBuilder().setPrimitiveValue(PrimitiveValue.BOOLEAN) :
                            CustomAccountDefEntry.newBuilder().setGroupScope(
                                GroupScopePropertySet.newBuilder()
                                    .setEntityType(EntityType.APPLICATION_COMPONENT)
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
                    .setEntityType(EntityType.APPLICATION_COMPONENT)
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
                    .setEntityType(EntityType.APPLICATION_COMPONENT)
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

        TestTopology withFullSchedule(TargetDiscoverySchedule schedule) {
            this.fullSchedule = schedule;
            return this;
        }

        TestTopology withIncrementalSchedule(TargetDiscoverySchedule incrementalSchedule) {
            this.incrementalSchedule = incrementalSchedule;
            return this;
        }

        TestTopology withDiscoveredGroupGroup() {
            this.discoveredGroupInfo = DiscoveredGroupInfo.newBuilder()
                .setDiscoveredGroup(makeGroupDTO())
                .setUploadedGroup(UploadedGroup.newBuilder()
                        .setSourceIdentifier("name")
                .setDefinition(GroupDefinition.newBuilder()
                    .setEntityFilters(EntityFilters.newBuilder()
                        .addEntityFilter(EntityFilter.newBuilder()
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
                                            .setNumberHops(3))))))))))
                .build();
            return this;
        }

        TestTopology withDiscoveredClusterGroup() {
            this.discoveredGroupInfo = DiscoveredGroupInfo.newBuilder()
                .setDiscoveredGroup(makeGroupDTO())
                .setUploadedGroup(UploadedGroup.newBuilder()
                        .setSourceIdentifier("name")
                    .setDefinition(GroupDefinition.newBuilder()
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                            .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(MemberType.newBuilder()
                                    .setEntity(EntityType.PHYSICAL_MACHINE_VALUE))
                                    .addMembers(4815162342108L)))))
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

}
