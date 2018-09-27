package com.vmturbo.topology.processor.conversions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.ZipInputStream;

import javax.annotation.Nonnull;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;

import com.vmturbo.common.protobuf.plan.DiscoveredTemplateDeploymentProfileServiceGrpc;
import com.vmturbo.common.protobuf.plan.DiscoveredTemplateDeploymentProfileServiceGrpc.DiscoveredTemplateDeploymentProfileServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.communication.ITransport;
import com.vmturbo.components.api.GrpcChannelFactory;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.kvstore.MapKeyValueStore;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata.PropertyMetadata;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.api.dto.InputField;
import com.vmturbo.topology.processor.api.impl.TargetRESTApi;
import com.vmturbo.topology.processor.conversions.cloud.CloudTopologyConverter;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.identity.IdentityService;
import com.vmturbo.topology.processor.identity.services.HeuristicsMatcher;
import com.vmturbo.topology.processor.identity.storage.IdentityDatabaseStore;
import com.vmturbo.topology.processor.identity.storage.IdentityServiceInMemoryUnderlyingStore;
import com.vmturbo.topology.processor.plan.DiscoveredTemplateDeploymentProfileNotifier;
import com.vmturbo.topology.processor.plan.DiscoveredTemplateDeploymentProfileUploader;
import com.vmturbo.topology.processor.probes.ProbeInfoCompatibilityChecker;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.probes.RemoteProbeStore;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingOperationStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.util.Probes;

/**
 * Unit test for {@link CloudTopologyConverter}.
 */
public class CloudTopologyConverterTest {

    private static final Logger logger = LogManager.getLogger();

    private final StitchingOperationStore stitchingOperationStore = Mockito.mock(StitchingOperationStore.class);
    private final ProbeInfoCompatibilityChecker compatibilityChecker = Mockito.mock(ProbeInfoCompatibilityChecker.class);

    private KeyValueStore keyValueStore = new MapKeyValueStore();
    {
        keyValueStore.put("id/probes/AWS", "11");
        keyValueStore.put("id/probes/Azure", "21");
    }
    private IdentityProvider identityProvider = new IdentityProviderImpl(
            new IdentityService(new IdentityServiceInMemoryUnderlyingStore(
                    Mockito.mock(IdentityDatabaseStore.class)), new HeuristicsMatcher()), keyValueStore, 0L);

    private final ProbeStore probeStore = new RemoteProbeStore(keyValueStore, identityProvider,
            stitchingOperationStore, compatibilityChecker);

    private final ITransport<MediationServerMessage, MediationClientMessage> transport =
            (ITransport<MediationServerMessage, MediationClientMessage>)Mockito.mock(ITransport.class);

    private final TargetStore targetStore = Mockito.mock(TargetStore.class);

    private EntityStore entityStore = new EntityStore(targetStore, identityProvider, Clock.systemUTC());

    private DiscoveredTemplateDeploymentProfileServiceBlockingStub templatesDeploymentProfileService =
            DiscoveredTemplateDeploymentProfileServiceGrpc.newBlockingStub(GrpcChannelFactory
                    .newChannelBuilder("127.0.0.1", 99)
                    .keepAliveTime(30, TimeUnit.SECONDS)
                    .build());

    private DiscoveredTemplateDeploymentProfileNotifier profileUploader =
            new DiscoveredTemplateDeploymentProfileUploader(entityStore, templatesDeploymentProfileService);

    private ProbeInfo awsProbeInfo = ProbeInfo.newBuilder(Probes.defaultProbe)
            .addAllEntityMetadata(createIdentityMetadata(Lists.newArrayList(
                    EntityType.VIRTUAL_MACHINE,
                    EntityType.PHYSICAL_MACHINE,
                    EntityType.STORAGE,
                    EntityType.DATACENTER,
                    EntityType.DATABASE,
                    EntityType.DATABASE_SERVER,
                    EntityType.LOAD_BALANCER,
                    EntityType.VIRTUAL_APPLICATION,
                    EntityType.APPLICATION,
                    EntityType.BUSINESS_ACCOUNT,
                    EntityType.DISK_ARRAY,
                    EntityType.RESERVED_INSTANCE,
                    EntityType.COMPUTE_TIER,
                    EntityType.STORAGE_TIER,
                    EntityType.DATABASE_TIER,
                    EntityType.AVAILABILITY_ZONE,
                    EntityType.REGION,
                    EntityType.CLOUD_SERVICE)))
            .setProbeType(SDKProbeType.AWS.getProbeType()).build();

    private ProbeInfo azureProbeInfo = ProbeInfo.newBuilder(Probes.defaultProbe)
            .addAllEntityMetadata(createIdentityMetadata(Lists.newArrayList(
                    EntityType.VIRTUAL_MACHINE,
                    EntityType.PHYSICAL_MACHINE,
                    EntityType.STORAGE,
                    EntityType.DATACENTER,
                    EntityType.DATABASE,
                    EntityType.DATABASE_SERVER,
                    EntityType.LOAD_BALANCER,
                    EntityType.VIRTUAL_APPLICATION,
                    EntityType.APPLICATION,
                    EntityType.BUSINESS_ACCOUNT,
                    EntityType.DISK_ARRAY,
                    EntityType.RESERVED_INSTANCE,
                    EntityType.COMPUTE_TIER,
                    EntityType.STORAGE_TIER,
                    EntityType.DATABASE_TIER,
                    EntityType.AVAILABILITY_ZONE,
                    EntityType.REGION,
                    EntityType.CLOUD_SERVICE)))
            .setProbeType(SDKProbeType.AZURE.getProbeType()).build();

    final long awsTargetId1 = 1;
    final long awsTargetId2 = 2;
    final long azureTargetId1 = 3;
    final long azureTargetId2 = 4;

    final long awsProbeId1 = 11;
    final long awsProbeId2 = 11;
    final long azureProbeId1 = 21;
    final long azureProbeId2 = 21;

    final TargetRESTApi.TargetSpec spec1 = new TargetRESTApi.TargetSpec(awsProbeId1,
            Collections.singletonList(new InputField("name", "value", Optional.of(
                    Collections.singletonList(Collections.singletonList("test"))))));
    final TargetRESTApi.TargetSpec spec2 = new TargetRESTApi.TargetSpec(awsProbeId2,
            Collections.singletonList(new InputField("name", "value", Optional.of(
                    Collections.singletonList(Collections.singletonList("test"))))));
    final TargetRESTApi.TargetSpec spec3 = new TargetRESTApi.TargetSpec(azureProbeId1,
            Collections.singletonList(new InputField("name", "value", Optional.of(
                    Collections.singletonList(Collections.singletonList("test"))))));
    final TargetRESTApi.TargetSpec spec4 = new TargetRESTApi.TargetSpec(azureProbeId2,
            Collections.singletonList(new InputField("name", "value", Optional.of(
                    Collections.singletonList(Collections.singletonList("test"))))));

    private Target aws_engineering;
    private Target aws_adveng;
    private Target azure_engineering;
    private Target azure_productmgmt;

    private static String AWS_ENGINEERING_FILE_PATH =
            "src/test/resources/protobuf/messages/aws_engineering.aws.amazon.com.txt";
    private static String AWS_ADVENG_FILE_PATH =
            "src/test/resources/protobuf/messages/aws_adveng.aws.amazon.com.txt";
    private static String AZURE_ENGINEERING_FILE_PATH =
            "src/test/resources/protobuf/messages/azure_engineering.management.core.windows.net.txt";
    private static String AZURE_PRODUCTMGMT_FILE_PATH =
            "src/test/resources/protobuf/messages/azure_productmgmt.management.core.windows.net.txt";

    @Before
    public void setup() throws Exception {
        probeStore.registerNewProbe(awsProbeInfo, transport);
        probeStore.registerNewProbe(azureProbeInfo, transport);

        aws_engineering = new Target(awsTargetId1, probeStore, spec1.toDto(), false);
        aws_adveng = new Target(awsTargetId2, probeStore, spec2.toDto(), false);
        azure_engineering = new Target(azureTargetId1, probeStore, spec3.toDto(), false);
        azure_productmgmt = new Target(azureTargetId2, probeStore, spec4.toDto(), false);

        Mockito.when(targetStore.getTarget(awsTargetId1)).thenReturn(Optional.of(aws_engineering));
        Mockito.when(targetStore.getTarget(awsTargetId2)).thenReturn(Optional.of(aws_adveng));
        Mockito.when(targetStore.getTarget(azureTargetId1)).thenReturn(Optional.of(azure_engineering));
        Mockito.when(targetStore.getTarget(azureTargetId2)).thenReturn(Optional.of(azure_productmgmt));

        Mockito.when(targetStore.getProbeTypeForTarget(awsTargetId1)).thenReturn(Optional.of(SDKProbeType.AWS));
        Mockito.when(targetStore.getProbeTypeForTarget(awsTargetId2)).thenReturn(Optional.of(SDKProbeType.AWS));
        Mockito.when(targetStore.getProbeTypeForTarget(azureTargetId1)).thenReturn(Optional.of(SDKProbeType.AZURE));
        Mockito.when(targetStore.getProbeTypeForTarget(azureTargetId2)).thenReturn(Optional.of(SDKProbeType.AZURE));

        Mockito.when(targetStore.getProbeIdForTarget(awsTargetId1)).thenReturn(Optional.of(awsProbeId1));
        Mockito.when(targetStore.getProbeIdForTarget(awsTargetId2)).thenReturn(Optional.of(awsProbeId2));
        Mockito.when(targetStore.getProbeIdForTarget(azureTargetId1)).thenReturn(Optional.of(azureProbeId1));
        Mockito.when(targetStore.getProbeIdForTarget(azureTargetId2)).thenReturn(Optional.of(azureProbeId2));
    }

    @Test
    public void testConvertTopologyForOneAWSTarget() throws Exception {
        // initialize target
        Mockito.when(targetStore.getAll()).thenReturn(Lists.newArrayList(aws_engineering));
        entityStore = new EntityStore(targetStore, identityProvider, Clock.systemUTC());
        profileUploader = new DiscoveredTemplateDeploymentProfileUploader(entityStore, templatesDeploymentProfileService);
        DiscoveryResponse discoveryResponse = readResponseFromFile(AWS_ENGINEERING_FILE_PATH);
        entityStore.entitiesDiscovered(awsProbeId1, awsTargetId1, discoveryResponse.getEntityDTOList());
        profileUploader.setTargetsTemplateDeploymentProfile(awsTargetId1, discoveryResponse
                .getEntityProfileList(), discoveryResponse.getDeploymentProfileList());

        // construct StitchingContext
        StitchingContext stitchingContext = entityStore.constructStitchingContext(targetStore,
                profileUploader.getTargetToEntityProfilesMap());

        // verify counts of new cloud entities, e.g. regions, availability_zones, storage tiers and
        // compute tiers
        Map<EntityType, Integer> entityTypeCounts = stitchingContext.entityTypeCounts();

        // 16 different entity types at this moment
        assertEquals(16, entityTypeCounts.size());

        // verify that we have regions and AZ's, but no PM's or DC's
        Assert.assertEquals(15, entityTypeCounts.get(EntityType.REGION).intValue());
        Assert.assertEquals(45, entityTypeCounts.get(EntityType.AVAILABILITY_ZONE).intValue());
        Assert.assertFalse(entityTypeCounts.containsKey(EntityType.DATACENTER));
        Assert.assertFalse(entityTypeCounts.containsKey(EntityType.PHYSICAL_MACHINE));
        // verify that we are creating storage tiers
        // there are 7 different storage tiers: STANDARD, ST1, SC1, SSD, GP2, HDD, IO1
        Assert.assertEquals(7, entityTypeCounts.get(EntityType.STORAGE_TIER).intValue());
        // verify that we are creating compute tiers
        // there are 146 different VM profiles + 43 DatabaseServer profiles
        Assert.assertEquals(146, entityTypeCounts.get(EntityType.COMPUTE_TIER).intValue());
        Assert.assertEquals(43, entityTypeCounts.get(EntityType.DATABASE_TIER).intValue());
        // we expect the storages to be removed in the topology map, first verify they are here.
        Assert.assertEquals(315, entityTypeCounts.get(EntityType.STORAGE).intValue());
        Assert.assertEquals(45, entityTypeCounts.get(EntityType.DISK_ARRAY).intValue());
        // verify vms
        Assert.assertEquals(135, entityTypeCounts.get(EntityType.VIRTUAL_MACHINE).intValue());
        // verify other entity types
        Assert.assertEquals(9, entityTypeCounts.get(EntityType.DATABASE).intValue());
        Assert.assertEquals(9, entityTypeCounts.get(EntityType.DATABASE_SERVER).intValue());
        Assert.assertEquals(24, entityTypeCounts.get(EntityType.LOAD_BALANCER).intValue());
        Assert.assertEquals(28, entityTypeCounts.get(EntityType.VIRTUAL_APPLICATION).intValue());
        Assert.assertEquals(187, entityTypeCounts.get(EntityType.APPLICATION).intValue());
        Assert.assertEquals(27, entityTypeCounts.get(EntityType.RESERVED_INSTANCE).intValue());
        Assert.assertEquals(3, entityTypeCounts.get(EntityType.CLOUD_SERVICE).intValue());

        // construct topology and group by entity type
        Map<Long, TopologyEntity.Builder> topology = stitchingContext.constructTopology();
        Map<EntityType, List<TopologyEntity.Builder>> topologyEntitiesByType = topology.values()
                .stream().collect(Collectors.groupingBy(builder ->
                        EntityType.forNumber(builder.getEntityType())));

        // verify there are 14 different entity types in new topology
        Assert.assertEquals(14, topologyEntitiesByType.size());

        // although the graph had storages, they should be gone from the topology map group the
        // topology by entity type. we expect NO storages in the resulting topology
        Assert.assertNull(topologyEntitiesByType.get(EntityType.STORAGE));
        // we do expect our storage tiers to be the same though
        Assert.assertEquals(7, topologyEntitiesByType.get(EntityType.STORAGE_TIER)
                .stream().filter(this::isValidStorageTier).count());
        // verify CT
        Assert.assertEquals(146, topologyEntitiesByType.get(EntityType.COMPUTE_TIER)
                .stream().filter(this::isValidComputeTier).count());
        // verify DT
        Assert.assertEquals(43, topologyEntitiesByType.get(EntityType.DATABASE_TIER)
                .stream().filter(this::isValidDatabaseTier).count());
        // verify CS
        Assert.assertEquals(3, topologyEntitiesByType.get(EntityType.CLOUD_SERVICE)
                .stream().filter(this::isValidCloudService).count());
        // verify AZs
        Assert.assertEquals(45, topologyEntitiesByType.get(EntityType.AVAILABILITY_ZONE).size());
        // verify Regions own AZs
        Assert.assertEquals(15, topologyEntitiesByType.get(EntityType.REGION).stream()
                .filter(this::isValidRegion).count());
        // verify all VM's are buying from both ST and CT and connected to AZ
        Assert.assertEquals(129, topologyEntitiesByType.get(EntityType.VIRTUAL_MACHINE)
                .stream().filter(this::isValidVM).count());

        Assert.assertEquals(9, topologyEntitiesByType.get(EntityType.DATABASE).stream()
                .filter(this::isValidDatabase).count());
        Assert.assertEquals(9, topologyEntitiesByType.get(EntityType.DATABASE_SERVER)
                .stream().filter(this::isValidDatabaseServer).count());
        Assert.assertEquals(3, topologyEntitiesByType.get(EntityType.BUSINESS_ACCOUNT)
                .stream().filter(builder -> isValidBusinessAccount(builder, topology)).count());

        // verify other entity types
        Assert.assertEquals(24, topologyEntitiesByType.get(EntityType.LOAD_BALANCER).size());
        Assert.assertEquals(28, topologyEntitiesByType.get(EntityType.VIRTUAL_APPLICATION).size());
        Assert.assertEquals(187, topologyEntitiesByType.get(EntityType.APPLICATION).size());
    }

    @Test
    public void testConvertTopologyForTwoAWSTargets_OneMasterAndOneSub() throws Exception {
        // initialize two AWS targets
        Mockito.when(targetStore.getAll()).thenReturn(Lists.newArrayList(aws_engineering, aws_adveng));
        entityStore = new EntityStore(targetStore, identityProvider, Clock.systemUTC());
        profileUploader = new DiscoveredTemplateDeploymentProfileUploader(entityStore, templatesDeploymentProfileService);

        DiscoveryResponse response1 = readResponseFromFile(AWS_ENGINEERING_FILE_PATH);
        entityStore.entitiesDiscovered(awsProbeId1, awsTargetId1, response1.getEntityDTOList());
        profileUploader.setTargetsTemplateDeploymentProfile(awsTargetId1, response1
                .getEntityProfileList(), response1.getDeploymentProfileList());

        DiscoveryResponse response2 = readResponseFromFile(AWS_ADVENG_FILE_PATH);
        entityStore.entitiesDiscovered(awsProbeId2, awsTargetId2, response2.getEntityDTOList());
        profileUploader.setTargetsTemplateDeploymentProfile(awsTargetId1, response2
                .getEntityProfileList(), response2.getDeploymentProfileList());

        // construct StitchingContext
        StitchingContext stitchingContext = entityStore.constructStitchingContext(targetStore,
                profileUploader.getTargetToEntityProfilesMap());

        // verify counts of new cloud entities, e.g. regions, availability_zones, storage tiers and
        // compute tiers
        Map<EntityType, Integer> entityTypeCounts = stitchingContext.entityTypeCounts();

        // 16 different entity types at this moment
        assertEquals(16, entityTypeCounts.size());

        // verify that we have regions and AZ's, but no PM's or DC's
        Assert.assertEquals(30, entityTypeCounts.get(EntityType.REGION).intValue());
        Assert.assertEquals(88, entityTypeCounts.get(EntityType.AVAILABILITY_ZONE).intValue());
        Assert.assertFalse(entityTypeCounts.containsKey(EntityType.DATACENTER));
        Assert.assertFalse(entityTypeCounts.containsKey(EntityType.PHYSICAL_MACHINE));
        // verify that we are creating storage tiers
        // there are 7 different storage tiers: STANDARD, ST1, SC1, SSD, GP2, HDD, IO1
        Assert.assertEquals(7, entityTypeCounts.get(EntityType.STORAGE_TIER).intValue());
        // verify that we are creating compute tiers, there are 146 different VM profiles
        Assert.assertEquals(146, entityTypeCounts.get(EntityType.COMPUTE_TIER).intValue());
        // verify that we are creating compute tiers, 43 DatabaseServer profiles
        Assert.assertEquals(43, entityTypeCounts.get(EntityType.DATABASE_TIER).intValue());
        // we expect the storages to be removed in the topology map, first verify they are here.
        Assert.assertEquals(616, entityTypeCounts.get(EntityType.STORAGE).intValue());
        Assert.assertEquals(88, entityTypeCounts.get(EntityType.DISK_ARRAY).intValue());
        // verify vms
        Assert.assertEquals(151, entityTypeCounts.get(EntityType.VIRTUAL_MACHINE).intValue());
        // verify other entity types
        Assert.assertEquals(10, entityTypeCounts.get(EntityType.DATABASE).intValue());
        Assert.assertEquals(10, entityTypeCounts.get(EntityType.DATABASE_SERVER).intValue());
        Assert.assertEquals(27, entityTypeCounts.get(EntityType.LOAD_BALANCER).intValue());
        Assert.assertEquals(31, entityTypeCounts.get(EntityType.VIRTUAL_APPLICATION).intValue());
        Assert.assertEquals(207, entityTypeCounts.get(EntityType.APPLICATION).intValue());
        Assert.assertEquals(28, entityTypeCounts.get(EntityType.RESERVED_INSTANCE).intValue());
        Assert.assertEquals(3, entityTypeCounts.get(EntityType.CLOUD_SERVICE).intValue());

        // construct topology and group by entity type
        Map<Long, TopologyEntity.Builder> topology = stitchingContext.constructTopology();
        Map<EntityType, List<TopologyEntity.Builder>> topologyEntitiesByType = topology.values()
                .stream().collect(Collectors.groupingBy(builder ->
                        EntityType.forNumber(builder.getEntityType())));

        // verify there are 14 different entity types in new topology
        Assert.assertEquals(14, topologyEntitiesByType.size());

        // although the graph had storages, they should be gone from the topology map group the
        // topology by entity type. we expect NO storages in the resulting topology
        Assert.assertNull(topologyEntitiesByType.get(EntityType.STORAGE));
        // we do expect our storage tiers to be the same though
        Assert.assertEquals(7, topologyEntitiesByType.get(EntityType.STORAGE_TIER)
                .stream().filter(this::isValidStorageTier).count());
        // verify CT, 189 CTs
        Assert.assertEquals(146, topologyEntitiesByType.get(EntityType.COMPUTE_TIER)
                .stream().filter(this::isValidComputeTier).count());
        // verify DT, 43 DTs
        Assert.assertEquals(43, topologyEntitiesByType.get(EntityType.DATABASE_TIER)
                .stream().filter(this::isValidDatabaseTier).count());
        // verify CS
        Assert.assertEquals(3, topologyEntitiesByType.get(EntityType.CLOUD_SERVICE)
                .stream().filter(this::isValidCloudService).count());

        // verify AZs
        Assert.assertEquals(46, topologyEntitiesByType.get(EntityType.AVAILABILITY_ZONE).size());
        // verify Regions own AZs
        Assert.assertEquals(15, topologyEntitiesByType.get(EntityType.REGION).stream()
                .filter(this::isValidRegion).count());
        // verify all VM's are buying from both ST and CT and connected to AZ
        Assert.assertEquals(144, topologyEntitiesByType.get(EntityType.VIRTUAL_MACHINE)
                .stream().filter(this::isValidVM).count());

        Assert.assertEquals(10, topologyEntitiesByType.get(EntityType.DATABASE).stream()
                .filter(this::isValidDatabase).count());
        Assert.assertEquals(10, topologyEntitiesByType.get(EntityType.DATABASE_SERVER)
                .stream().filter(this::isValidDatabaseServer).count());
        Assert.assertEquals(3, topologyEntitiesByType.get(EntityType.BUSINESS_ACCOUNT)
                .stream().filter(builder -> isValidBusinessAccount(builder, topology)).count());

        // verify other entity types
        Assert.assertEquals(27, topologyEntitiesByType.get(EntityType.LOAD_BALANCER).size());
        Assert.assertEquals(31, topologyEntitiesByType.get(EntityType.VIRTUAL_APPLICATION).size());
        Assert.assertEquals(207, topologyEntitiesByType.get(EntityType.APPLICATION).size());
    }

    @Test
    public void testConvertTopologyForOneAzureTarget() throws Exception {
        // initialize target
        Mockito.when(targetStore.getAll()).thenReturn(Lists.newArrayList(azure_engineering));
        entityStore = new EntityStore(targetStore, identityProvider, Clock.systemUTC());
        profileUploader = new DiscoveredTemplateDeploymentProfileUploader(entityStore, templatesDeploymentProfileService);
        DiscoveryResponse discoveryResponse = readResponseFromFile(AZURE_ENGINEERING_FILE_PATH);
        entityStore.entitiesDiscovered(azureProbeId1, azureTargetId1, discoveryResponse.getEntityDTOList());
        profileUploader.setTargetsTemplateDeploymentProfile(azureTargetId1, discoveryResponse
                .getEntityProfileList(), discoveryResponse.getDeploymentProfileList());

        // construct StitchingContext
        StitchingContext stitchingContext = entityStore.constructStitchingContext(targetStore,
                profileUploader.getTargetToEntityProfilesMap());

        // verify counts of new cloud entities, e.g. regions, availability_zones, storage tiers and
        // compute tiers
        Map<EntityType, Integer> entityTypeCounts = stitchingContext.entityTypeCounts();
        Assert.assertEquals(13, entityTypeCounts.size());

        // verify that we have regions and AZ's, but no PM's or DC's
        Assert.assertEquals(30, entityTypeCounts.get(EntityType.REGION).intValue());
        Assert.assertEquals(30, entityTypeCounts.get(EntityType.AVAILABILITY_ZONE).intValue());
        Assert.assertFalse(entityTypeCounts.containsKey(EntityType.DATACENTER));
        Assert.assertFalse(entityTypeCounts.containsKey(EntityType.PHYSICAL_MACHINE));

        // verify one BusinessAccount
        Assert.assertEquals(1, entityTypeCounts.get(EntityType.BUSINESS_ACCOUNT).intValue());

        // verify that we are creating storage tiers
        // there are 4 different storage tiers:
        // managed_standard, managed_premium, unmanaged_standard, unmanaged_premium
        Assert.assertEquals(4, entityTypeCounts.get(EntityType.STORAGE_TIER).intValue());
        // verify that we are creating database tiers
        Assert.assertEquals(216, entityTypeCounts.get(EntityType.COMPUTE_TIER).intValue());
        // verify that we are creating compute tiers
        Assert.assertEquals(19, entityTypeCounts.get(EntityType.DATABASE_TIER).intValue());
        // we expect the storages to be removed in the topology map, first verify they are here.
        Assert.assertEquals(120, entityTypeCounts.get(EntityType.STORAGE).intValue());
        Assert.assertEquals(30, entityTypeCounts.get(EntityType.DISK_ARRAY).intValue());
        // verify vms
        Assert.assertEquals(44, entityTypeCounts.get(EntityType.VIRTUAL_MACHINE).intValue());
        // verify other entity types
        Assert.assertEquals(2, entityTypeCounts.get(EntityType.DATABASE).intValue());
        Assert.assertEquals(1, entityTypeCounts.get(EntityType.DATABASE_SERVER).intValue());
        Assert.assertEquals(46, entityTypeCounts.get(EntityType.APPLICATION).intValue());
        Assert.assertEquals(3, entityTypeCounts.get(EntityType.CLOUD_SERVICE).intValue());

        // construct topology and group by entity type
        Map<Long, TopologyEntity.Builder> topology = stitchingContext.constructTopology();
        Map<EntityType, List<TopologyEntity.Builder>> topologyEntitiesByType = topology.values()
                .stream().collect(Collectors.groupingBy(builder ->
                        EntityType.forNumber(builder.getEntityType())));

        // verify there are 9 different entity types left: Storage, AZ, DatabaseServer are removed
        Assert.assertEquals(9, topologyEntitiesByType.size());

        // verify AZ, Storage, DatabaseBServer, DiskArray not existing for Azure
        Assert.assertFalse(topologyEntitiesByType.containsKey(EntityType.AVAILABILITY_ZONE));
        Assert.assertFalse(topologyEntitiesByType.containsKey(EntityType.STORAGE));
        Assert.assertFalse(topologyEntitiesByType.containsKey(EntityType.DATABASE_SERVER));
        Assert.assertFalse(topologyEntitiesByType.containsKey(EntityType.DISK_ARRAY));

        // verify Regions no connected, no commodities sold or bought
        Assert.assertEquals(30, topologyEntitiesByType.get(EntityType.REGION).stream()
                .filter(this::isValidRegion).count());

        // verify storage tiers to be there
        Assert.assertEquals(4, topologyEntitiesByType.get(EntityType.STORAGE_TIER)
                .stream().filter(this::isValidStorageTier).count());

        // verify 216 CT
        Assert.assertEquals(216, topologyEntitiesByType.get(EntityType.COMPUTE_TIER)
                .stream().filter(this::isValidComputeTier).count());

        // verify 19 DT
        Assert.assertEquals(19, topologyEntitiesByType.get(EntityType.DATABASE_TIER)
                .stream().filter(this::isValidDatabaseTier).count());

        // verify 3 CS
        Assert.assertEquals(3, topologyEntitiesByType.get(EntityType.CLOUD_SERVICE)
                .stream().filter(this::isValidCloudService).count());

        // verify all VM's are buying from both ST and CT and connected to AZ
        Assert.assertEquals(44, topologyEntitiesByType.get(EntityType.VIRTUAL_MACHINE)
                .stream().filter(this::isValidVM).count());
        Assert.assertEquals(2, topologyEntitiesByType.get(EntityType.DATABASE).stream()
                .filter(this::isValidDatabase).count());
        Assert.assertEquals(1, topologyEntitiesByType.get(EntityType.BUSINESS_ACCOUNT)
                .stream().filter(builder -> isValidBusinessAccount(builder, topology)).count());

        // verify other entity types
        Assert.assertEquals(46, topologyEntitiesByType.get(EntityType.APPLICATION).size());
    }

    @Test
    public void testConvertTopologyForTwoAzureTargets() throws Exception {
        // initialize two Azure targets
        Mockito.when(targetStore.getAll()).thenReturn(Lists.newArrayList(azure_engineering, azure_productmgmt));
        entityStore = new EntityStore(targetStore, identityProvider, Clock.systemUTC());
        profileUploader = new DiscoveredTemplateDeploymentProfileUploader(entityStore, templatesDeploymentProfileService);

        DiscoveryResponse response1 = readResponseFromFile(AZURE_ENGINEERING_FILE_PATH);
        entityStore.entitiesDiscovered(azureProbeId1, azureTargetId1, response1.getEntityDTOList());
        profileUploader.setTargetsTemplateDeploymentProfile(azureTargetId1, response1
                .getEntityProfileList(), response1.getDeploymentProfileList());

        DiscoveryResponse response2 = readResponseFromFile(AZURE_PRODUCTMGMT_FILE_PATH);
        entityStore.entitiesDiscovered(azureProbeId2, azureTargetId2, response2.getEntityDTOList());
        profileUploader.setTargetsTemplateDeploymentProfile(azureTargetId2, response2
                .getEntityProfileList(), response2.getDeploymentProfileList());

        // construct StitchingContext
        StitchingContext stitchingContext = entityStore.constructStitchingContext(targetStore,
                profileUploader.getTargetToEntityProfilesMap());

        // verify counts of new cloud entities, e.g. regions, availability_zones, storage tiers and
        // compute tiers
        Map<EntityType, Integer> entityTypeCounts = stitchingContext.entityTypeCounts();
        Assert.assertEquals(13, entityTypeCounts.size());

        // verify that we have regions and AZ's, but no PM's or DC's
        Assert.assertEquals(60, entityTypeCounts.get(EntityType.REGION).intValue());
        Assert.assertEquals(60, entityTypeCounts.get(EntityType.AVAILABILITY_ZONE).intValue());
        Assert.assertFalse(entityTypeCounts.containsKey(EntityType.DATACENTER));
        Assert.assertFalse(entityTypeCounts.containsKey(EntityType.PHYSICAL_MACHINE));

        // verify one BusinessAccount
        Assert.assertEquals(2, entityTypeCounts.get(EntityType.BUSINESS_ACCOUNT).intValue());

        // verify that we are creating storage tiers
        // there are 4 different storage tiers:
        // managed_standard, managed_premium, unmanaged_standard, unmanaged_premium
        Assert.assertEquals(4, entityTypeCounts.get(EntityType.STORAGE_TIER).intValue());
        // verify that we are creating compute tiers
        Assert.assertEquals(216, entityTypeCounts.get(EntityType.COMPUTE_TIER).intValue());
        // verify that we are creating database tiers
        Assert.assertEquals(19, entityTypeCounts.get(EntityType.DATABASE_TIER).intValue());
        // we expect the storages to be removed in the topology map, first verify they are here.
        Assert.assertEquals(240, entityTypeCounts.get(EntityType.STORAGE).intValue());
        Assert.assertEquals(60, entityTypeCounts.get(EntityType.DISK_ARRAY).intValue());
        // verify vms
        Assert.assertEquals(50, entityTypeCounts.get(EntityType.VIRTUAL_MACHINE).intValue());
        // verify other entity types
        Assert.assertEquals(2, entityTypeCounts.get(EntityType.DATABASE).intValue());
        Assert.assertEquals(1, entityTypeCounts.get(EntityType.DATABASE_SERVER).intValue());
        Assert.assertEquals(52, entityTypeCounts.get(EntityType.APPLICATION).intValue());
        Assert.assertEquals(3, entityTypeCounts.get(EntityType.CLOUD_SERVICE).intValue());

        // construct topology and group by entity type
        Map<Long, TopologyEntity.Builder> topology = stitchingContext.constructTopology();
        Map<EntityType, List<TopologyEntity.Builder>> topologyEntitiesByType = topology.values()
                .stream().collect(Collectors.groupingBy(builder ->
                        EntityType.forNumber(builder.getEntityType())));

        // verify there are 9 different entity types left: Storage, AZ, DatabaseServer are removed
        Assert.assertEquals(9, topologyEntitiesByType.size());

        // verify AZ, Storage, DatabaseBServer, DiskArray not existing for Azure
        Assert.assertFalse(topologyEntitiesByType.containsKey(EntityType.AVAILABILITY_ZONE));
        Assert.assertFalse(topologyEntitiesByType.containsKey(EntityType.STORAGE));
        Assert.assertFalse(topologyEntitiesByType.containsKey(EntityType.DATABASE_SERVER));
        Assert.assertFalse(topologyEntitiesByType.containsKey(EntityType.DISK_ARRAY));

        // verify Regions no connected, no commodities sold or bought
        Assert.assertEquals(30, topologyEntitiesByType.get(EntityType.REGION).stream()
                .filter(this::isValidRegion).count());

        // verify storage tiers to be there
        Assert.assertEquals(4, topologyEntitiesByType.get(EntityType.STORAGE_TIER)
                .stream().filter(this::isValidStorageTier).count());

        // verify 216 CT
        Assert.assertEquals(216, topologyEntitiesByType.get(EntityType.COMPUTE_TIER)
                .stream().filter(this::isValidComputeTier).count());

        // verify 19 DT
        Assert.assertEquals(19, topologyEntitiesByType.get(EntityType.DATABASE_TIER)
                .stream().filter(this::isValidDatabaseTier).count());

        // verify 3 CS
        Assert.assertEquals(3, topologyEntitiesByType.get(EntityType.CLOUD_SERVICE)
                .stream().filter(this::isValidCloudService).count());

        // verify all VM's are buying from both ST and CT and connected to AZ
        Assert.assertEquals(50, topologyEntitiesByType.get(EntityType.VIRTUAL_MACHINE)
                .stream().filter(this::isValidVM).count());
        Assert.assertEquals(2, topologyEntitiesByType.get(EntityType.DATABASE).stream()
                .filter(this::isValidDatabase).count());
        Assert.assertEquals(2, topologyEntitiesByType.get(EntityType.BUSINESS_ACCOUNT)
                .stream().filter(builder -> isValidBusinessAccount(builder, topology)).count());

        // verify other entity types
        Assert.assertEquals(52, topologyEntitiesByType.get(EntityType.APPLICATION).size());
    }

    @Test
    public void testConvertTopologyForTwoAWSTargetsAndTwoAzureTargets() throws Exception {
        // initialize two AWS targets and two Azure targets
        Mockito.when(targetStore.getAll()).thenReturn(Lists.newArrayList(
                aws_engineering, aws_adveng, azure_engineering, azure_productmgmt));
        entityStore = new EntityStore(targetStore, identityProvider, Clock.systemUTC());
        profileUploader = new DiscoveredTemplateDeploymentProfileUploader(entityStore, templatesDeploymentProfileService);

        DiscoveryResponse response1 = readResponseFromFile(AWS_ENGINEERING_FILE_PATH);
        entityStore.entitiesDiscovered(awsProbeId1, awsTargetId1, response1.getEntityDTOList());
        profileUploader.setTargetsTemplateDeploymentProfile(awsTargetId1, response1
                .getEntityProfileList(), response1.getDeploymentProfileList());

        DiscoveryResponse response2 = readResponseFromFile(AWS_ADVENG_FILE_PATH);
        entityStore.entitiesDiscovered(awsProbeId2, awsTargetId2, response2.getEntityDTOList());
        profileUploader.setTargetsTemplateDeploymentProfile(awsTargetId1, response2
                .getEntityProfileList(), response2.getDeploymentProfileList());

        DiscoveryResponse response3 = readResponseFromFile(AZURE_ENGINEERING_FILE_PATH);
        entityStore.entitiesDiscovered(azureProbeId1, azureTargetId1, response3.getEntityDTOList());
        profileUploader.setTargetsTemplateDeploymentProfile(azureTargetId1, response3
                .getEntityProfileList(), response3.getDeploymentProfileList());

        DiscoveryResponse response4 = readResponseFromFile(AZURE_PRODUCTMGMT_FILE_PATH);
        entityStore.entitiesDiscovered(azureProbeId2, azureTargetId2, response4.getEntityDTOList());
        profileUploader.setTargetsTemplateDeploymentProfile(azureTargetId2, response4
                .getEntityProfileList(), response4.getDeploymentProfileList());

        // construct StitchingContext
        StitchingContext stitchingContext = entityStore.constructStitchingContext(targetStore,
                profileUploader.getTargetToEntityProfilesMap());

        // verify counts of new cloud entities, e.g. regions, availability_zones, storage tiers and
        // compute tiers
        Map<EntityType, Integer> entityTypeCounts = stitchingContext.entityTypeCounts();
        Assert.assertEquals(16, entityTypeCounts.size());

        // verify that we have regions and AZ's, but no PM's or DC's
        Assert.assertEquals(90, entityTypeCounts.get(EntityType.REGION).intValue());
        Assert.assertEquals(148, entityTypeCounts.get(EntityType.AVAILABILITY_ZONE).intValue());
        Assert.assertFalse(entityTypeCounts.containsKey(EntityType.DATACENTER));
        Assert.assertFalse(entityTypeCounts.containsKey(EntityType.PHYSICAL_MACHINE));

        // verify one BusinessAccount
        Assert.assertEquals(6, entityTypeCounts.get(EntityType.BUSINESS_ACCOUNT).intValue());

        // verify that we are creating storage tiers
        // there are 4 different storage tiers:
        // managed_standard, managed_premium, unmanaged_standard, unmanaged_premium
        Assert.assertEquals(11, entityTypeCounts.get(EntityType.STORAGE_TIER).intValue());
        // verify that we are creating compute tiers
        Assert.assertEquals(362, entityTypeCounts.get(EntityType.COMPUTE_TIER).intValue());
        // verify that we are creating database tiers
        Assert.assertEquals(62, entityTypeCounts.get(EntityType.DATABASE_TIER).intValue());
        // we expect the storages to be removed in the topology map, first verify they are here.
        Assert.assertEquals(856, entityTypeCounts.get(EntityType.STORAGE).intValue());
        Assert.assertEquals(148, entityTypeCounts.get(EntityType.DISK_ARRAY).intValue());
        // verify vms
        Assert.assertEquals(201, entityTypeCounts.get(EntityType.VIRTUAL_MACHINE).intValue());
        // verify other entity types
        Assert.assertEquals(12, entityTypeCounts.get(EntityType.DATABASE).intValue());
        Assert.assertEquals(11, entityTypeCounts.get(EntityType.DATABASE_SERVER).intValue());
        Assert.assertEquals(27, entityTypeCounts.get(EntityType.LOAD_BALANCER).intValue());
        Assert.assertEquals(31, entityTypeCounts.get(EntityType.VIRTUAL_APPLICATION).intValue());
        Assert.assertEquals(259, entityTypeCounts.get(EntityType.APPLICATION).intValue());
        Assert.assertEquals(28, entityTypeCounts.get(EntityType.RESERVED_INSTANCE).intValue());
        Assert.assertEquals(6, entityTypeCounts.get(EntityType.CLOUD_SERVICE).intValue());

        // construct topology and group by entity type
        Map<Long, TopologyEntity.Builder> topology = stitchingContext.constructTopology();
        Map<EntityType, List<TopologyEntity.Builder>> topologyEntitiesByType = topology.values()
                .stream().collect(Collectors.groupingBy(builder ->
                        EntityType.forNumber(builder.getEntityType())));

        // verify there are 14 different entity types left: Storage, DiskArray are removed
        Assert.assertEquals(14, topologyEntitiesByType.size());

        // although the graph had storages, they should be gone from the topology map group the
        // topology by entity type. we expect NO storages in the resulting topology
        Assert.assertNull(topologyEntitiesByType.get(EntityType.STORAGE));
        // we do expect our storage tiers to be the same though
        Assert.assertEquals(11, topologyEntitiesByType.get(EntityType.STORAGE_TIER)
                .stream().filter(this::isValidStorageTier).count());
        // verify CT
        Assert.assertEquals(362, topologyEntitiesByType.get(EntityType.COMPUTE_TIER)
                .stream().filter(this::isValidComputeTier).count());
        // verify DT
        Assert.assertEquals(62, topologyEntitiesByType.get(EntityType.DATABASE_TIER)
                .stream().filter(this::isValidDatabaseTier).count());
        // verify 6 CS
        Assert.assertEquals(6, topologyEntitiesByType.get(EntityType.CLOUD_SERVICE)
                .stream().filter(this::isValidCloudService).count());
        // verify AZs
        Assert.assertEquals(46, topologyEntitiesByType.get(EntityType.AVAILABILITY_ZONE).size());
        // verify Regions own AZs
        Assert.assertEquals(45, topologyEntitiesByType.get(EntityType.REGION).stream()
                .filter(this::isValidRegion).count());
        // verify all VM's are buying from both ST and CT and connected to AZ
        Assert.assertEquals(194, topologyEntitiesByType.get(EntityType.VIRTUAL_MACHINE)
                .stream().filter(this::isValidVM).count());

        Assert.assertEquals(12, topologyEntitiesByType.get(EntityType.DATABASE).stream()
                .filter(this::isValidDatabase).count());
        Assert.assertEquals(10, topologyEntitiesByType.get(EntityType.DATABASE_SERVER)
                .stream().filter(this::isValidDatabaseServer).count());
        Assert.assertEquals(5, topologyEntitiesByType.get(EntityType.BUSINESS_ACCOUNT)
                .stream().filter(builder -> isValidBusinessAccount(builder, topology)).count());

        // verify other entity types
        Assert.assertEquals(27, topologyEntitiesByType.get(EntityType.LOAD_BALANCER).size());
        Assert.assertEquals(31, topologyEntitiesByType.get(EntityType.VIRTUAL_APPLICATION).size());
        Assert.assertEquals(259, topologyEntitiesByType.get(EntityType.APPLICATION).size());
    }

    private boolean isValidRegion(TopologyEntity.Builder builder) {
        TopologyEntityDTO.Builder region = builder.getEntityBuilder();

        if (isFromAWS(region)) {
            assertTrue(region.getConnectedEntityListCount() > 0);

            Set<EntityType> connectedEntityTypes = region.getConnectedEntityListList().stream()
                    .map(connectedEntity -> EntityType.forNumber(connectedEntity.getConnectedEntityType()))
                    .collect(Collectors.toSet());

            assertTrue(connectedEntityTypes.size() == 1);
            assertTrue(connectedEntityTypes.contains(EntityType.AVAILABILITY_ZONE));
        } else {
            assertEquals(0, region.getConnectedEntityListCount());
        }

        assertEquals(0, region.getCommoditySoldListCount());
        assertEquals(0, region.getCommoditiesBoughtFromProvidersCount());

        return true;
    }

    private boolean isValidVM(TopologyEntity.Builder teBuilder) {
        // validate that the VM is connected to a CT and ST
        boolean isBuyingCT = false;
        boolean isBuyingST = false;
        for (CommoditiesBoughtFromProvider commsBought
                : teBuilder.getEntityBuilder().getCommoditiesBoughtFromProvidersList()) {
            int providerEntityType = commsBought.getProviderEntityType();
            if (providerEntityType == EntityType.STORAGE_TIER.getNumber()) {
                isBuyingST = true;
            } else if (providerEntityType == EntityType.COMPUTE_TIER.getNumber()) {
                isBuyingCT = true;
            }
        }

        assertTrue(isBuyingCT);
        assertTrue(isBuyingST);

        // validate connections
        Map<EntityType, List<ConnectedEntity>> connectedEntitiesByType = teBuilder
                .getEntityBuilder().getConnectedEntityListList().stream()
                .collect(Collectors.groupingBy(t -> EntityType.forNumber(t.getConnectedEntityType())));

        assertEquals(1, connectedEntitiesByType.size());
        assertEquals(1, connectedEntitiesByType.values().size());

        if (isFromAWS(teBuilder.getEntityBuilder())) {
            assertEquals(EntityType.AVAILABILITY_ZONE, connectedEntitiesByType.keySet().iterator().next());
        } else {
            assertEquals(EntityType.REGION, connectedEntitiesByType.keySet().iterator().next());
        }

        return true;
    }

    /**
     * Check if this ComputeTier has required relationship and commodities ready after cloud
     * topology conversion.
     */
    private boolean isValidComputeTier(TopologyEntity.Builder ct) {
        TopologyEntityDTO.Builder builder = ct.getEntityBuilder();

        assertTrue(builder.getConnectedEntityListCount() > 0);

        Set<EntityType> connectedEntityTypes = builder.getConnectedEntityListList().stream()
                .map(connectedEntity -> EntityType.forNumber(connectedEntity.getConnectedEntityType()))
                .collect(Collectors.toSet());

        // connected to region
        assertTrue(connectedEntityTypes.contains(EntityType.REGION));

        assertTrue(ct.getDisplayName(), connectedEntityTypes.contains(EntityType.STORAGE_TIER));
        assertEquals(2, connectedEntityTypes.size());

        // no bought commodities
        assertEquals(0, builder.getCommoditiesBoughtFromProvidersCount());
        // has sold commodities
        assertTrue(builder.getCommoditySoldListCount() > 0);

        return true;
    }

    /**
     * Check if this ComputeTier has required relationship and commodities ready after cloud
     * topology conversion.
     */
    private boolean isValidDatabaseTier(TopologyEntity.Builder dt) {
        TopologyEntityDTO.Builder builder = dt.getEntityBuilder();

        assertTrue(builder.getConnectedEntityListCount() > 0);

        Set<EntityType> connectedEntityTypes = builder.getConnectedEntityListList().stream()
                .map(connectedEntity -> EntityType.forNumber(connectedEntity.getConnectedEntityType()))
                .collect(Collectors.toSet());

        // connected to region
        assertTrue(connectedEntityTypes.contains(EntityType.REGION));
        assertEquals(1, connectedEntityTypes.size());

        // no bought commodities
        assertEquals(0, builder.getCommoditiesBoughtFromProvidersCount());
        // has sold commodities
        assertTrue(builder.getCommoditySoldListCount() > 0);

        return true;
    }

    private boolean isValidCloudService(TopologyEntity.Builder builder) {
        TopologyEntityDTO.Builder cs = builder.getEntityBuilder();

        assertTrue(cs.getConnectedEntityListCount() > 0);

        Set<EntityType> connectedEntityTypes = cs.getConnectedEntityListList().stream()
                .map(connectedEntity -> EntityType.forNumber(connectedEntity.getConnectedEntityType()))
                .collect(Collectors.toSet());

        assertEquals(1, connectedEntityTypes.size());

        // check owns
        if (CloudTopologyConverter.AWS_EC2.equals(cs.getDisplayName()) ||
                CloudTopologyConverter.AZURE_VIRTUAL_MACHINES.equals(cs.getDisplayName())) {
            assertTrue(connectedEntityTypes.contains(EntityType.COMPUTE_TIER));
        } else if (CloudTopologyConverter.AWS_RDS.equals(cs.getDisplayName()) ||
                CloudTopologyConverter.AZURE_DATA_SERVICES.equals(cs.getDisplayName())) {
            assertTrue(connectedEntityTypes.contains(EntityType.DATABASE_TIER));
        } else if (CloudTopologyConverter.AWS_EBS.equals(cs.getDisplayName()) ||
                CloudTopologyConverter.AZURE_STORAGE.equals(cs.getDisplayName())) {
            assertTrue(connectedEntityTypes.contains(EntityType.STORAGE_TIER));
        }

        // no bought commodities
        assertEquals(0, cs.getCommoditiesBoughtFromProvidersCount());
        // no sold commodities
        assertEquals(0, cs.getCommoditySoldListCount());

        return true;
    }

    private boolean isValidStorageTier(TopologyEntity.Builder st) {
        TopologyEntityDTO.Builder builder = st.getEntityBuilder();

        assertTrue(builder.getConnectedEntityListCount() > 0);

        Set<EntityType> connectedEntityTypes = builder.getConnectedEntityListList().stream()
                .map(connectedEntity -> EntityType.forNumber(connectedEntity.getConnectedEntityType()))
                .collect(Collectors.toSet());

        assertTrue(connectedEntityTypes.contains(EntityType.REGION));

        // check bought commodities
        assertEquals(0, builder.getCommoditiesBoughtFromProvidersCount());
        // check sold commodities
        assertTrue(builder.getCommoditySoldListCount() > 0);

        return true;
    }

    private boolean isValidDatabase(TopologyEntity.Builder dbBuilder) {
        TopologyEntityDTO.Builder db = dbBuilder.getEntityBuilder();
        boolean isAWS = isFromAWS(db);

        assertTrue(db.getConnectedEntityListCount() > 0);

        if (isAWS) {
            assertEquals(EntityType.AVAILABILITY_ZONE.getNumber(), db.getConnectedEntityList(0)
                    .getConnectedEntityType());
        } else {
            assertEquals(EntityType.REGION.getNumber(), db.getConnectedEntityList(0).getConnectedEntityType());
        }

        List<CommoditiesBoughtFromProvider> cb = dbBuilder.getEntityBuilder()
                .getCommoditiesBoughtFromProvidersList();

        assertEquals(1, cb.size());

        if (isAWS) {
            assertEquals(EntityType.DATABASE_SERVER.getNumber(), cb.get(0).getProviderEntityType());
        } else {
            assertEquals(EntityType.DATABASE_TIER.getNumber(), cb.get(0).getProviderEntityType());
        }

        return true;
    }

    private boolean isValidDatabaseServer(TopologyEntity.Builder builder) {
        TopologyEntityDTO.Builder dbServer = builder.getEntityBuilder();

        assertEquals(1, dbServer.getConnectedEntityListCount());
        assertEquals(EntityType.AVAILABILITY_ZONE.getNumber(),
                dbServer.getConnectedEntityList(0).getConnectedEntityType());

        if (builder.getEntityBuilder().getCommoditiesBoughtFromProvidersList().size() != 1) {
            return false;
        }

        List<CommoditiesBoughtFromProvider> cb = builder.getEntityBuilder()
                .getCommoditiesBoughtFromProvidersList();

        assertEquals(1, cb.size());
        assertEquals(EntityType.DATABASE_TIER.getNumber(), cb.get(0).getProviderEntityType());

        // check sold commodities

        return true;
    }

    private boolean isValidBusinessAccount(TopologyEntity.Builder builder,
            Map<Long, TopologyEntity.Builder> topology) {
        TopologyEntityDTO.Builder ba = builder.getEntityBuilder();
        if (isFromAWS(ba)) {
            // check a specific master account
            if (ba.getDisplayName().equals("Yuri Rabover")) {
                // master account connected all
                assertEquals(388, ba.getConnectedEntityListCount());

                Map<EntityType, List<TopologyEntity.Builder>> connectedEntities = ba
                        .getConnectedEntityListList()
                        .stream()
                        .map(connectedEntity -> topology.get(connectedEntity.getConnectedEntityId()))
                        .collect(Collectors.groupingBy(b -> EntityType.forNumber(b.getEntityType())));

                assertEquals(7, connectedEntities.size());
                assertEquals(2, connectedEntities.get(EntityType.BUSINESS_ACCOUNT).size());
                assertEquals(129, connectedEntities.get(EntityType.VIRTUAL_MACHINE).size());
                assertEquals(24, connectedEntities.get(EntityType.LOAD_BALANCER).size());
                assertEquals(28, connectedEntities.get(EntityType.VIRTUAL_APPLICATION).size());
                assertEquals(187, connectedEntities.get(EntityType.APPLICATION).size());
                assertEquals(9, connectedEntities.get(EntityType.DATABASE).size());
                assertEquals(9, connectedEntities.get(EntityType.DATABASE_SERVER).size());
            } else {
                //todo: more accounts
                //assertEquals(0, ba.getConnectedEntityListCount());
            }
        } else {
            Map<EntityType, List<TopologyEntity.Builder>> connectedEntities = ba.getConnectedEntityListList()
                    .stream()
                    .map(connectedEntity -> topology.get(connectedEntity.getConnectedEntityId()))
                    .collect(Collectors.groupingBy(b -> EntityType.forNumber(b.getEntityType())));

            // azure
            if (ba.getDisplayName().equals("Pay-As-You-Go - Development")) {
                assertEquals(92, ba.getConnectedEntityListCount());
                assertEquals(3, connectedEntities.size());
                assertEquals(2, connectedEntities.get(EntityType.DATABASE).size());
                assertEquals(44, connectedEntities.get(EntityType.VIRTUAL_MACHINE).size());
                assertEquals(46, connectedEntities.get(EntityType.APPLICATION).size());
            } else if (ba.getDisplayName().equals("Pay-As-You-Go - Product Management")) {
                assertEquals(12, ba.getConnectedEntityListCount());
                assertEquals(2, connectedEntities.size());
                assertEquals(6, connectedEntities.get(EntityType.VIRTUAL_MACHINE).size());
                assertEquals(6, connectedEntities.get(EntityType.APPLICATION).size());
            }
        }

        // no bought commodities
        assertEquals(0, ba.getCommoditiesBoughtFromProvidersCount());
        // no sold commodities
        assertEquals(0, ba.getCommoditySoldListCount());

        return true;
    }

    /**
     * Check if the topology entity comes from AWS or Azure.
     */
    private boolean isFromAWS(TopologyEntityDTO.Builder builder) {
        List<Long> targetIds = builder.getOrigin().getDiscoveryOrigin().getDiscoveringTargetIdsList();
        return targetIds.contains(awsTargetId1) || targetIds.contains(awsTargetId2);
    }

    /**
     * Create a list of EntityIdentityMetadata for cloud probes.
     */
    private List<EntityIdentityMetadata> createIdentityMetadata(List<EntityType> entityTypes) {
        List<EntityIdentityMetadata> metadata = entityTypes.stream().map(entityType -> EntityIdentityMetadata.newBuilder()
                .setEntityType(entityType)
                .addNonVolatileProperties(PropertyMetadata.newBuilder().setName("id"))
                .build()
            ).collect(Collectors.toList());
        return metadata;
    }

    /**
     * Parses a file and constructs a discovery response object.
     *
     * @param filePath path of the file to be parsed.
     * @return discovery response; response with error if file cannot be parsed.
     */
    public static DiscoveryResponse readResponseFromFile(@Nonnull String filePath) {
        // try to parse as binary
        try (final InputStream fis = getInputStream(filePath)) {
            return DiscoveryResponse.parseFrom(fis);
        } catch (InvalidProtocolBufferException e) {
            // failed to parse as binary; fall through to text parsing
        } catch (IOException p) {
            final String message = "Error reading file " + filePath;
            return errorResponse(message);
        }

        // try to parse as text
        try (final InputStream fis = getInputStream(filePath)) {
            final String drText = IOUtils.toString(fis, Charset.defaultCharset());
            final DiscoveryResponse.Builder builder = DiscoveryResponse.newBuilder();
            TextFormat.getParser().merge(drText, builder);
            return builder.build();
        } catch (IOException p) {
            final String message = "Error reading file " + filePath;
            return errorResponse(message);
        }
    }

    // get an input stream, creating a zipinputstream for .zip files
    private static InputStream getInputStream(@Nonnull String filePath) throws IOException {
        File file = new File(filePath);

        FileInputStream fis = new FileInputStream(file);
        // if this is a zip file, get the input stream as a zip.
        if (filePath.endsWith("zip")) {
            // We assume a single entry in the zip file.
            ZipInputStream zis = new ZipInputStream(fis);
            zis.getNextEntry();
            return zis;
        }
        return fis;
    }

    /**
     * Returns a discovery response with an error message.
     *
     * @param message error message.
     * @return discovery response containing error message.
     */
    private static DiscoveryResponse errorResponse(String message) {
        return DiscoveryResponse.newBuilder().addErrorDTO(error(message)).build();
    }

    /**
     * Constructs an ErrorDTO object.
     *
     * @param message error message.
     * @return {@link com.vmturbo.platform.common.dto.Discovery.ErrorDTO} object.
     */
    private static Discovery.ErrorDTO error(String message) {
        return Discovery.ErrorDTO.newBuilder().setDescription(message).
                setSeverity(Discovery.ErrorDTO.ErrorSeverity.CRITICAL).build();
    }

    /**
     * Helper function to write all TopologyEntityDTOs to a file for easier verification.
     *
     * @param topology
     * @param fileName
     */
    private void writeTopologyToFile(Map<Long, TopologyEntity.Builder> topology, String fileName) {
        Map<EntityType, List<TopologyEntity.Builder>> topologyEntitiesByType = topology.values()
                .stream().collect(Collectors.groupingBy(b -> EntityType.forNumber(b.getEntityType())));

        try {
            FileWriter fileWriter = new FileWriter(fileName);
            PrintWriter printWriter = new PrintWriter(fileWriter);
            topologyEntitiesByType.forEach((entityType, list) ->
                list.forEach(entity -> {
                    printWriter.println(entityType);
                    printWriter.println(entity.getEntityBuilder().toString());
                })
            );
            printWriter.close();
        } catch (Exception e) {
            logger.error("Error while writing topology to file", e);
        }
    }
}
