package com.vmturbo.topology.processor.entity;

import static com.vmturbo.platform.common.builders.CommodityBuilders.storageAmount;
import static com.vmturbo.platform.common.builders.CommodityBuilders.vCpuMHz;
import static com.vmturbo.platform.common.builders.EntityBuilders.physicalMachine;
import static com.vmturbo.platform.common.builders.EntityBuilders.storage;
import static com.vmturbo.platform.common.builders.EntityBuilders.virtualMachine;
import static com.vmturbo.topology.processor.entity.EntityStore.TARGET_COUNT_GAUGE;
import static junitparams.JUnitParamsRunner.$;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealth;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealthSubCategory;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntitiesWithNewState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.identity.exceptions.IdentityServiceException;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.AutomationLevel;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.PhysicalMachineData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityIdentifyingPropertyValues;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.proactivesupport.DataMetricGauge.GaugeData;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.test.utils.FeatureFlagTestRule;
import com.vmturbo.topology.processor.api.server.TopologyProcessorNotificationSender;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.rpc.TargetHealthRetriever;
import com.vmturbo.topology.processor.staledata.StalenessInformationProvider;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.stitching.TopologyStitchingGraph;
import com.vmturbo.topology.processor.targets.DuplicateTargetException;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.targets.TargetStoreException;
import com.vmturbo.topology.processor.targets.TargetStoreListener;

import common.HealthCheck.HealthState;

/**
 * Test the {@link EntityStore} methods.
 */
@RunWith(JUnitParamsRunner.class)
public class EntityStoreTest {
    /**
     * Rule to manage enablements via a mutable store.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule(FeatureFlags.DELAYED_DATA_HANDLING);

    private final long targetId = 1234;

    private final long target2Id = 9137L;

    private final Target target = mock(Target.class);

    private final Target target2 = mock(Target.class);

    private final String targetDisplayName = "Target1";

    private final String target2DisplayName = "Target2";

    private final TargetStore targetStore = Mockito.mock(TargetStore.class);

    private final ProbeInfo probeInfoHypervisorBar = ProbeInfo.newBuilder()
            .setProbeCategory(ProbeCategory.HYPERVISOR.getCategory())
            .setProbeType(SDKProbeType.HYPERV.name())
            .build();

    private final IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);

    private final TopologyProcessorNotificationSender sender = Mockito.mock(TopologyProcessorNotificationSender.class);

    private final Target firstTarget = mock(Target.class);

    private final Target duplicateTarget = mock(Target.class);

    private final Target secondDuplicateTarget = mock(Target.class);

    private final long firstTargetId = 1111L;

    private final long duplicateTargetId = 2222L;

    private final long secondDuplicateTargetId = 3333L;

    private final String firstTargetDisplayName = "firstTarget";

    private final String duplicateTargetDisplayName = "duplicateTarget";

    private final String secondDuplicateTargetDisplayName = "duplicateTarget2";

    private final ProbeInfo probeInfoHypervisor = ProbeInfo.newBuilder()
            .setProbeCategory(ProbeCategory.HYPERVISOR.getCategory())
            .setProbeType("foo")
            .build();

    private EntityStore entityStore = spy(new EntityStore(targetStore, identityProvider, 0.3F, true,
                    Collections.singletonList(sender), Clock.systemUTC(), Collections.emptySet(),
            true));

    private final TargetHealthRetriever targetHealthRetriever = mock(TargetHealthRetriever.class);

    /**
     * Expected exception rule.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Captor
    private ArgumentCaptor<Supplier<Set<Long>>> initializeStaleOidManagerArgumentCaptor;

    Object[] generateTestData() {
        return $($(false), $(true));
    }

    /**
     * Setup the targets.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        TARGET_COUNT_GAUGE.resetForTesting();
        when(target.getId()).thenReturn(targetId);
        when(target.getDisplayName()).thenReturn(targetDisplayName);
        when(target.toString()).thenReturn(targetDisplayName);
        when(target2.getId()).thenReturn(target2Id);
        when(target2.getDisplayName()).thenReturn(target2DisplayName);
        when(target2.toString()).thenReturn(target2DisplayName);
        when(targetStore.getTarget(targetId)).thenReturn(Optional.of(target));
        when(target.getProbeInfo()).thenReturn(probeInfoHypervisorBar);
        when(targetStore.getTarget(target2Id)).thenReturn(Optional.of(target2));
        when(target2.getProbeInfo()).thenReturn(probeInfoHypervisorBar);
        when(targetStore.getTargetDisplayName(targetId)).thenReturn(Optional.of(targetDisplayName));
        when(targetStore.getTargetDisplayName(target2Id)).thenReturn(Optional.of(target2DisplayName));
        when(targetStore.getAll()).thenReturn(ImmutableList.of(target, target2));
        Mockito.when(targetStore.getProbeCategoryForTarget(Mockito.anyLong()))
                        .thenReturn(Optional.of(ProbeCategory.HYPERVISOR));
        doNothing().when(identityProvider).initializeStaleOidManager(initializeStaleOidManagerArgumentCaptor.capture());
        entityStore.setTargetHealthRetriever(targetHealthRetriever);
    }

    /**
     * Test querying entities added to the repository.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testEntitiesDiscovered() throws Exception {
        final String id1 = "en-hypervisorTarget";
        final EntityDTO entity1 = EntityDTO.newBuilder().setEntityType(EntityType.VIRTUAL_MACHINE)
                .setId(id1).build();
        Map<Long, EntityDTO> entitiesMap = ImmutableMap.of(1L, entity1);

        addEntities(entitiesMap);

        Assert.assertTrue(entityStore.getTargetEntityIdMap(targetId).isPresent());
        Assert.assertTrue(entityStore.getEntity(1L).isPresent());
        assertEquals(entitiesMap, entityStore.discoveredByTarget(targetId));
    }

    /**
     * Test that {@link IdentityProvider#getIdsFromIdentifyingPropertiesValues(long, List)} is invoked
     * when {@link EntityStore#entityIdentifyingPropertyValuesDiscovered(long, long, List)}
     * is invoked with a non-empty list of {@link EntityIdentifyingPropertyValues}.
     *
     * @throws IdentityServiceException if there is an error in the Identity Service.
     */
    @Test
    public void testEntityIdentifyingPropertyValuesDiscovered() throws IdentityServiceException {
        final String localId = "vm-123";
        final EntityIdentifyingPropertyValues vm1 = createEntityIdentifyingPropertyValues(localId);
        final List<EntityIdentifyingPropertyValues> identifyingPropertyValues = Collections.singletonList(vm1);
        final long probeId = 1L;
        final long entityOid = 123L;
        Mockito.when(identityProvider.getIdsFromIdentifyingPropertiesValues(probeId, identifyingPropertyValues))
                .thenReturn(Collections.singletonMap(entityOid, vm1));
        final long targetId = 2L;
        entityStore.entityIdentifyingPropertyValuesDiscovered(probeId, targetId, identifyingPropertyValues);
        final StitchingContext stitchingContext = entityStore.constructStitchingContext();
        Assert.assertEquals(Collections.singletonMap(localId, entityOid),
            stitchingContext.getTargetEntityLocalIdToOid(targetId));
    }

    /**
     * Test that when {@link EntityStore#entityIdentifyingPropertyValuesDiscovered(long, long, List)} is invoked twice
     * with different inputs for the same target id, then {@link StitchingContext#getTargetEntityLocalIdToOid(long)}
     * returns 2 entries.
     *
     * @throws IdentityServiceException if there is an error in the Identity Service.
     */
    @Test
    public void testIncrementalDiscoveryEntityIdentifyingPropValues() throws IdentityServiceException {
        final String localId = "vm-123";
        final EntityIdentifyingPropertyValues vm1 = createEntityIdentifyingPropertyValues(localId);
        final List<EntityIdentifyingPropertyValues> identifyingPropertyValues = Collections.singletonList(vm1);
        final long probeId = 1L;
        final long entityOid = 123L;
        Mockito.when(identityProvider.getIdsFromIdentifyingPropertiesValues(probeId, identifyingPropertyValues))
            .thenReturn(Collections.singletonMap(entityOid, vm1));
        final long targetId = 2L;
        entityStore.entityIdentifyingPropertyValuesDiscovered(probeId, targetId, identifyingPropertyValues);
        final String localId2 = "vm-456";
        final long entityOid2 = 456L;
        final EntityIdentifyingPropertyValues vm2 = createEntityIdentifyingPropertyValues(localId2);
        final List<EntityIdentifyingPropertyValues> identifyingPropertyValues2 = Collections.singletonList(vm2);
        Mockito.when(identityProvider.getIdsFromIdentifyingPropertiesValues(probeId, Collections.singletonList(vm2)))
            .thenReturn(Collections.singletonMap(entityOid2, vm2));
        entityStore.entityIdentifyingPropertyValuesDiscovered(probeId, targetId, identifyingPropertyValues2);
        final StitchingContext stitchingContext2 = entityStore.constructStitchingContext();
        Assert.assertEquals(ImmutableMap.of(localId, entityOid, localId2, entityOid2),
            stitchingContext2.getTargetEntityLocalIdToOid(targetId));
    }

    private EntityIdentifyingPropertyValues createEntityIdentifyingPropertyValues(final String entityId) {
        return EntityIdentifyingPropertyValues.newBuilder()
            .setEntityId(entityId)
            .putIdentifyingPropertyValues("id", entityId)
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .build();
    }

    /**
     * Test that when an empty {@link EntityIdentifyingPropertyValues} list is input to
     * {@link EntityStore#entityIdentifyingPropertyValuesDiscovered(long, long, List)} for a given target id,
     * {@link StitchingContext#getTargetEntityLocalIdToOid(long)} (long)} (long)}
     * returns an empty map for the
     * target id.
     *
     * @throws IdentityServiceException if there is an error in the Identity Service.
     */
    @Test
    public void testEmptyIdentityList() throws IdentityServiceException {
        final long probeId = 1L;
        final long targetId = 2L;
        entityStore.entityIdentifyingPropertyValuesDiscovered(probeId, targetId, Collections.emptyList());
        final StitchingContext stitchingContext = entityStore.constructStitchingContext();
        Assert.assertTrue(stitchingContext.getTargetEntityLocalIdToOid(targetId)
            .isEmpty());
    }

    /**
     * Test that {@link EntityStore#entityIdentifyingPropertyValuesDiscovered(long, long, List)}
     * throws {@link IdentityServiceException} when
     * {@link IdentityProvider#getIdsFromIdentifyingPropertiesValues(long, List)} throws
     * {@link IdentityServiceException}.
     *
     * @throws Exception if test encounters error.
     */
    @Test(expected = IdentityServiceException.class)
    public void testExceptionWhileAssigningIdentity() throws Exception {
        final long probeId = 1L;
        final List<EntityIdentifyingPropertyValues> identifyingPropertyValues = Collections.singletonList(
            createEntityIdentifyingPropertyValues("123"));
       when(identityProvider.getIdsFromIdentifyingPropertiesValues(anyLong(),
           anyListOf(EntityIdentifyingPropertyValues.class))).thenThrow(new IdentityServiceException(""));
       entityStore.entityIdentifyingPropertyValuesDiscovered(probeId, 2L, identifyingPropertyValues);
    }

    /**
     * Test that when {@link EntityStore#entityIdentifyingPropertyValuesDiscovered(long, long, List)} or
     * {@link EntityStore#entitiesDiscovered(long, long, int, DiscoveryType, List)} has not been invoked with valid
     * inputs, then the {@link Supplier} argument passed to {@link IdentityProvider#initializeStaleOidManager(Supplier)}
     * returns {@link Collections#emptySet()}.
     */
    @Test
    public void testIdentityProviderInitializeStaleOidManagerEmptySupplier() {
        Mockito.verify(identityProvider).initializeStaleOidManager(initializeStaleOidManagerArgumentCaptor.capture());
        final Supplier<Set<Long>> oidSupplier = initializeStaleOidManagerArgumentCaptor.getValue();
        Assert.assertEquals(Collections.emptySet(), oidSupplier.get());
    }

    /**
     * Test that when {@link EntityStore#entityIdentifyingPropertyValuesDiscovered(long, long, List)} is invoked with
     * valid inputs, the {@link Supplier} argument passed to
     * {@link IdentityProvider#initializeStaleOidManager(Supplier)} contains the corresponding oid.
     *
     * @throws IdentityServiceException if there is an error in the Identity Service
     */
    @Test
    public void testIdentityProviderInitializeStaleOidManagerOidSuppliedFromIdPropValues()
        throws IdentityServiceException {
        final EntityIdentifyingPropertyValues vm1 = createEntityIdentifyingPropertyValues("vm-123");
        final List<EntityIdentifyingPropertyValues> identifyingPropertyValues = Collections.singletonList(vm1);
        final long probeId = 1L;
        final long entityOid = 123L;
        Mockito.when(identityProvider.getIdsFromIdentifyingPropertiesValues(probeId, identifyingPropertyValues))
            .thenReturn(Collections.singletonMap(entityOid, vm1));
        entityStore.entityIdentifyingPropertyValuesDiscovered(probeId, 2L, identifyingPropertyValues);
        Mockito.verify(identityProvider).initializeStaleOidManager(initializeStaleOidManagerArgumentCaptor.capture());
        final Supplier<Set<Long>> oidSupplier = initializeStaleOidManagerArgumentCaptor.getValue();
        Assert.assertEquals(Collections.singleton(entityOid), oidSupplier.get());
    }

    /**
     * Test that when {@link EntityStore#entitiesDiscovered(long, long, int, DiscoveryType, List)} has been invoked with
     * a valid input, the {@link Supplier} argument passed to
     * {@link IdentityProvider#initializeStaleOidManager(Supplier)} contains the corresponding oid.
     *
     * @throws Exception if error is encountered.
     */
    @Test
    public void testIdentityProviderInitializeStaleOidManagerOidSuppliedFromEntityMap() throws Exception {
        final long entityOid = 1L;
        final EntityDTO entity1 = createEntity("vm-id", EntityType.VIRTUAL_MACHINE);
        Map<Long, EntityDTO> entitiesMap = ImmutableMap.of(entityOid, entity1);
        addEntities(entitiesMap);
        Mockito.verify(identityProvider).initializeStaleOidManager(initializeStaleOidManagerArgumentCaptor.capture());
        final Supplier<Set<Long>> oidSupplier = initializeStaleOidManagerArgumentCaptor.getValue();
        Assert.assertEquals(Collections.singleton(entityOid), oidSupplier.get());
    }

    /**
     * Test that when {@link EntityStore#entitiesDiscovered(long, long, int, DiscoveryType, List)}
     * and {@link EntityStore#entityIdentifyingPropertyValuesDiscovered(long, long, List)} have been invoked with
     * valid inputs, the {@link Supplier} argument passed to
     * {@link IdentityProvider#initializeStaleOidManager(Supplier)} contain the corresponding oids.
     *
     * @throws Exception if error is encountered.
     */
    @Test
    public void testIdentityProviderInitializeStaleOidManagerOidSuppliedFromAllSources() throws Exception {
        final long probeId = 1L;
        final EntityIdentifyingPropertyValues vm1 = createEntityIdentifyingPropertyValues("456");
        final List<EntityIdentifyingPropertyValues> identifyingPropertyValues = Collections.singletonList(vm1);
        Mockito.when(identityProvider.getIdsFromIdentifyingPropertiesValues(probeId, identifyingPropertyValues))
            .thenReturn(Collections.singletonMap(123L, vm1));
        entityStore.entityIdentifyingPropertyValuesDiscovered(probeId, 2L, identifyingPropertyValues);
        final EntityDTO entity1 = createEntity("vm-id", EntityType.VIRTUAL_MACHINE);
        Map<Long, EntityDTO> entitiesMap = ImmutableMap.of(1L, entity1);
        addEntities(entitiesMap);
        Mockito.verify(identityProvider).initializeStaleOidManager(initializeStaleOidManagerArgumentCaptor.capture());
        final Supplier<Set<Long>> oidSupplier = initializeStaleOidManagerArgumentCaptor.getValue();
        Assert.assertEquals(new HashSet<>(Arrays.asList(1L, 123L)), oidSupplier.get());
    }

    private EntityDTO createEntity(@Nonnull String id, @Nonnull EntityType entityType) {
        return EntityDTO.newBuilder().setEntityType(entityType)
                .setId(id).build();
    }

    private Map<Long, EntityDTO> createEntityMap(long firstOid, @Nonnull EntityType entityType,
            int size) {
        Map<Long, EntityDTO> retVal = new HashMap<>();
        for (int i = 0; i < size; i++) {
            retVal.put(firstOid + i, createEntity(String.valueOf(firstOid + i), entityType));
        }
        return retVal;
    }

    private void setupMocksForTarget(@Nonnull Target target, long targetId, String displayName,
            @Nonnull ProbeInfo probeInfo) {
        when(targetStore.getTarget(targetId)).thenReturn(Optional.of(target));
        when(targetStore.getTargetDisplayName(targetId))
                .thenReturn(Optional.of(displayName));
        when(target.getProbeInfo()).thenReturn(probeInfo);
        when(target.getDisplayName()).thenReturn(displayName);
        when(target.getId()).thenReturn(targetId);
        when(target.toString()).thenReturn(displayName);

    }

    /**
     * Test that 2 hypervisor targets that overlap in 33% of their VMs are identified as duplicates.
     *
     * @throws Exception if an exception is encountered.
     */
    @Test
    public void testDuplicateTargetDetected() throws Exception {
        setupMocksForTarget(firstTarget, firstTargetId, firstTargetDisplayName, probeInfoHypervisor);
        setupMocksForTarget(duplicateTarget, duplicateTargetId, duplicateTargetDisplayName,
                probeInfoHypervisor);
        setupMocksForTarget(secondDuplicateTarget, secondDuplicateTargetId,
                secondDuplicateTargetDisplayName, probeInfoHypervisor);
        when(targetStore.getAll()).thenReturn(ImmutableList.of(firstTarget, duplicateTarget,
                secondDuplicateTarget));
        final EntityDTO commonVM = createEntity("one", EntityType.VIRTUAL_MACHINE);
        final Map<Long, EntityDTO> firstTargetEntitiesMap =
                createEntityMap(2L, EntityType.VIRTUAL_MACHINE, 2);
        firstTargetEntitiesMap.put(1L, commonVM);
        final Map<Long, EntityDTO> duplicateTargetEntitiesMap =
                createEntityMap(4L, EntityType.VIRTUAL_MACHINE, 2);
        duplicateTargetEntitiesMap.put(1L, commonVM);
        final Map<Long, EntityDTO> secondDuplicateTargetEntitiesMap =
                createEntityMap(6L, EntityType.VIRTUAL_MACHINE, 2);
        secondDuplicateTargetEntitiesMap.put(1L, commonVM);
        addEntities(firstTargetEntitiesMap, firstTargetId, 0L, DiscoveryType.FULL, 0);
        assertEquals(firstTargetEntitiesMap, entityStore.discoveredByTarget(firstTargetId));
        try {
            addEntities(duplicateTargetEntitiesMap, duplicateTargetId, 0L, DiscoveryType.FULL, 0);
            fail("Did not get exception when target " + duplicateTarget + " was added.");
        } catch (DuplicateTargetException ex) {
            assertTrue(ex.getLocalizedMessage().startsWith("Target " + duplicateTargetDisplayName
                    + " is a duplicate of target " + firstTargetDisplayName));
            try {
                addEntities(secondDuplicateTargetEntitiesMap, secondDuplicateTargetId, 0L, DiscoveryType.FULL, 0);
                fail("Did not get exception when target " + secondDuplicateTarget + " was added.");
            } catch (DuplicateTargetException ex2) {
                assertTrue(ex2.getLocalizedMessage().startsWith("Target " + secondDuplicateTargetDisplayName
                        + " is a duplicate of targets " + firstTargetDisplayName + ", "
                        + duplicateTargetDisplayName));
            }
        }

        final ArgumentCaptor<TargetStoreListener> captor =
                ArgumentCaptor.forClass(TargetStoreListener.class);
        Mockito.verify(targetStore, times(2)).addListener(captor.capture());
        final TargetStoreListener storeListener1 = captor.getAllValues().get(0);
        final TargetStoreListener storeListener2 = captor.getAllValues().get(1);
        storeListener1.onTargetRemoved(firstTarget);
        storeListener2.onTargetRemoved(firstTarget);
        // after deleting firstTarget, duplicateTarget is now the target we keep and
        // secondDuplicateTarget is a duplicate of duplicateTarget
        entityStore.entitiesDiscovered(0L, duplicateTargetId, 0,
                DiscoveryType.INCREMENTAL, Collections.emptyList());
        try {
            entityStore.entitiesDiscovered(0L, secondDuplicateTargetId, 0,
                    DiscoveryType.INCREMENTAL, Collections.emptyList());
            fail("Expected DuplicateTargetException when processing discovery of "
                    + secondDuplicateTarget);
        } catch (DuplicateTargetException e) {
            assertTrue(e.getLocalizedMessage().startsWith("Target " + secondDuplicateTargetDisplayName
                    + " is a duplicate of target " + duplicateTargetDisplayName));
        }

        // now delete duplicateTarget and secondDuplicateTarget should no longer be a duplicate
        storeListener1.onTargetRemoved(duplicateTarget);
        storeListener2.onTargetRemoved(duplicateTarget);
        addEntities(secondDuplicateTargetEntitiesMap, secondDuplicateTargetId, 0L, DiscoveryType.FULL, 0);
        Assert.assertTrue(entityStore.getTargetEntityIdMap(secondDuplicateTargetId).isPresent());
        Assert.assertTrue(entityStore.getEntity(1L).isPresent());
        assertEquals(secondDuplicateTargetEntitiesMap,
                entityStore.discoveredByTarget(secondDuplicateTargetId));
    }

    /**
     * Test that if we have a Hypervisor target with no Virtual Machines, we don't automatically
     * assume it is a duplicate of other Hypervisor targets. This is to make sure that we don't
     * use an overlap size of 0 when we have 0 VMs in the target we are testing for duplication.
     *
     * @throws TargetNotFoundException if the target is not in the target store
     * @throws DuplicateTargetException if the target is found to be a duplicate
     * @throws IdentityServiceException if there is a problem with the identity service
     */
    @Test
    public void testTargetWithNoVmsNotDuplicate()
            throws TargetNotFoundException, DuplicateTargetException, IdentityServiceException {
        setupMocksForTarget(firstTarget, firstTargetId, firstTargetDisplayName, probeInfoHypervisor);
        setupMocksForTarget(duplicateTarget, duplicateTargetId, duplicateTargetDisplayName,
                probeInfoHypervisor);
        when(targetStore.getAll()).thenReturn(ImmutableList.of(firstTarget, duplicateTarget));
        final Map<Long, EntityDTO> firstTargetEntitiesMap =
                createEntityMap(1L, EntityType.VIRTUAL_MACHINE, 3);
        final Map<Long, EntityDTO> duplicateTargetEntitiesMap =
                createEntityMap(4L, EntityType.STORAGE, 3);
        addEntities(firstTargetEntitiesMap, firstTargetId, 0L, DiscoveryType.FULL, 0);
        assertEquals(firstTargetEntitiesMap, entityStore.discoveredByTarget(firstTargetId));
        // this will throw a DuplicateTargetException if duplicate detection logic does not properly
        // handle the 0 VM case
        addEntities(duplicateTargetEntitiesMap, duplicateTargetId, 0L, DiscoveryType.FULL, 0);
        assertEquals(duplicateTargetEntitiesMap, entityStore.discoveredByTarget(duplicateTargetId));
    }

    /**
     * Test that two Kubernetes targets are treated as duplicates even if the probe types have
     * different suffixes.
     * @throws Exception if an exception occurs.
     */
    @Test
    public void testKubernetesDuplicateTargets() throws Exception {
        final String probe1Type = "Kubernetes-foo";
        final String probe2Type = "Kubernetes-bar";
        final ProbeInfo.Builder probeInfoKubernetesBuilder = ProbeInfo.newBuilder()
                .setProbeCategory(ProbeCategory.CLOUD_NATIVE.getCategory());
        final ProbeInfo probeInfo1 = probeInfoKubernetesBuilder.setProbeType(probe1Type).build();
        final ProbeInfo probeInfo2 = probeInfoKubernetesBuilder.setProbeType(probe2Type).build();

        setupMocksForTarget(firstTarget, firstTargetId, firstTargetDisplayName, probeInfo1);
        setupMocksForTarget(duplicateTarget, duplicateTargetId, duplicateTargetDisplayName,
                probeInfo2);
        when(targetStore.getAll()).thenReturn(ImmutableList.of(firstTarget, duplicateTarget));

        final EntityDTO commonNamespace = createEntity("one", EntityType.NAMESPACE);
        final Map<Long, EntityDTO> firstTargetEntitiesMap =
                createEntityMap(2L, EntityType.NAMESPACE, 2);
        firstTargetEntitiesMap.put(1L, commonNamespace);
        final Map<Long, EntityDTO> duplicateTargetEntitiesMap =
                createEntityMap(4L, EntityType.NAMESPACE, 2);
        duplicateTargetEntitiesMap.put(1L, commonNamespace);
        addEntities(firstTargetEntitiesMap, firstTargetId, 0L, DiscoveryType.FULL, 0);
        assertEquals(firstTargetEntitiesMap, entityStore.discoveredByTarget(firstTargetId));
        expectedException.expect(DuplicateTargetException.class);
        expectedException.expectMessage("Target " + duplicateTargetDisplayName
                + " is a duplicate of target " + firstTargetDisplayName);
        addEntities(duplicateTargetEntitiesMap, duplicateTargetId, 0L, DiscoveryType.FULL, 0);
    }

    /**
     * Check that two targets are not treated as duplicates if they overlap in the checked
     * EntityType (VirtualMachine) by less than 30%. This is true even if they overlap in other
     * entity types (Storage in this case).
     *
     * @throws Exception if an exception is thrown.
     */
    @Test
    public void testNoFalseDuplicateTargets() throws Exception {
        final ProbeInfo probeInfoPublicCloud = ProbeInfo.newBuilder()
                .setProbeCategory(ProbeCategory.PUBLIC_CLOUD.getCategory())
                .setProbeType("foo")
                .build();
        setupMocksForTarget(firstTarget, firstTargetId, firstTargetDisplayName,
                probeInfoPublicCloud);
        setupMocksForTarget(duplicateTarget, duplicateTargetId, duplicateTargetDisplayName,
                probeInfoPublicCloud);
        final EntityDTO commonVM = createEntity("one", EntityType.VIRTUAL_MACHINE);
        final Map<Long, EntityDTO> firstTargetEntitiesMap =
                createEntityMap(2L, EntityType.VIRTUAL_MACHINE, 3);
        firstTargetEntitiesMap.put(1L, commonVM);
        final Map<Long, EntityDTO> duplicateTargetEntitiesMap =
                createEntityMap(5L, EntityType.VIRTUAL_MACHINE, 3);
        duplicateTargetEntitiesMap.put(1L, commonVM);
        final Map<Long, EntityDTO> storageEntities = createEntityMap(10L, EntityType.STORAGE, 4);
        firstTargetEntitiesMap.putAll(storageEntities);
        duplicateTargetEntitiesMap.putAll(storageEntities);
        addEntities(firstTargetEntitiesMap, firstTargetId, 0L, DiscoveryType.FULL, 0);
        assertEquals(firstTargetEntitiesMap, entityStore.discoveredByTarget(firstTargetId));
        addEntities(duplicateTargetEntitiesMap, duplicateTargetId, 0L, DiscoveryType.FULL, 0);
        assertEquals(duplicateTargetEntitiesMap, entityStore.discoveredByTarget(duplicateTargetId));
    }

    /**
     * Test that incremental discovery response for different targets are cached.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testIncrementalEntitiesCached() throws Exception {
        // Pretend that any target exists
        when(targetStore.getTarget(anyLong())).thenReturn(Optional.of(Mockito.mock(Target.class)));

        final long targetId1 = 2001;
        final long targetId2 = 2002;
        final long oid1 = 1L;
        final long oid2 = 2L;
        final EntityDTO entity1 = EntityDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE)
            .setId("pm1")
            .build();
        final EntityDTO entity2 = EntityDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE)
            .setId("pm2")
            .build();
        final EntityDTO entity3 = EntityDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE)
            .setId("pm1")
            .setMaintenance(true)
            .build();
        Map<Long, EntityDTO> entitiesMap1 = ImmutableMap.of(oid1, entity1);

        final int messageId1 = 0;
        final int messageId2 = 1;
        final int messageId3 = 2;

        // target 1: first incremental discovery
        // before
        assertFalse(entityStore.getIncrementalEntities(targetId1).isPresent());
        addEntities(entitiesMap1, targetId1, 0, DiscoveryType.INCREMENTAL, messageId1);
        // after
        assertTrue(entityStore.getIncrementalEntities(targetId1).isPresent());
        SortedMap<Integer, EntityDTO> entitiesOrderedByDiscovery1 =
            entityStore.getIncrementalEntities(targetId1).get().getEntitiesInDiscoveryOrder(oid1);
        assertEquals(1, entitiesOrderedByDiscovery1.size());
        assertEquals(entity1, entitiesOrderedByDiscovery1.get(messageId1));

        // target 2: first incremental discovery
        // before
        assertFalse(entityStore.getIncrementalEntities(targetId2).isPresent());
        Map<Long, EntityDTO> entitiesMap2 = ImmutableMap.of(oid2, entity2);
        addEntities(entitiesMap2, targetId2, 0, DiscoveryType.INCREMENTAL, messageId2);
        // after
        assertTrue(entityStore.getIncrementalEntities(targetId2).isPresent());
        // incremental cache for target1 is still there
        assertTrue(entityStore.getIncrementalEntities(targetId1).isPresent());
        SortedMap<Integer, EntityDTO> entitiesOrderedByDiscovery2 =
            entityStore.getIncrementalEntities(targetId2).get().getEntitiesInDiscoveryOrder(oid2);
        assertEquals(1, entitiesOrderedByDiscovery2.size());
        assertEquals(entity2, entitiesOrderedByDiscovery2.get(messageId2));

        // target 1: second incremental discovery
        // before
        Map<Long, EntityDTO> entitiesMap3 = ImmutableMap.of(oid1, entity3);
        addEntities(entitiesMap3, targetId1, 0, DiscoveryType.INCREMENTAL, messageId3);
        // after
        assertTrue(entityStore.getIncrementalEntities(targetId1).isPresent());
        assertTrue(entityStore.getIncrementalEntities(targetId2).isPresent());
        // now incremental cache for target1 contains two entries, from two different discoveries,
        SortedMap<Integer, EntityDTO> entitiesOrderedByDiscovery3 =
            entityStore.getIncrementalEntities(targetId1).get().getEntitiesInDiscoveryOrder(oid1);

        assertEquals(2, entitiesOrderedByDiscovery3.size());
        assertEquals(entity1, entitiesOrderedByDiscovery3.get(messageId1));
        assertEquals(entity3, entitiesOrderedByDiscovery3.get(messageId3));
        // verify the discovery order is preserved
        assertEquals(new ArrayList<>(entitiesOrderedByDiscovery3.keySet()),
            Lists.newArrayList(messageId1, messageId3));
    }

    /**
     * Test that we can properly get incremental entities by their message id.
     * @throws TargetNotFoundException if the target can't be found
     * @throws DuplicateTargetException if a duplicate target is found
     * @throws IdentityServiceException if the identity store was not initialized
     */
    @Test
    public void testGetEntitiesByMessageId()
            throws TargetNotFoundException, DuplicateTargetException, IdentityServiceException {
        when(targetStore.getTarget(anyLong())).thenReturn(Optional.of(Mockito.mock(Target.class)));

        final long targetId1 = 2001;
        final long oid1 = 1L;
        final long oid2 = 2L;
        final EntityDTO entity1 = EntityDTO.newBuilder()
                .setEntityType(EntityType.PHYSICAL_MACHINE)
                .setId("pm1")
                .build();
        final EntityDTO entity2 = EntityDTO.newBuilder()
                .setEntityType(EntityType.PHYSICAL_MACHINE)
                .setId("pm2")
                .build();
        Map<Long, EntityDTO> entitiesMap1 = ImmutableMap.of(oid1, entity1);
        Map<Long, EntityDTO> entitiesMap2 = ImmutableMap.of(oid2, entity2);
        final int messageId = 1;
        addEntities(entitiesMap1, targetId1, 0, DiscoveryType.INCREMENTAL, messageId);
        addEntities(entitiesMap2, targetId1, 0, DiscoveryType.INCREMENTAL, messageId);

        Assert.assertTrue(entityStore.getIncrementalEntities(targetId1).isPresent());
        LinkedHashMap<Integer, Collection<EntityDTO>>
                entities = entityStore.getIncrementalEntities(targetId1).get().getEntitiesByMessageId();
        Assert.assertEquals( 2, entities.get(messageId).size());
        Assert.assertEquals(entities.get(messageId), Arrays.asList(entity1, entity2));
    }

    /**
     * Test getting a missing entity.
     */
    @Test
    public void testGetWhenAbsent() {
        Assert.assertFalse(entityStore.getTargetEntityIdMap(targetId).isPresent());
        Assert.assertFalse(entityStore.getEntity(1L).isPresent());
    }

    /**
     * Tests for target removal after some data is reported for the target. The {@link EntityStore} is
     * expected to remove all its data after target has been removed.
     *
     * @throws Exception on exceptions occur.
     */
    @Test
    public void testTargetRemoval() throws Exception {
        final String id1 = "en-hypervisorTarget";
        final EntityDTO entity1 = EntityDTO.newBuilder().setEntityType(EntityType.VIRTUAL_MACHINE)
                .setId(id1).build();

        addEntities(ImmutableMap.of(1L, entity1));

        Assert.assertTrue(entityStore.getTargetEntityIdMap(targetId).isPresent());
        Assert.assertTrue(entityStore.getEntity(1L).isPresent());

        final ArgumentCaptor<TargetStoreListener> captor =
                ArgumentCaptor.forClass(TargetStoreListener.class);
        Mockito.verify(targetStore, times(2)).addListener(captor.capture());
        final TargetStoreListener storeListener1 = captor.getAllValues().get(0);
        final TargetStoreListener storeListener2 = captor.getAllValues().get(1);
        storeListener1.onTargetRemoved(target);
        storeListener2.onTargetRemoved(target);

        Assert.assertFalse(entityStore.getTargetEntityIdMap(targetId).isPresent());
        Assert.assertFalse(entityStore.getEntity(1L).isPresent());
    }

    /**
     * Tests {@link EntityStore#getEntityIdByLocalId} method.
     *
     * @throws TargetNotFoundException Should not happen.
     * @throws IdentityServiceException Should not happen.
     * @throws DuplicateTargetException Should not happen.
     */
    @Test
    public void testGetEntityIdByLocalId()
            throws TargetNotFoundException, IdentityServiceException, DuplicateTargetException {

        // Add entity 1 from target 1
        final String localId1 = "localId1";
        final long id1 = 1L;
        addVmEntity(id1, localId1, targetId);

        // Add entity 2 from target 2
        final String localId2 = "localId2";
        final long id2 = 2L;
        addVmEntity(id2, localId2, target2Id);

        // Check entity 1
        final Optional<Long> id1Opt = entityStore.getEntityIdByLocalId(localId1);
        assertTrue(id1Opt.isPresent());
        assertEquals(id1, id1Opt.get().longValue());

        // Check entity 2
        final Optional<Long> id2Opt = entityStore.getEntityIdByLocalId(localId2);
        assertTrue(id2Opt.isPresent());
        assertEquals(id2, id2Opt.get().longValue());

        // Check not existing entity
        assertFalse(entityStore.getEntityIdByLocalId("nonExistingLocalId").isPresent());
    }

    private void addVmEntity(final long id, final String localId, final long targetId)
            throws TargetNotFoundException, IdentityServiceException, DuplicateTargetException {
        final EntityDTO entity = EntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                .setId(localId)
                .build();
        addEntities(ImmutableMap.of(id, entity), targetId, 0, DiscoveryType.FULL, 0);
    }

    /**
     * Tests construct stitching graph for single target.
     *
     * @param useSerializedEntities whether to use serialized dtos or not.
     * @throws IdentityServiceException on service exceptions
     * @throws TargetNotFoundException if target not found
     * @throws DuplicateTargetException if target is a duplicate of existing target
     */
    @Test
    @Parameters(method = "generateTestData")
    public void testConstructStitchingGraphSingleTarget(boolean useSerializedEntities)
            throws IdentityServiceException, TargetNotFoundException, DuplicateTargetException {
        final Map<Long, EntityDTO> entities = ImmutableMap.of(
            1L, virtualMachine("foo")
                .buying(vCpuMHz().from("bar").used(100.0))
                .buying(storageAmount().from("baz").used(200.0))
                .build(),
            2L, physicalMachine("bar").build(),
            3L, storage("baz").build());

        final Clock mockClock = Mockito.mock(Clock.class);
        Mockito.when(mockClock.millis()).thenReturn(12345L);
        entityStore = new EntityStore(targetStore, identityProvider, 0.3F, true, Collections.singletonList(sender),
                mockClock, Collections.emptySet(), useSerializedEntities);

        addEntities(entities);
        // the probe type doesn't matter here, just return any non-cloud probe type so it gets
        // treated as normal probe
        when(targetStore.getProbeTypeForTarget(Mockito.anyLong())).thenReturn(Optional.of(SDKProbeType.HYPERV));
        final TopologyStitchingGraph graph = entityStore.constructStitchingContext()
                .getStitchingGraph();

        final TopologyStitchingEntity foo = entityByLocalId(graph, "foo");
        assertEquals(12345L, foo.getLastUpdatedTime());
        assertThat(entityByLocalId(graph, "bar")
            .getConsumers().stream()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), contains("foo"));
        assertThat(entityByLocalId(graph, "baz")
            .getConsumers().stream()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), contains("foo"));
        assertThat(foo.getConsumers().stream()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), is(empty()));
        assertThat(foo.getProviders().stream()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), containsInAnyOrder("bar", "baz"));
    }

    /**
     * Tests construct stitching graph for multiple targets.
     *
     * @throws IdentityServiceException on service exceptions
     * @throws TargetNotFoundException if target not found
     * @throws DuplicateTargetException if target is a duplicate of existing target
     */
    @Test
    public void testConstructStitchingGraphMultipleTargets()
            throws IdentityServiceException, TargetNotFoundException, DuplicateTargetException {

        final Map<Long, EntityDTO> firstTargetEntities = ImmutableMap.of(
            1L, virtualMachine("foo")
                .buying(vCpuMHz().from("bar").used(100.0))
                .buying(storageAmount().from("baz").used(200.0))
                .build(),
            2L, physicalMachine("bar").build(),
            3L, storage("baz").build());
        final Map<Long, EntityDTO> secondTargetEntities = ImmutableMap.of(
            4L, virtualMachine("vampire")
                .buying(vCpuMHz().from("werewolf").used(100.0))
                .buying(storageAmount().from("dragon").used(200.0))
                .build(),
            5L, physicalMachine("werewolf").build(),
            6L, storage("dragon").build());

        addEntities(firstTargetEntities, targetId, 0L, DiscoveryType.FULL, 0);
        addEntities(secondTargetEntities, target2Id, 1L, DiscoveryType.FULL, 1);
        // the probe type doesn't matter here, just return any non-cloud probe type so it gets
        // treated as normal probe
        when(targetStore.getProbeTypeForTarget(Mockito.anyLong())).thenReturn(Optional.of(SDKProbeType.HYPERV));
        final TopologyStitchingGraph graph = entityStore.constructStitchingContext()
                .getStitchingGraph();

        assertEquals(6, graph.entityCount());
        assertThat(entityByLocalId(graph, "foo")
            .getProviders().stream()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), containsInAnyOrder("bar", "baz"));
        assertThat(entityByLocalId(graph, "vampire")
            .getProviders().stream()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()), containsInAnyOrder("werewolf", "dragon"));

        assertEquals(targetId, entityByLocalId(graph, "foo").getTargetId());
        assertEquals(target2Id, entityByLocalId(graph, "werewolf").getTargetId());
    }

    /**
     * Tests target linking in constructing the stitching graph, using a basic cloud infrastructure scenario.
     *
     * @throws IdentityServiceException on service exceptions
     * @throws TargetNotFoundException if target not found
     * @throws DuplicateTargetException if target is a duplicate of existing target
     */
    @Test
    public void testConstructStitchingGraphLinkedTargets()
            throws TargetNotFoundException, DuplicateTargetException, IdentityServiceException {

        final Map<Long, EntityDTO> firstTargetEntities = ImmutableMap.of(
                1L, virtualMachine("vmTestId")
                        .buying(vCpuMHz().from("computeTierTestId"))
                        .aggregatedBy("regionTestId")
                        .build());

        final Map<Long, EntityDTO> secondTargetEntities = ImmutableMap.of(
                2L, EntityDTO.newBuilder()
                        .setEntityType(EntityType.COMPUTE_TIER)
                        .setId("computeTierTestId")
                        .build(),
                3L, EntityDTO.newBuilder()
                        .setEntityType(EntityType.REGION)
                        .setId("regionTestId")
                        .build());

        addEntities(firstTargetEntities, targetId, 0L, DiscoveryType.FULL, 0);
        addEntities(secondTargetEntities, target2Id, 1L, DiscoveryType.FULL, 1);

        // set up target linking
        when(targetStore.getLinkedTargetIds(targetId)).thenReturn(ImmutableSortedSet.of(target2Id));
        when(targetStore.getLinkedTargetIds(target2Id)).thenReturn(ImmutableSortedSet.of(targetId));
        when(targetStore.getProbeTypeForTarget(Mockito.anyLong())).thenReturn(Optional.empty());

        // create the topology stitching graph
        final TopologyStitchingGraph graph = entityStore.constructStitchingContext()
                .getStitchingGraph();

        assertThat(graph.entityCount(), equalTo(3));

        final TopologyStitchingEntity vmStitchingEntity = entityByLocalId(graph, "vmTestId");
        assertThat(vmStitchingEntity.getProviders()
                .stream()
                .map(StitchingEntity::getLocalId)
                .collect(ImmutableList.toImmutableList()),
                containsInAnyOrder("computeTierTestId"));

        assertThat(vmStitchingEntity.getConnectedToByType()
                        .getOrDefault(ConnectionType.AGGREGATED_BY_CONNECTION, Collections.emptySet())
                        .stream()
                        .map(StitchingEntity::getLocalId)
                        .collect(ImmutableList.toImmutableList()),
                containsInAnyOrder("regionTestId"));

    }

    /**
     * Tests construct stitching graph for single target.
     *
     * @param useSerializedEntities whether to use serialized dtos or not.
     * @throws IdentityServiceException on service exceptions
     * @throws TargetNotFoundException if target not found
     * @throws DuplicateTargetException if target is a duplicate of existing target
     */
    @Test
    @Parameters(method = "generateTestData")
    public void testApplyIncrementalEntities(boolean useSerializedEntities)
            throws IdentityServiceException, TargetNotFoundException, DuplicateTargetException {
        final Clock mockClock = Mockito.mock(Clock.class);
        Mockito.when(mockClock.millis()).thenReturn(12345L);
        entityStore = new EntityStore(targetStore, identityProvider, 0.3F, true, Collections.singletonList(sender),
                mockClock, Collections.emptySet(), useSerializedEntities);
        // the probe type doesn't matter here, just return any non-cloud probe type so it gets
        // treated as normal probe
        when(targetStore.getProbeTypeForTarget(Mockito.anyLong())).thenReturn(Optional.of(SDKProbeType.HYPERV));

        // full discovery: maintenance is false
        final Map<Long, EntityDTO> fullEntities1 = ImmutableMap.of(
            1L, physicalMachine("host").maintenance(false).build());
        addEntities(fullEntities1, targetId, 0, DiscoveryType.FULL, 0);
        // incremental discovery: maintenance is true
        final Map<Long, EntityDTO> incrementalEntities = ImmutableMap.of(
            1L, physicalMachine("host").maintenance(true).build());
        addEntities(incrementalEntities, targetId, 0, DiscoveryType.INCREMENTAL, 1);

        // first broadcast
        final TopologyStitchingGraph graph = entityStore.constructStitchingContext()
            .getStitchingGraph();
        final TopologyStitchingEntity pm = entityByLocalId(graph, "host");
        // verify that host is in maintenance, due to the incremental discovery
        assertTrue(pm.getEntityBuilder().getMaintenance());

        // do second broadcast, the maintenance is still true
        final TopologyStitchingGraph graph2 = entityStore.constructStitchingContext()
            .getStitchingGraph();
        final TopologyStitchingEntity pm2 = entityByLocalId(graph2, "host");
        assertTrue(pm2.getEntityBuilder().getMaintenance());

        // another incremental discovery: maintenance is false
        final Map<Long, EntityDTO> incrementalEntities2 = ImmutableMap.of(
            1L, physicalMachine("host").maintenance(false).build());
        addEntities(incrementalEntities2, targetId, 0, DiscoveryType.INCREMENTAL, 2);
        // incremental result is cached
        assertEquals(2, entityStore.getIncrementalEntities(targetId).get()
            .getEntitiesInDiscoveryOrder(1L).size());

        // then a new full discovery (maintenance is true)
        final Map<Long, EntityDTO> fullEntities2 = ImmutableMap.of(
            1L, physicalMachine("host").maintenance(true).build());
        addEntities(fullEntities2, targetId, 0, DiscoveryType.FULL, 3);
        // verify that old incremental cache is cleared
        assertTrue(entityStore.getIncrementalEntities(targetId).get()
            .getEntitiesInDiscoveryOrder(1L).isEmpty());
        // full response should be used in third broadcast
        final TopologyStitchingGraph graph3 = entityStore.constructStitchingContext()
            .getStitchingGraph();
        final TopologyStitchingEntity pm3 = entityByLocalId(graph3, "host");
        assertTrue(pm3.getEntityBuilder().getMaintenance());
        assertTrue(entityStore.getIncrementalEntities(targetId).get()
            .getEntitiesInDiscoveryOrder(1L).isEmpty());
    }

    /**
     * Tests entities restored.
     *
     * @throws TargetNotFoundException if target not found
     * @throws DuplicateTargetException if target is a duplicate of existing target
     */
    @Test
    public void testEntitiesRestored() throws TargetNotFoundException, DuplicateTargetException {
        final String id1 = "en-hypervisorTarget";
        final EntityDTO entity1 = EntityDTO.newBuilder().setEntityType(EntityType.VIRTUAL_MACHINE)
            .setId(id1).build();
        Map<Long, EntityDTO> entitiesMap = ImmutableMap.of(1L, entity1);

        entityStore.entitiesRestored(targetId, 5678L, entitiesMap);

        Assert.assertTrue(entityStore.getTargetEntityIdMap(targetId).isPresent());
        Assert.assertTrue(entityStore.getEntity(1L).isPresent());
        assertEquals(entitiesMap, entityStore.discoveredByTarget(targetId));
    }

    /**
     * Test that incremental discovery response for different targets are cached.
     *
     * @param useSerializedEntities whether to use serialized dtos or not.
     * @throws Exception If anything goes wrong.
     */
    @Test
    @Parameters(method = "generateTestData")
    public void testSendEntitiesWithNewState(boolean useSerializedEntities) throws Exception {
        entityStore = spy(new EntityStore(targetStore, identityProvider, 0.3F, true,
                    Collections.singletonList(sender), Clock.systemUTC(), Collections.emptySet(),
                true));

        when(targetStore.getTarget(anyLong())).thenReturn(Optional.of(Mockito.mock(Target.class)));
        final long targetId1 = 2001;
        final long oid1 = 1L;
        final EntityDTO entity1 = EntityDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE)
            .setId("pm1")
            .build();
        Map<Long, EntityDTO> entitiesMap1 = ImmutableMap.of(oid1, entity1);

        final int messageId1 = 0;

        // target 1: first incremental discovery
        // before
        addEntities(entitiesMap1, targetId1, 0, DiscoveryType.INCREMENTAL, messageId1);
        verify(sender, times(1)).onEntitiesWithNewState(Mockito.any());

        // second incremental discovery
        final long oid2 = 2L;
        final EntityDTO entityDTO2 = EntityDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE)
            .setId("pm2")
            .setPhysicalMachineData(PhysicalMachineData.newBuilder().setAutomationLevel(AutomationLevel.FULLY_AUTOMATED))
            .setMaintenance(true)
            .build();
        final Entity entity2 = new Entity(oid2, EntityType.PHYSICAL_MACHINE, useSerializedEntities);
        entity2.addTargetInfo(targetId1, entityDTO2);
        when(entityStore.getEntity(oid2)).thenReturn(Optional.of(entity2));
        addEntities(ImmutableMap.of(oid2, entityDTO2), targetId1, 0, DiscoveryType.INCREMENTAL, 2);
        verify(sender, times(1)).onEntitiesWithNewState(
            EntitiesWithNewState.newBuilder()
                .setStateChangeId(1L)
                .addTopologyEntity(
                    TopologyEntityDTO.newBuilder().setOid(oid2)
                        .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                        .setEntityState(EntityState.MAINTENANCE)
                        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setPhysicalMachine(
                            PhysicalMachineInfo.newBuilder().setAutomationLevel(
                                AutomationLevel.FULLY_AUTOMATED))))
                .build());
    }

    private void restoreWithSharedEntity(@Nonnull EntityType entityType)
            throws TargetNotFoundException, DuplicateTargetException {
        final String sharedId = "en-hypervisorTarget";
        final long sharedOid = 1L;

        final EntityDTO entity1 = EntityDTO.newBuilder().setEntityType(entityType)
                .setId(sharedId).build();
        Map<Long, EntityDTO> target1Map = ImmutableMap.of(sharedOid, entity1);
        entityStore.entitiesRestored(targetId, 5678L, target1Map);

        final EntityDTO entity2 = EntityDTO.newBuilder().setEntityType(entityType)
                .setId(sharedId).build();
        Map<Long, EntityDTO> target2Map = ImmutableMap.of(sharedOid, entity2);
        entityStore.entitiesRestored(target2Id, 87942L, target2Map);

        Assert.assertTrue(entityStore.getTargetEntityIdMap(targetId).isPresent());
        Assert.assertTrue(entityStore.getTargetEntityIdMap(target2Id).isPresent());
        Assert.assertEquals(2, entityStore.getEntity(sharedOid).get().getPerTargetInfo().size());

        assertEquals(target1Map, entityStore.discoveredByTarget(targetId));
        assertEquals(target1Map, entityStore.discoveredByTarget(target2Id));

    }

    /**
     * Tests restoring multiple entities with same OIDs on different targets.
     *
     * @throws TargetNotFoundException if target not found
     * @throws DuplicateTargetException if target is a duplicate of existing target
     */
    @Test
    public void testRestoreMultipleEntitiesSameOidDifferentTargets() throws TargetNotFoundException,
            DuplicateTargetException {
        restoreWithSharedEntity(EntityType.STORAGE);
    }

    /**
     * Test that if you restore two targets that are considered duplicates, we detect the
     * duplicate target.
     *
     * @throws Exception if underlying calls throw an exception.
     */
    @Test
    public void testRestoreDuplicateTarget() throws Exception {
        expectedException.expect(DuplicateTargetException.class);
        expectedException.expectMessage("Target " + target2DisplayName
                + " is a duplicate of target " + targetDisplayName);
        restoreWithSharedEntity(EntityType.VIRTUAL_MACHINE);
    }

    private TopologyStitchingEntity entityByLocalId(@Nonnull final TopologyStitchingGraph graph,
                                                    @Nonnull final String id) {
        return graph.entities()
            .filter(e -> e.getLocalId().equals(id))
            .findFirst()
            .get();
    }

    private void addEntities(@Nonnull final Map<Long, EntityDTO> entities)
            throws IdentityServiceException, TargetNotFoundException, DuplicateTargetException {
        addEntities(entities, targetId, 0, DiscoveryType.FULL, 0);
    }

    private void addEntities(@Nonnull final Map<Long, EntityDTO> entities, final long targetId,
                             final long probeId, DiscoveryType discoveryType, int messageId)
            throws IdentityServiceException, TargetNotFoundException, DuplicateTargetException {
        Mockito.when(identityProvider.getIdsForEntities(
            Mockito.eq(probeId), Mockito.eq(new ArrayList<>(entities.values()))))
            .thenReturn(entities);
        Mockito.when(identityProvider.generateTopologyId()).thenReturn(1L);
        entityStore.entitiesDiscovered(probeId, targetId, messageId, discoveryType, new ArrayList<>(entities.values()));
    }

    /**
     * Make sure telemetry data is accurate and make sure there's no ConcurrentModificationException.
     *
     * @throws Exception exception
     */
    @Test
    public void testTelemetry() throws Exception {
        // Add targets.
        final Target target1 = mock(Target.class);
        when(target1.getProbeInfo()).thenReturn(ProbeInfo.newBuilder().setProbeType("AWS")
            .setProbeCategory("").setUiProbeCategory("Public Cloud").build());
        final Target target2 = mock(Target.class);
        when(target2.getProbeInfo()).thenReturn(ProbeInfo.newBuilder().setProbeType("AWS")
            .setProbeCategory("").setUiProbeCategory("Public Cloud").build());
        final Target target3 = mock(Target.class);
        when(target3.getProbeInfo()).thenReturn(ProbeInfo.newBuilder().setProbeType("Pure")
            .setProbeCategory("").setUiProbeCategory("STORAGE").build());
        final Target target4 = mock(Target.class);
        when(target4.getProbeInfo()).thenReturn(ProbeInfo.newBuilder().setProbeType("Pure")
            .setProbeCategory("").setUiProbeCategory("STORAGE").build());
        final Target target5 = mock(Target.class);
        when(target5.getProbeInfo()).thenReturn(ProbeInfo.newBuilder().setProbeType("vCenter")
            .setProbeCategory("").setUiProbeCategory("HYPERVISOR").build());
        when(targetStore.getAll()).thenReturn(Arrays.asList(target1, target2, target3, target4, target5));

        final long targetId1 = 1L;
        final long targetId2 = 2L;
        final long targetId5 = 5L;
        when(targetStore.getTarget(targetId1)).thenReturn(Optional.of(target1));
        when(targetStore.getTarget(targetId2)).thenReturn(Optional.of(target2));
        when(targetStore.getTarget(targetId5)).thenReturn(Optional.of(target5));

        // Add target entities
        final Map<Long, EntityDTO> firstTargetEntities = ImmutableMap.of(
            1L, virtualMachine("foo").build(),
            2L, physicalMachine("bar").build(),
            3L, storage("baz").build());
        final Map<Long, EntityDTO> secondTargetEntities = ImmutableMap.of(
            4L, virtualMachine("vampire").build(),
            5L, physicalMachine("werewolf").build(),
            6L, storage("dragon").build(),
            7L, virtualMachine("fooo").build());
        final Map<Long, EntityDTO> thirdTargetEntities = ImmutableMap.of(
            8L, virtualMachine("vampire").build(),
            9L, physicalMachine("werewolf").build(),
            10L, storage("dragon").build());

        addEntities(firstTargetEntities, targetId1, 0L, DiscoveryType.FULL, 0);
        addEntities(secondTargetEntities, targetId2, 2L, DiscoveryType.FULL, 2);
        addEntities(thirdTargetEntities, targetId5, 5L, DiscoveryType.FULL, 5);

        entityStore.sendMetricsEntityAndTargetData();

        Map<List<String>, GaugeData> values = TARGET_COUNT_GAUGE.getLabeledMetrics();
        assertEquals(3, values.size());

        double delta = 0.001d;
        assertEquals(2, values.get(Arrays.asList("Public Cloud", "AWS", StringConstants.UNKNOWN,
                                                 StringConstants.UNKNOWN)).getData(), delta);
        assertEquals(2, values.get(Arrays.asList("Storage", "Pure", StringConstants.UNKNOWN,
                                                 StringConstants.UNKNOWN)).getData(), delta);
        assertEquals(1, values.get(Arrays.asList("Hypervisor", "vCenter", StringConstants.UNKNOWN,
                                                 StringConstants.UNKNOWN)).getData(), delta);

        values = EntityStore.DISCOVERED_ENTITIES_GAUGE.getLabeledMetrics();
        assertEquals(6, values.size());

        assertEquals(2, values.get(Arrays.asList("Public Cloud", "AWS", "PHYSICAL_MACHINE")).getData(), delta);
        assertEquals(3, values.get(Arrays.asList("Public Cloud", "AWS", "VIRTUAL_MACHINE")).getData(), delta);
        assertEquals(2, values.get(Arrays.asList("Public Cloud", "AWS", "STORAGE")).getData(), delta);
        assertEquals(1, values.get(Arrays.asList("Hypervisor", "vCenter", "PHYSICAL_MACHINE")).getData(), delta);
        assertEquals(1, values.get(Arrays.asList("Hypervisor", "vCenter", "VIRTUAL_MACHINE")).getData(), delta);
        assertEquals(1, values.get(Arrays.asList("Hypervisor", "vCenter", "STORAGE")).getData(), delta);

        // Test ConcurrentModificationException.
        new Thread(() -> {
            final long start = System.currentTimeMillis();
            while (true) {
                try {
                    addEntities(firstTargetEntities, targetId1, 0L, DiscoveryType.FULL, 0);
                } catch (Exception e) {

                }
                if (System.currentTimeMillis() - start > 3000) {
                    break;
                }
            }
        }).start();

        final long start = System.currentTimeMillis();
        while (true) {
            entityStore.sendMetricsEntityAndTargetData();
            if (System.currentTimeMillis() - start > 3000) {
                break;
            }
        }
    }

    private static TargetHealth makeTargetHealth(HealthState healthState,
                                                 TargetHealthSubCategory healthSubCategory) {
        return TargetHealth.newBuilder()
                        .setHealthState(healthState)
                        .setSubcategory(healthSubCategory)
                        .build();
    }

    /**
     * Make sure telemetry data is accurate and make sure there's no ConcurrentModificationException.
     *
     * <p>Create the following targets with associated states and subcategory states:
     * 1, AWS, Public Cloud, NORMAL, DISCOVERY
     * 2, AWS, Public Cloud, NORMAL, DISCOVERY
     * 3, Pure, STORAGE, "UKNOWN", "UKNOWN"
     * 4, Pure, STORAGE, CRITICAL, VALIDATION
     * 5, vCenter, HYPERVISOR, MINOR, VALIDATION
     *
     */
    @Test
    public void testTargetStateTelemetry() {
        // Add targets.
        final Target target1 = mock(Target.class);
        when(target1.getId()).thenReturn(1L);
        when(target1.getProbeInfo()).thenReturn(ProbeInfo.newBuilder().setProbeType("AWS")
                                                                .setProbeCategory("").setUiProbeCategory("Public Cloud").build());
        final Target target2 = mock(Target.class);
        when(target2.getId()).thenReturn(2L);
        when(target2.getProbeInfo()).thenReturn(ProbeInfo.newBuilder().setProbeType("AWS")
                                                                .setProbeCategory("").setUiProbeCategory("Public Cloud").build());
        final Target target3 = mock(Target.class);
        when(target3.getId()).thenReturn(3L);
        when(target3.getProbeInfo()).thenReturn(ProbeInfo.newBuilder().setProbeType("Pure")
                                                                .setProbeCategory("").setUiProbeCategory("STORAGE").build());
        final Target target4 = mock(Target.class);
        when(target4.getId()).thenReturn(4L);
        when(target4.getProbeInfo()).thenReturn(ProbeInfo.newBuilder().setProbeType("Pure")
                                                                .setProbeCategory("").setUiProbeCategory("STORAGE").build());
        final Target target5 = mock(Target.class);
        when(target5.getId()).thenReturn(5L);
        when(target5.getProbeInfo()).thenReturn(ProbeInfo.newBuilder().setProbeType("vCenter")
                                                                .setProbeCategory("").setUiProbeCategory("HYPERVISOR").build());
        when(targetStore.getAll())
                        .thenReturn(Arrays.asList(target1, target2, target3, target4, target5));
        when(targetHealthRetriever.getTargetHealth(Collections.emptySet(), true))
                        .thenReturn(ImmutableMap.of(
                                        1L, makeTargetHealth(HealthState.NORMAL,
                                                             TargetHealthSubCategory.DISCOVERY),
                                        2L, makeTargetHealth(HealthState.NORMAL,
                                                             TargetHealthSubCategory.DISCOVERY),
                                        // Intentionally omitting 3
                                        4L, makeTargetHealth(HealthState.CRITICAL,
                                                             TargetHealthSubCategory.VALIDATION),
                                        5L, makeTargetHealth(HealthState.MINOR,
                                                             TargetHealthSubCategory.VALIDATION)));

        entityStore.sendMetricsEntityAndTargetData();

        Map<List<String>, GaugeData> values = TARGET_COUNT_GAUGE.getLabeledMetrics();
        assertEquals(4, values.size());

        double delta = 0.001d;
        assertEquals(2, values.get(Arrays.asList("Public Cloud", "AWS", "NORMAL", "DISCOVERY"))
                        .getData(), delta);
        assertEquals(1, values.get(Arrays.asList("Storage", "Pure", "CRITICAL", "VALIDATION"))
                        .getData(), delta);
        assertEquals(1, values.get(Arrays.asList("Storage", "Pure", StringConstants.UNKNOWN,
                                                 StringConstants.UNKNOWN)).getData(), delta);
        assertEquals(1, values.get(Arrays.asList("Hypervisor", "vCenter", "MINOR", "VALIDATION"))
                        .getData(), delta);
    }

    /**
     * Test that when target has health problems all store entities are marked stale.
     * Also test that when the target does not have health problems all entities are not marked stale.
     *
     * @throws IdentityServiceException when oid cannot be constructed
     * @throws TargetStoreException when target operation fails
     */
    @Test
    public void testStaleTargetMakesStaleEntities() throws TargetStoreException, IdentityServiceException {
        final Map<Long, EntityDTO> entities = ImmutableMap.of(
                        1L, virtualMachine("foo").buying(vCpuMHz().from("bar").used(100.0))
                                        .buying(storageAmount().from("baz").used(200.0)).build(),
                        2L, physicalMachine("bar").build(),
                        3L, storage("baz").build());

        final Clock mockClock = Mockito.mock(Clock.class);
        Mockito.when(mockClock.millis()).thenReturn(12345L);
        entityStore = new EntityStore(targetStore, identityProvider, 0.3F, true,
                        Collections.singletonList(sender), mockClock, Collections.emptySet(),
                true);
        addEntities(entities);

        Mockito.when(targetStore.getProbeTypeForTarget(Mockito.anyLong()))
                        .thenReturn(Optional.of(SDKProbeType.HYPERV));

        final TopologyStitchingGraph criticalStateGraph =
                        entityStore.constructStitchingContext(new StalenessInformationProvider() {
                            @Override
                            public TargetHealth getLastKnownTargetHealth(long targetOid) {
                                return TargetHealth.newBuilder().setHealthState(HealthState.CRITICAL).build();
                            }
                        }).getStitchingGraph();
        Assert.assertTrue(criticalStateGraph.entities().allMatch(StitchingEntity::isStale));

        final TopologyStitchingGraph majorStateGraph =
                entityStore.constructStitchingContext(new StalenessInformationProvider() {
                    @Override
                    public TargetHealth getLastKnownTargetHealth(long targetOid) {
                        return TargetHealth.newBuilder().setHealthState(HealthState.MAJOR).build();
                    }
                }).getStitchingGraph();
        Assert.assertTrue(majorStateGraph.entities().noneMatch(StitchingEntity::isStale));

        final TopologyStitchingGraph minorStateGraph =
                entityStore.constructStitchingContext(new StalenessInformationProvider() {
                    @Override
                    public TargetHealth getLastKnownTargetHealth(long targetOid) {
                        return TargetHealth.newBuilder().setHealthState(HealthState.MINOR).build();
                    }
                }).getStitchingGraph();
        Assert.assertTrue(minorStateGraph.entities().noneMatch(StitchingEntity::isStale));

        final TopologyStitchingGraph normalStateGraph =
                entityStore.constructStitchingContext(new StalenessInformationProvider() {
                    @Override
                    public TargetHealth getLastKnownTargetHealth(long targetOid) {
                        return TargetHealth.newBuilder().setHealthState(HealthState.NORMAL).build();
                    }
                }).getStitchingGraph();
        Assert.assertTrue(normalStateGraph.entities().noneMatch(StitchingEntity::isStale));
    }
}
