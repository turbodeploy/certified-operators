package com.vmturbo.group.group;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.common.CloudTypeEnum.CloudType;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithOnlyEnvironmentTypeAndTargets;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.util.ImmutableThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ImmutableThinTargetInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * Unit tests for {@link GroupEnvironmentTypeResolver}.
 */
public class GroupEnvironmentTypeResolverTest {

    private final ThinTargetCache targetCache = Mockito.mock(ThinTargetCache.class);

    private List<ThinTargetInfo> targets;

    private IGroupStore groupStore = mock(GroupDAO.class);

    private final GroupEnvironmentTypeResolver resolver =
            new GroupEnvironmentTypeResolver(targetCache);

    private static final ThinTargetCache.ThinTargetInfo AWS_TARGET = ImmutableThinTargetInfo.builder()
            .probeInfo(ImmutableThinProbeInfo.builder()
                    .category(ProbeCategory.CLOUD_MANAGEMENT.getCategoryInUpperCase())
                    .uiCategory(ProbeCategory.PUBLIC_CLOUD.getCategoryInUpperCase())
                    .type(SDKProbeType.AWS.getProbeType())
                    .oid(111111L)
                    .build())
            .displayName("Aws target")
            .oid(11111L)
            .isHidden(false)
            .build();
    private static final ThinTargetCache.ThinTargetInfo AWS_BILLING_TARGET = ImmutableThinTargetInfo.builder()
            .probeInfo(ImmutableThinProbeInfo.builder()
                    .category(ProbeCategory.CLOUD_MANAGEMENT.getCategoryInUpperCase())
                    .uiCategory(ProbeCategory.PUBLIC_CLOUD.getCategoryInUpperCase())
                    .type(SDKProbeType.AWS_BILLING.getProbeType())
                    .oid(111112L)
                    .build())
            .displayName("AWS Billing target")
            .oid(11112L)
            .isHidden(false)
            .build();
    private static final ThinTargetCache.ThinTargetInfo VC_TARGET = ImmutableThinTargetInfo.builder()
            .probeInfo(ImmutableThinProbeInfo.builder()
                    .category(ProbeCategory.HYPERVISOR.getCategoryInUpperCase())
                    .uiCategory(ProbeCategory.HYPERVISOR.getCategoryInUpperCase())
                    .type(SDKProbeType.VCENTER.getProbeType())
                    .oid(111113L)
                    .build())
            .displayName("VC target")
            .oid(11113L)
            .isHidden(false)
            .build();
    private static final ThinTargetCache.ThinTargetInfo AZURE_TARGET = ImmutableThinTargetInfo.builder()
            .probeInfo(ImmutableThinProbeInfo.builder()
                    .category(ProbeCategory.CLOUD_MANAGEMENT.getCategoryInUpperCase())
                    .uiCategory(ProbeCategory.PUBLIC_CLOUD.getCategoryInUpperCase())
                    .type(SDKProbeType.AZURE.getProbeType())
                    .oid(111114L)
                    .build())
            .displayName("Azure Target")
            .oid(11114L)
            .isHidden(false)
            .build();
    private static final ThinTargetCache.ThinTargetInfo APPD_TARGET = ImmutableThinTargetInfo.builder()
            .probeInfo(ImmutableThinProbeInfo.builder()
                    .category(ProbeCategory.GUEST_OS_PROCESSES.getCategoryInUpperCase())
                    .uiCategory(ProbeCategory.PUBLIC_CLOUD.getCategoryInUpperCase())
                    .type(SDKProbeType.APPDYNAMICS.getProbeType())
                    .oid(11115L)
                    .build())
            .displayName("AppD Target")
            .oid(11115L)
            .isHidden(false)
            .build();

    /**
     * Common setup for tests.
     */
    @Before
    public void setup() {
        targets = new ArrayList<>();
        targets.add(VC_TARGET);
        targets.add(AZURE_TARGET);
        targets.add(AWS_TARGET);
        targets.add(AWS_BILLING_TARGET);
        targets.add(APPD_TARGET);
        Mockito.when(targetCache.getAllTargets()).thenReturn(targets);
        Mockito.when(targetCache.getTargetInfo(Mockito.anyLong()))
                .thenAnswer(invocation -> targets.stream()
                        .filter(target -> target.oid() == invocation.getArgumentAt(0, Long.class))
                        .findFirst());
    }

    /**
     * On prem entities.
     */
    @Test
    public void testOnlyOnPremEntities() {
        // GIVEN
        Set<EntityWithOnlyEnvironmentTypeAndTargets> entities = new HashSet<>(2);
        entities.add(EntityWithOnlyEnvironmentTypeAndTargets.newBuilder()
                .setOid(1L)
                .setEnvironmentType(EnvironmentType.ON_PREM)
                .addDiscoveringTargetIds(VC_TARGET.oid())
                .build());
        entities.add(EntityWithOnlyEnvironmentTypeAndTargets.newBuilder()
                .setOid(2L)
                .setEnvironmentType(EnvironmentType.ON_PREM)
                .addDiscoveringTargetIds(VC_TARGET.oid())
                .build());
        // WHEN
        final GroupEnvironment groupEnvironment = resolver.getEnvironmentAndCloudTypeForGroup(groupStore, 1L,
                entities, ArrayListMultimap.create());
        // THEN
        Assert.assertEquals(EnvironmentType.ON_PREM, groupEnvironment.getEnvironmentType());
        Assert.assertEquals(CloudType.UNKNOWN_CLOUD, groupEnvironment.getCloudType());
    }

    /**
     * Cloud entities (same cloud type).
     */
    @Test
    public void testOnlyCloudEntitiesSameCloudType() {
        // GIVEN
        Set<EntityWithOnlyEnvironmentTypeAndTargets> entities = new HashSet<>(2);
        entities.add(EntityWithOnlyEnvironmentTypeAndTargets.newBuilder()
                .setOid(1L)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .addDiscoveringTargetIds(AZURE_TARGET.oid())
                .build());
        entities.add(EntityWithOnlyEnvironmentTypeAndTargets.newBuilder()
                .setOid(2L)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .addDiscoveringTargetIds(AZURE_TARGET.oid())
                .build());
        // WHEN
        final GroupEnvironment groupEnvironment = resolver.getEnvironmentAndCloudTypeForGroup(groupStore, 1L,
                entities, ArrayListMultimap.create());
        // THEN
        Assert.assertEquals(EnvironmentType.CLOUD, groupEnvironment.getEnvironmentType());
        Assert.assertEquals(CloudType.AZURE, groupEnvironment.getCloudType());
    }

    /**
     * Cloud entities (hybrid cloud type).
     */
    @Test
    public void testOnlyCloudEntitiesHybridCloudType() {
        // GIVEN
        Set<EntityWithOnlyEnvironmentTypeAndTargets> entities = new HashSet<>(2);
        entities.add(EntityWithOnlyEnvironmentTypeAndTargets.newBuilder()
                .setOid(1L)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .addDiscoveringTargetIds(AZURE_TARGET.oid())
                .build());
        entities.add(EntityWithOnlyEnvironmentTypeAndTargets.newBuilder()
                .setOid(2L)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .addDiscoveringTargetIds(AWS_TARGET.oid())
                .build());
        // WHEN
        final GroupEnvironment groupEnvironment = resolver.getEnvironmentAndCloudTypeForGroup(groupStore, 1L,
                entities, ArrayListMultimap.create());
        // THEN
        Assert.assertEquals(EnvironmentType.CLOUD, groupEnvironment.getEnvironmentType());
        Assert.assertEquals(CloudType.HYBRID_CLOUD, groupEnvironment.getCloudType());
    }

    /**
     * Both on prem and cloud entities (single cloud type).
     */
    @Test
    public void testBothOnPremAndCloudEntitiesSingleCloudType() {
        // GIVEN
        Set<EntityWithOnlyEnvironmentTypeAndTargets> entities = new HashSet<>(2);
        entities.add(EntityWithOnlyEnvironmentTypeAndTargets.newBuilder()
                .setOid(1L)
                .setEnvironmentType(EnvironmentType.ON_PREM)
                .addDiscoveringTargetIds(VC_TARGET.oid())
                .build());
        entities.add(EntityWithOnlyEnvironmentTypeAndTargets.newBuilder()
                .setOid(2L)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .addDiscoveringTargetIds(AZURE_TARGET.oid())
                .build());
        // WHEN
        final GroupEnvironment groupEnvironment = resolver.getEnvironmentAndCloudTypeForGroup(groupStore, 1L,
                entities, ArrayListMultimap.create());
        // THEN
        Assert.assertEquals(EnvironmentType.HYBRID, groupEnvironment.getEnvironmentType());
        Assert.assertEquals(CloudType.AZURE, groupEnvironment.getCloudType());
    }

    /**
     * Both on prem and cloud entities (hybrid cloud type).
     */
    @Test
    public void testBothOnPremAndCloudEntitiesHybridCloudType() {
        // GIVEN
        Set<EntityWithOnlyEnvironmentTypeAndTargets> entities = new HashSet<>(3);
        entities.add(EntityWithOnlyEnvironmentTypeAndTargets.newBuilder()
                .setOid(1L)
                .setEnvironmentType(EnvironmentType.ON_PREM)
                .addDiscoveringTargetIds(VC_TARGET.oid())
                .build());
        entities.add(EntityWithOnlyEnvironmentTypeAndTargets.newBuilder()
                .setOid(2L)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .addDiscoveringTargetIds(AZURE_TARGET.oid())
                .build());
        entities.add(EntityWithOnlyEnvironmentTypeAndTargets.newBuilder()
                .setOid(3L)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .addDiscoveringTargetIds(AWS_TARGET.oid())
                .build());
        // WHEN
        final GroupEnvironment groupEnvironment = resolver.getEnvironmentAndCloudTypeForGroup(groupStore, 1L,
                entities, ArrayListMultimap.create());
        // THEN
        Assert.assertEquals(EnvironmentType.HYBRID, groupEnvironment.getEnvironmentType());
        Assert.assertEquals(CloudType.HYBRID_CLOUD, groupEnvironment.getCloudType());
    }

    /**
     * ARM members (discovered by ARM target -> hybrid target).
     */
    @Test
    public void testArmEntities() {
        // GIVEN
        Set<EntityWithOnlyEnvironmentTypeAndTargets> entities = new HashSet<>(1);
        entities.add(EntityWithOnlyEnvironmentTypeAndTargets.newBuilder()
                .setOid(1L)
                .setEnvironmentType(EnvironmentType.HYBRID)
                .addDiscoveringTargetIds(APPD_TARGET.oid())
                .build());
        // WHEN
        final GroupEnvironment groupEnvironment = resolver.getEnvironmentAndCloudTypeForGroup(groupStore, 1L,
                entities, ArrayListMultimap.create());
        // THEN
        Assert.assertEquals(EnvironmentType.HYBRID, groupEnvironment.getEnvironmentType());
        Assert.assertEquals(CloudType.UNKNOWN_CLOUD, groupEnvironment.getCloudType());
    }

    /**
     * A cloud entity in a group that belongs to a Cloud target and a non-cloud target.
     */
    @Test
    public void testCloudEntityStitchedByNonCloudTarget() {
        // GIVEN
        Set<EntityWithOnlyEnvironmentTypeAndTargets> entities = new HashSet<>(2);
        entities.add(EntityWithOnlyEnvironmentTypeAndTargets.newBuilder()
                .setOid(1L)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .addDiscoveringTargetIds(AWS_TARGET.oid())
                .addDiscoveringTargetIds(APPD_TARGET.oid())
                .build());
        // WHEN
        final GroupEnvironment groupEnvironment = resolver.getEnvironmentAndCloudTypeForGroup(groupStore, 1L,
                entities, ArrayListMultimap.create());
        // THEN
        Assert.assertEquals(EnvironmentType.CLOUD, groupEnvironment.getEnvironmentType());
        Assert.assertEquals(CloudType.AWS, groupEnvironment.getCloudType());
    }

    /**
     * Empty discovered on prem group.
     */
    @Test
    public void testEmptyDiscoveredOnPremGroup() {
        // GIVEN
        final long groupId = 1L;
        Multimap<Long, Long> discoveredGroups = ArrayListMultimap.create();
        discoveredGroups.put(groupId, VC_TARGET.oid());
        when(groupStore.getGroupType(groupId)).thenReturn(GroupType.REGULAR);
        Table<Long, MemberType, Boolean> expectedTypes = HashBasedTable.create();
        expectedTypes.put(groupId, MemberType.newBuilder().setEntity(14).build(), true);
        when(groupStore.getExpectedMemberTypesForGroup(groupId)).thenReturn(expectedTypes);
        // WHEN
        final GroupEnvironment groupEnvironment = resolver.getEnvironmentAndCloudTypeForGroup(groupStore,
                groupId, new HashSet<>(), discoveredGroups);
        // THEN
        Assert.assertEquals(EnvironmentType.ON_PREM, groupEnvironment.getEnvironmentType());
        Assert.assertEquals(CloudType.UNKNOWN_CLOUD, groupEnvironment.getCloudType());
    }

    /**
     * Empty discovered cloud group.
     */
    @Test
    public void testEmptyDiscoveredCloudGroup() {
        // GIVEN
        final long groupId = 1L;
        Multimap<Long, Long> discoveredGroups = ArrayListMultimap.create();
        discoveredGroups.put(groupId, AWS_TARGET.oid());
        when(groupStore.getGroupType(groupId)).thenReturn(GroupType.REGULAR);
        Table<Long, MemberType, Boolean> expectedTypes = HashBasedTable.create();
        expectedTypes.put(groupId, MemberType.newBuilder().setEntity(10).build(), true);
        when(groupStore.getExpectedMemberTypesForGroup(groupId)).thenReturn(expectedTypes);
        // WHEN
        final GroupEnvironment groupEnvironment = resolver.getEnvironmentAndCloudTypeForGroup(groupStore,
                groupId, new HashSet<>(), discoveredGroups);
        // THEN
        Assert.assertEquals(EnvironmentType.CLOUD, groupEnvironment.getEnvironmentType());
        Assert.assertEquals(CloudType.AWS, groupEnvironment.getCloudType());
    }

    /**
     * Empty resource group.
     */
    @Test
    public void testEmptyResourceGroup() {
        // GIVEN
        final long groupId = 1L;
        when(groupStore.getGroupType(groupId)).thenReturn(GroupType.RESOURCE);
        Table<Long, MemberType, Boolean> expectedTypes = HashBasedTable.create();
        expectedTypes.put(groupId, MemberType.newBuilder().setEntity(10).build(), true);
        when(groupStore.getExpectedMemberTypesForGroup(groupId)).thenReturn(expectedTypes);
        // WHEN
        final GroupEnvironment groupEnvironment = resolver.getEnvironmentAndCloudTypeForGroup(groupStore,
                groupId, new HashSet<>(), ArrayListMultimap.create());
        // THEN
        Assert.assertEquals(EnvironmentType.CLOUD, groupEnvironment.getEnvironmentType());
        Assert.assertEquals(CloudType.UNKNOWN_CLOUD, groupEnvironment.getCloudType());
    }

    /**
     * Empty billing family.
     */
    @Test
    public void testEmptyBillingFamily() {
        // GIVEN
        final long groupId = 1L;
        when(groupStore.getGroupType(groupId)).thenReturn(GroupType.BILLING_FAMILY);
        Table<Long, MemberType, Boolean> expectedTypes = HashBasedTable.create();
        expectedTypes.put(groupId, MemberType.newBuilder().setEntity(10).build(), true);
        when(groupStore.getExpectedMemberTypesForGroup(groupId)).thenReturn(expectedTypes);
        // WHEN
        final GroupEnvironment groupEnvironment = resolver.getEnvironmentAndCloudTypeForGroup(groupStore,
                groupId, new HashSet<>(), ArrayListMultimap.create());
        // THEN
        Assert.assertEquals(EnvironmentType.CLOUD, groupEnvironment.getEnvironmentType());
        Assert.assertEquals(CloudType.UNKNOWN_CLOUD, groupEnvironment.getCloudType());
    }

    /**
     * Empty group of resource groups.
     */
    @Test
    public void testEmptyGroupOfResourceGroups() {
        // GIVEN
        final long groupId = 1L;
        when(groupStore.getGroupType(groupId)).thenReturn(GroupType.REGULAR);
        Table<Long, MemberType, Boolean> expectedTypes = HashBasedTable.create();
        expectedTypes.put(
                groupId, MemberType.newBuilder().setGroup(GroupType.RESOURCE).build(), true);
        when(groupStore.getExpectedMemberTypesForGroup(groupId)).thenReturn(expectedTypes);
        // WHEN
        final GroupEnvironment groupEnvironment = resolver.getEnvironmentAndCloudTypeForGroup(groupStore,
                groupId, new HashSet<>(), ArrayListMultimap.create());
        // THEN
        Assert.assertEquals(EnvironmentType.CLOUD, groupEnvironment.getEnvironmentType());
        Assert.assertEquals(CloudType.UNKNOWN_CLOUD, groupEnvironment.getCloudType());
    }

    /**
     * Empty user group.
     */
    @Test
    public void testEmptyUserGroup() {
        // GIVEN
        final long groupId = 1L;
        when(groupStore.getGroupType(groupId)).thenReturn(GroupType.REGULAR);
        Table<Long, MemberType, Boolean> expectedTypes = HashBasedTable.create();
        expectedTypes.put(groupId, MemberType.newBuilder().setEntity(10).build(), true);
        when(groupStore.getExpectedMemberTypesForGroup(groupId)).thenReturn(expectedTypes);
        // WHEN
        final GroupEnvironment groupEnvironment = resolver.getEnvironmentAndCloudTypeForGroup(groupStore,
                groupId, new HashSet<>(), ArrayListMultimap.create());
        // THEN
        Assert.assertEquals(EnvironmentType.UNKNOWN_ENV, groupEnvironment.getEnvironmentType());
        Assert.assertEquals(CloudType.UNKNOWN_CLOUD, groupEnvironment.getCloudType());
    }
}
