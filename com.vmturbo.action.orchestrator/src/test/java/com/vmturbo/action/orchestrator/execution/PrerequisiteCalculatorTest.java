package com.vmturbo.action.orchestrator.execution;

import static com.vmturbo.components.common.setting.EntitySettingSpecs.IgnoreNvmePreRequisite;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.action.orchestrator.action.constraint.CoreQuotaStore;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.Prerequisite;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.PrerequisiteType;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ApplicationInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.Architecture;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualizationType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;

/**
 * Tests for calculating pre-requisites of an action.
 */
public class PrerequisiteCalculatorTest {

    private static final Long VM1 = 1L;
    private static final String LOCK_MESSAGE = "[Scope: vm1, name: vm-lock-1, notes: VM lock]";
    private final EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);

    private CoreQuotaStore coreQuotaStore = mock(CoreQuotaStore.class);

    /**
     * Setup
     */
    @Before
    public void setup() {
        Setting defaultIgnoreNvmeSetting = Setting.newBuilder().setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(false)).build();
        Map<String, Setting> settingMap = ImmutableMap.of(IgnoreNvmePreRequisite.getSettingName(), defaultIgnoreNvmeSetting);
        when(snapshot.getSettingsForEntity(VM1)).thenReturn(settingMap);
    }

    /**
     * Build a Move action.
     *
     * @param sourceId source if of an action
     * @param destinationId destination id of an action
     * @return an action
     */
    private Action buildMoveAction(final long sourceId, final long destinationId) {
        return Action.newBuilder().setId(0).setDeprecatedImportance(0)
            .setInfo(ActionInfo.newBuilder().setMove(Move.newBuilder()
                .setTarget(ActionEntity.newBuilder().setId(VM1)
                    .setType(EntityType.VIRTUAL_MACHINE_VALUE))
                .addChanges(ChangeProvider.newBuilder()
                    .setSource(ActionEntity.newBuilder()
                        .setId(sourceId).setType(EntityType.COMPUTE_TIER_VALUE))
                    .setDestination(ActionEntity.newBuilder()
                        .setId(destinationId).setType(EntityType.COMPUTE_TIER_VALUE)
                    ))))
            .setExplanation(Explanation.getDefaultInstance()).build();
    }

    /**
     * Build a general vm {@link ActionPartialEntity}.
     *
     * @param prerequisiteTypes an array of {@link PrerequisiteType}
     * @return an {@link ActionPartialEntity}
     */
    private ActionPartialEntity buildGeneralVMActionPartialEntity(PrerequisiteType... prerequisiteTypes) {
        final VirtualMachineInfo.Builder builder = VirtualMachineInfo.newBuilder();
        for (PrerequisiteType type : prerequisiteTypes) {
            switch (type) {
                case ENA:
                    builder.getDriverInfoBuilder().setHasEnaDriver(false);
                    break;
                case NVME:
                    builder.getDriverInfoBuilder().setHasNvmeDriver(false);
                    break;
                case ARCHITECTURE:
                    builder.setArchitecture(Architecture.BIT_32);
                    break;
                case LOCKS:
                    builder.setLocks(LOCK_MESSAGE);
                    break;
                case VIRTUALIZATION_TYPE:
                    builder.setVirtualizationType(VirtualizationType.HVM);
                    break;
            }
        }

        return ActionPartialEntity.newBuilder().setOid(VM1)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setVirtualMachine(builder)).build();
    }

    /**
     * Build a general compute tier {@link ActionPartialEntity}.
     *
     * @param prerequisiteTypes an array of {@link PrerequisiteType}
     * @return an {@link ActionPartialEntity}
     */
    private ActionPartialEntity buildGeneralComputeTierActionPartialEntity(
            @Nonnull final PrerequisiteType... prerequisiteTypes) {
        final ComputeTierInfo.Builder builder = ComputeTierInfo.newBuilder();
        for (PrerequisiteType type : prerequisiteTypes) {
            switch (type) {
                case ENA:
                    builder.getSupportedCustomerInfoBuilder().setSupportsOnlyEnaVms(true);
                    break;
                case NVME:
                    builder.getSupportedCustomerInfoBuilder().setSupportsOnlyNVMeVms(true);
                    break;
                case ARCHITECTURE:
                    builder.getSupportedCustomerInfoBuilder()
                        .addSupportedArchitectures(Architecture.BIT_64);
                    break;
                case VIRTUALIZATION_TYPE:
                    builder.getSupportedCustomerInfoBuilder()
                        .addSupportedVirtualizationTypes(VirtualizationType.PVM);
                    break;
            }
        }

        return ActionPartialEntity.newBuilder().setOid(0)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setComputeTier(builder)).build();
    }

    /**
     * Test {@link PrerequisiteCalculator#calculateGeneralPrerequisites}.
     */
    @Test
    public void testCalculateGeneralPrerequisitesEna() {
        final long destinationId = 1;
        final Action action = buildMoveAction(2, destinationId);
        final ActionPartialEntity target = buildGeneralVMActionPartialEntity(PrerequisiteType.ENA);
        final ActionPartialEntity destination =
            buildGeneralComputeTierActionPartialEntity(PrerequisiteType.ENA);

        doReturn(Optional.of(destination)).when(snapshot).getEntityFromOid(destinationId);

        assertThat(PrerequisiteCalculator.calculateGeneralPrerequisites(
            action, target, snapshot, ProbeCategory.CLOUD_MANAGEMENT),
            is(Collections.singleton(Prerequisite.newBuilder()
                .setPrerequisiteType(PrerequisiteType.ENA).build())));
    }

    /**
     * Test {@link PrerequisiteCalculator#calculateGeneralPrerequisites}.
     */
    @Test
    public void testCalculateGeneralPrerequisitesNVMe() {
        final long destinationId = 1;
        final Action action = buildMoveAction(2, destinationId);
        final ActionPartialEntity target = buildGeneralVMActionPartialEntity(PrerequisiteType.NVME);
        final ActionPartialEntity destination =
            buildGeneralComputeTierActionPartialEntity(PrerequisiteType.NVME);

        doReturn(Optional.of(destination)).when(snapshot).getEntityFromOid(destinationId);

        assertThat(PrerequisiteCalculator.calculateGeneralPrerequisites(
            action, target, snapshot, ProbeCategory.CLOUD_MANAGEMENT),
            is(Collections.singleton(Prerequisite.newBuilder()
                .setPrerequisiteType(PrerequisiteType.NVME).build())));
    }

    /**
     * Test {@link PrerequisiteCalculator#calculateGeneralPrerequisites}. If the ignore nvme
     * constraint is true for the VM, then we will not evaluate the nvme pre-requisite.
     */
    @Test
    public void testCalculateGeneralPrerequisitesNVMeIgnoreNvmeConstraint() {
        Setting ignoreNvmeSetting = Setting.newBuilder().setBooleanSettingValue(
            BooleanSettingValue.newBuilder().setValue(true)).build();
        Map<String, Setting> settingMap = ImmutableMap.of(
            IgnoreNvmePreRequisite.getSettingName(), ignoreNvmeSetting);
        when(snapshot.getSettingsForEntity(VM1)).thenReturn(settingMap);

        final long destinationId = 1;
        final Action action = buildMoveAction(2, destinationId);
        final ActionPartialEntity target = buildGeneralVMActionPartialEntity(PrerequisiteType.NVME);
        final ActionPartialEntity destination =
            buildGeneralComputeTierActionPartialEntity(PrerequisiteType.NVME);

        doReturn(Optional.of(destination)).when(snapshot).getEntityFromOid(destinationId);

        assertTrue(PrerequisiteCalculator.calculateGeneralPrerequisites(
            action, target, snapshot, ProbeCategory.CLOUD_MANAGEMENT).isEmpty());
    }

    /**
     * Test {@link PrerequisiteCalculator#calculateGeneralPrerequisites}.
     */
    @Test
    public void testCalculateGeneralPrerequisitesArchitecture() {
        final long destinationId = 1;
        final Action action = buildMoveAction(2, destinationId);
        final ActionPartialEntity target = buildGeneralVMActionPartialEntity(PrerequisiteType.ARCHITECTURE);
        final ActionPartialEntity destination =
            buildGeneralComputeTierActionPartialEntity(PrerequisiteType.ARCHITECTURE);

        doReturn(Optional.of(destination)).when(snapshot).getEntityFromOid(destinationId);

        assertThat(PrerequisiteCalculator.calculateGeneralPrerequisites(
            action, target, snapshot, ProbeCategory.CLOUD_MANAGEMENT),
            is(Collections.singleton(Prerequisite.newBuilder()
                .setPrerequisiteType(PrerequisiteType.ARCHITECTURE).build())));
    }

    /**
     * Test {@link PrerequisiteCalculator#calculateGeneralPrerequisites}.
     */
    @Test
    public void testCalculateGeneralPrerequisitesVirtualizationType() {
        final long destinationId = 1;
        final Action action = buildMoveAction(2, destinationId);
        final ActionPartialEntity target =
            buildGeneralVMActionPartialEntity(PrerequisiteType.VIRTUALIZATION_TYPE);
        final ActionPartialEntity destination =
            buildGeneralComputeTierActionPartialEntity(PrerequisiteType.VIRTUALIZATION_TYPE);

        doReturn(Optional.of(destination)).when(snapshot).getEntityFromOid(destinationId);

        assertThat(PrerequisiteCalculator.calculateGeneralPrerequisites(
            action, target, snapshot, ProbeCategory.CLOUD_MANAGEMENT),
            is(Collections.singleton(Prerequisite.newBuilder()
                .setPrerequisiteType(PrerequisiteType.VIRTUALIZATION_TYPE).build())));
    }

    /**
     * Tests whether prerequisite for (Azure) VM read-only lock is being set/computed correctly.
     */
    @Test
    public void testCalculateGeneralPrerequisitesLocks() {
        final long destinationId = 1;
        final Action action = buildMoveAction(2, destinationId);
        final ActionPartialEntity target = buildGeneralVMActionPartialEntity(PrerequisiteType.LOCKS);
        final ActionPartialEntity destination =
                buildGeneralComputeTierActionPartialEntity(PrerequisiteType.LOCKS);

        doReturn(Optional.of(destination)).when(snapshot).getEntityFromOid(destinationId);

        assertThat(PrerequisiteCalculator.calculateGeneralPrerequisites(
                action, target, snapshot, ProbeCategory.CLOUD_MANAGEMENT),
                is(Collections.singleton(Prerequisite.newBuilder()
                        .setPrerequisiteType(PrerequisiteType.LOCKS)
                        .setLocks(LOCK_MESSAGE)
                        .build())));
    }

    /**
     * Test {@link PrerequisiteCalculator#calculateGeneralPrerequisites}.
     */
    @Test
    public void testCalculateGeneralPrerequisitesMultiplePrerequisites() {
        final long destinationId = 1;
        final Action action = buildMoveAction(2, destinationId);
        final ActionPartialEntity target = buildGeneralVMActionPartialEntity(PrerequisiteType.ENA,
            PrerequisiteType.NVME, PrerequisiteType.ARCHITECTURE, PrerequisiteType.VIRTUALIZATION_TYPE);
        final ActionPartialEntity destination =
            buildGeneralComputeTierActionPartialEntity(PrerequisiteType.ENA,
                PrerequisiteType.NVME, PrerequisiteType.ARCHITECTURE, PrerequisiteType.VIRTUALIZATION_TYPE);

        doReturn(Optional.of(destination)).when(snapshot).getEntityFromOid(destinationId);

        assertThat(new HashSet<>(PrerequisiteCalculator.calculateGeneralPrerequisites(
            action, target, snapshot, ProbeCategory.CLOUD_MANAGEMENT)),
            is(new HashSet<>(Arrays.asList(
                Prerequisite.newBuilder().setPrerequisiteType(PrerequisiteType.ENA).build(),
                Prerequisite.newBuilder().setPrerequisiteType(PrerequisiteType.NVME).build(),
                Prerequisite.newBuilder().setPrerequisiteType(PrerequisiteType.ARCHITECTURE).build(),
                Prerequisite.newBuilder().setPrerequisiteType(PrerequisiteType.VIRTUALIZATION_TYPE).build()))));
    }

    /**
     * Test {@link PrerequisiteCalculator#calculateGeneralPrerequisites}.
     */
    @Test
    public void testCalculateGeneralPrerequisitesReturnsEmptyWithCorrectInput() {
        final long destinationId = 1;
        doReturn(Optional.empty()).when(snapshot).getEntityFromOid(destinationId);
        assertTrue(PrerequisiteCalculator.calculateGeneralPrerequisites(
            buildMoveAction(2, destinationId),
            buildGeneralVMActionPartialEntity(PrerequisiteType.ENA),
            snapshot,
            ProbeCategory.CLOUD_MANAGEMENT).isEmpty());
        verify(snapshot, times(1)).getEntityFromOid(destinationId);
    }

    /**
     * Test {@link PrerequisiteCalculator#calculateGeneralPrerequisites}.
     */
    @Test
    public void testCalculateGeneralPrerequisitesReturnsEmptyWithWrongProbeCategory() {
        assertTrue(PrerequisiteCalculator.calculateGeneralPrerequisites(
            Action.getDefaultInstance(),
            ActionPartialEntity.getDefaultInstance(),
            snapshot,
            ProbeCategory.HYPERVISOR).isEmpty());
    }

    /**
     * Test {@link PrerequisiteCalculator#calculateGeneralPrerequisites}.
     */
    @Test
    public void testCalculateGeneralPrerequisitesReturnsEmptyWithWrongActionType() {
        assertTrue(PrerequisiteCalculator.calculateGeneralPrerequisites(
            Action.newBuilder().setId(0).setDeprecatedImportance(0)
                .setInfo(ActionInfo.newBuilder().setReconfigure(Reconfigure.newBuilder()
                    .setTarget(ActionEntity.newBuilder().setId(1)
                        .setType(EntityType.VIRTUAL_MACHINE_VALUE))))
                .setExplanation(Explanation.getDefaultInstance()).build(),
            ActionPartialEntity.getDefaultInstance(),
            snapshot,
            ProbeCategory.CLOUD_MANAGEMENT).isEmpty());
    }

    /**
     * Test {@link PrerequisiteCalculator#calculateGeneralPrerequisites}.
     */
    @Test
    public void testCalculateGeneralPrerequisitesReturnsEmptyWithoutVirtualMachineInfo() {
        assertTrue(PrerequisiteCalculator.calculateGeneralPrerequisites(
            buildMoveAction(3, 0),
            ActionPartialEntity.newBuilder().setOid(2)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setApplication(ApplicationInfo.getDefaultInstance())).build(),
            snapshot,
            ProbeCategory.CLOUD_MANAGEMENT).isEmpty());
    }

    /**
     * Test {@link PrerequisiteCalculator#calculateQuotaPrerequisite}.
     */
    @Test
    public void testCalculateCoreQuotaPrerequisiteMoveToDifferentFamilyIncreaseBothRegionAndFamily() {
        final long regionId = 4;
        final String sourceFamily = "standardDSv3Family";
        final int sourceNumCores = 12;
        final String destinationFamily = "standardGSFamily";
        final int destinationNumCores = 64;
        final int regionalCoreQuota = 32;
        final int destinationFamilyCoreQuota = 24;

        final Object[] result = mockCoreQuota(sourceFamily, sourceNumCores, destinationFamily,
            destinationNumCores, regionId, regionalCoreQuota, destinationFamilyCoreQuota);

        assertThat(new HashSet<>(PrerequisiteCalculator.calculateQuotaPrerequisite(
            (Action)result[0], (ActionPartialEntity)result[1], snapshot,
            ProbeCategory.CLOUD_MANAGEMENT, coreQuotaStore)),
            is(Collections.singleton(Prerequisite.newBuilder()
                .setPrerequisiteType(PrerequisiteType.CORE_QUOTAS)
                .setRegionId(regionId)
                .setQuotaName(StringConstants.TOTAL_REGIONAL_VCPUS_QUOTA_DISPLAYNAME +
                    " and " + destinationFamily).build())));
    }

    /**
     * Test {@link PrerequisiteCalculator#calculateQuotaPrerequisite}.
     */
    @Test
    public void testCalculateCoreQuotaPrerequisiteToDifferentFamilyIncreaseOnlyRegion() {
        final long regionId = 4;
        final String sourceFamily = "standardDSv3Family";
        final int sourceNumCores = 12;
        final String destinationFamily = "standardGSFamily";
        final int destinationNumCores = 64;
        final int regionalCoreQuota = 32;
        final int destinationFamilyCoreQuota = 100;

        final Object[] result = mockCoreQuota(sourceFamily, sourceNumCores, destinationFamily,
            destinationNumCores, regionId, regionalCoreQuota, destinationFamilyCoreQuota);

        assertThat(new HashSet<>(PrerequisiteCalculator.calculateQuotaPrerequisite(
            (Action)result[0], (ActionPartialEntity)result[1], snapshot,
            ProbeCategory.CLOUD_MANAGEMENT, coreQuotaStore)),
            is(Collections.singleton(Prerequisite.newBuilder()
                .setPrerequisiteType(PrerequisiteType.CORE_QUOTAS)
                .setRegionId(regionId)
                .setQuotaName(StringConstants.TOTAL_REGIONAL_VCPUS_QUOTA_DISPLAYNAME).build())));
    }

    /**
     * Test {@link PrerequisiteCalculator#calculateQuotaPrerequisite}.
     */
    @Test
    public void testCalculateCoreQuotaPrerequisiteMoveToDifferentFamilyIncreaseOnlyFamily() {
        final long regionId = 4;
        final String sourceFamily = "standardDSv3Family";
        final int sourceNumCores = 12;
        final String destinationFamily = "standardGSFamily";
        final int destinationNumCores = 64;
        final int regionalCoreQuota = 100;
        final int destinationFamilyCoreQuota = 24;

        final Object[] result = mockCoreQuota(sourceFamily, sourceNumCores, destinationFamily,
            destinationNumCores, regionId, regionalCoreQuota, destinationFamilyCoreQuota);

        assertThat(new HashSet<>(PrerequisiteCalculator.calculateQuotaPrerequisite(
            (Action)result[0], (ActionPartialEntity)result[1], snapshot,
            ProbeCategory.CLOUD_MANAGEMENT, coreQuotaStore)),
            is(Collections.singleton(Prerequisite.newBuilder()
                .setPrerequisiteType(PrerequisiteType.CORE_QUOTAS)
                .setRegionId(regionId)
                .setQuotaName(destinationFamily).build())));
    }

    /**
     * Test {@link PrerequisiteCalculator#calculateQuotaPrerequisite}.
     */
    @Test
    public void testCalculateCoreQuotaPrerequisiteMoveToDifferentFamilyNoIncrease() {
        final long regionId = 4;
        final String sourceFamily = "standardDSv3Family";
        final int sourceNumCores = 12;
        final String destinationFamily = "standardGSFamily";
        final int destinationNumCores = 64;
        final int regionalCoreQuota = 100;
        final int destinationFamilyCoreQuota = 100;

        final Object[] result = mockCoreQuota(sourceFamily, sourceNumCores, destinationFamily,
            destinationNumCores, regionId, regionalCoreQuota, destinationFamilyCoreQuota);

        assertThat(new HashSet<>(PrerequisiteCalculator.calculateQuotaPrerequisite(
            (Action)result[0], (ActionPartialEntity)result[1], snapshot,
            ProbeCategory.CLOUD_MANAGEMENT, coreQuotaStore)),
            is(Collections.emptySet()));
    }

    /**
     * Mock snapshot and coreQuotaStore.
     *
     * @param sourceFamily the family name of the source compute tier
     * @param sourceNumCores the number of cores of the source compute tier
     * @param destinationFamily the family name of the destination compute tier
     * @param destinationNumCores the number of cores of the destination compute tier
     * @param regionId the id of the region
     * @param regionalCoreQuota  the core quota of the region
     * @param destinationFamilyCoreQuota the core quota of the destination family
     * @return action and target
     */
    private Object[] mockCoreQuota(
            final String sourceFamily, final int sourceNumCores,
            final String destinationFamily, final int destinationNumCores,
            final long regionId, final int regionalCoreQuota, final int destinationFamilyCoreQuota) {
        final long sourceId = 1;
        final long destinationId = 2;
        final long businessAccountId = 3;

        final Action action = buildMoveAction(sourceId, destinationId);
        final ActionPartialEntity target = buildCoreQuotaVMActionPartialEntity(regionId);
        final ActionPartialEntity source =
            buildCoreQuotaComputeTierActionPartialEntity(sourceNumCores, sourceFamily);
        final ActionPartialEntity destination =
            buildCoreQuotaComputeTierActionPartialEntity(destinationNumCores, destinationFamily);
        final EntityWithConnections businessAccount =
            EntityWithConnections.newBuilder().setOid(businessAccountId).build();
        final ConnectedEntity region = ConnectedEntity.newBuilder()
            .setConnectedEntityId(regionId).setConnectedEntityType(EntityType.REGION_VALUE).build();

        doReturn(Optional.of(source)).when(snapshot).getEntityFromOid(sourceId);
        doReturn(Optional.of(destination)).when(snapshot).getEntityFromOid(destinationId);
        doReturn(Optional.of(businessAccount)).when(snapshot).getOwnerAccountOfEntity(target.getOid());
        doReturn(regionalCoreQuota).when(coreQuotaStore)
            .getCoreQuota(businessAccountId, regionId, StringConstants.TOTAL_CORE_QUOTA);
        doReturn(destinationFamilyCoreQuota).when(coreQuotaStore)
            .getCoreQuota(businessAccountId, regionId, destinationFamily);

        return new Object[] {action, target};
    }

    /**
     * Build a CoreQuota vm {@link ActionPartialEntity}.
     *
     * @param regionId the id of the region
     * @return an {@link ActionPartialEntity}
     */
    private ActionPartialEntity buildCoreQuotaVMActionPartialEntity(final long regionId) {
        return ActionPartialEntity.newBuilder().setOid(0)
            .setTypeSpecificInfo(TypeSpecificInfo.getDefaultInstance())
            .addConnectedEntities(ConnectedEntity.newBuilder()
                .setConnectedEntityId(regionId).setConnectedEntityType(EntityType.REGION_VALUE))
            .build();
    }

    /**
     * Build a CoreQuota compute tier {@link ActionPartialEntity}.
     *
     * @param numCores num of cores
     * @param quotaFamily the quota family
     * @return an {@link ActionPartialEntity}
     */
    private ActionPartialEntity buildCoreQuotaComputeTierActionPartialEntity(
            final int numCores, final String quotaFamily) {
        return ActionPartialEntity.newBuilder().setOid(0)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setComputeTier(ComputeTierInfo.newBuilder()
                    .setNumCores(numCores).setQuotaFamily(quotaFamily))).build();
    }
}