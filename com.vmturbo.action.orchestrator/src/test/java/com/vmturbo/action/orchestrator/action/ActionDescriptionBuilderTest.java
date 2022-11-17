package com.vmturbo.action.orchestrator.action;

import static com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils.createReasonCommodity;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.Builder;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.Allocate;
import com.vmturbo.common.protobuf.action.ActionDTO.AtomicResize;
import com.vmturbo.common.protobuf.action.ActionDTO.BuyRI;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.AtomicResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation.CommodityNewCapacityEntry;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionBySupplyExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure.SettingChange;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity.ActionEntityTypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity.ActionEntityTypeSpecificInfo.ActionVirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.commons.Units;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit test for {@link ActionDescriptionBuilder}.
 */
public class ActionDescriptionBuilderTest {

    private ActionDTO.Action moveRecommendation;
    private ActionDTO.Action scaleRecommendation;
    private ActionDTO.Action cloudVolumeScaleProviderChangeRecommendation;
    private ActionDTO.Action cloudDBScaleProviderChangeRecommendation;
    private ActionDTO.Action cloudVolumeScaleCommodityChangeRecommendation;
    private ActionDTO.Action scaleTypeRecommendation;
    private ActionDTO.Action resizeRecommendation;
    private ActionDTO.Action resizeMemRecommendation;
    private static final Long SWITCH1_ID = 77L;
    private ActionDTO.Action resizeVStorageRecommendation;
    private ActionDTO.Action resizeMemReservationRecommendation;
    private ActionDTO.Action resizeVcpuRecommendationForVM;
    private ActionDTO.Action resizeVcpuReservationRecommendationForVM;
    private ActionDTO.Action resizeVcpuRecommendationForContainer;
    private ActionDTO.Action resizeVcpuReservationRecommendationForContainer;
    private ActionDTO.Action resizeVemRequestRecommendationForContainer;
    private ActionDTO.Action resizeVcpuLimitQuotaRecommendationForNamespace;
    private ActionDTO.Action resizeVmemLimitQuotaRecommendationForNamespace;
    private ActionDTO.Action resizeStorageAmountRecommendationForVSanStorageUp;
    private ActionDTO.Action resizeStorageAmountRecommendationForVSanStorageDown;
    private ActionDTO.Action deactivateRecommendation;
    private ActionDTO.Action activateRecommendation;
    private ActionDTO.Action reconfigureReasonCommoditiesRecommendation;
    private ActionDTO.Action reconfigureTaintCommoditiesRecommendation;
    private ActionDTO.Action reconfigureLabelCommoditiesRecommendation;
    private ActionDTO.Action reconfigureReasonSettingsRecommendation;
    private ActionDTO.Action reconfigureWithoutSourceRecommendation;
    private ActionDTO.Action provisionBySupplyRecommendation;
    private ActionDTO.Action provisionByDemandRecommendation;
    private ActionDTO.Action deleteRecommendation;
    private ActionDTO.Action deleteVirtualMachineSpecRecommendation;
    private ActionDTO.Action deleteCloudStorageRecommendation;
    private ActionDTO.Action deleteCloudStorageRecommendationWithNoSourceEntity;
    private ActionDTO.Action buyRIRecommendation;
    private ActionDTO.Action allocateRecommendation;

    /**
     * Cloud->Cloud migration related action for compute.
     */
    private ActionDTO.Action cloudToCloudComputeMove;

    /**
     * Cloud->Cloud migration related action for storage.
     */
    private ActionDTO.Action cloudToCloudStorageMove;

    /**
     * onPrem->Cloud migration related action for compute.
     */
    private ActionDTO.Action onPremToCloudComputeMove;

    /**
     * onPrem->Cloud migration related action for storage.
     */
    private ActionDTO.Action onPremToCloudStorageMove;

    /**
     * Cloud zone (AWS) to region (Azure) compute move.
     */
    private ActionDTO.Action zoneToRegionComputeMove;

    private static final ReasonCommodity BALLOONING = createReasonCommodity(CommodityDTO.CommodityType.BALLOONING_VALUE, null);
    private static final ReasonCommodity CPU_ALLOCATION = createReasonCommodity(CommodityDTO.CommodityType.CPU_ALLOCATION_VALUE, null);
    private static final ReasonCommodity TAINT = createReasonCommodity(CommodityDTO.CommodityType.TAINT_VALUE, "key1=value1:NoSchedule");
    private static final ReasonCommodity LABEL = createReasonCommodity(CommodityDTO.CommodityType.LABEL_VALUE, "key1=value1");
    private static final Long VM1_ID = 11L;
    private static final String VM1_DISPLAY_NAME = "vm1_test";
    private static final Long PM_SOURCE_ID = 22L;
    private static final String PM_SOURCE_DISPLAY_NAME = "pm_source_test";
    private static final Long PM_DESTINATION_ID = 33L;
    private static final String PM_DESTINATION_DISPLAY_NAME = "pm_destination_test";
    private static final Long ST_SOURCE_ID = 44L;
    private static final String ST_SOURCE_DISPLAY_NAME = "storage_source_test";
    private static final Long ST_DESTINATION_ID = 55L;
    private static final String ST_DESTINATION_DISPLAY_NAME = "storage_destination_test";
    private static final Long VV_ID = 66L;
    private static final String VV_DISPLAY_NAME = "volume_display_name";
    private static final Long VV_TIER_ID = 88L;
    private static final String VV_TIER_DISPLAY_NAME = "GP2";
    private static final Long DB_SOURCE_ID = 45L;
    private static final String DB_SOURCE_DISPLAY_NAME = "db_source_test";
    private static final Long DB_DESTINATION_ID = 56L;
    private static final String DB_DESTINATION_DISPLAY_NAME = "db_destination_test";
    private static final Long DB_ID = 77L;
    private static final String DB_DISPLAY_NAME = "db_display_name";
    private static final Long COMPUTE_TIER_SOURCE_ID = 100L;
    private static final String COMPUTE_TIER_SOURCE_DISPLAY_NAME = "tier_t1";
    private static final Long COMPUTE_TIER_DESTINATION_ID = 200L;
    private static final String COMPUTE_TIER_DESTINATION_DISPLAY_NAME = "tier_t2";
    private static final Long MASTER_ACCOUNT_ID = 101L;
    private static final String MASTER_ACCOUNT_DISPLAY_NAME = "my.super.account";
    private static final Long REGION_ID = 102L;
    private static final String REGION_DISPLAY_NAME = "Manhattan";
    private static final Long CONTAINER1_ID = 11L;
    private static final String CONTAINER1_DISPLAY_NAME = "container1_test";
    private static final Long CONTAINER_POD_ID = 222L;
    private static final String CONTAINER_POD_DISPLAY_NAME = "container_pod_test";
    private static final Long VM_SOURCE_ID = 777L;
    private static final String VM_SOURCE_DISPLAY_NAME = "vm_source_test";
    private static final Long VSAN_STORAGE_ID = 333L;
    private static final String VSAN_STORAGE_DISPLAY_NAME = "vsan_storage";
    private static final Long VSAN_PM_ID = 444L;
    private static final String VSAN_PM_DISPLAY_NAME = "vsan_host";
    private static final String SWITCH1_DISPLAY_NAME = "switch1_test";
    private static final Long CONTROLLER1_ID = 500L;
    private static final String CONTROLLER1_DISPLAY_NAME = "controller1_test";
    private static final Long CONTAINER_SPEC1_ID = 501L;
    private static final String SPEC1_DISPLAY_NAME = "spec1_test";
    private static final Long CONTAINER_SPEC2_ID = 502L;
    private static final String SPEC2_DISPLAY_NAME = "spec2_test";
    private static final String VSTORAGE_KEY = "1";
    private static final long NAMESPACE_ID = 601L;
    private static final String NAMESPACE_DISPLAY_NAME = "namespace_test";
    private static final long BUSINESS_ACCOUNT_OID = 88L;
    private static final String BUSINESS_ACCOUNT_NAME = "Development";
    private static final Long CT_SOURCE_ID = 77L;
    private static final String CT_SOURCE_DISPLAY_NAME = "compute_source_test";
    private static final long VM_SPEC_ID = 99L;
    private static final String VM_SPEC_DISPLAY_NAME = "vm_spec_test";

    /**
     * ID of Azure VM being used for cloud->cloud migration.
     */
    private static final Long AZURE_VM_ID = 73320335294003L;

    /**
     * Name of Azure VM being migrated.
     */
    private static final String AZURE_VM_NAME = "Enlin-Demo-VM";

    /**
     * ID of AWS VM being used for cloud->cloud migration.
     */
    private static final Long AWS_VM_ID = 73320835644076L;

    /**
     * Name of AWS VM being migrated.
     */
    private static final String AWS_VM_NAME = "Turbo-datadog";

    /**
     * ID of onPrem VM being used for onPrem->cloud migration.
     */
    private static final Long ON_PREM_VM_ID = 73433887038912L;

    /**
     * Name of onPrem VM being migrated.
     */
    private static final String ON_PREM_VM_NAME = "Tomcat-172.216";

    /**
     * ID of PhysicalMachine that onPrem VM is on before migration.
     */
    private static final Long ON_PREM_SOURCE_PM_ID = 73433887033680L;

    /**
     * Name of PhysicalMachine that onPrem VM is on before migration.
     */
    private static final String ON_PREM_SOURCE_PM_NAME = "dc17-host-04.eng.vmturbo.com";

    /**
     * ID of onPrem storage being cloud migrated from.
     */
    private static final Long ON_PREM_SOURCE_STORAGE_ID = 73433887031974L;

    /**
     * Name of onPrem storage being cloud migrated from.
     */
    private static final String ON_PREM_SOURCE_STORAGE_NAME = "NIMHF40:ACMDS1";

    /**
     * ID of onPrem volume one (disk) that is being migrated to cloud.
     */
    private static final Long ON_PREM_VOLUME_RESOURCE_ID = 73433887060876L;

    /**
     * ID of onPrem volume two (disk) that is being migrated to cloud (same storage as the first).
     */
    private static final Long ON_PREM_VOLUME_RESOURCE_ID_2 = 157L;

    /**
     * Name of onPrem volume one (disk) that is being migrated to cloud.
     */
    private static final String ON_PREM_VOLUME_RESOURCE_NAME = "Vol-Tomcat-172.216-NIMHF40:ACMDS1";

    /**
     * Name of onPrem volume two (disk) that is being migrated to cloud.
     */
    private static final String ON_PREM_VOLUME_RESOURCE_NAME_2 = "Disk 2";

    /**
     * Source compute tier id being migrated from.
     */
    private static final Long CLOUD_SOURCE_COMPUTE_TIER_ID = 73320334249387L;

    /**
     * Source compute tier name being migrated from.
     */
    private static final String CLOUD_SOURCE_COMPUTE_TIER_NAME = "Standard_E2s_v3";

    /**
     * Destination compute tier id being migrated to.
     */
    private static final Long CLOUD_DESTINATION_COMPUTE_TIER_ID = 73320835643834L;

    /**
     * Destination compute tier name being migrated to.
     */
    private static final String CLOUD_DESTINATION_COMPUTE_TIER_NAME = "r5a.large";

    /**
     * Source storage tier id being migrated from.
     */
    private static final Long CLOUD_SOURCE_STORAGE_TIER_ID = 73320334249139L;

    /**
     * Source storage tier name being migrated from.
     */
    private static final String CLOUD_SOURCE_STORAGE_TIER_NAME = "MANAGED_PREMIUM";

    /**
     * Destination storage tier id being migrated to.
     */
    private static final Long CLOUD_DESTINATION_STORAGE_TIER_ID = 73320835644404L;

    /**
     * Destination storage tier name being migrated to.
     */
    private static final String CLOUD_DESTINATION_STORAGE_TIER_NAME = "GP2";

    /**
     * Azure source or destination region id for cloud migration.
     */
    private static final Long AZURE_REGION_ID = 73320334248977L;

    /**
     * Azure source or destination region name for cloud migration.
     */
    private static final String AZURE_REGION_NAME = "azure-East US";

    /**
     * AWS region id being migrated to.
     */
    private static final Long AWS_REGION_ID = 73320835643948L;

    /**
     * AWS region name being migrated to.
     */
    private static final String AWS_REGION_NAME = "aws-EU (Ireland)";

    /**
     * AWS zone id being migrated from.
     */
    private static final Long AWS_ZONE_ID = 73320835643859L;

    /**
     * AWS zone name being migrated from.
     */
    private static final String AWS_ZONE_NAME = "aws-us-east-1d";

    /**
     * ID of cloud volume (disk) that is being migrated.
     */
    private static final Long CLOUD_VOLUME_RESOURCE_ID = 73320335294657L;

    /**
     * Name of cloud volume (disk) that is being migrated.
     */
    private static final String CLOUD_VOLUME_RESOURCE_NAME =
            "Enlin-Demo-VM_4ed76baa3016488f82f3027dd8adb53f";

    private ActionDTO.Action resizePortChannelRecommendation;

    private EntitiesAndSettingsSnapshot entitySettingsCache = mock(EntitiesAndSettingsSnapshot.class);

    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
        moveRecommendation = makeRec(makeMoveInfo(VM1_ID, PM_SOURCE_ID, EntityType.PHYSICAL_MACHINE.getNumber(),
                PM_DESTINATION_ID, EntityType.PHYSICAL_MACHINE.getNumber()), SupportLevel.SUPPORTED).build();
        Builder action = makeScaleInfoWithProviderChangeOnly(DB_ID, DB_SOURCE_ID, EntityType.DATABASE_TIER.getNumber(), DB_DESTINATION_ID,
                EntityType.DATABASE_TIER.getNumber());
        action = addResizeCommodityToAction(action, CommodityDTO.CommodityType.STORAGE_AMOUNT);
        cloudDBScaleProviderChangeRecommendation = makeRec(action, SupportLevel.SUPPORTED).build();
        cloudVolumeScaleProviderChangeRecommendation = makeRec(
                makeScaleInfoWithProviderChangeOnly(VV_ID, ST_SOURCE_ID,
                        EntityType.STORAGE_TIER.getNumber(), ST_DESTINATION_ID,
                        EntityType.STORAGE_TIER.getNumber()), SupportLevel.SUPPORTED).build();
        cloudVolumeScaleCommodityChangeRecommendation = makeRec(
                makeCloudVolumeScaleInfoWithCommodityChangeOnly(VV_ID), SupportLevel.SUPPORTED).build();
        scaleRecommendation = makeRec(makeMoveInfo(VM1_ID, COMPUTE_TIER_SOURCE_ID, EntityType.COMPUTE_TIER.getNumber(),
                COMPUTE_TIER_DESTINATION_ID, EntityType.COMPUTE_TIER.getNumber()), SupportLevel.SUPPORTED).build();
        scaleTypeRecommendation = makeRec(makeScaleInfo(VM1_ID, 0, 0, COMPUTE_TIER_SOURCE_ID,
                EntityType.COMPUTE_TIER.getNumber(), COMPUTE_TIER_DESTINATION_ID, EntityType.COMPUTE_TIER.getNumber()), SupportLevel.SUPPORTED).build();
        resizeRecommendation = makeRec(makeResizeInfo(VM1_ID), SupportLevel.SUPPORTED).build();
        resizeMemRecommendation = makeRec(makeResizeMemInfo(VM1_ID), SupportLevel.SUPPORTED).build();
        resizePortChannelRecommendation = makeRec(makeResizeInfo(SWITCH1_ID, CommodityDTO.CommodityType.PORT_CHANEL_VALUE,
                "PortChannelFI-IO:10.0.100.132:sys/switch-A/sys/chassis-1/slot-1", 1000, 2000), SupportLevel.SUPPORTED).build();
        resizeVStorageRecommendation = makeRec(
                makeResizeInfo(VM1_ID, CommodityDTO.CommodityType.VSTORAGE_VALUE, VSTORAGE_KEY, 2000, 4000),
                SupportLevel.SUPPORTED).build();
        resizeMemReservationRecommendation = makeRec(makeResizeReservationMemInfo(VM1_ID),
                SupportLevel.SUPPORTED).build();
        resizeVcpuRecommendationForVM =
                        makeRec(makeResizeInfo(VM1_ID, CommodityDTO.CommodityType.VCPU_VALUE, null,
                                        16, 8, 1, 1), SupportLevel.SUPPORTED).build();
        resizeVcpuReservationRecommendationForVM = makeRec(
                makeResizeReservationVcpuInfo(VM1_ID, 16, 8), SupportLevel.SUPPORTED).build();
        resizeVcpuRecommendationForContainer = makeRec(makeResizeVcpuInfo(CONTAINER1_ID, 16.111f, 8.111f), SupportLevel.SUPPORTED).build();
        resizeVcpuReservationRecommendationForContainer = makeRec(
                makeResizeReservationVcpuInfo(CONTAINER1_ID, 16.111f, 8.111f), SupportLevel.SUPPORTED).build();
        resizeVemRequestRecommendationForContainer = makeRec(makeVMemRequestInfo(CONTAINER1_ID, 3200f, 2200f), SupportLevel.SUPPORTED).build();

        resizeVcpuLimitQuotaRecommendationForNamespace = makeRec(makeResizeVCPULimitQuotaInfo(NAMESPACE_ID, 100f, 200f),
                SupportLevel.SUPPORTED).build();
        resizeVmemLimitQuotaRecommendationForNamespace = makeRec(makeResizeVMemLimitQuotaInfo(NAMESPACE_ID, 131072f, 262144f),
                SupportLevel.SUPPORTED).build();

        resizeStorageAmountRecommendationForVSanStorageUp = makeRec(makeVSanStorageAmountRequestInfo(51200f, 76800f), SupportLevel.SUPPORTED).build();

        resizeStorageAmountRecommendationForVSanStorageDown = makeRec(
                makeVSanStorageAmountRequestInfo(76800f, 51200f), SupportLevel.SUPPORTED).build();

        deactivateRecommendation = makeRec(makeDeactivateInfo(VM1_ID), SupportLevel.SUPPORTED).build();
        activateRecommendation = makeRec(makeActivateInfo(VM1_ID), SupportLevel.SUPPORTED).build();

        Explanation reconfigureExplanation1 = Explanation.newBuilder().setReconfigure(
                makeReconfigureReasonCommoditiesExplanation()).build();
        reconfigureReasonCommoditiesRecommendation = makeRec(
                makeReconfigureInfo(VM1_ID, PM_SOURCE_ID), SupportLevel.SUPPORTED,
                reconfigureExplanation1).build();
        Explanation reconfigureExplanation2 = Explanation.newBuilder().setReconfigure(
                makeReconfigureReasonSettingsExplanation()).build();
        reconfigureReasonSettingsRecommendation = makeRec(makeReconfigureInfo(VM1_ID, PM_SOURCE_ID),
                SupportLevel.SUPPORTED, reconfigureExplanation2).build();
        reconfigureWithoutSourceRecommendation = makeRec(makeReconfigureInfo(VM1_ID), SupportLevel.SUPPORTED).build();
        Explanation reconfigureExplanationTaint = Explanation.newBuilder().setReconfigure(
                makeReconfigureTaintCommoditiesExplanation()).build();
        reconfigureTaintCommoditiesRecommendation = makeRec(
                makeReconfigureInfo(CONTAINER_POD_ID, VM_SOURCE_ID), SupportLevel.SUPPORTED,
                reconfigureExplanationTaint).build();

        Explanation reconfigureExplanationLabel = Explanation.newBuilder()
            .setReconfigure(makeReconfigureLabelCommoditiesExplanation())
            .build();
        reconfigureLabelCommoditiesRecommendation = makeRec(
            makeReconfigureInfo(CONTAINER_POD_ID, VM_SOURCE_ID), SupportLevel.SUPPORTED,
            reconfigureExplanationLabel).build();

        Explanation provisionExplanation1 = Explanation.newBuilder().setProvision(
                makeProvisionBySupplyExplanation()).build();
        provisionBySupplyRecommendation = makeRec(makeProvisionInfo(ST_SOURCE_ID), SupportLevel.SUPPORTED, provisionExplanation1).build();
        Explanation provisionExplanation2 = Explanation.newBuilder().setProvision(
                makeProvisionByDemandExplanation()).build();
        provisionByDemandRecommendation = makeRec(makeProvisionInfo(PM_SOURCE_ID), SupportLevel.SUPPORTED, provisionExplanation2).build();

        deleteRecommendation = makeRec(makeDeleteInfo(ST_SOURCE_ID), SupportLevel.SUPPORTED).build();
        deleteVirtualMachineSpecRecommendation = makeRec(makeDeleteVirtualMachineSpecInfo(VM_SPEC_ID, CT_SOURCE_ID), SupportLevel.SUPPORTED).build();
        deleteCloudStorageRecommendation = makeRec(makeDeleteCloudStorageInfo(VV_ID, Optional.of(ST_SOURCE_ID)),
                SupportLevel.SUPPORTED).build();
        deleteCloudStorageRecommendationWithNoSourceEntity = makeRec(makeDeleteCloudStorageInfo(VV_ID, Optional.empty()),
                SupportLevel.SUPPORTED).build();

        buyRIRecommendation = makeRec(
                makeBuyRIInfo(COMPUTE_TIER_SOURCE_ID, MASTER_ACCOUNT_ID, REGION_ID),
                SupportLevel.SUPPORTED).build();

        allocateRecommendation = makeRec(makeAllocateInfo(VM1_ID, COMPUTE_TIER_SOURCE_ID),
                SupportLevel.UNSUPPORTED).build();

        // Cloud->Cloud compute move.
        final ActionInfo.Builder cloudComputeMove = makeMoveInfo(AZURE_VM_ID,
                CLOUD_SOURCE_COMPUTE_TIER_ID, EntityType.COMPUTE_TIER_VALUE,
                CLOUD_DESTINATION_COMPUTE_TIER_ID, EntityType.COMPUTE_TIER_VALUE);
        cloudComputeMove.getMoveBuilder().addChanges(ChangeProvider.newBuilder().setSource(
                ActionEntity.newBuilder()
                        .setId(AZURE_REGION_ID)
                        .setType(EntityType.REGION_VALUE)
                        .build()).setDestination(ActionEntity.newBuilder()
                .setId(AWS_REGION_ID)
                .setType(EntityType.REGION_VALUE)
                .build()).build());
        cloudToCloudComputeMove = makeRec(cloudComputeMove, SupportLevel.SUPPORTED).build();
        makeMockEntityCondition(AZURE_VM_ID, EntityType.VIRTUAL_MACHINE_VALUE, AZURE_VM_NAME);
        makeMockEntityCondition(AZURE_REGION_ID, EntityType.REGION_VALUE, AZURE_REGION_NAME);
        makeMockEntityCondition(AWS_REGION_ID, EntityType.REGION_VALUE, AWS_REGION_NAME);
        makeMockEntityCondition(CLOUD_SOURCE_COMPUTE_TIER_ID, EntityType.COMPUTE_TIER_VALUE,
                CLOUD_SOURCE_COMPUTE_TIER_NAME);
        makeMockEntityCondition(CLOUD_DESTINATION_COMPUTE_TIER_ID, EntityType.COMPUTE_TIER_VALUE,
                CLOUD_DESTINATION_COMPUTE_TIER_NAME);

        // Cloud->Cloud storage move.
        final ActionInfo.Builder cloudStorageMove = makeMoveInfo(AZURE_VM_ID,
                Collections.singleton(CLOUD_VOLUME_RESOURCE_ID), EntityType.VIRTUAL_VOLUME_VALUE,
                CLOUD_SOURCE_STORAGE_TIER_ID, EntityType.STORAGE_TIER_VALUE,
                CLOUD_DESTINATION_STORAGE_TIER_ID, EntityType.STORAGE_TIER_VALUE);
        cloudStorageMove.getMoveBuilder().addChanges(ChangeProvider.newBuilder().setSource(
                ActionEntity.newBuilder()
                        .setId(AZURE_REGION_ID)
                        .setType(EntityType.REGION_VALUE)
                        .build()).setDestination(ActionEntity.newBuilder()
                .setId(AWS_REGION_ID)
                .setType(EntityType.REGION_VALUE)
                .build()).build());
        makeMockEntityCondition(CLOUD_SOURCE_STORAGE_TIER_ID, EntityType.STORAGE_TIER_VALUE,
                CLOUD_SOURCE_STORAGE_TIER_NAME);
        makeMockEntityCondition(CLOUD_DESTINATION_STORAGE_TIER_ID, EntityType.STORAGE_TIER_VALUE,
                CLOUD_DESTINATION_STORAGE_TIER_NAME);
        makeMockEntityCondition(CLOUD_VOLUME_RESOURCE_ID, EntityType.VIRTUAL_VOLUME_VALUE,
                CLOUD_VOLUME_RESOURCE_NAME);
        cloudToCloudStorageMove = makeRec(cloudStorageMove, SupportLevel.SUPPORTED).build();

        // onPrem->Cloud compute move.
        final ActionInfo.Builder onPremComputeMove = makeMoveInfo(ON_PREM_VM_ID,
                ON_PREM_SOURCE_PM_ID, EntityType.PHYSICAL_MACHINE_VALUE,
                CLOUD_DESTINATION_COMPUTE_TIER_ID, EntityType.COMPUTE_TIER_VALUE);
        onPremComputeMove.getMoveBuilder().addChanges(ChangeProvider.newBuilder().setSource(
                ActionEntity.newBuilder()
                        .setId(ON_PREM_SOURCE_PM_ID)
                        .setType(EntityType.PHYSICAL_MACHINE_VALUE)
                        .build()).setDestination(ActionEntity.newBuilder()
                .setId(AWS_REGION_ID)
                .setType(EntityType.REGION_VALUE)
                .build()).build());
        onPremToCloudComputeMove = makeRec(onPremComputeMove, SupportLevel.SUPPORTED).build();
        makeMockEntityCondition(ON_PREM_VM_ID, EntityType.VIRTUAL_MACHINE_VALUE, ON_PREM_VM_NAME);
        makeMockEntityCondition(ON_PREM_SOURCE_PM_ID, EntityType.PHYSICAL_MACHINE_VALUE,
                ON_PREM_SOURCE_PM_NAME);

        // onPrem->Cloud storage move.
        final ActionInfo.Builder onPremStorageMove = makeMoveInfo(ON_PREM_VM_ID,
                ImmutableSet.of(ON_PREM_VOLUME_RESOURCE_ID, ON_PREM_VOLUME_RESOURCE_ID_2),
                EntityType.VIRTUAL_VOLUME_VALUE, ON_PREM_SOURCE_STORAGE_ID, EntityType.STORAGE_VALUE, CLOUD_DESTINATION_STORAGE_TIER_ID,
                EntityType.STORAGE_TIER_VALUE);
        onPremStorageMove.getMoveBuilder().addChanges(ChangeProvider.newBuilder().setSource(
                ActionEntity.newBuilder()
                        .setId(ON_PREM_SOURCE_STORAGE_ID)
                        .setType(EntityType.STORAGE_VALUE)
                        .build()).setDestination(ActionEntity.newBuilder()
                .setId(AWS_REGION_ID)
                .setType(EntityType.REGION_VALUE)
                .build()).build());
        onPremToCloudStorageMove = makeRec(onPremStorageMove, SupportLevel.SUPPORTED).build();
        makeMockEntityCondition(ON_PREM_SOURCE_STORAGE_ID, EntityType.STORAGE_VALUE,
                ON_PREM_SOURCE_STORAGE_NAME);
        makeMockEntityCondition(ON_PREM_VOLUME_RESOURCE_ID, EntityType.VIRTUAL_VOLUME_VALUE,
                ON_PREM_VOLUME_RESOURCE_NAME);
        makeMockEntityCondition(ON_PREM_VOLUME_RESOURCE_ID_2, EntityType.VIRTUAL_VOLUME_VALUE,
                ON_PREM_VOLUME_RESOURCE_NAME_2);

        // Cloud zone (AWS) -> region (Azure) compute move.
        final ActionInfo.Builder zoneToRegionMove = makeMoveInfo(AWS_VM_ID,
                CLOUD_SOURCE_COMPUTE_TIER_ID, EntityType.COMPUTE_TIER_VALUE,
                CLOUD_DESTINATION_COMPUTE_TIER_ID, EntityType.COMPUTE_TIER_VALUE);
        zoneToRegionMove.getMoveBuilder().addChanges(ChangeProvider.newBuilder().setSource(
                ActionEntity.newBuilder()
                        .setId(AWS_ZONE_ID)
                        .setType(EntityType.AVAILABILITY_ZONE_VALUE)
                        .build()).setDestination(ActionEntity.newBuilder()
                .setId(AZURE_REGION_ID)
                .setType(EntityType.REGION_VALUE)
                .build()).build());
        zoneToRegionComputeMove = makeRec(zoneToRegionMove, SupportLevel.SUPPORTED).build();
        makeMockEntityCondition(AWS_VM_ID, EntityType.VIRTUAL_MACHINE_VALUE, AWS_VM_NAME);
        makeMockEntityCondition(AWS_ZONE_ID, EntityType.AVAILABILITY_ZONE_VALUE, AWS_ZONE_NAME);
    }

    /**
     * Build an {@link ActionDTO.Action}.
     *
     * @param infoBuilder {@link ActionInfo.Builder}
     * @param supportLevel {@link SupportLevel}
     * @return {@link ActionDTO.Action.Builder}
     */
    private static ActionDTO.Action.Builder makeRec(final ActionInfo.Builder infoBuilder, final SupportLevel supportLevel) {
        return makeRec(infoBuilder, supportLevel, Explanation.newBuilder().build());
    }

    /**
     * Build an {@link ActionDTO.Action}.
     *
     * @param infoBuilder {@link ActionInfo.Builder}
     * @param supportLevel {@link SupportLevel}
     * @param explanation {@link Explanation}
     * @return {@link ActionDTO.Action.Builder}
     */
    private static ActionDTO.Action.Builder makeRec(final ActionInfo.Builder infoBuilder, final SupportLevel supportLevel,
            final Explanation explanation) {
        return ActionDTO.Action.newBuilder()
                .setId(IdentityGenerator.next())
                .setDeprecatedImportance(0)
                .setExecutable(true)
                .setSupportingLevel(supportLevel)
                .setInfo(infoBuilder)
                .setExplanation(explanation);
    }

    /**
     * Create a resize action info.
     *
     * @param targetId the target entity id
     * @param commodityType type of the commodity to resize
     * @param oldCapacity old capacity
     * @param newCapacity new capacity
     * @return {@link ActionInfo.Builder}
     */
    private static ActionInfo.Builder makeResizeInfo(long targetId, int commodityType,
            float oldCapacity, float newCapacity) {
        return makeResizeInfo(targetId, commodityType, null, oldCapacity, newCapacity);
    }

    /**
     * Create a resize action info.
     *
     * @param targetId the target entity id
     * @param commodityType type of the commodity to resize
     * @param key key of the commodity to resize
     * @param oldCapacity old capacity
     * @param newCapacity new capacity
     * @return {@link ActionInfo.Builder}
     */
    private static ActionInfo.Builder makeResizeInfo(long targetId, int commodityType, String key,
            float oldCapacity, float newCapacity) {
        return makeResizeInfo(targetId, commodityType, key, oldCapacity, newCapacity, 1, 1);
    }

    /**
     * Create a resize action info.
     *
     * @param targetId the target entity id
     * @param commodityType type of the commodity to resize
     * @param key key of the commodity to resize
     * @param oldCapacity old capacity
     * @param newCapacity new capacity
     * @return {@link ActionInfo.Builder}
     */
    private static ActionInfo.Builder makeResizeInfo(long targetId, int commodityType, String key,
                    float oldCapacity, float newCapacity, int oldCps, int newCps) {
        CommodityType.Builder commType = CommodityType.newBuilder().setType(commodityType);
        if (key != null) {
            commType.setKey(key);
        }
        return ActionInfo.newBuilder().setResize(Resize.newBuilder()
                        .setCommodityType(commType)
                        .setOldCapacity(oldCapacity)
                        .setNewCapacity(newCapacity)
                        .setOldCpsr(oldCps)
                        .setNewCpsr(newCps)
                        .setTarget(ActionOrchestratorTestUtils.createActionEntity(targetId)));
    }

    /**
     * Create a resize action for vcpu commodity.
     *
     * @param targetId the target entity id
     * @return {@link ActionInfo.Builder}
     */
    private ActionInfo.Builder makeResizeInfo(long targetId) {
        return makeResizeInfo(targetId, 26, 10, 20);
    }

    /**
     * Create a resize action for mem commodity.
     *
     * @param targetId the target entity id
     * @return {@link ActionInfo.Builder}
     */
    private ActionInfo.Builder makeResizeMemInfo(long targetId) {
        return makeResizeInfo(targetId, 21, 16 * Units.MBYTE, 8 * Units.MBYTE);
    }

    /**
     * Create a resize action for mem commodity and on reserved attribute.
     *
     * @param targetId the target entity id
     * @return {@link ActionInfo.Builder}
     */
    private ActionInfo.Builder makeResizeReservationMemInfo(long targetId) {
        ActionInfo.Builder builder = makeResizeInfo(targetId, 21, 16 * Units.MBYTE, 8 * Units.MBYTE);
        builder.getResizeBuilder().setCommodityAttribute(CommodityAttribute.RESERVED);
        return builder;
    }

    /**
     * Create a resize action for vcpu commodity.
     *
     * @param targetId the target entity id
     * @param oldCapacity the capacity before resize
     * @param newCapacity the capacity after resize
     * @return {@link ActionInfo.Builder}
     */
    private ActionInfo.Builder makeResizeVcpuInfo(long targetId, float oldCapacity, float newCapacity) {
        return makeResizeInfo(targetId, CommodityDTO.CommodityType.VCPU_VALUE, oldCapacity,
                newCapacity);
    }

    /**
     * Create a resize action for vcpu commodity.
     *
     * @param targetId the target entity id
     * @param oldCapacity the capacity before resize
     * @param newCapacity the capacity after resize
     * @return {@link ActionInfo.Builder}
     */
    private static ActionInfo.Builder makeResizeVmemInfo(long targetId, float oldCapacity,
            float newCapacity) {
        return makeResizeInfo(targetId, CommodityDTO.CommodityType.VMEM_VALUE,
                oldCapacity * Units.MBYTE, newCapacity * Units.MBYTE);
    }
    /**
     * Create a resize action for VCPULimitQuota commodity.
     *
     * @param targetId the target entity id.
     * @param oldCapacity the capacity before resize.
     * @param newCapacity the capacity after resize.
     * @return {@link ActionInfo.Builder}
     */
    private ActionInfo.Builder makeResizeVCPULimitQuotaInfo(long targetId, float oldCapacity, float newCapacity) {
        return makeResizeInfo(targetId, CommodityDTO.CommodityType.VCPU_LIMIT_QUOTA_VALUE, oldCapacity, newCapacity);
    }

    /**
     * Create a resize action for VMemLimitQuota commodity.
     *
     * @param targetId the target entity id.
     * @param oldCapacity the capacity before resize.
     * @param newCapacity the capacity after resize.
     * @return {@link ActionInfo.Builder}
     */
    private ActionInfo.Builder makeResizeVMemLimitQuotaInfo(long targetId, float oldCapacity, float newCapacity) {
        return makeResizeInfo(targetId, CommodityDTO.CommodityType.VMEM_LIMIT_QUOTA_VALUE, oldCapacity, newCapacity);
    }

    /**
     * Create a resize action for vcpu commodity and on reserved attribute.
     *
     * @param targetId the target entity id
     * @param oldCapacity the capacity before resize
     * @param newCapacity the capacity after resize
     * @return {@link ActionInfo.Builder}
     */
    private ActionInfo.Builder makeResizeReservationVcpuInfo(long targetId, float oldCapacity, float newCapacity) {
        ActionInfo.Builder builder = makeResizeInfo(targetId, CommodityDTO.CommodityType.VCPU_VALUE, oldCapacity, newCapacity);
        builder.getResizeBuilder().setCommodityAttribute(CommodityAttribute.RESERVED);
        return builder;
    }

    /**
     * Create a resize action for VMemRequest commodity.
     *
     * @param targetId the target entity id
     * @param oldCapacity the capacity before resize
     * @param newCapacity the capacity after resize
     * @return {@link ActionInfo.Builder}
     */
    private ActionInfo.Builder makeVMemRequestInfo(long targetId, float oldCapacity, float newCapacity) {
        return makeResizeInfo(targetId, CommodityDTO.CommodityType.VMEM_REQUEST_VALUE, oldCapacity, newCapacity);
    }

    /**
     * Create a resize action for StorageAmount commodity of vsan storage.
     *
     * @param oldCapacity the capacity before resize
     * @param newCapacity the capacity after resize
     * @return {@link ActionInfo.Builder}
     */
    private ActionInfo.Builder makeVSanStorageAmountRequestInfo(float oldCapacity, float newCapacity) {
        ActionInfo.Builder builder = makeResizeInfo(VSAN_STORAGE_ID,
                        CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE, oldCapacity, newCapacity);
        builder.setResize(builder.getResize().toBuilder());
        return builder;
    }

    private ActionInfo.Builder makeDeactivateInfo(long targetId) {
        return ActionInfo.newBuilder().setDeactivate(Deactivate.newBuilder()
                .setTarget(ActionOrchestratorTestUtils.createActionEntity(targetId))
                .addTriggeringCommodities(CommodityType.newBuilder().setType(26).build()));
    }

    private ActionInfo.Builder makeActivateInfo(long targetId) {
        return ActionInfo.newBuilder().setActivate(Activate.newBuilder()
                .setTarget(ActionOrchestratorTestUtils.createActionEntity(targetId))
                .addTriggeringCommodities(CommodityType.newBuilder().setType(26).build()));
    }

    private ActionInfo.Builder makeReconfigureInfo(long targetId, SettingChange... changes) {
        final Reconfigure.Builder reconfigureBuilder = Reconfigure.newBuilder()
                        .setTarget(ActionOrchestratorTestUtils.createActionEntity(targetId));
        Arrays.stream(changes).forEach(reconfigureBuilder::addSettingChange);
        return ActionInfo.newBuilder().setReconfigure(reconfigureBuilder
            .setIsProvider(false)
            .build());
    }

    private ActionInfo.Builder makeReconfigureInfo(long targetId, long sourceId) {
        return ActionInfo.newBuilder().setReconfigure(Reconfigure.newBuilder()
                .setTarget(ActionOrchestratorTestUtils.createActionEntity(targetId))
                .setSource(ActionOrchestratorTestUtils.createActionEntity(sourceId))
                .setIsProvider(false)
                .build());
    }

    private ActionInfo.Builder makeProvisionInfo(long targetId) {
        return ActionInfo.newBuilder().setProvision(Provision.newBuilder()
            .setEntityToClone(ActionEntity.newBuilder()
                .setId(targetId)
                .setType(EntityType.STORAGE_VALUE)
                .build())
            .build());
    }

    private ActionInfo.Builder makeDeleteInfo(long targetId) {
        return ActionInfo.newBuilder().setDelete(Delete.newBuilder()
            .setTarget(ActionEntity.newBuilder()
                .setId(targetId)
                .setType(EntityType.STORAGE_VALUE)
                .build())
            .setFilePath("/file/to/delete/filename.test")
            .build());
    }

    private ActionInfo.Builder makeDeleteVirtualMachineSpecInfo(long targetId, long sourceIdOpt) {
        Delete.Builder deleteBuilder = Delete.newBuilder()
                .setTarget(ActionEntity.newBuilder()
                        .setId(targetId)
                        .setType(EntityType.VIRTUAL_MACHINE_SPEC_VALUE)
                        .setEnvironmentType(EnvironmentType.CLOUD)
                        .build());


        deleteBuilder.setSource(ActionEntity.newBuilder()
                .setId(sourceIdOpt)
                .setType(EntityType.COMPUTE_TIER_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build());

        return ActionInfo.newBuilder().setDelete(deleteBuilder.build());
    }

    private ActionInfo.Builder makeDeleteCloudStorageInfo(long targetId, Optional<Long> sourceIdOpt) {
        Delete.Builder deleteBuilder = Delete.newBuilder()
            .setTarget(ActionEntity.newBuilder()
                .setId(targetId)
                .setType(EntityType.VIRTUAL_VOLUME_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build());

        if (sourceIdOpt.isPresent()) {
            deleteBuilder.setSource(ActionEntity.newBuilder()
                .setId(sourceIdOpt.get())
                .setType(EntityType.STORAGE_TIER_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build());
        }

        return ActionInfo.newBuilder().setDelete(deleteBuilder.build());
    }

    private ActionInfo.Builder makeBuyRIInfo(long computeTier, long masterAccount, long region) {
        return ActionInfo.newBuilder().setBuyRi(BuyRI.newBuilder()
            .setComputeTier(ActionEntity.newBuilder()
                .setId(computeTier)
                .setType(EntityType.COMPUTE_TIER_VALUE)
                .build())
            .setMasterAccount(ActionEntity.newBuilder()
                .setId(masterAccount)
                .setType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .build())
            .setRegion(ActionEntity.newBuilder()
                .setId(region)
                .setType(EntityType.REGION_VALUE)
                .build())
            .build());
    }

    private static ActionInfo.Builder makeMoveInfo(
        long targetId,
        long sourceId,
        int sourceType,
        long destinationId,
        int destinationType) {
        return makeMoveInfo(targetId, Collections.emptySet(), 0, sourceId, sourceType,
                        destinationId, destinationType);
    }

    private static ActionInfo.Builder makeMoveInfo(
        long targetId,
        Set<Long> resourceIds,
        int resourceType,
        long sourceId,
        int sourceType,
        long destinationId,
        int destinationType) {

        final ChangeProvider.Builder changeBuilder = ChangeProvider.newBuilder()
            .setSource(ActionEntity.newBuilder()
                .setId(sourceId)
                .setType(sourceType)
                .build())
            .setDestination(ActionEntity.newBuilder()
                .setId(destinationId)
                .setType(destinationType)
                .build());
        if (resourceType != 0) {
            for (Long resourceId : resourceIds) {
                changeBuilder.addResource(ActionEntity.newBuilder()
                                .setType(resourceType)
                                .setId(resourceId)
                                .build());
            }
        }
        return ActionInfo.newBuilder().setMove(Move.newBuilder()
            .setTarget(ActionOrchestratorTestUtils.createActionEntity(targetId))
            .addChanges(changeBuilder.build())
            .build());
    }

    private ActionInfo.Builder makeScaleInfo(
            long targetId,
            long resourceId,
            int resourceType,
            long sourceId,
            int sourceType,
            long destinationId,
            int destinationType) {

        final ChangeProvider.Builder changeBuilder = ChangeProvider.newBuilder()
                .setSource(ActionEntity.newBuilder()
                        .setId(sourceId)
                        .setType(sourceType)
                        .build())
                .setDestination(ActionEntity.newBuilder()
                        .setId(destinationId)
                        .setType(destinationType)
                        .build());
        if (resourceId != 0 && resourceType != 0) {
            changeBuilder.addResource(ActionEntity.newBuilder()
                    .setType(resourceType)
                    .setId(resourceId)
                    .build());
        }
        return ActionInfo.newBuilder().setScale(Scale.newBuilder()
                .setTarget(ActionOrchestratorTestUtils.createActionEntity(targetId))
                .addChanges(changeBuilder.build())
                .build());
    }

    private ActionInfo.Builder makeScaleInfoWithProviderChangeOnly(long targetId,
                                                                   long sourceId,
                                                                   int sourceType,
                                                                   long destinationId,
                                                                   int destinationType) {
        final ChangeProvider.Builder changeBuilder = ChangeProvider.newBuilder()
                .setSource(ActionEntity.newBuilder()
                        .setId(sourceId)
                        .setType(sourceType)
                        .build())
                .setDestination(ActionEntity.newBuilder()
                        .setId(destinationId)
                        .setType(destinationType)
                        .build());
        return ActionInfo.newBuilder().setScale(Scale.newBuilder()
                .setTarget(ActionOrchestratorTestUtils.createActionEntity(targetId))
                .addChanges(changeBuilder.build())
                .build());
    }


    private ActionInfo.Builder addResizeCommodityToAction(ActionInfo.Builder action, CommodityDTO.CommodityType commodityType) {
        Scale currenScaleRecommendation = action.getScale();
        final ResizeInfo resizeCommodityChangeBuilder = ResizeInfo.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(commodityType.getNumber()))
                .setOldCapacity(100)
                .setNewCapacity(200).build();
        return action.setScale(currenScaleRecommendation.toBuilder()
                .addCommodityResizes(resizeCommodityChangeBuilder).build());
    }

    private ActionInfo.Builder makeCloudVolumeScaleInfoWithCommodityChangeOnly(long targetId) {
        final ResizeInfo.Builder commodityChangeBuilder = ResizeInfo.newBuilder()
                .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE))
                .setOldCapacity(100)
                .setNewCapacity(200);

        return ActionInfo.newBuilder().setScale(Scale.newBuilder()
                .setTarget(ActionOrchestratorTestUtils.createActionEntity(targetId))
                .addCommodityResizes(commodityChangeBuilder)
                .build());
    }
    /**
     * Create a {@link ProvisionBySupplyExplanation}.
     *
     * @return {@link ProvisionExplanation}
     */
    private ProvisionExplanation makeProvisionBySupplyExplanation() {
        return ProvisionExplanation.newBuilder()
            .setProvisionBySupplyExplanation(
                ProvisionBySupplyExplanation.newBuilder()
                    .setMostExpensiveCommodityInfo(ReasonCommodity.newBuilder()
                        .setCommodityType(CommodityType.newBuilder().setType(22)))
                    .build())
            .build();
    }

    /**
     * Create a {@link ProvisionByDemandExplanation}
     *
     * @return {@link ProvisionExplanation}
     */
    private ProvisionExplanation makeProvisionByDemandExplanation() {
        List<CommodityNewCapacityEntry> capacityPerType = new ArrayList<>();
        capacityPerType.add(ProvisionByDemandExplanation.CommodityNewCapacityEntry.newBuilder()
            .setCommodityBaseType(CommodityDTO.CommodityType.MEM_VALUE)
            .setNewCapacity(10).build());
        return ProvisionExplanation.newBuilder()
            .setProvisionByDemandExplanation(ProvisionByDemandExplanation
                .newBuilder().setBuyerId(VM1_ID)
                .addAllCommodityNewCapacityEntry(capacityPerType)
                .addAllCommodityMaxAmountAvailable(new ArrayList<>()).build())
            .build();
    }

    /**
     * Create a {@link ReconfigureExplanation} with reason commodities.
     *
     * @return {@link ReconfigureExplanation}
     */
    private ReconfigureExplanation makeReconfigureReasonCommoditiesExplanation() {
        return ReconfigureExplanation.newBuilder()
            .addReconfigureCommodity(BALLOONING)
            .addReconfigureCommodity(CPU_ALLOCATION)
            .build();
    }

    /**
     * Create a {@link ReconfigureExplanation} with taint commodities.
     * @return {@link ReconfigureExplanation}
     */
    private ReconfigureExplanation makeReconfigureTaintCommoditiesExplanation() {
        return ReconfigureExplanation.newBuilder()
                .addReconfigureCommodity(TAINT)
                .build();
    }

    /**
     * Create a {@link ReconfigureExplanation} with label commodities
     *
     * @return {@link ReconfigureExplanation}
     */
    private ReconfigureExplanation makeReconfigureLabelCommoditiesExplanation() {
        return ReconfigureExplanation.newBuilder()
            .addReconfigureCommodity(LABEL)
            .build();
    }

    /**
     * Create a {@link ReconfigureExplanation} with reason settings.
     *
     * @return {@link ReconfigureExplanation}
     */
    private ReconfigureExplanation makeReconfigureReasonSettingsExplanation() {
        return ReconfigureExplanation.newBuilder()
            .addReasonSettings(1L)
            .build();
    }

    private ReconfigureExplanation makeReconfigureReasonSettingsExplanation(Long id) {
        return ReconfigureExplanation.newBuilder()
                .addReasonSettings(id)
                .build();
    }

    private ActionInfo.Builder makeAllocateInfo(long vmId, long computeTierId) {
        return ActionInfo.newBuilder().setAllocate(Allocate.newBuilder()
                .setTarget(ActionEntity.newBuilder()
                        .setId(vmId)
                        .setType(EntityType.VIRTUAL_MACHINE_VALUE))
                .setWorkloadTier(ActionEntity.newBuilder()
                        .setId(computeTierId)
                        .setType(EntityType.COMPUTE_TIER_VALUE))
                .build());
    }

    private static Optional<ActionPartialEntity> createEntity(Long oid, int entityType,
            String displayName, double cpuReservation, double memReservation) {
        return Optional.of(ActionPartialEntity.newBuilder()
                .setOid(oid)
                .setEntityType(entityType)
                .setDisplayName(displayName)
                .setTypeSpecificInfo(ActionEntityTypeSpecificInfo.newBuilder()
                        .setVirtualMachine(ActionVirtualMachineInfo.newBuilder()
                                .putReservationInfo(CommodityDTO.CommodityType.CPU_VALUE,
                                        cpuReservation)
                                .putReservationInfo(CommodityDTO.CommodityType.MEM_VALUE,
                                        memReservation)
                                .setCpuCoreMhz(2600.0)
                                .build())
                        .build())
                .build());
    }

    @Test
    public void testBuildMoveActionDescription() throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
            .thenReturn((createEntity(VM1_ID,
                EntityType.VIRTUAL_MACHINE.getNumber(),
                VM1_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getEntityFromOid(eq(PM_SOURCE_ID)))
            .thenReturn((createEntity(PM_SOURCE_ID,
                EntityType.PHYSICAL_MACHINE.getNumber(),
                PM_SOURCE_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getEntityFromOid(eq(PM_DESTINATION_ID)))
            .thenReturn((createEntity(PM_DESTINATION_ID,
                EntityType.PHYSICAL_MACHINE.getNumber(),
                PM_DESTINATION_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VM1_ID))).thenReturn(
                Optional.of(EntityWithConnections.newBuilder()
                        .setOid(BUSINESS_ACCOUNT_OID)
                        .setEntityType(EntityType.BUSINESS_ACCOUNT.getNumber())
                        .setDisplayName(BUSINESS_ACCOUNT_NAME)
                        .build()));

        String description = ActionDescriptionBuilder.buildActionDescription(entitySettingsCache,
                moveRecommendation);
        assertEquals(description,
                "Move Virtual Machine vm1_test from pm_source_test to pm_destination_test in Development");
    }

    /**
     * Test that description is accurate for a move action with no account name.
     *
     * @throws UnsupportedActionException if something is extraordinarily wrong.
     */
    @Test
    public void testBuildMoveActionDescriptionWithNoAccountName() throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
                .thenReturn((createEntity(VM1_ID,
                        EntityType.VIRTUAL_MACHINE.getNumber(),
                        VM1_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getEntityFromOid(eq(PM_SOURCE_ID)))
                .thenReturn((createEntity(PM_SOURCE_ID,
                        EntityType.PHYSICAL_MACHINE.getNumber(),
                        PM_SOURCE_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getEntityFromOid(eq(PM_DESTINATION_ID)))
                .thenReturn((createEntity(PM_DESTINATION_ID,
                        EntityType.PHYSICAL_MACHINE.getNumber(),
                        PM_DESTINATION_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VM1_ID)))
                .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, moveRecommendation);
        assertEquals(description, "Move Virtual Machine vm1_test from pm_source_test to pm_destination_test");
    }

    /**
     * Test that for an action involving a volume moving from one storage tier to another, the
     * description is accurate.
     *
     * @throws UnsupportedActionException if something is extraordinarily wrong.
     */
    @Test
    public void testBuildCloudVolumeScaleActionWithProviderChangeDescription() throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VV_ID)))
                .thenReturn((createEntity(VV_ID,
                        EntityType.VIRTUAL_VOLUME.getNumber(),
                        VM1_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getEntityFromOid(eq(ST_SOURCE_ID)))
                .thenReturn((createEntity(ST_SOURCE_ID,
                        EntityType.STORAGE_TIER.getNumber(),
                        ST_SOURCE_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getEntityFromOid(eq(ST_DESTINATION_ID)))
                .thenReturn((createEntity(ST_DESTINATION_ID,
                        EntityType.STORAGE_TIER.getNumber(),
                        ST_DESTINATION_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VV_ID))).thenReturn(
                Optional.of(EntityWithConnections.newBuilder()
                        .setOid(BUSINESS_ACCOUNT_OID)
                        .setEntityType(EntityType.BUSINESS_ACCOUNT.getNumber())
                        .setDisplayName(BUSINESS_ACCOUNT_NAME)
                        .build()));

        String description = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, cloudVolumeScaleProviderChangeRecommendation);
        Assert.assertEquals(
                "Scale Volume vm1_test from storage_source_test to storage_destination_test in Development",
                description);
    }

    /**
     * Test that for an action involving a volume moving from one storage tier to another,
     * with no account name the description is accurate.
     *
     * @throws UnsupportedActionException if something is extraordinarily wrong.
     */
    @Test
    public void testBuildCloudVolumeScaleActionWithProviderChangeDescriptionWithNoAccountName() throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VV_ID)))
                .thenReturn((createEntity(VV_ID,
                        EntityType.VIRTUAL_VOLUME.getNumber(),
                        VM1_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getEntityFromOid(eq(ST_SOURCE_ID)))
                .thenReturn((createEntity(ST_SOURCE_ID,
                        EntityType.STORAGE_TIER.getNumber(),
                        ST_SOURCE_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getEntityFromOid(eq(ST_DESTINATION_ID)))
                .thenReturn((createEntity(ST_DESTINATION_ID,
                        EntityType.STORAGE_TIER.getNumber(),
                        ST_DESTINATION_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VV_ID)))
                .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, cloudVolumeScaleProviderChangeRecommendation);
        Assert.assertEquals("Scale Volume vm1_test from storage_source_test to storage_destination_test", description);
    }

    @Test
    public void testBuildCloudVolumeScaleActionWithProviderChangeDescriptionDB() throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(DB_ID)))
                .thenReturn((createEntity(DB_ID,
                        EntityType.DATABASE.getNumber(),
                        DB_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getEntityFromOid(eq(DB_SOURCE_ID)))
                .thenReturn((createEntity(DB_SOURCE_ID,
                        EntityType.DATABASE_TIER.getNumber(),
                        DB_SOURCE_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getEntityFromOid(eq(DB_DESTINATION_ID)))
                .thenReturn((createEntity(DB_DESTINATION_ID,
                        EntityType.DATABASE_TIER.getNumber(),
                        DB_DESTINATION_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(DB_ID)))
                .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, cloudDBScaleProviderChangeRecommendation);
        Assert.assertEquals("Scale Database db_display_name from db_source_test to db_destination_test, "
                + "Disk size up from 100 MB to 200 MB", description);
    }

    /**
     * Test that for an action involving a cloud volume commodity scaling from one capacity to another,
     * the description is accurate and includes provider name.
     *
     * @throws UnsupportedActionException if something is extraordinarily wrong.
     */
    @Test
    public void testBuildCloudVolumeScaleActionWithCommodityChangeDescriptionWithProviderId() throws UnsupportedActionException {
        ActionPartialEntity volumeOptEntity = ActionPartialEntity.newBuilder()
                .setOid(VV_ID)
                .setEntityType(EntityType.VIRTUAL_VOLUME.getNumber())
                .setDisplayName(VM1_DISPLAY_NAME)
                .setPrimaryProviderId(VV_TIER_ID)
                .build();

        when(entitySettingsCache.getEntityFromOid(eq(VV_ID))).thenReturn(Optional.of(volumeOptEntity));
        when(entitySettingsCache.getEntityFromOid(eq(VV_TIER_ID)))
                .thenReturn((createEntity(VV_TIER_ID, EntityType.STORAGE_TIER.getNumber(), VV_TIER_DISPLAY_NAME,
                        0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VV_ID))).thenReturn(
                Optional.of(EntityWithConnections.newBuilder()
                        .setOid(BUSINESS_ACCOUNT_OID)
                        .setEntityType(EntityType.BUSINESS_ACCOUNT.getNumber())
                        .setDisplayName(BUSINESS_ACCOUNT_NAME)
                        .build()));

        String description = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, cloudVolumeScaleCommodityChangeRecommendation);
        Assert.assertEquals(
                "Scale up IOPS for Volume vm1_test on GP2 from 100 IOPS to 200 IOPS in Development",
                description);
    }

    /**
     * Test that for an action involving a cloud volume commodity scaling from one capacity to another,
     * the description is accurate and includes provider name and no account name.
     *
     * @throws UnsupportedActionException if something is extraordinarily wrong.
     */
    @Test
    public void testBuildCloudVolumeScaleActionWithCommodityChangeDescriptionWithProviderIdWithNoAccountName()
            throws UnsupportedActionException {
        ActionPartialEntity volumeOptEntity = ActionPartialEntity.newBuilder()
                .setOid(VV_ID)
                .setEntityType(EntityType.VIRTUAL_VOLUME.getNumber())
                .setDisplayName(VM1_DISPLAY_NAME)
                .setPrimaryProviderId(VV_TIER_ID)
                .build();

        when(entitySettingsCache.getEntityFromOid(eq(VV_ID))).thenReturn(Optional.of(volumeOptEntity));
        when(entitySettingsCache.getEntityFromOid(eq(VV_TIER_ID)))
                .thenReturn((createEntity(VV_TIER_ID, EntityType.STORAGE_TIER.getNumber(), VV_TIER_DISPLAY_NAME,
                        0, 0)));
        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VV_ID)))
                .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, cloudVolumeScaleCommodityChangeRecommendation);
        Assert.assertEquals("Scale up IOPS for Volume vm1_test on GP2 from 100 IOPS to 200 IOPS", description);
    }

    /**
     * Test that for an action involving a cloud volume commodity scaling from one capacity to another,
     * the description is accurate.
     *
     * @throws UnsupportedActionException if something is extraordinarily wrong.
     */
    @Test
    public void testBuildCloudVolumeScaleActionWithCommodityChangeDescriptionWithoutProviderId() throws UnsupportedActionException {
        ActionPartialEntity volumeOptEntity = ActionPartialEntity.newBuilder()
                .setOid(VV_ID)
                .setEntityType(EntityType.VIRTUAL_VOLUME.getNumber())
                .setDisplayName(VM1_DISPLAY_NAME)
                .build();

        when(entitySettingsCache.getEntityFromOid(eq(VV_ID))).thenReturn(Optional.of(volumeOptEntity));
        when(entitySettingsCache.getEntityFromOid(eq(VV_TIER_ID)))
                .thenReturn((createEntity(VV_TIER_ID, EntityType.STORAGE_TIER.getNumber(), VV_TIER_DISPLAY_NAME,
                        0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VV_ID))).thenReturn(
                Optional.of(EntityWithConnections.newBuilder()
                        .setOid(BUSINESS_ACCOUNT_OID)
                        .setEntityType(EntityType.BUSINESS_ACCOUNT.getNumber())
                        .setDisplayName(BUSINESS_ACCOUNT_NAME)
                        .build()));

        String description = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, cloudVolumeScaleCommodityChangeRecommendation);
        Assert.assertEquals("Scale up IOPS for Volume vm1_test from 100 IOPS to 200 IOPS in Development", description);
    }

    /**
     * Test that for an action involving a cloud volume commodity scaling from one capacity to another,
     * without the account name, the description is accurate.
     *
     * @throws UnsupportedActionException if something is extraordinarily wrong.
     */
    @Test
    public void testBuildCloudVolumeScaleActionWithCommodityChangeDescriptionWithoutProviderIdWithNoAccountName()
            throws UnsupportedActionException {
        ActionPartialEntity volumeOptEntity = ActionPartialEntity.newBuilder()
                .setOid(VV_ID)
                .setEntityType(EntityType.VIRTUAL_VOLUME.getNumber())
                .setDisplayName(VM1_DISPLAY_NAME)
                .build();

        when(entitySettingsCache.getEntityFromOid(eq(VV_ID))).thenReturn(Optional.of(volumeOptEntity));
        when(entitySettingsCache.getEntityFromOid(eq(VV_TIER_ID)))
                .thenReturn((createEntity(VV_TIER_ID, EntityType.STORAGE_TIER.getNumber(), VV_TIER_DISPLAY_NAME,
                        0, 0)));
        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VV_ID)))
                .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, cloudVolumeScaleCommodityChangeRecommendation);
        Assert.assertEquals("Scale up IOPS for Volume vm1_test from 100 IOPS to 200 IOPS", description);
    }

    /**
     * When there are multiple additional resizing commodity.
     *
     * @throws UnsupportedActionException if exception action.
     */
    @Test
    public void testBuildCloudDBSActionWithMultipleAdditionalCommodity() throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(DB_ID)))
                .thenReturn((createEntity(DB_ID,
                        EntityType.DATABASE.getNumber(),
                        DB_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getEntityFromOid(eq(DB_SOURCE_ID)))
                .thenReturn((createEntity(DB_SOURCE_ID,
                        EntityType.DATABASE_TIER.getNumber(),
                        DB_SOURCE_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getEntityFromOid(eq(DB_DESTINATION_ID)))
                .thenReturn((createEntity(DB_DESTINATION_ID,
                        EntityType.DATABASE_TIER.getNumber(),
                        DB_DESTINATION_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(DB_ID)))
                .thenReturn(Optional.empty());

        Builder action = makeScaleInfoWithProviderChangeOnly(DB_ID, DB_SOURCE_ID,
                EntityType.DATABASE_TIER.getNumber(),
                DB_DESTINATION_ID,
                EntityType.DATABASE_TIER.getNumber());
        action = addResizeCommodityToAction(action, CommodityDTO.CommodityType.STORAGE_AMOUNT);
        action = addResizeCommodityToAction(action, CommodityDTO.CommodityType.STORAGE_ACCESS);
        ActionDTO.Action dbScaleAction = makeRec(action, SupportLevel.SUPPORTED).build();
        String description = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, dbScaleAction);
        Assert.assertEquals("Scale Database db_display_name from db_source_test to db_destination_test, "
                + "Disk size up from 100 MB to 200 MB, IOPS up from 100 IOPS to 200 IOPS", description);
    }


    @Test
    public void testBuildCloudDBWithoutProviderIdMultipleAdditionalCommodities() throws UnsupportedActionException {
        ActionPartialEntity volumeOptEntity = ActionPartialEntity.newBuilder()
                .setOid(DB_ID)
                .setEntityType(EntityType.DATABASE_SERVER.getNumber())
                .setDisplayName(DB_SOURCE_DISPLAY_NAME)
                .build();

        when(entitySettingsCache.getEntityFromOid(eq(DB_ID))).thenReturn(Optional.of(volumeOptEntity));
        when(entitySettingsCache.getEntityFromOid(eq(DB_SOURCE_ID)))
                .thenReturn((createEntity(DB_SOURCE_ID, EntityType.DATABASE_SERVER_TIER.getNumber(), DB_SOURCE_DISPLAY_NAME,
                        0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(DB_ID)))
                .thenReturn(Optional.empty());

        final ResizeInfo.Builder iopsResize = ResizeInfo.newBuilder()
                .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE))
                .setOldCapacity(100)
                .setNewCapacity(200);

        final ResizeInfo.Builder storageResize = ResizeInfo.newBuilder()
                .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE))
                .setOldCapacity(100)
                .setNewCapacity(200);

        Builder action = ActionInfo.newBuilder().setScale(Scale.newBuilder()
                .setTarget(ActionOrchestratorTestUtils.createActionEntity(DB_ID))
                .addCommodityResizes(iopsResize)
                .addCommodityResizes(storageResize)
                .build());
        String description = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, makeRec(action, SupportLevel.SUPPORTED).build());
        Assert.assertEquals("Scale up IOPS for Database Server db_source_test from 100 IOPS to 200 IOPS"
                + ", Disk size up from 100 MB to 200 MB", description);
    }

    /**
     * Tests updated descriptions for cloud migration moves (from onPrem and cloud).
     *
     * @throws UnsupportedActionException Thrown on action problems.
     */
    @Test
    public void cloudMigrationMoveDescriptions() throws UnsupportedActionException {
        when(entitySettingsCache.getOwnerAccountOfEntity(AZURE_VM_ID))
                .thenReturn(Optional.of(EntityWithConnections.newBuilder()
                        .setOid(BUSINESS_ACCOUNT_OID)
                        .setEntityType(EntityType.BUSINESS_ACCOUNT.getNumber())
                        .setDisplayName(BUSINESS_ACCOUNT_NAME)
                        .build()));
        when(entitySettingsCache.getOwnerAccountOfEntity(CLOUD_VOLUME_RESOURCE_ID))
                .thenReturn(Optional.of(EntityWithConnections.newBuilder()
                        .setOid(BUSINESS_ACCOUNT_OID)
                        .setEntityType(EntityType.BUSINESS_ACCOUNT.getNumber())
                        .setDisplayName(BUSINESS_ACCOUNT_NAME)
                        .build()));
        when(entitySettingsCache.getOwnerAccountOfEntity(eq(ON_PREM_VM_ID)))
                .thenReturn(Optional.empty());
        when(entitySettingsCache.getOwnerAccountOfEntity(CLOUD_VOLUME_RESOURCE_ID))
                .thenReturn(Optional.of(EntityWithConnections.newBuilder()
                        .setOid(BUSINESS_ACCOUNT_OID)
                        .setEntityType(EntityType.BUSINESS_ACCOUNT.getNumber())
                        .setDisplayName(BUSINESS_ACCOUNT_NAME)
                        .build()));
        when(entitySettingsCache.getOwnerAccountOfEntity(VM1_ID))
                .thenReturn(Optional.of(EntityWithConnections.newBuilder()
                        .setOid(BUSINESS_ACCOUNT_OID)
                        .setEntityType(EntityType.BUSINESS_ACCOUNT.getNumber())
                        .setDisplayName(BUSINESS_ACCOUNT_NAME)
                        .build()));
        when(entitySettingsCache.getOwnerAccountOfEntity(AWS_VM_ID))
                .thenReturn(Optional.of(EntityWithConnections.newBuilder()
                        .setOid(BUSINESS_ACCOUNT_OID)
                        .setEntityType(EntityType.BUSINESS_ACCOUNT.getNumber())
                        .setDisplayName(BUSINESS_ACCOUNT_NAME)
                        .build()));
        // Cloud->Cloud compute move.
        String actualDescription = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, cloudToCloudComputeMove);
        String expectedDescription = String.format("Move Virtual Machine %s from %s to %s in Development",
                AZURE_VM_NAME, AZURE_REGION_NAME, AWS_REGION_NAME);
        assertEquals(expectedDescription, actualDescription);

        // Cloud->Cloud storage move.
        actualDescription = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, cloudToCloudStorageMove);
        expectedDescription = String.format("Move Volume %s from %s to %s in Development",
                CLOUD_VOLUME_RESOURCE_NAME, AZURE_REGION_NAME, AWS_REGION_NAME);
        assertEquals(expectedDescription, actualDescription);

        // onPrem->Cloud compute move.
        actualDescription = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, onPremToCloudComputeMove);
        expectedDescription = String.format("Move Virtual Machine %s from %s to %s",
                ON_PREM_VM_NAME, ON_PREM_SOURCE_PM_NAME, AWS_REGION_NAME);
        assertEquals(expectedDescription, actualDescription);

        // onPrem->Cloud storage move.
        actualDescription = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, onPremToCloudStorageMove);
        expectedDescription = String.format("Move Volume %s, Volume %s of Virtual Machine %s from %s to %s",
                        ON_PREM_VOLUME_RESOURCE_NAME_2, ON_PREM_VOLUME_RESOURCE_NAME,
                        ON_PREM_VM_NAME, ON_PREM_SOURCE_STORAGE_NAME, AWS_REGION_NAME);
        assertEquals(expectedDescription, actualDescription);

        // AWS zone to Azure region compute move.
        actualDescription = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, zoneToRegionComputeMove);
        expectedDescription = String.format("Move Virtual Machine %s from %s to %s in Development",
                AWS_VM_NAME, AWS_ZONE_NAME, AZURE_REGION_NAME);
        assertEquals(expectedDescription, actualDescription);

        // same region scale.
        makeMockEntityCondition(VM1_ID, EntityType.VIRTUAL_MACHINE_VALUE, AWS_VM_NAME);
        makeMockEntityCondition(COMPUTE_TIER_SOURCE_ID, EntityType.COMPUTE_TIER.getNumber(),
                COMPUTE_TIER_SOURCE_DISPLAY_NAME);
        makeMockEntityCondition(COMPUTE_TIER_DESTINATION_ID, EntityType.COMPUTE_TIER.getNumber(),
                COMPUTE_TIER_DESTINATION_DISPLAY_NAME);
        actualDescription = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, scaleTypeRecommendation);
        expectedDescription = String.format("Scale Virtual Machine %s from %s to %s in Development",
                AWS_VM_NAME, COMPUTE_TIER_SOURCE_DISPLAY_NAME, COMPUTE_TIER_DESTINATION_DISPLAY_NAME);
        assertEquals(expectedDescription, actualDescription);
    }

    /**
     * Convenience method to add mock condition for entity id -> name mapping.
     *
     * @param entityId Id of entity.
     * @param entityType Type of entity.
     * @param entityName Display name of entity that shows up in action description.
     */
    private void makeMockEntityCondition(@Nonnull Long entityId, int entityType,
            @Nonnull String entityName) {
        when(entitySettingsCache.getEntityFromOid(eq(entityId)))
                .thenReturn((createEntity(entityId, entityType, entityName, 0, 0)));
    }

    /**
     * Test that an action moving a VM from one compute tier to another is described as scaling it.
     *
     * @throws UnsupportedActionException if something is extraordinarily wrong.
     */
    @Test
    public void testBuildScaleActionDescription() throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
            .thenReturn((createEntity(VM1_ID,
                EntityType.VIRTUAL_MACHINE.getNumber(),
                VM1_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getEntityFromOid(eq(COMPUTE_TIER_SOURCE_ID)))
            .thenReturn((createEntity(COMPUTE_TIER_SOURCE_ID,
                EntityType.COMPUTE_TIER.getNumber(),
                COMPUTE_TIER_SOURCE_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getEntityFromOid(eq(COMPUTE_TIER_DESTINATION_ID)))
            .thenReturn((createEntity(COMPUTE_TIER_DESTINATION_ID,
                EntityType.COMPUTE_TIER.getNumber(),
                COMPUTE_TIER_DESTINATION_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VM1_ID)))
                .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, scaleRecommendation);
        assertEquals("Scale Virtual Machine vm1_test from tier_t1 to tier_t2", description);
    }

    @Test
    public void testBuildReconfigureActionTaintCommoditiesDescription()
            throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(CONTAINER_POD_ID)))
                .thenReturn((createEntity(CONTAINER_POD_ID,
                                          EntityType.CONTAINER_POD_VALUE,
                                          CONTAINER_POD_DISPLAY_NAME, 0, 0)));
        when(entitySettingsCache.getEntityFromOid(eq(VM_SOURCE_ID)))
                .thenReturn((createEntity(VM_SOURCE_ID,
                                          EntityType.VIRTUAL_MACHINE_VALUE,
                                          VM_SOURCE_DISPLAY_NAME, 0, 0)));
        when(entitySettingsCache.getOwnerAccountOfEntity(eq(CONTAINER_POD_ID)))
                .thenReturn(Optional.empty());
        String description = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, reconfigureTaintCommoditiesRecommendation);
        assertEquals("Reconfigure Container Pod container_pod_test", description);
    }

    /**
     * Test the description of reconfigure action with label commodities.
     */
    @Test
    public void testBuildReconfigureActionLabelCommoditiesDescription()
        throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(CONTAINER_POD_ID))).thenReturn(
            (createEntity(CONTAINER_POD_ID, EntityType.CONTAINER_POD_VALUE,
                CONTAINER_POD_DISPLAY_NAME, 0, 0)));
        when(entitySettingsCache.getEntityFromOid(eq(VM_SOURCE_ID))).thenReturn(
            (createEntity(VM_SOURCE_ID, EntityType.VIRTUAL_MACHINE_VALUE, VM_SOURCE_DISPLAY_NAME, 0,
                0)));
        when(entitySettingsCache.getOwnerAccountOfEntity(eq(CONTAINER_POD_ID))).thenReturn(
            Optional.empty());
        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, reconfigureLabelCommoditiesRecommendation);
        assertEquals("Reconfigure nodeSelector for Container Pod container_pod_test", description);
    }

    /**
     * Test the description of reconfigure action with reason commodities.
     */
    @Test
    public void testBuildReconfigureActionReasonCommoditiesDescription()
            throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
            .thenReturn((createEntity(VM1_ID,
                EntityType.VIRTUAL_MACHINE.getNumber(),
                VM1_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getEntityFromOid(eq(PM_SOURCE_ID)))
            .thenReturn((createEntity(PM_SOURCE_ID,
                EntityType.PHYSICAL_MACHINE.getNumber(),
                PM_SOURCE_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VM1_ID))).thenReturn(
                Optional.of(EntityWithConnections.newBuilder()
                        .setOid(BUSINESS_ACCOUNT_OID)
                        .setEntityType(EntityType.BUSINESS_ACCOUNT.getNumber())
                        .setDisplayName(BUSINESS_ACCOUNT_NAME)
                        .build()));

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, reconfigureReasonCommoditiesRecommendation);
        assertEquals(description, "Reconfigure Virtual Machine vm1_test to provide "
            + "Ballooning, CPU Allocation in Development");
    }

    /**
     * Test the description of reconfigure action with reason commodities.
     * and without an account number
     */
    @Test
    public void testBuildReconfigureActionReasonCommoditiesDescriptionWthNoAccountName()
            throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
                .thenReturn((createEntity(VM1_ID,
                        EntityType.VIRTUAL_MACHINE.getNumber(),
                        VM1_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getEntityFromOid(eq(PM_SOURCE_ID)))
                .thenReturn((createEntity(PM_SOURCE_ID,
                        EntityType.PHYSICAL_MACHINE.getNumber(),
                        PM_SOURCE_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VM1_ID)))
                .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, reconfigureReasonCommoditiesRecommendation);
        assertEquals(description, "Reconfigure Virtual Machine vm1_test to provide "
                + "Ballooning, CPU Allocation");
    }

    /**
     * Test the description of reconfigure action with reason settings.
     */
    @Test
    public void testBuildReconfigureActionReasonSettingsDescription()
            throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
            .thenReturn((createEntity(VM1_ID,
                EntityType.VIRTUAL_MACHINE.getNumber(),
                VM1_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getEntityFromOid(eq(PM_SOURCE_ID)))
            .thenReturn((createEntity(PM_SOURCE_ID,
                EntityType.PHYSICAL_MACHINE.getNumber(),
                PM_SOURCE_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VM1_ID))).thenReturn(
                Optional.of(EntityWithConnections.newBuilder()
                        .setOid(BUSINESS_ACCOUNT_OID)
                        .setEntityType(EntityType.BUSINESS_ACCOUNT.getNumber())
                        .setDisplayName(BUSINESS_ACCOUNT_NAME)
                        .build()));

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, reconfigureReasonSettingsRecommendation);
        assertEquals(description, "Reconfigure Virtual Machine vm1_test in Development");
    }

    /**
     * Test the description of reconfigure action with reason settings and no account name.
     */
    @Test
    public void testBuildReconfigureActionReasonSettingsDescriptionWithNoAccountName()
            throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
                .thenReturn((createEntity(VM1_ID,
                        EntityType.VIRTUAL_MACHINE.getNumber(),
                        VM1_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getEntityFromOid(eq(PM_SOURCE_ID)))
                .thenReturn((createEntity(PM_SOURCE_ID,
                        EntityType.PHYSICAL_MACHINE.getNumber(),
                        PM_SOURCE_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VM1_ID)))
                .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, reconfigureReasonSettingsRecommendation);
        assertEquals(description, "Reconfigure Virtual Machine vm1_test");
    }

    /**
     * Test the description of reconfigure action without source.
     */
    @Test
    public void testBuildReconfigureActionWithoutSourceDescription()
            throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
            .thenReturn((createEntity(VM1_ID,
                EntityType.VIRTUAL_MACHINE.getNumber(),
                VM1_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VM1_ID))).thenReturn(
                Optional.of(EntityWithConnections.newBuilder()
                        .setOid(BUSINESS_ACCOUNT_OID)
                        .setEntityType(EntityType.BUSINESS_ACCOUNT.getNumber())
                        .setDisplayName(BUSINESS_ACCOUNT_NAME)
                        .build()));

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, reconfigureWithoutSourceRecommendation);
        assertEquals(description,
                "Reconfigure Virtual Machine vm1_test as it is unplaced in Development");
    }

    /**
     * Test the description of reconfigure action without source and account name.
     */
    @Test
    public void testBuildReconfigureActionWithoutSourceDescriptionAndAccountName()
            throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
                .thenReturn((createEntity(VM1_ID,
                        EntityType.VIRTUAL_MACHINE.getNumber(),
                        VM1_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VM1_ID)))
                .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, reconfigureWithoutSourceRecommendation);
        assertEquals(description, "Reconfigure Virtual Machine vm1_test as it is unplaced");
    }

    /**
     * Test the description of reconfigure action without source.
     */
    @Test
    public void testBuildReconfigureActionSettingChange()
                    throws UnsupportedActionException {
        checkReconfigureSettingChanges(ImmutableList.of("Virtual Machine", "vm1_test", "reconfig_policy_1"), 12L,
                createSettingChange(EntityAttribute.SOCKET));
    }

    @Nonnull
    protected SettingChange createSettingChange(EntityAttribute attribute) {
        return SettingChange.newBuilder().setEntityAttribute(attribute)
                        .setCurrentValue(1).setNewValue(2).build();
    }

    /**
     * Test the description of reconfigure action without source.
     */
    @Test
    public void testBuildReconfigureActionSettingChanges() throws UnsupportedActionException {
        checkReconfigureSettingChanges(
                        ImmutableList.of("Virtual Machine", "vm1_test", "reconfig_policy_2"), 13L,
                createSettingChange(EntityAttribute.SOCKET),
                createSettingChange(EntityAttribute.CORES_PER_SOCKET));
    }

    protected void checkReconfigureSettingChanges(List<String> expectedDescriptionParts,
            Long policyId, SettingChange... settingChanges) throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID))).thenReturn(
                        (createEntity(VM1_ID, EntityType.VIRTUAL_MACHINE.getNumber(),
                                        VM1_DISPLAY_NAME, 0, 0)));
        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VM1_ID)))
                .thenReturn(Optional.empty());

        when(entitySettingsCache.getPolicyIdToDisplayName()).thenReturn(policyIdToDisplayNameMap());

        Explanation explanation = Explanation.newBuilder().setReconfigure(
                makeReconfigureReasonSettingsExplanation(policyId)).build();

        final ActionDTO.Action action = makeRec(makeReconfigureInfo(VM1_ID, settingChanges),
                        SupportLevel.SUPPORTED, explanation).build();
        String description = ActionDescriptionBuilder.buildActionDescription(
                        entitySettingsCache, action);
        Assert.assertThat(description, Matchers.stringContainsInOrder(expectedDescriptionParts));
    }

    private Map<Long, String> policyIdToDisplayNameMap(){
        return ImmutableMap.of(
                12L, "reconfig_policy_1",
                13L, "reconfig_policy_2");
    }

    @Test
    public void testBuildResizeActionDescription() throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
            .thenReturn((createEntity(VM1_ID,
                EntityType.VIRTUAL_MACHINE.getNumber(),
                VM1_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VM1_ID))).thenReturn(
                Optional.of(EntityWithConnections.newBuilder()
                        .setOid(BUSINESS_ACCOUNT_OID)
                        .setEntityType(EntityType.BUSINESS_ACCOUNT.getNumber())
                        .setDisplayName(BUSINESS_ACCOUNT_NAME)
                        .build()));

        String description = ActionDescriptionBuilder.buildActionDescription(
                                                     entitySettingsCache, resizeRecommendation);

        Assert.assertThat(description,
                Matchers.stringContainsInOrder(CommodityDTO.CommodityType.VCPU.name(), "vm1_test", "10", "20", "Development"));
    }

    /**
     * Test the description of resize action without account name.
     *
     * @throws UnsupportedActionException if something is extraordinarily wrong.
     */
    @Test
    public void testBuildResizeActionDescriptionWithNoAccountName() throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
                .thenReturn((createEntity(VM1_ID,
                        EntityType.VIRTUAL_MACHINE.getNumber(),
                        VM1_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VM1_ID)))
                .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, resizeRecommendation);

        Assert.assertThat(description,
                Matchers.stringContainsInOrder(CommodityDTO.CommodityType.VCPU.name(), "vm1_test", "10", "20"));
    }

    @Test
    public void testBuildResizeMemActionDescription() throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
            .thenReturn((createEntity(VM1_ID,
                EntityType.VIRTUAL_MACHINE.getNumber(),
                VM1_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VM1_ID)))
                .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, resizeMemRecommendation);

        assertEquals(description, "Resize down Mem for Virtual Machine vm1_test from 16 GB to 8 GB");
    }

    @Test
    public void testBuildResizeVmemAndReservationActionDescription()
            throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID))).thenReturn(
                (createEntity(VM1_ID, EntityType.VIRTUAL_MACHINE.getNumber(), VM1_DISPLAY_NAME, 0,
                        14 * Units.MBYTE)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VM1_ID)))
                .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(entitySettingsCache,
                makeRec(makeResizeVmemInfo(VM1_ID, 16, 8), SupportLevel.SUPPORTED).build());

        assertEquals(
                "Resize down VMem and Reservation for Virtual Machine vm1_test from 16 GB to 8 GB",
                description);
    }

    @Test
    public void testBuildResizeVcpuAndReservationActionDescription()
            throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID))).thenReturn(
                (createEntity(VM1_ID, EntityType.VIRTUAL_MACHINE.getNumber(), VM1_DISPLAY_NAME, 12 * 2600,
                        0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VM1_ID)))
                .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(entitySettingsCache,
                makeRec(makeResizeVcpuInfo(VM1_ID, 16, 8), SupportLevel.SUPPORTED).build());

        Assert.assertThat(description,
                Matchers.stringContainsInOrder(CommodityDTO.CommodityType.VCPU.name(), "Reservation", "vm1_test", "16", "8"));
    }


    /**
     * Tests the description for VStorage action.
     * @throws UnsupportedActionException
     */
    @Test
    public void testBuildResizeVStorageActionDescription() throws UnsupportedActionException {
        ActionVirtualMachineInfo vmInfo = ActionVirtualMachineInfo.newBuilder()
                .putPartitions(VSTORAGE_KEY, "/tmp").build();
        ActionEntityTypeSpecificInfo info = ActionEntityTypeSpecificInfo.newBuilder()
                .setVirtualMachine(vmInfo).build();
        ActionPartialEntity vm = ActionPartialEntity.newBuilder().setOid(VM1_ID)
                .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
                .setDisplayName(VM1_DISPLAY_NAME).setTypeSpecificInfo(info).build();
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
                .thenReturn(Optional.of(vm));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VM1_ID)))
                .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, resizeVStorageRecommendation);

        assertEquals("Resize up VStorage /tmp for Virtual Machine vm1_test from 1.95 GB to 3.91 GB",
                description);
    }

    /**
     * Test resize reservation action description.
     */
    @Test
    public void testBuildResizeMemReservationActionDescription() throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
                .thenReturn((createEntity(VM1_ID,
                        EntityType.VIRTUAL_MACHINE.getNumber(),
                        VM1_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VM1_ID)))
                .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, resizeMemReservationRecommendation);

        assertEquals(description,
                "Resize down Mem reservation for Virtual Machine vm1_test from 16 GB to 8 GB");
    }

    /**
     * Test resize Vcpu action description for VM.
     */
    @Test
    public void testBuildResizeVcpuActionDescriptionForVM() throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
            .thenReturn((createEntity(VM1_ID,
                EntityType.VIRTUAL_MACHINE.getNumber(),
                VM1_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VM1_ID)))
                .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, resizeVcpuRecommendationForVM);

        Assert.assertThat(description,
                Matchers.stringContainsInOrder(CommodityDTO.CommodityType.VCPU.name(), "vm1_test", "16", "8"));
    }

    /**
     * Test resize VCPU action description for the case when CPS value has to be adjusted as well
     * for VM.
     */
    @Test
    public void testBuildResizeVcpuCpsActionDescriptionForVM() throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID))).thenReturn(
                        (createEntity(VM1_ID, EntityType.VIRTUAL_MACHINE.getNumber(),
                                        VM1_DISPLAY_NAME, 0, 0)));
        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VM1_ID)))
                .thenReturn(Optional.empty());
        resizeVcpuRecommendationForVM =
                        makeRec(makeResizeInfo(VM1_ID, CommodityDTO.CommodityType.VCPU_VALUE, null,
                                        8, 16, 1, 2), SupportLevel.SUPPORTED).build();
        String description = ActionDescriptionBuilder.buildActionDescription(entitySettingsCache,
                        resizeVcpuRecommendationForVM);
        Assert.assertThat(description,
                        Matchers.stringContainsInOrder(CommodityDTO.CommodityType.VCPU.name(), "vm1_test", "8", "16"));
    }

    /**
     * Test resize Vcpu reservation action description for VM.
     */
    @Test
    public void testBuildResizeVcpuReservationActionDescriptionForVM()
            throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
            .thenReturn((createEntity(VM1_ID,
                EntityType.VIRTUAL_MACHINE.getNumber(),
                VM1_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VM1_ID)))
                .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, resizeVcpuReservationRecommendationForVM);

        Assert.assertThat(description,
                Matchers.stringContainsInOrder(CommodityDTO.CommodityType.VCPU.name(), "reservation", "vm1_test", "16", "8"));
    }

    /**
     * Test resize Vcpu action description for container.
     */
    @Test
    public void testBuildResizeVcpuActionDescriptionForContainer()
            throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(CONTAINER1_ID)))
            .thenReturn((createEntity(CONTAINER1_ID,
                EntityType.CONTAINER.getNumber(),
                CONTAINER1_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(CONTAINER1_ID)))
                .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, resizeVcpuRecommendationForContainer);

        assertEquals(description,
            "Resize down VCPU for Container container1_test from 16 mCores to 8 mCores");
    }

    /**
     * Test resize Vcpu reservation action description for container.
     */
    @Test
    public void testBuildResizeVcpuReservationActionDescriptionForContainer()
            throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(CONTAINER1_ID)))
            .thenReturn((createEntity(CONTAINER1_ID,
                EntityType.CONTAINER.getNumber(),
                CONTAINER1_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(CONTAINER1_ID)))
                .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, resizeVcpuReservationRecommendationForContainer);

        assertEquals(description,
            "Resize down VCPU reservation for Container container1_test from 16 mCores to 8 mCores");
    }

    /**
     * Test resize VMemRequest for container.
     *
     * @throws UnsupportedActionException Exception if action is not supported.
     */
    @Test
    public void testBuildResizeVMemRequestActionDescriptionForContainer()
        throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(CONTAINER1_ID)))
            .thenReturn((createEntity(CONTAINER1_ID,
                EntityType.CONTAINER.getNumber(),
                CONTAINER1_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(CONTAINER1_ID)))
                .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, resizeVemRequestRecommendationForContainer);

        assertEquals(description,
            "Resize down VMem Request for Container container1_test from 3.12 MB to 2.15 MB");
    }

    /**
     * Test resize VCPULimitQuota for namespace.
     *
     * @throws UnsupportedActionException Exception if action is not supported.
     */
    @Test
    public void testBuildResizeVCPULimitQuotaActionDescriptionForNamespace()
        throws UnsupportedActionException {
            when(entitySettingsCache.getEntityFromOid(eq(NAMESPACE_ID)))
                .thenReturn((createEntity(NAMESPACE_ID,
                    EntityType.NAMESPACE_VALUE,
                    NAMESPACE_DISPLAY_NAME, 0, 0)));

            when(entitySettingsCache.getOwnerAccountOfEntity(eq(NAMESPACE_ID)))
                .thenReturn(Optional.empty());

            String description = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, resizeVcpuLimitQuotaRecommendationForNamespace);

            assertEquals(description,
                "Resize up VCPU Limit Quota for Namespace namespace_test from 100 mCores to 200 mCores");
    }

    /**
     * Test resize VMemLimitQuota for namespace.
     *
     * @throws UnsupportedActionException Exception if action is not supported.
     */
    @Test
    public void testBuildResizeVMemLimitQuotaActionDescriptionForNamespace()
        throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(NAMESPACE_ID)))
            .thenReturn((createEntity(NAMESPACE_ID,
                EntityType.NAMESPACE_VALUE,
                NAMESPACE_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(NAMESPACE_ID)))
                .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, resizeVmemLimitQuotaRecommendationForNamespace);

        assertEquals(description,
            "Resize up VMem Limit Quota for Namespace namespace_test from 128 MB to 256 MB");
    }

    /**
     * Test that the unit is converted to more readable format in the final description for
     * resize DBMem action.
     */
    @Test
    public void testBuildResizeDBMemActionDescription() throws UnsupportedActionException {
        final long dbsId = 19L;
        when(entitySettingsCache.getEntityFromOid(eq(dbsId))).thenReturn(
                createEntity(dbsId, EntityType.DATABASE_SERVER_VALUE, "sqlServer1", 0, 0));
        when(entitySettingsCache.getOwnerAccountOfEntity(eq(dbsId)))
                .thenReturn(Optional.empty());
        ActionDTO.Action resizeDBMem = makeRec(makeResizeInfo(dbsId,
                CommodityDTO.CommodityType.DB_MEM_VALUE, 15857614.848f, 6159335.424f),
                SupportLevel.SUPPORTED).build();
        String description = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, resizeDBMem);
        assertEquals(description,
                "Resize down DB Mem for Database Server sqlServer1 from 15.12 GB to 5.87 GB");
    }

    /**
     * Test vSan Storage StorageAmount resize up action description.
     * @throws UnsupportedActionException unsupported error
     */
    @Test
    public void testBuildResizeUpVSanStorageAmountActionDescription() throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VSAN_STORAGE_ID)))
        .thenReturn((createEntity(VSAN_STORAGE_ID,
            EntityType.STORAGE.getNumber(),
            VSAN_STORAGE_DISPLAY_NAME, 0, 0)));
        when(entitySettingsCache.getEntityFromOid(eq(VSAN_PM_ID)))
        .thenReturn((createEntity(VSAN_PM_ID,
            EntityType.PHYSICAL_MACHINE.getNumber(),
            VSAN_PM_DISPLAY_NAME, 0, 0)));
        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VSAN_STORAGE_ID)))
                .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(
                        entitySettingsCache, resizeStorageAmountRecommendationForVSanStorageUp);

        assertEquals("Resize up Storage Amount for Storage vsan_storage from 50 GB to 75 GB",
                        description);
    }

    /**
     * Test vSan Storage StorageAmount resize down action description.
     * @throws UnsupportedActionException unsupported error
     */
    @Test
    public void testBuildResizeDownVSanStorageAmountActionDescription() throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VSAN_STORAGE_ID)))
        .thenReturn((createEntity(VSAN_STORAGE_ID,
            EntityType.STORAGE.getNumber(),
            VSAN_STORAGE_DISPLAY_NAME, 0, 0)));
        when(entitySettingsCache.getEntityFromOid(eq(VSAN_PM_ID)))
        .thenReturn((createEntity(VSAN_PM_ID,
            EntityType.PHYSICAL_MACHINE.getNumber(),
            VSAN_PM_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VSAN_STORAGE_ID)))
                .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(
                        entitySettingsCache, resizeStorageAmountRecommendationForVSanStorageDown);

        assertEquals("Resize down Storage Amount for Storage vsan_storage from 75 GB to 50 GB",
                        description);
    }

    @Test
    public void testBuildDeactivateActionDescription() throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
            .thenReturn((createEntity(VM1_ID,
                EntityType.VIRTUAL_MACHINE.getNumber(),
                VM1_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VM1_ID))).thenReturn(
                Optional.of(EntityWithConnections.newBuilder()
                        .setOid(BUSINESS_ACCOUNT_OID)
                        .setEntityType(EntityType.BUSINESS_ACCOUNT.getNumber())
                        .setDisplayName(BUSINESS_ACCOUNT_NAME)
                        .build()));

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, deactivateRecommendation);

        assertEquals(description, "Suspend Virtual Machine vm1_test in Development");
    }

    /**
     * Test Deactivate action description without account name.
     * @throws UnsupportedActionException unsupported error
     */
    @Test
    public void testBuildDeactivateActionDescriptionWithNoAccountName() throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
                .thenReturn((createEntity(VM1_ID,
                        EntityType.VIRTUAL_MACHINE.getNumber(),
                        VM1_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VM1_ID)))
                .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, deactivateRecommendation);

        assertEquals(description, "Suspend Virtual Machine vm1_test");
    }

    @Test
    public void testBuildActivateActionDescription() throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
            .thenReturn((createEntity(VM1_ID,
                EntityType.VIRTUAL_MACHINE.getNumber(),
                VM1_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VM1_ID))).thenReturn(
                Optional.of(EntityWithConnections.newBuilder()
                        .setOid(BUSINESS_ACCOUNT_OID)
                        .setEntityType(EntityType.BUSINESS_ACCOUNT.getNumber())
                        .setDisplayName(BUSINESS_ACCOUNT_NAME)
                        .build()));

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, activateRecommendation);

        assertEquals(description,
                "Start Virtual Machine vm1_test in Development");
    }

    /**
     * Test Activate action description without account name.
     * @throws UnsupportedActionException unsupported error
     */
    @Test
    public void testBuildActivateActionDescriptionWithNoAccountName() throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
                .thenReturn((createEntity(VM1_ID,
                        EntityType.VIRTUAL_MACHINE.getNumber(),
                        VM1_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VM1_ID)))
                .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, activateRecommendation);

        assertEquals(description, "Start Virtual Machine vm1_test");
    }

    /**
     * Test ProvisionBySupply action description.
     */
    @Test
    public void testBuildProvisionBySupplyActionDescription() throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(ST_SOURCE_ID)))
            .thenReturn(createEntity(ST_SOURCE_ID,
                EntityType.STORAGE.getNumber(),
                ST_SOURCE_DISPLAY_NAME, 0, 0));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(ST_SOURCE_ID))).thenReturn(
                Optional.of(EntityWithConnections.newBuilder()
                        .setOid(BUSINESS_ACCOUNT_OID)
                        .setEntityType(EntityType.BUSINESS_ACCOUNT.getNumber())
                        .setDisplayName(BUSINESS_ACCOUNT_NAME)
                        .build()));

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, provisionBySupplyRecommendation);

        assertEquals("Provision Storage similar to storage_source_test in Development",
                description);
    }

    /**
     * Test ProvisionBySupply action description with no account number.
     * @throws UnsupportedActionException unsupported error
     */
    @Test
    public void testBuildProvisionBySupplyActionDescriptionWithNoAccountNumber() throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(ST_SOURCE_ID)))
                .thenReturn(createEntity(ST_SOURCE_ID,
                        EntityType.STORAGE.getNumber(),
                        ST_SOURCE_DISPLAY_NAME, 0, 0));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(ST_SOURCE_ID)))
                .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, provisionBySupplyRecommendation);

        assertEquals("Provision Storage similar to storage_source_test", description);
    }

    /**
     * Test ProvisionByDemand action description.
     */
    @Test
    public void testBuildProvisionByDemandActionDescription() throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(PM_SOURCE_ID)))
            .thenReturn(createEntity(PM_SOURCE_ID,
                EntityType.PHYSICAL_MACHINE.getNumber(),
                PM_SOURCE_DISPLAY_NAME, 0, 0));
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
            .thenReturn(createEntity(VM1_ID,
                EntityType.VIRTUAL_MACHINE.getNumber(),
                VM1_DISPLAY_NAME, 0, 0));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(PM_SOURCE_ID)))
                .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, provisionByDemandRecommendation);

        assertEquals(description,
                "Provision Physical Machine similar to pm_source_test with scaled up Mem due to vm1_test");
    }


    /**
     * Test the construction of description for host provision action when vSAN scaling is the reason.
     * @throws UnsupportedActionException   an exception that can be thrown by {@link ActionDescriptionBuilder}
     */
    public void testProvisionHostForVSANDescription() throws UnsupportedActionException {
        ConnectedEntity vsanConnectedEntity = ConnectedEntity.newBuilder()
                        .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                        .setConnectedEntityType(EntityType.STORAGE_VALUE)
                        .setConnectedEntityId(VSAN_STORAGE_ID)
                        .build();
        Optional<ActionPartialEntity> optionalHostEntity = Optional.of(ActionPartialEntity.newBuilder()
                        .setOid(VSAN_PM_ID)
                        .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                        .setDisplayName(VSAN_PM_DISPLAY_NAME)
                        .addConnectedEntities(vsanConnectedEntity)
                        .build());

        when(entitySettingsCache.getEntityFromOid(eq(VSAN_PM_ID)))
            .thenReturn(optionalHostEntity);
        when(entitySettingsCache.getEntityFromOid(eq(VSAN_STORAGE_ID)))
            .thenReturn((createEntity(VSAN_STORAGE_ID, EntityType.STORAGE_VALUE,
                            VSAN_STORAGE_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VM1_ID)))
                .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(
                        entitySettingsCache, provisionBySupplyRecommendation);
        String expected = new StringBuilder("Provision Host similar to ")
                        .append(VSAN_PM_DISPLAY_NAME)
                        .append(" to scale Storage ").append(VSAN_STORAGE_DISPLAY_NAME)
                        .toString();
        assertEquals(expected, description);
    }

    /**
     * Test port channel resize action description.
     */
    @Test
    public void testBuildResizePortChannelOnSwitchActionDescription() throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(SWITCH1_ID)))
            .thenReturn(createEntity(SWITCH1_ID,
                EntityType.SWITCH.getNumber(),
                SWITCH1_DISPLAY_NAME, 0, 0));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(SWITCH1_ID)))
                .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, resizePortChannelRecommendation);

        assertEquals("Resize up Port Channel 10.0.100.132:sys/switch-A/sys/chassis-1/slot-1 for Switch switch1_test from 1,000 to 2,000", description);
    }

    @Test
    public void testBuildDeleteActionDescription() throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(ST_SOURCE_ID)))
            .thenReturn((createEntity(ST_SOURCE_ID,
                EntityType.STORAGE.getNumber(),
                ST_SOURCE_DISPLAY_NAME, 0, 0)));

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, deleteRecommendation);

        assertEquals(description, "Delete wasted file 'filename.test' from Storage storage_source_test to free up 0 bytes");
    }

    /**
     * Test virtual machine spec delete action description.
     */
    @Test
    public void testBuildDeleteVirtualMachineSpecActionDescription() throws UnsupportedActionException {
        final long businessAccountOid = 88L;
        final String businessAccountName = "business_account_name";
        when(entitySettingsCache.getEntityFromOid(eq(VM_SPEC_ID)))
                .thenReturn((createEntity(VM_SPEC_ID,
                        EntityType.VIRTUAL_MACHINE_SPEC.getNumber(),
                        VM_SPEC_DISPLAY_NAME, 0, 0)));
        when(entitySettingsCache.getEntityFromOid(eq(CT_SOURCE_ID)))
                .thenReturn((createEntity(CT_SOURCE_ID,
                        EntityType.COMPUTE_TIER.getNumber(),
                        CT_SOURCE_DISPLAY_NAME, 0, 0)));
        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VM_SPEC_ID)))
                .thenReturn(Optional.of(EntityWithConnections.newBuilder()
                        .setOid(businessAccountOid)
                        .setEntityType(EntityType.BUSINESS_ACCOUNT.getNumber())
                        .setDisplayName(businessAccountName)
                        .build()));

        String description = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, deleteVirtualMachineSpecRecommendation);

        assertEquals(description, "Delete Empty compute_source_test App Service Plan vm_spec_test from business_account_name");
    }

    @Test
    public void testBuildDeleteCloudStorageActionDescription_whenSearchServiceHasNoBAInfo()
            throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VV_ID)))
            .thenReturn((createEntity(VV_ID,
                EntityType.VIRTUAL_VOLUME.getNumber(),
                VV_DISPLAY_NAME, 0, 0)));
        when(entitySettingsCache.getEntityFromOid(eq(ST_SOURCE_ID)))
            .thenReturn((createEntity(ST_SOURCE_ID,
                EntityType.STORAGE_TIER.getNumber(),
                ST_SOURCE_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VV_ID)))
            .thenReturn(Optional.empty());

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, deleteCloudStorageRecommendation);

        assertEquals(description, "Delete Unattached storage_source_test Volume volume_display_name");
    }

    @Test
    public void testBuildDeleteCloudStorageActionDescription_whenSearchServiceHasBAInfo()
            throws UnsupportedActionException {
        final long businessAccountOid = 88L;
        final String businessAccountName = "business_account_name";
        when(entitySettingsCache.getEntityFromOid(eq(VV_ID)))
            .thenReturn((createEntity(VV_ID,
                EntityType.VIRTUAL_VOLUME.getNumber(),
                VV_DISPLAY_NAME, 0, 0)));
        when(entitySettingsCache.getEntityFromOid(eq(ST_SOURCE_ID)))
            .thenReturn((createEntity(ST_SOURCE_ID,
                EntityType.STORAGE_TIER.getNumber(),
                ST_SOURCE_DISPLAY_NAME, 0, 0)));
        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VV_ID)))
            .thenReturn(Optional.of(EntityWithConnections.newBuilder()
                .setOid(businessAccountOid)
                .setEntityType(EntityType.BUSINESS_ACCOUNT.getNumber())
                .setDisplayName(businessAccountName)
                .build()));

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, deleteCloudStorageRecommendation);

        assertEquals(description, "Delete Unattached storage_source_test Volume volume_display_name from business_account_name");
    }

    /**
     * Test Delete Cloud Storage Action when snapshot has no BA info.
     */
    @Test
    public void testBuildDeleteCloudStorageActionDescription_whenSnapshotDoesNotHaveSourceEntity_SearchServiceHasBAInfo()
            throws UnsupportedActionException {
        final long businessAccountOid = 88L;
        final String businessAccountName = "business_account_name";
        when(entitySettingsCache.getEntityFromOid(eq(VV_ID)))
            .thenReturn((createEntity(VV_ID,
                EntityType.VIRTUAL_VOLUME.getNumber(),
                VV_DISPLAY_NAME, 0, 0)));
        when(entitySettingsCache.getEntityFromOid(eq(ST_SOURCE_ID)))
            .thenReturn(Optional.empty());
        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VV_ID)))
            .thenReturn(Optional.of(EntityWithConnections.newBuilder()
                .setOid(businessAccountOid)
                .setEntityType(EntityType.BUSINESS_ACCOUNT.getNumber())
                .setDisplayName(businessAccountName)
                .build()));

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, deleteCloudStorageRecommendation);

        assertEquals(description, "Delete Unattached  Volume volume_display_name from business_account_name");
    }

    /**
     * Test Delete Cloud Storage Action Description when action has no source entity.
     */
    @Test
    public void testBuildDeleteCloudStorageActionDescription_whenActionHasNoSourceEntity_SearchServiceHasBAInfo()
            throws UnsupportedActionException {
        final long businessAccountOid = 88L;
        final String businessAccountName = "business_account_name";
        when(entitySettingsCache.getEntityFromOid(eq(VV_ID)))
            .thenReturn((createEntity(VV_ID,
                EntityType.VIRTUAL_VOLUME.getNumber(),
                VV_DISPLAY_NAME, 0, 0)));
        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VV_ID)))
            .thenReturn(Optional.of(EntityWithConnections.newBuilder()
                .setOid(businessAccountOid)
                .setEntityType(EntityType.BUSINESS_ACCOUNT.getNumber())
                .setDisplayName(businessAccountName)
                .build()));

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, deleteCloudStorageRecommendationWithNoSourceEntity);

        assertEquals(description, "Delete Unattached  Volume volume_display_name from business_account_name");
    }

    @Test
    public void testBuildBuyRIActionDescription() throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(COMPUTE_TIER_SOURCE_ID)))
            .thenReturn((createEntity(COMPUTE_TIER_SOURCE_ID,
                EntityType.COMPUTE_TIER.getNumber(),
                COMPUTE_TIER_SOURCE_DISPLAY_NAME, 0, 0)));
        when(entitySettingsCache.getEntityFromOid(eq(MASTER_ACCOUNT_ID)))
            .thenReturn((createEntity(MASTER_ACCOUNT_ID,
                EntityType.BUSINESS_ACCOUNT.getNumber(),
                MASTER_ACCOUNT_DISPLAY_NAME, 0, 0)));
        when(entitySettingsCache.getEntityFromOid(eq(REGION_ID)))
            .thenReturn((createEntity(REGION_ID,
                EntityType.REGION.getNumber(),
                REGION_DISPLAY_NAME, 0, 0)));

        String description = ActionDescriptionBuilder.buildActionDescription(
            entitySettingsCache, buyRIRecommendation);

        assertEquals(description, "Buy 0 tier_t1 RIs for my.super.account in Manhattan");
    }

    /**
     * Test building description for Allocation action.
     *
     * @throws UnsupportedActionException In case of unsupported action type.
     */
    @Test
    public void testBuildAllocateActionDescription() throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
                .thenReturn((createEntity(VM1_ID,
                        EntityType.VIRTUAL_MACHINE.getNumber(),
                        VM1_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getEntityFromOid(eq(COMPUTE_TIER_SOURCE_ID)))
            .thenReturn(createEntity(COMPUTE_TIER_SOURCE_ID,
                EntityType.COMPUTE_TIER.getNumber(),
                COMPUTE_TIER_SOURCE_DISPLAY_NAME, 0, 0));

        final long businessAccountOid = 88L;
        final String businessAccountName = "Development";
        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VM1_ID)))
                .thenReturn(Optional.of(EntityWithConnections.newBuilder()
                        .setOid(businessAccountOid)
                        .setEntityType(EntityType.BUSINESS_ACCOUNT.getNumber())
                        .setDisplayName(businessAccountName)
                        .build()));

        final String description = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, allocateRecommendation);
        assertEquals(description,
                "Increase RI coverage for Virtual Machine vm1_test (tier_t1) in Development");
    }

    /**
     * Test building description for Allocation action when no account is present.
     *
     * @throws UnsupportedActionException In case of unsupported action type.
     */
    @Test
    public void testBuildAllocateActionDescriptionWithNoAccount()
            throws UnsupportedActionException {
        when(entitySettingsCache.getEntityFromOid(eq(VM1_ID)))
                .thenReturn((createEntity(VM1_ID,
                        EntityType.VIRTUAL_MACHINE.getNumber(),
                        VM1_DISPLAY_NAME, 0, 0)));

        when(entitySettingsCache.getEntityFromOid(eq(COMPUTE_TIER_SOURCE_ID)))
            .thenReturn(createEntity(COMPUTE_TIER_SOURCE_ID,
                EntityType.COMPUTE_TIER.getNumber(),
                COMPUTE_TIER_SOURCE_DISPLAY_NAME, 0, 0));

        when(entitySettingsCache.getOwnerAccountOfEntity(eq(VM1_ID)))
                .thenReturn(Optional.empty());

        final String description = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, allocateRecommendation);
        assertEquals(description,
                "Increase RI coverage for Virtual Machine vm1_test (tier_t1) in ");
    }

    /**
     * Test atomic resize description.
     *
     * @throws UnsupportedActionException  In case of unsupported action type.
     */
    @Test
    public void testAtomicResizeDescription() throws UnsupportedActionException {
        final ActionDTO.Action.Builder atomicAction = ActionDTO.Action.newBuilder()
                .setId(0).setDeprecatedImportance(0)
                .setInfo(ActionInfo.newBuilder()
                        .setAtomicResize(AtomicResize.newBuilder()
                                .setExecutionTarget(ActionEntity.newBuilder()
                                        .setId(CONTROLLER1_ID)
                                        .setType(EntityType.WORKLOAD_CONTROLLER_VALUE))
                                .addResizes(ResizeInfo.newBuilder()
                                        .setTarget(ActionEntity.newBuilder()
                                                .setId(CONTAINER_SPEC1_ID)
                                                .setType(EntityType.CONTAINER_SPEC_VALUE))
                                        .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE))
                                        .setCommodityAttribute(CommodityAttribute.CAPACITY)
                                        .setOldCapacity(123).setNewCapacity(456))
                                .addResizes(ResizeInfo.newBuilder()
                                        .setTarget(ActionEntity.newBuilder()
                                                .setId(CONTAINER_SPEC1_ID)
                                                .setType(EntityType.CONTAINER_SPEC_VALUE))
                                        .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VMEM_VALUE))
                                        .setCommodityAttribute(CommodityAttribute.CAPACITY)
                                        .setOldCapacity(890).setNewCapacity(567))
                                .addResizes(ResizeInfo.newBuilder()
                                        .setTarget(ActionEntity.newBuilder()
                                                .setId(CONTAINER_SPEC2_ID)
                                                .setType(EntityType.CONTAINER_SPEC_VALUE))
                                        .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VMEM_REQUEST_VALUE))
                                        .setCommodityAttribute(CommodityAttribute.CAPACITY)
                                        .setOldCapacity(890).setNewCapacity(567))
                                .addResizes(ResizeInfo.newBuilder()
                                        .setTarget(ActionEntity.newBuilder()
                                                .setId(CONTAINER_SPEC2_ID)
                                                .setType(EntityType.CONTAINER_SPEC_VALUE))
                                        .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_REQUEST_VALUE))
                                        .setCommodityAttribute(CommodityAttribute.CAPACITY)
                                        .setOldCapacity(123).setNewCapacity(456))))
                .setExplanation(Explanation.newBuilder()
                        .setAtomicResize(AtomicResizeExplanation.newBuilder()
                                .setMergeGroupId("bar")));

        when(entitySettingsCache.getEntityFromOid(eq(CONTROLLER1_ID)))
                .thenReturn((createEntity(CONTROLLER1_ID,
                        EntityType.WORKLOAD_CONTROLLER_VALUE, CONTROLLER1_DISPLAY_NAME, 0, 0)));
        when(entitySettingsCache.getEntityFromOid(eq(CONTAINER_SPEC1_ID)))
                .thenReturn((createEntity(CONTAINER_SPEC1_ID,
                        EntityType.CONTAINER_SPEC_VALUE, SPEC1_DISPLAY_NAME, 0, 0)));
        when(entitySettingsCache.getEntityFromOid(eq(CONTAINER_SPEC2_ID)))
                .thenReturn((createEntity(CONTAINER_SPEC2_ID,
                        EntityType.CONTAINER_SPEC_VALUE, SPEC2_DISPLAY_NAME, 0, 0)));
        when(entitySettingsCache.getOwnerAccountOfEntity(eq(CONTROLLER1_ID))).thenReturn(
                Optional.of(EntityWithConnections.newBuilder()
                        .setOid(BUSINESS_ACCOUNT_OID)
                        .setEntityType(EntityType.BUSINESS_ACCOUNT.getNumber())
                        .setDisplayName(BUSINESS_ACCOUNT_NAME)
                        .build()));

        final String description = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, atomicAction.build());
        assertTrue(description.startsWith("Resize"));
        assertTrue(description.contains("VCPU Request")
                        && description.contains("VMem Request")
                        && description.contains("VCPU Limit")
                        && description.contains("VMem Limit"));
        assertTrue(description.endsWith("Workload Controller controller1_test in Development"));
    }

    /**
     * Test atomic resize description with no account number.
     *
     * @throws UnsupportedActionException  In case of unsupported action type.
     */
    @Test
    public void testAtomicResizeDescriptionWithNoAccountNumber() throws UnsupportedActionException {
        final ActionDTO.Action.Builder atomicAction = ActionDTO.Action.newBuilder()
                .setId(0).setDeprecatedImportance(0)
                .setInfo(ActionInfo.newBuilder()
                        .setAtomicResize(AtomicResize.newBuilder()
                                .setExecutionTarget(ActionEntity.newBuilder()
                                        .setId(CONTROLLER1_ID)
                                        .setType(EntityType.WORKLOAD_CONTROLLER_VALUE))
                                .addResizes(ResizeInfo.newBuilder()
                                        .setTarget(ActionEntity.newBuilder()
                                                .setId(CONTAINER_SPEC1_ID)
                                                .setType(EntityType.CONTAINER_SPEC_VALUE))
                                        .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE))
                                        .setCommodityAttribute(CommodityAttribute.CAPACITY)
                                        .setOldCapacity(123).setNewCapacity(456))
                                .addResizes(ResizeInfo.newBuilder()
                                        .setTarget(ActionEntity.newBuilder()
                                                .setId(CONTAINER_SPEC1_ID)
                                                .setType(EntityType.CONTAINER_SPEC_VALUE))
                                        .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VMEM_VALUE))
                                        .setCommodityAttribute(CommodityAttribute.CAPACITY)
                                        .setOldCapacity(890).setNewCapacity(567))
                                .addResizes(ResizeInfo.newBuilder()
                                        .setTarget(ActionEntity.newBuilder()
                                                .setId(CONTAINER_SPEC2_ID)
                                                .setType(EntityType.CONTAINER_SPEC_VALUE))
                                        .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VMEM_REQUEST_VALUE))
                                        .setCommodityAttribute(CommodityAttribute.CAPACITY)
                                        .setOldCapacity(890).setNewCapacity(567))
                                .addResizes(ResizeInfo.newBuilder()
                                        .setTarget(ActionEntity.newBuilder()
                                                .setId(CONTAINER_SPEC2_ID)
                                                .setType(EntityType.CONTAINER_SPEC_VALUE))
                                        .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_REQUEST_VALUE))
                                        .setCommodityAttribute(CommodityAttribute.CAPACITY)
                                        .setOldCapacity(123).setNewCapacity(456))))
                .setExplanation(Explanation.newBuilder()
                        .setAtomicResize(AtomicResizeExplanation.newBuilder()
                                .setMergeGroupId("bar")));

        when(entitySettingsCache.getEntityFromOid(eq(CONTROLLER1_ID)))
                .thenReturn((createEntity(CONTROLLER1_ID,
                        EntityType.WORKLOAD_CONTROLLER_VALUE, CONTROLLER1_DISPLAY_NAME, 0, 0)));
        when(entitySettingsCache.getEntityFromOid(eq(CONTAINER_SPEC1_ID)))
                .thenReturn((createEntity(CONTAINER_SPEC1_ID,
                        EntityType.CONTAINER_SPEC_VALUE, SPEC1_DISPLAY_NAME, 0, 0)));
        when(entitySettingsCache.getEntityFromOid(eq(CONTAINER_SPEC2_ID)))
                .thenReturn((createEntity(CONTAINER_SPEC2_ID,
                        EntityType.CONTAINER_SPEC_VALUE, SPEC2_DISPLAY_NAME, 0, 0)));
        when(entitySettingsCache.getOwnerAccountOfEntity(eq(CONTROLLER1_ID)))
                .thenReturn(Optional.empty());

        final String description = ActionDescriptionBuilder.buildActionDescription(
                entitySettingsCache, atomicAction.build());
        assertTrue(description.startsWith("Resize"));
        assertTrue(description.contains("VCPU Request")
                && description.contains("VMem Request")
                && description.contains("VCPU Limit")
                && description.contains("VMem Limit"));
        assertTrue(description.endsWith("Workload Controller controller1_test"));
    }
}
