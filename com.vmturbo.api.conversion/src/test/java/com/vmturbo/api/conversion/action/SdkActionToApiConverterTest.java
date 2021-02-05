package com.vmturbo.api.conversion.action;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.time.OffsetDateTime;
import java.util.Collections;

import org.junit.Test;

import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionExecutionCharacteristicApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.notification.LogEntryApiDTO;
import com.vmturbo.api.enums.ActionDisruptiveness;
import com.vmturbo.api.enums.ActionReversibility;
import com.vmturbo.api.enums.ActionState;
import com.vmturbo.api.enums.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution;
import com.vmturbo.platform.common.dto.CommonDTO;

/**
 * Tests the case where we are converting different SDK messages to API messages.
 */
public class SdkActionToApiConverterTest {
    private static final long ACTION_OID = 144151046183132L;
    private static final long ACTION_CREATION_TIME = 1609367961000L;
    private static final long ACTION_UPDATE_TIME = 1609367994000L;
    private static final String ACCEPTED_BY = "administrator(3391249354768)";
    private static final long ACTION_UUID = 144151046183109L;
    private static final long VM_UUID = 23525323L;
    private static final String VM_LOCAL_NAME = "vm-27442";
    private static final String VM_DISPLAY_NAME = "turbonomic-t8c-8.0.6-20201219141928000-168.161";
    private static final String TARGET_ADDRESS = "hp-dl365.corp.vmturbo.com";
    private static final long PM_1_UUID = 3252352L;
    private static final String PM_1_DISPLAY_NAME = "hp-dl378.eng.vmturbo.com";
    private static final String PM_1_LOCAL_NAME = "host-17056";
    private static final long PM_2_UUID = 34303935L;
    private static final String PM_2_DISPLAY_NAME = "hp-dl562.eng.vmturbo.com";
    private static final String PM_2_LOCAL_NAME = "host-13104";
    private static final String VCENTER = "vCenter";
    private static final String AZURE_SUBSCRIPTION = "Azure Subscription";
    private static final String MOVE_ACTION_DESC = "Move Virtual Machine turbonomic-t8c-8.0"
        + ".6-20201219141928000-168.161 from hp-dl378.eng.vmturbo.com to hp-dl562.eng.vmturbo.com";
    private static final String MOVE_RISK_DESC = "Q4 VCPU, Mem Congestion";
    private static final int OLD_CPU_CAPACITY = 2;
    private static final int NEW_CPU_CAPACITY = 3;
    private static final long VOLUME_UUID = 5829352389L;
    private static final String VOLUME_LOCAL_NAME = "/subscriptions/758ad253-cbf5-4b18-8863-3eed0825bf07/resourceGroups/OLEGN_RG/providers/Microsoft.Compute/disks/oleg-disc31GB-CentralIndia";
    private static final String VOLUME_DISPLAY_NAME = "disc31GB-CentralIndia";
    private static final long STORAGE_TIER_1_UUID = 3950335L;
    private static final String STORAGE_TIER_1_DISPLAY_NAME = "MANAGED_STANDARD_SSD";
    private static final String STORAGE_TIER_1_LOCAL_NAME = "azure::ST::MANAGED_STANDARD_SSD";
    private static final long STORAGE_TIER_2_UUID = 5329509L;
    private static final String STORAGE_TIER_2_DISPLAY_NAME = "MANAGED_PREMIUM";
    private static final String STORAGE_TIER_2_LOCAL_NAME = "azure::ST::MANAGED_PREMIUM";

    private static final ActionExecution.ActionExecutionDTO MOVE_ACTION_PERFORMANCE =
        ActionExecution.ActionExecutionDTO.newBuilder()
          .setActionOid(ACTION_OID)
          .setActionState(ActionExecution.ActionResponseState.IN_PROGRESS)
          .setCreateTime(ACTION_CREATION_TIME)
          .setUpdateTime(ACTION_UPDATE_TIME)
          .setAcceptedBy(ACCEPTED_BY)
          .setActionType(ActionExecution.ActionItemDTO.ActionType.MOVE)
          .addActionItem(ActionExecution.ActionItemDTO.newBuilder()
            .setActionType(ActionExecution.ActionItemDTO.ActionType.MOVE)
            .setUuid(String.valueOf(ACTION_UUID))
            .setTargetSE(CommonDTO.EntityDTO.newBuilder()
              .setEntityType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE)
              .setId("Test1")
              .setTurbonomicInternalId(VM_UUID)
              .setDisplayName(VM_DISPLAY_NAME)
              .addEntityProperties(CommonDTO.EntityDTO.EntityProperty.newBuilder()
                .setNamespace("DEFAULT")
                .setName("LocalName")
                .setValue(VM_LOCAL_NAME)
                .build())
              .addEntityProperties(CommonDTO.EntityDTO.EntityProperty.newBuilder()
                .setNamespace("DEFAULT")
                .setName("targetAddress")
                .setValue(TARGET_ADDRESS)
                .build())
              .setPowerState(CommonDTO.EntityDTO.PowerState.POWERED_ON))
            .setCurrentSE(CommonDTO.EntityDTO.newBuilder()
              .setEntityType(CommonDTO.EntityDTO.EntityType.PHYSICAL_MACHINE)
              .setId("Test2")
              .setTurbonomicInternalId(PM_1_UUID)
              .setDisplayName(PM_1_DISPLAY_NAME)
              .addEntityProperties(CommonDTO.EntityDTO.EntityProperty.newBuilder()
                .setNamespace("DEFAULT")
                .setName("LocalName")
                .setValue(PM_1_LOCAL_NAME)
                .build())
              .addEntityProperties(CommonDTO.EntityDTO.EntityProperty.newBuilder()
                .setNamespace("DEFAULT")
                .setName("TargetType")
                .setValue(VCENTER)
                .build())
              .setPowerState(CommonDTO.EntityDTO.PowerState.SUSPENDED))
            .setNewSE(CommonDTO.EntityDTO.newBuilder()
              .setEntityType(CommonDTO.EntityDTO.EntityType.PHYSICAL_MACHINE)
              .setId("Test3")
              .setTurbonomicInternalId(PM_2_UUID)
              .setDisplayName(PM_2_DISPLAY_NAME)
              .addEntityProperties(CommonDTO.EntityDTO.EntityProperty.newBuilder()
                  .setNamespace("DEFAULT")
                  .setName("LocalName")
                  .setValue(PM_2_LOCAL_NAME)
                  .build())
              .addEntityProperties(CommonDTO.EntityDTO.EntityProperty.newBuilder()
                  .setNamespace("DEFAULT")
                  .setName("TargetType")
                  .setValue(VCENTER)
                  .build())
              .setPowerState(CommonDTO.EntityDTO.PowerState.POWERED_ON))
            .setDescription(MOVE_ACTION_DESC)
            .setRisk(ActionExecution.ActionItemDTO.Risk.newBuilder()
              .setSeverity(ActionExecution.ActionItemDTO.Risk.Severity.CRITICAL)
              .setCategory(ActionExecution.ActionItemDTO.Risk.Category.PERFORMANCE_ASSURANCE)
              .setDescription(MOVE_RISK_DESC)
            )
          )
          .build();

    private static final ActionExecution.ActionExecutionDTO ON_PREM_RESIZE_ACTION =
        ActionExecution.ActionExecutionDTO.newBuilder()
            .setActionOid(ACTION_OID)
            .setActionState(ActionExecution.ActionResponseState.IN_PROGRESS)
            .setCreateTime(ACTION_CREATION_TIME)
            .setUpdateTime(ACTION_UPDATE_TIME)
            .setAcceptedBy(ACCEPTED_BY)
            .setActionType(ActionExecution.ActionItemDTO.ActionType.RESIZE)
            .addActionItem(ActionExecution.ActionItemDTO.newBuilder()
                .setActionType(ActionExecution.ActionItemDTO.ActionType.RESIZE)
                .setUuid(String.valueOf(ACTION_UUID))
                .setTargetSE(CommonDTO.EntityDTO.newBuilder()
                    .setEntityType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE)
                    .setId("Test1")
                    .setTurbonomicInternalId(VM_UUID)
                    .setDisplayName(VM_DISPLAY_NAME)
                    .addEntityProperties(CommonDTO.EntityDTO.EntityProperty.newBuilder()
                        .setNamespace("DEFAULT")
                        .setName("LocalName")
                        .setValue(VM_LOCAL_NAME)
                        .build())
                    .addEntityProperties(CommonDTO.EntityDTO.EntityProperty.newBuilder()
                        .setNamespace("DEFAULT")
                        .setName("targetAddress")
                        .setValue(TARGET_ADDRESS)
                        .build())
                    .setPowerState(CommonDTO.EntityDTO.PowerState.POWERED_ON))
                .setCurrentComm(CommonDTO.CommodityDTO.newBuilder()
                    .setCommodityType(CommonDTO.CommodityDTO.CommodityType.VCPU)
                    .setCapacity(OLD_CPU_CAPACITY)
                    .build())
                .setNewComm(CommonDTO.CommodityDTO.newBuilder()
                    .setCommodityType(CommonDTO.CommodityDTO.CommodityType.VCPU)
                    .setCapacity(NEW_CPU_CAPACITY)
                    .build())
                .setDescription(MOVE_ACTION_DESC)
                .setRisk(ActionExecution.ActionItemDTO.Risk.newBuilder()
                    .setSeverity(ActionExecution.ActionItemDTO.Risk.Severity.CRITICAL)
                    .setCategory(ActionExecution.ActionItemDTO.Risk.Category.PERFORMANCE_ASSURANCE)
                    .setDescription(MOVE_RISK_DESC)
                    .addAffectedCommodity(CommonDTO.CommodityDTO.CommodityType.VCPU)
                )
            )
            .build();

    private static final ActionExecution.ActionExecutionDTO CLOUD_VOLUME_SCALE_ACTION =
        ActionExecution.ActionExecutionDTO.newBuilder()
            .setActionOid(ACTION_OID)
            .setActionState(ActionExecution.ActionResponseState.IN_PROGRESS)
            .setCreateTime(ACTION_CREATION_TIME)
            .setUpdateTime(ACTION_UPDATE_TIME)
            .setAcceptedBy(ACCEPTED_BY)
            .setActionType(ActionExecution.ActionItemDTO.ActionType.SCALE)
            .addActionItem(ActionExecution.ActionItemDTO.newBuilder()
                .setActionType(ActionExecution.ActionItemDTO.ActionType.SCALE)
                .setUuid(String.valueOf(ACTION_UUID))
                .setTargetSE(CommonDTO.EntityDTO.newBuilder()
                    .setEntityType(CommonDTO.EntityDTO.EntityType.VIRTUAL_VOLUME)
                    .setId("Test1")
                    .setTurbonomicInternalId(VOLUME_UUID)
                    .setDisplayName(VOLUME_DISPLAY_NAME)
                    .addEntityProperties(CommonDTO.EntityDTO.EntityProperty.newBuilder()
                        .setNamespace("DEFAULT")
                        .setName("LocalName")
                        .setValue(VOLUME_LOCAL_NAME)
                        .build())
                    .addEntityProperties(CommonDTO.EntityDTO.EntityProperty.newBuilder()
                        .setNamespace("758ad253-cbf5-4b18-8863-3eed0825bf07")
                        .setName("TargetType")
                        .setValue(AZURE_SUBSCRIPTION)
                        .build())
                    .setPowerState(CommonDTO.EntityDTO.PowerState.POWERED_ON))
                .setCurrentSE(CommonDTO.EntityDTO.newBuilder()
                    .setEntityType(CommonDTO.EntityDTO.EntityType.STORAGE_TIER)
                    .setId("Test2")
                    .setTurbonomicInternalId(STORAGE_TIER_1_UUID)
                    .setDisplayName(STORAGE_TIER_1_DISPLAY_NAME)
                    .addEntityProperties(CommonDTO.EntityDTO.EntityProperty.newBuilder()
                        .setNamespace("DEFAULT")
                        .setName("LocalName")
                        .setValue(STORAGE_TIER_1_LOCAL_NAME)
                        .build())
                    .addEntityProperties(CommonDTO.EntityDTO.EntityProperty.newBuilder()
                        .setNamespace("DEFAULT")
                        .setName("TargetType")
                        .setValue(AZURE_SUBSCRIPTION)
                        .build())
                    .setPowerState(CommonDTO.EntityDTO.PowerState.POWERED_ON))
                .setNewSE(CommonDTO.EntityDTO.newBuilder()
                    .setEntityType(CommonDTO.EntityDTO.EntityType.STORAGE_TIER)
                    .setId("Test1")
                    .setTurbonomicInternalId(STORAGE_TIER_2_UUID)
                    .setDisplayName(STORAGE_TIER_2_DISPLAY_NAME)
                    .addEntityProperties(CommonDTO.EntityDTO.EntityProperty.newBuilder()
                        .setNamespace("DEFAULT")
                        .setName("LocalName")
                        .setValue(STORAGE_TIER_2_LOCAL_NAME)
                        .build())
                    .addEntityProperties(CommonDTO.EntityDTO.EntityProperty.newBuilder()
                        .setNamespace("DEFAULT")
                        .setName("TargetType")
                        .setValue(AZURE_SUBSCRIPTION)
                        .build())
                    .setPowerState(CommonDTO.EntityDTO.PowerState.POWERED_ON))
                .setDescription(MOVE_ACTION_DESC)
                .setCharacteristics(ActionExecution.ActionItemDTO.ExecutionCharacteristics.newBuilder()
                    .setReversible(false)
                    .setDisruptive(true)
                    .build())
                .setRisk(ActionExecution.ActionItemDTO.Risk.newBuilder()
                    .setSeverity(ActionExecution.ActionItemDTO.Risk.Severity.CRITICAL)
                    .setCategory(ActionExecution.ActionItemDTO.Risk.Category.PERFORMANCE_ASSURANCE)
                    .setDescription(MOVE_RISK_DESC)
                )
            )
            .build();

    private ActionToApiConverter converter = new ActionToApiConverter();

    /**
     * Tests the case that we convert the on-prem move SDK action to API format.
     */
    @Test
    public void testPerformanceMoveConversion() {
        // ARRANGE
        SdkActionInformationProvider provider =
            new SdkActionInformationProvider(MOVE_ACTION_PERFORMANCE);

        // ACT
        final ActionApiDTO apiMessage = converter.convert(provider, false, 0L, false);
        // ASSERT
        assertThat(apiMessage.getActionID(), is(ACTION_UUID));
        assertThat(apiMessage.getActionImpactID(), is(ACTION_OID));
        assertThat(OffsetDateTime.parse(apiMessage.getCreateTime()).toInstant().toEpochMilli(),
            is(ACTION_CREATION_TIME));
        assertThat(OffsetDateTime.parse(apiMessage.getUpdateTime()).toInstant().toEpochMilli(),
            is(ACTION_UPDATE_TIME));
        assertThat(apiMessage.getActionType(), is(ActionType.MOVE));
        assertThat(apiMessage.getActionState(), is(ActionState.IN_PROGRESS));
        assertThat(apiMessage.getUserName(), is(ACCEPTED_BY));
        assertThat(apiMessage.getDetails(), is(MOVE_ACTION_DESC));

        // verify target entity
        assertEntityData(apiMessage.getTarget(), "ACTIVE", null, TARGET_ADDRESS, TARGET_ADDRESS,
            VM_LOCAL_NAME, VM_UUID, VM_DISPLAY_NAME, "VirtualMachine");


        // verify current entity
        assertEntityData(apiMessage.getCurrentEntity(), "SUSPEND", VCENTER, null, "",
            PM_1_LOCAL_NAME, PM_1_UUID, PM_1_DISPLAY_NAME, "PhysicalMachine");

        // verify new entity
        assertEntityData(apiMessage.getNewEntity(), "ACTIVE", VCENTER, null, "",
            PM_2_LOCAL_NAME, PM_2_UUID, PM_2_DISPLAY_NAME, "PhysicalMachine");

        // assert current and new value
        assertThat(apiMessage.getCurrentValue(), is(String.valueOf(PM_1_UUID)));
        assertThat(apiMessage.getNewValue(), is(String.valueOf(PM_2_UUID)));

        // assert risk
        LogEntryApiDTO risk = apiMessage.getRisk();
        assertThat(risk.getSubCategory(), is("PERFORMANCE_ASSURANCE"));
        assertThat(risk.getDescription(), is(MOVE_RISK_DESC));
        assertThat(risk.getSeverity(), is("CRITICAL"));

    }

    /**
     * Tests the case that we convert the SDK on-prem resize action to API format.
     */
    @Test
    public void testOnPremResizeConversion() {
        // ARRANGE
        SdkActionInformationProvider provider =
            new SdkActionInformationProvider(ON_PREM_RESIZE_ACTION);

        // ACT
        final ActionApiDTO apiMessage = converter.convert(provider, true, 0L, false);

        // ASSERT
        assertThat(apiMessage.getActionID(), is(ACTION_OID));
        assertThat(apiMessage.getActionImpactID(), is(ACTION_OID));
        assertThat(OffsetDateTime.parse(apiMessage.getCreateTime()).toInstant().toEpochMilli(),
            is(ACTION_CREATION_TIME));
        assertThat(OffsetDateTime.parse(apiMessage.getUpdateTime()).toInstant().toEpochMilli(),
            is(ACTION_UPDATE_TIME));
        assertThat(apiMessage.getActionType(), is(ActionType.RESIZE));
        assertThat(apiMessage.getActionState(), is(ActionState.IN_PROGRESS));
        assertThat(apiMessage.getUserName(), is(ACCEPTED_BY));
        assertThat(apiMessage.getDetails(), is(MOVE_ACTION_DESC));

        // verify target entity
        assertEntityData(apiMessage.getTarget(), "ACTIVE", null, TARGET_ADDRESS, TARGET_ADDRESS,
            VM_LOCAL_NAME, VM_UUID, VM_DISPLAY_NAME, "VirtualMachine");

        // assert current and new value
        assertThat(apiMessage.getCurrentValue(), is("2.0"));
        assertThat(apiMessage.getNewValue(), is("3.0"));

        // assert risk
        LogEntryApiDTO risk = apiMessage.getRisk();
        assertThat(risk.getSubCategory(), is("PERFORMANCE_ASSURANCE"));
        assertThat(risk.getDescription(), is(MOVE_RISK_DESC));
        assertThat(risk.getSeverity(), is("CRITICAL"));
        assertThat(risk.getReasonCommodities(), is(Collections.singleton("VCPU")));

    }


    /**
     * Tests the case that we convert the cloud volume scale SDK action to API format.
     */
    @Test
    public void testCloudVolumeScaleConversion() {
        // ARRANGE
        SdkActionInformationProvider provider =
            new SdkActionInformationProvider(CLOUD_VOLUME_SCALE_ACTION);

        // ACT
        final ActionApiDTO apiMessage = converter.convert(provider, false, 0L, false);

        // ASSERT
        assertThat(apiMessage.getActionID(), is(ACTION_UUID));
        assertThat(apiMessage.getActionImpactID(), is(ACTION_OID));
        assertThat(OffsetDateTime.parse(apiMessage.getCreateTime()).toInstant().toEpochMilli(),
            is(ACTION_CREATION_TIME));
        assertThat(OffsetDateTime.parse(apiMessage.getUpdateTime()).toInstant().toEpochMilli(),
            is(ACTION_UPDATE_TIME));
        assertThat(apiMessage.getActionType(), is(ActionType.SCALE));
        assertThat(apiMessage.getActionState(), is(ActionState.IN_PROGRESS));
        assertThat(apiMessage.getUserName(), is(ACCEPTED_BY));
        assertThat(apiMessage.getDetails(), is(MOVE_ACTION_DESC));

        // verify target entity
        assertEntityData(apiMessage.getTarget(), "ACTIVE", AZURE_SUBSCRIPTION, null, "",
            VOLUME_LOCAL_NAME, VOLUME_UUID, VOLUME_DISPLAY_NAME, "VirtualVolume");


        // verify current entity
        assertEntityData(apiMessage.getCurrentEntity(), "ACTIVE", AZURE_SUBSCRIPTION, null, "",
            STORAGE_TIER_1_LOCAL_NAME, STORAGE_TIER_1_UUID, STORAGE_TIER_1_DISPLAY_NAME, "StorageTier");

        // verify new entity
        assertEntityData(apiMessage.getNewEntity(), "ACTIVE", AZURE_SUBSCRIPTION, null, "",
            STORAGE_TIER_2_LOCAL_NAME, STORAGE_TIER_2_UUID, STORAGE_TIER_2_DISPLAY_NAME, "StorageTier");

        // assert current and new value
        assertThat(apiMessage.getCurrentValue(), is(String.valueOf(STORAGE_TIER_1_UUID)));
        assertThat(apiMessage.getNewValue(), is(String.valueOf(STORAGE_TIER_2_UUID)));

        // assert risk
        LogEntryApiDTO risk = apiMessage.getRisk();
        assertThat(risk.getSubCategory(), is("PERFORMANCE_ASSURANCE"));
        assertThat(risk.getDescription(), is(MOVE_RISK_DESC));
        assertThat(risk.getSeverity(), is("CRITICAL"));

        // assert execution characteristics
        final ActionExecutionCharacteristicApiDTO characteristics =
            apiMessage.getExecutionCharacteristics();
        assertThat(characteristics.getDisruptiveness(), is(ActionDisruptiveness.DISRUPTIVE));
        assertThat(characteristics.getReversibility(), is(ActionReversibility.IRREVERSIBLE));

    }

    private void assertEntityData(ServiceEntityApiDTO entityApiDTO, String state, String type,
                                  String targetAddress, String vendorIdKey, String vendorIdValue,
                                  long uuid, String displayName, String entityType) {
        assertThat(entityApiDTO.getState(), is(state));
        assertThat(entityApiDTO.getDiscoveredBy().getType(), is(type));
        assertThat(entityApiDTO.getDiscoveredBy().getDisplayName(), is(targetAddress));
        assertThat(entityApiDTO.getVendorIds().get(vendorIdKey), is(vendorIdValue));
        assertThat(entityApiDTO.getDisplayName(), is(displayName));
        assertThat(entityApiDTO.getUuid(), is(String.valueOf(uuid)));
        assertThat(entityApiDTO.getClassName(), is(entityType));
    }
}
