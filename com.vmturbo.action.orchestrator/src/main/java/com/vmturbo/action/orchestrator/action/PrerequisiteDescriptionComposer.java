package com.vmturbo.action.orchestrator.action;

import static com.vmturbo.common.protobuf.action.ActionDTOUtil.buildEntityNameOrType;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.Prerequisite;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.PrerequisiteType;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A utility class with static methods that assists in composing pre-requisite descriptions for actions.
 */
public class PrerequisiteDescriptionComposer {

    private static final Logger logger = LogManager.getLogger();

    private static final String ENA_PREREQUISITE_FORMAT =
        "To unblock, enable ENA for {0}. " +
            "Alternatively, you can exclude templates that require ENA";
    private static final String NVME_PREREQUISITE_FORMAT =
        "To unblock, enable NVMe for {0} and change instance type in the AWS Console. " +
            "Alternatively, you can exclude templates that require NVMe";
    private static final String ARCHITECTURE_PREREQUISITE_FORMAT =
        "To unblock, enable 64-bit AMIs for {0}. " +
            "Alternatively, you can exclude templates that require 64-bit AMIs";
    private static final String VIRTUALIZATION_TYPE_PREREQUISITE_FORMAT =
        "To unblock, enable HVM AMIs for {0}. " +
            "Alternatively, you can exclude templates that require HVM AMIs";
    private static final String CORE_QUOTA_PREREQUISITE_FORMAT =
        "Request a quota increase for {0} in {1} to allow resize of {2}";
    private static final String ACTION_TYPE_ERROR_MESSAGE =
        "Can not give a proper pre-requisite description as action type is not defined";
    private static final String LOCK_PREREQUISITE_FORMAT =
            "To execute action on {0}, please remove these read-only locks: {1}";
    private static final String SCALESET_PREREQUISITE_FORMAT =
            "To execute action on {0}, navigate to the Azure portal and adjust the scale set instance size";
    private static final String SCALESET_VOLUME_PREREQUISITE_FORMAT =
            "To execute action on {0}, navigate to the Azure portal and adjust at the scale set";
    private static final String AVAILABILITY_SET_PREREQUISITE_FORMAT =
            "Execution temporarily disabled for {0} due to a previous execution error in this availability set";

    /**
     * Message for GCP VMs with local SSDs attached. These VM actions cannot be executed from
     * within Turbo. User has to shut down the guest OS and execute resize in GCP portal/CLI.
     */
    private static final String LOCAL_SSD_ATTACHED_PREREQUISITE_FORMAT =
            "This VM {0} has {1} Local SSDs attached. Please shut down the VM from inside guest OS "
                    + "and resize to new machine type from GCP Console UI or gCloud CLI.";

    // A mapping from PrerequisiteType to the display string.
    private static final Map<PrerequisiteType, String> prerequisiteTypeToString = ImmutableMap.of(
            PrerequisiteType.ENA, ENA_PREREQUISITE_FORMAT,
            PrerequisiteType.NVME, NVME_PREREQUISITE_FORMAT,
            PrerequisiteType.ARCHITECTURE, ARCHITECTURE_PREREQUISITE_FORMAT,
            PrerequisiteType.VIRTUALIZATION_TYPE, VIRTUALIZATION_TYPE_PREREQUISITE_FORMAT
    );

    /**
     * Private to prevent instantiation.
     */
    private PrerequisiteDescriptionComposer() {}

    /**
     * Compose the pre-requisite descriptions for the given action.
     *
     * @param action the action pre-requisite descriptions will be composed for
     * @return a list of pre-requisite descriptions
     */
    @Nonnull
    @VisibleForTesting
    public static List<String> composePrerequisiteDescription(@Nonnull final ActionDTO.Action action) {
        return action.getPrerequisiteList().stream()
            .filter(Prerequisite::hasPrerequisiteType).map(prerequisite -> {
                try {
                    switch (prerequisite.getPrerequisiteType()) {
                        case CORE_QUOTAS:
                            return ActionDTOUtil.TRANSLATION_PREFIX + MessageFormat.format(
                                    CORE_QUOTA_PREREQUISITE_FORMAT,
                                    prerequisite.getQuotaName(),
                                    buildEntityNameOrType(ActionEntity.newBuilder()
                                            .setId(prerequisite.getRegionId())
                                            .setType(EntityType.REGION_VALUE).build()),
                                    buildEntityNameOrType(ActionDTOUtil.getPrimaryEntity(action)));
                        case LOCKS:
                            return ActionDTOUtil.TRANSLATION_PREFIX + MessageFormat.format(
                                    LOCK_PREREQUISITE_FORMAT,
                                    buildEntityNameOrType(ActionDTOUtil.getPrimaryEntity(action)),
                                    prerequisite.getLocks());
                        case SCALE_SET:
                            ActionEntity actionEntity = ActionDTOUtil.getPrimaryEntity(action);
                            return ActionDTOUtil.TRANSLATION_PREFIX + MessageFormat.format(
                                    actionEntity.getType() == EntityType.VIRTUAL_VOLUME_VALUE
                                            ? SCALESET_VOLUME_PREREQUISITE_FORMAT : SCALESET_PREREQUISITE_FORMAT,
                                    buildEntityNameOrType(actionEntity));
                        case AVAILABILITY_SET:
                            actionEntity = ActionDTOUtil.getPrimaryEntity(action);
                            return ActionDTOUtil.TRANSLATION_PREFIX + MessageFormat.format(
                                    AVAILABILITY_SET_PREREQUISITE_FORMAT,
                                    buildEntityNameOrType(actionEntity));
                        case LOCAL_SSD_ATTACHED:
                            return ActionDTOUtil.TRANSLATION_PREFIX + MessageFormat.format(
                                    LOCAL_SSD_ATTACHED_PREREQUISITE_FORMAT,
                                    buildEntityNameOrType(ActionDTOUtil.getPrimaryEntity(action)),
                                    prerequisite.getAttachedEphemeralVolumes());
                        default:
                            return ActionDTOUtil.TRANSLATION_PREFIX + MessageFormat.format(
                            prerequisiteTypeToString.get(prerequisite.getPrerequisiteType()),
                            buildEntityNameOrType(ActionDTOUtil.getPrimaryEntity(action)));
                    }
                } catch (UnsupportedActionException e) {
                    logger.error("Cannot build action pre-requisite {} description",
                            prerequisite, e);
                    return ACTION_TYPE_ERROR_MESSAGE;
                }
            })
            .collect(Collectors.toList());
    }
}
