package com.vmturbo.action.orchestrator.action;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.store.EntitySettingsCache;
import com.vmturbo.common.protobuf.ActionDTOUtil;
import com.vmturbo.common.protobuf.UnsupportedActionException;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A utility class to capture the logic to calculate an {@link ActionMode} for an action,
 * given some user settings.
 */
public class ActionModeCalculator {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Get the action mode for a particular action. The action mode is determined by the
     * settings for the action, or, if the settings are not available, by the system defaults of
     * the relevant automation settings.
     *
     * @param action The protobuf representation of the action to calculate action mode for.
     * @param entitySettingsCache The {@link EntitySettingsCache} to retrieve settings for. May
     *                            be null.
     *                            TODO (roman, Aug 7 2018): Can we make this non-null? The cache
     *                            should exist as a spring object and be injected appropriately.
     * @return The {@link ActionMode} to use for the action.
     */
    @Nonnull
    public static ActionMode calculateActionMode(@Nonnull final ActionDTO.Action action,
                @Nullable final EntitySettingsCache entitySettingsCache) {
        try {
            final long targetId = ActionDTOUtil.getTargetEntityId(action);

            final Map<String, Setting> settingsForTarget = entitySettingsCache == null ?
                    Collections.emptyMap() : entitySettingsCache.getSettingsForEntity(targetId);

            return specsApplicableToAction(action)
                    .map(spec -> {
                        final Setting setting = settingsForTarget.get(spec.getSettingName());
                        if (setting == null) {
                            // If there is no setting for this spec that applies to the target
                            // of the action, we use the system default (which comes from the
                            // enum definitions).
                            return spec.getSettingSpec().getEnumSettingValueType().getDefault();
                        } else {
                            // In all other cases, we use the default value from the setting.
                            return setting.getEnumSettingValue().getValue();
                        }
                    })
                    .map(ActionMode::valueOf)
                    // We're not using a proper tiebreaker because we're comparing across setting specs.
                    .min(ActionMode::compareTo)
                    .orElse(ActionMode.RECOMMEND);
        } catch (UnsupportedActionException e) {
            logger.error("Unable to calculate action mode.", e);
            return ActionMode.RECOMMEND;
        }
    }

    /**
     * Get the setting specs applicable to an action. The applicable setting specs are derived
     * from the type of the action and the entities it involves.
     *
     * @param action The protobuf representation of the action.
     * @return The stream of applicable {@link EntitySettingSpecs}. This will be a stream of
     *         size one in most cases.
     */
    @Nonnull
    private static Stream<EntitySettingSpecs> specsApplicableToAction(@Nonnull final ActionDTO.Action action) {
        final ActionTypeCase type = action.getInfo().getActionTypeCase();
        switch (type) {
            case MOVE:
                // This may result in one applicable entity spec (for a host move or storage move),
                // or two applicable entity spec for cases where the host move has to be
                // accompanied by a storage move.
                return action.getInfo().getMove().getChangesList().stream()
                        .map(provider -> provider.getDestination().getType())
                        .map(EntityType::forNumber)
                        .map(destinationEntityType -> {
                            if (destinationEntityType == EntityType.STORAGE) {
                                return EntitySettingSpecs.StorageMove;
                            } else {
                                // TODO (roman, Aug 6 2018): Should we check explicitly for
                                // physical machine, or are there other valid non-storage destination
                                // types for move actions?
                                return EntitySettingSpecs.Move;
                            }
                        })
                        .distinct();
            case RECONFIGURE:
                return Stream.of(EntitySettingSpecs.Reconfigure);
            case PROVISION:
                return Stream.of(EntitySettingSpecs.Provision);
            case RESIZE:
                return Stream.of(EntitySettingSpecs.Resize);
            case ACTIVATE:
                return Stream.of(EntitySettingSpecs.Activate);
            case DEACTIVATE:
                return Stream.of(EntitySettingSpecs.Suspend);
            case ACTIONTYPE_NOT_SET:
                return Stream.empty();
        }
        return Stream.empty();
    }
}
