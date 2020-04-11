package com.vmturbo.common.protobuf.action;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;

/**
 * The environment type of a particular {@link ActionDTO.Action}, derived from the environment
 * types of entities involved in the action.
 * <p>
 * This is a utility enum, used as an alternative to a set of {@link EnvironmentType} - mainly
 * to save memory when computing environment types of lots of actions.
 */
public enum ActionEnvironmentType {
    /**
     * This action is entirely on-premises.
     */
    ON_PREM,

    /**
     * This action is entirely in the cloud.
     */
    CLOUD,

    /**
     * This action involves entities in the cloud and on-prem - e.g. a move from on-prem to cloud.
     */
    ON_PREM_AND_CLOUD;

    private static final Logger logger = LogManager.getLogger();


    /**
     * Get the {@link ActionEnvironmentType} for a particular action.
     *
     * @param action The action recommended by the market.
     * @return The {@link ActionEnvironmentType}.
     * @throws UnsupportedActionException If the action is not supported.
     */
    @Nonnull
    public static ActionEnvironmentType forAction(@Nonnull final ActionDTO.Action action)
            throws UnsupportedActionException {
        boolean isCloud = false;
        boolean isOnPrem = false;
        for (ActionEntity involvedEntity : ActionDTOUtil.getInvolvedEntities(action)) {
            if (involvedEntity.getEnvironmentType() == EnvironmentType.CLOUD) {
                isCloud = true;
            } else {
                isOnPrem = true;
            }
        }

        if (isCloud && isOnPrem) {
            return ActionEnvironmentType.ON_PREM_AND_CLOUD;
        } else if (isCloud) {
            return ActionEnvironmentType.CLOUD;
        } else if (isOnPrem) {
            return ActionEnvironmentType.ON_PREM;
        } else {
            // The default option is ON_PREM, but this should never be reached because we always
            // have involved entities, and we always infer SOME entity type from involved
            // entities.
            logger.warn("Action {} not cloud or on-prem. Defaulting to on-prem.", action);
            return ActionEnvironmentType.ON_PREM;
        }
    }

    /**
     * Whether or not this {@link ActionEnvironmentType} matches a particular {@link EnvironmentType}.
     *
     * @param environmentType The {@link EnvironmentType}.
     * @return True if this action affects entities in the specified environment type.
     */
    public boolean matchesEnvType(@Nonnull final EnvironmentType environmentType) {
        switch (environmentType) {
            case ON_PREM:
                return this != ActionEnvironmentType.CLOUD;
            case CLOUD:
                return this != ActionEnvironmentType.ON_PREM;
            case HYBRID:
                // if the entity environment type is HYBRID,
                // then all action environment types should be accepted
                // there is no way (or reason) to query for
                // "entities with hybrid environment type only"
                return true;
            case UNKNOWN_ENV:
                logger.warn("Asking if action env type matches {}. Always false.", environmentType);
                return false;
            default:
                logger.error("Unsupported environment type: {}. Returning false.", environmentType);
                return false;
        }
    }

}
