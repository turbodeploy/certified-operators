package com.vmturbo.api.conversion.action;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionExecutionCharacteristicApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.enums.ActionMode;
import com.vmturbo.api.enums.ActionState;
import com.vmturbo.api.enums.ActionType;

/**
 * The interface for a class that provides information about action for conversion.
 */
public interface ActionInformationProvider {
    /**
     * Returns the stable ID of the action.
     *
     * @return the stable ID of action.
     */
    long getRecommendationId();

    /**
     * Returns the UUID of the action.
     *
     * @return the UUID of the action.
     */
    long getUuid();

    /**
     * Returns the mode of action.
     *
     * @return the mode of action.
     */
    @Nonnull
    Optional<ActionMode> getActionMode();

    /**
     * Returns the state of action.
     *
     * @return the state of action.
     */
    @Nonnull
    Optional<ActionState> getActionState();

    /**
     * Returns the list of action pre-requisites.
     *
     * @return the list of action pre-requisite.
     */
    @Nonnull
    List<String> getPrerequisiteDescriptionList();

    /**
     * Returns the description for the risk.
     *
     * @return the description for the risk.
     */
    @Nullable
    String getRiskDescription();

    /**
     * Returns the category for the risk.
     *
     * @return the category for the risk.
     */
    @Nullable
    String getCategory();

    /**
     * Returns the description for the action.
     *
     * @return the description for the action.
     */
    @Nullable
    String getDescription();

    /**
     * Returns the stats associated to the action.
     *
     * @return the stats for the actions.
     */
    @Nonnull
    List<StatApiDTO> getStats();

    /**
     * Returns the severity for the risk.
     *
     * @return the severity for the risk.
     */
    @Nullable
    String getSeverity();

    /**
     * Returns the commodities that are at risk.
     *
     * @return the list of commodities that are at risk.
     */
    @Nonnull
    Set<String> getReasonCommodities();

    /**
     * Returns the the action type.
     *
     * @return the type of action.
     */
    @Nonnull
    ActionType getActionType();

    /**
     * Returns the related policy to this action. For example, when the action is caused by
     * a segmentation policy.
     *
     * @return the related policy that caused the action.
     */
    @Nonnull
    Optional<PolicyApiDTO> getRelatedPolicy();

    /**
     * Returns the time that action is recommended first.
     *
     * @return the time that the action is recommended first.
     */
    long getCreationTime();

    /**
     * Returns the time the action decision is updated.
     *
     * @return the time the action decision is updated.
     */
    @Nonnull
    Optional<Long> getUpdatingTime();

    /**
     * Returns the user who accepted the action.
     *
     * @return the user who accepted the action.
     */
    @Nonnull
    Optional<String> getAcceptingUser();

    /**
     * Returns the API object for the target entity of the action.
     *
     * @return the API object for the target entity of the action.
     */
    @Nonnull
    ServiceEntityApiDTO getTarget();

    /**
     * Returns the current entity for the action if present.
     *
     * @return the current entity for the action.
     */
    @Nonnull
    Optional<ServiceEntityApiDTO> getCurrentEntity();

    /**
     * Returns the new entity for the action if present.
     *
     * @return the new entity for the action.
     */
    @Nonnull
    Optional<ServiceEntityApiDTO> getNewEntity();

    /**
     * Returns the current value of the action.
     *
     * @return the current value of the action.
     */
    @Nonnull
    Optional<String> getCurrentValue();

    /**
     * Returns the new value of the action.
     *
     * @return the new value of the action.
     */
    @Nonnull
    Optional<String> getNewValue();

    /**
     * The unit for new and old value.
     *
     * @return the unit for new and old value.
     */
    @Nonnull
    Optional<String> getValueUnit();

    /**
     * Returns the execution characteristics of action execution.
     *
     * @return the execution characteristics of action execution.
     */
    @Nonnull
    Optional<ActionExecutionCharacteristicApiDTO> getActionExecutionCharacteristics();

    /**
     * Returns the individual actions of a compound action.
     *
     * @return the individual actions of a compound action.
     */
    @Nonnull
    List<ActionApiDTO> getCompoundActions();
}
