package com.vmturbo.topology.processor.actions.data.spec;

import java.util.List;
import java.util.function.Function;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.platform.common.dto.CommonDTO.ContextData;

/**
 * Expresses a requirement for additional data for a specific action execution. This data is
 * returned as a list of context data, which is then included in the ActionItemDTO that is sent
 * to the probe for action execution.
 *
 * Also supports adding criteria that must be met in order for this data requirement to take effect.
 * This criteria allows the spec to determine if a provided action is an instance of the special
 * case that the spec was created to address.
 */
public interface DataRequirementSpec {

    /**
     * Tests whether a particular action matches the special case that this data requirement spec
     * addresses. This should be used by a caller to determine whether to call
     * {@link #retrieveRequiredData(ActionInfo)}.
     *
     * @param action the action to be examined
     * @return true, if the specified action matches all the criteria for this data requirement
     */
    boolean matchesAllCriteria(@Nonnull ActionInfo action);

    /**
     * Retrieves all of the required data specified in this spec and adds it to the returned list of
     * {@link ContextData}. Data is retrieved and stored based on the requirements defined when
     * creating this spec.
     *
     * @param action the action to generate context data for
     * @return a list of {@link ContextData}, containing the data specified in this spec
     */
    @Nonnull
    List<ContextData> retrieveRequiredData(@Nonnull ActionInfo action);

}
