package com.vmturbo.topology.processor.actions.data;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;

public class DataRequirementSpecBuilder {

    /**
     * A set of criteria that an action needs to match in order for this special case to apply.
     * Special cases are applied iff all criteria match the supplied action
     */
    private final Set<ActionContextFilter> matchCriteria = new HashSet<>();

    /**
     * A map of data items that must be included in the ContextData for matching actions
     * The structure of the map is key -> valueExtractorFunction where:
     *   - the key will be used for inserting the data into the context of the action.
     *   - the valueExtractorFunction extracts the required data.
     */
    private final Map<String, ActionContextValueExtractor> dataRequirements = new HashMap<>();


    /**
     * An interface for defining functions that can determine if a provided action is an instance
     * of the special case that a spec was created to address.
     */
    @FunctionalInterface
    public interface ActionContextFilter {
        boolean isMatch(ActionInfo actionInfo);
    }

    /**
     * An interface for defining functions that can extract data related to an action; the returned
     * String is used to populate a {@link com.vmturbo.platform.common.dto.CommonDTO.ContextData}
     * item.
     */
    @FunctionalInterface
    public interface ActionContextValueExtractor {
        String extractValue(ActionInfo actionInfo);
    }

    public DataRequirementSpecBuilder() {
        //no-op
    }

    /** Add a criteria that an action needs to match in order for this special case to apply.
     * Special cases are applied iff all criteria match the supplied action.
     *
     * @param criterion a function that returns true if the ActionInfo meets the criterion
     * @return a reference to this {@link DataRequirementSpecBuilder}, for chained method invocation
     */
    public DataRequirementSpecBuilder addMatchCriteria(@Nonnull ActionContextFilter criterion) {
        matchCriteria.add(Objects.requireNonNull(criterion));
        return this;
    }

    /**
     * Specify a data item that must be included in the ContextData for matching actions
     *
     * @param key to be used for inserting the data into the context of the action. This string
     *            should be unique, as conflicts will result in overwritten data.
     * @param valueExtractor a function describing how to extract the required data
     * @return a reference to this {@link DataRequirementSpecBuilder}, for chained method invocation
     */
    public DataRequirementSpecBuilder addDataRequirement(@Nonnull String key,
                                   @Nonnull ActionContextValueExtractor valueExtractor) {
        dataRequirements.put(Objects.requireNonNull(key), Objects.requireNonNull(valueExtractor));
        return this;
    }

    /**
     * Construct an instance of {@link DataRequirementSpec} by passing the criteria and requirements
     * that have been added.
     *
     * @return an instance of {@link DataRequirementSpec} which reflects the criteria and
     *         requirements added to this builder
     */
    public DataRequirementSpec build() {
        return new ImmutableDataRequirementSpec(matchCriteria, dataRequirements);
    }
}
