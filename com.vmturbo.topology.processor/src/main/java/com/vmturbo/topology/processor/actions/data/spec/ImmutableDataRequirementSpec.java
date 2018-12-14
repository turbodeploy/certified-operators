package com.vmturbo.topology.processor.actions.data.spec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.ContextData;
import com.vmturbo.platform.common.dto.CommonDTOREST;
import com.vmturbo.topology.processor.actions.data.spec.DataRequirementSpecBuilder.ActionContextFilter;
import com.vmturbo.topology.processor.actions.data.spec.DataRequirementSpecBuilder.ActionContextMultiValueExtractor;
import com.vmturbo.topology.processor.actions.data.spec.DataRequirementSpecBuilder.ActionContextSingleValueExtractor;

@Immutable
public class ImmutableDataRequirementSpec implements DataRequirementSpec {

    /**
     * A set of criteria that an action needs to match in order for this special case to apply.
     * Special cases are applied iff all criteria match the supplied action
     */
    private final Set<ActionContextFilter> matchCriteria;

    /**
     * A map of data items that must be included in the ContextData for matching actions
     * The structure of the map is key -> valueExtractorFunction where:
     *   - the key will be used for inserting the data into the context of the action.
     *   - the valueExtractorFunction describes how to extract the required data.
     */
    private final Map<String, ActionContextSingleValueExtractor> dataRequirements;

    /**
     * A list of functions to extract multiple context key/value pairs at once.
     * This is beneficial, for example, when bulk calls need to be made or when the exact number
     * of context entries is not predefined.
     */
    private final List<ActionContextMultiValueExtractor> multiValueRequirements;

    /**
     * A non-public constructor allows the {@link DataRequirementSpecBuilder} to create an instance
     *  @param matchCriteria a set of criteria that an action needs to match in order for this
     *                     special case to apply.
     * @param dataRequirements a map of data items that must be included in the ContextData in order
     * @param multiValueRequirements
     */
    ImmutableDataRequirementSpec(
            @Nonnull final Set<ActionContextFilter> matchCriteria,
            @Nonnull final Map<String, ActionContextSingleValueExtractor> dataRequirements,
            @Nonnull final List<ActionContextMultiValueExtractor> multiValueRequirements) {
        this.matchCriteria = Collections.unmodifiableSet(Objects.requireNonNull(matchCriteria));
        this.dataRequirements =
                Collections.unmodifiableMap(Objects.requireNonNull(dataRequirements));
        this.multiValueRequirements =
                Collections.unmodifiableList(Objects.requireNonNull(multiValueRequirements));
    }

    /**
     * Tests whether a particular action matches the special case that this data requirement spec
     * addresses.
     *
     * @param action the action to be examined
     * @return true, if the specified action matches all the criteria for this data requirement
     */
    @Override
    public boolean matchesAllCriteria(@Nonnull final ActionInfo action) {
        return matchCriteria.stream()
                .allMatch(matchingFunction -> matchingFunction.isMatch(action));
    }

    /**
     * Retrieves all of the required data specified in this spec and adds it to the returned list of
     * {@link ContextData}. Data is retrieved and stored based on the requirements defined when
     * creating this spec.
     *
     * @param action the action to generate context data for
     * @return a list of {@link ContextData}, containing the data specified in this spec
     */
    @Override
    @Nonnull
    public List<ContextData> retrieveRequiredData(@Nonnull final ActionInfo action) {
        List<ContextData> contextDataList = new ArrayList<>();
        // Add all the single-value data
        dataRequirements.forEach((contextKey, extractorFunction) -> {
            String extractedValue = extractorFunction.extractValue(action);
            ContextData contextData = ContextData.newBuilder()
                    .setContextKey(contextKey)
                    .setContextValue(extractedValue)
                    .build();
            contextDataList.add(contextData);
        });
        // Add all the multiple-entry data
        multiValueRequirements.forEach(multiValueExtractor ->
        {
            final List<CommonDTO.ContextData> extractedContextDataList =
                    multiValueExtractor.extractContextData(action);
            contextDataList.addAll(extractedContextDataList);
        });
        return contextDataList;
    }
}
