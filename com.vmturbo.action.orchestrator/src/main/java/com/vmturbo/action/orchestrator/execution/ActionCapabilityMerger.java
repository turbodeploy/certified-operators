package com.vmturbo.action.orchestrator.execution;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability.ActionCapabilityElement;

/**
 * This class is used to merge action capabilities sent by the probe for the given combination of
 * entity and action types.
 */
public class ActionCapabilityMerger {

    private static final Predicate<ActionCapabilityElement> HAS_SCOPE =
            (@Nonnull final ActionCapabilityElement element) ->
                    element.hasProviderScope() || element.hasCommodityScope();

    /**
     * Merges action capabilities into {@link MergedActionCapability}.
     *
     * @param capabilities Collection of action capabilities sent by the probe.
     * @return New instance of {@link MergedActionCapability} with merged capabilities.
     */
    public MergedActionCapability merge(
            @Nonnull final Collection<ActionCapabilityElement> capabilities) {

        final List<ActionCapabilityElement> elementsWithScope = capabilities.stream()
                .filter(HAS_SCOPE)
                .collect(Collectors.toList());

        // If we don't have any capabilities with scope then use capability without scope.
        if (elementsWithScope.isEmpty()) {
            // Select capability element without scope. In case of multiple elements, use the one with
            // the lowest ActionCapability.
            final Optional<ActionCapabilityElement> elementWithoutScope = capabilities.stream()
                    .filter(HAS_SCOPE.negate())
                    .min(Comparator.comparing(elem -> elem.getActionCapability().getNumber()));
            // If the probe has not specified a support level, use a default value of NOT_EXECUTABLE
            // (that is, show the action in the UI but do not permit execution).
            return elementWithoutScope.map(MergedActionCapability::from)
                    .orElseGet(MergedActionCapability::createShowOnly);
        }

        // Otherwise merge all capabilities with scope.
        final ActionCapabilityElement.Builder mergedCapability = ActionCapabilityElement
                .newBuilder()
                .setActionType(elementsWithScope.get(0).getActionType());
        elementsWithScope.forEach(capability -> {
            if (!mergedCapability.hasActionCapability()
                    || capability.getActionCapability().getNumber()
                    < mergedCapability.getActionCapability().getNumber()) {
                mergedCapability.setActionCapability(capability.getActionCapability());
            }
            if (capability.hasDisruptive()) {
                if (mergedCapability.hasDisruptive()) {
                    mergedCapability.setDisruptive(
                            mergedCapability.getDisruptive() || capability.getDisruptive());
                } else {
                    mergedCapability.setDisruptive(capability.getDisruptive());
                }
            }
            if (capability.hasReversible()) {
                if (mergedCapability.hasReversible()) {
                    mergedCapability.setReversible(
                            mergedCapability.getReversible() && capability.getReversible());
                } else {
                    mergedCapability.setReversible(capability.getReversible());
                }
            }
        });

        return MergedActionCapability.from(mergedCapability.build());
    }
}
