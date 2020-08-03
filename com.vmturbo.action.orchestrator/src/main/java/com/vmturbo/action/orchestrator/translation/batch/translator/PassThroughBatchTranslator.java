package com.vmturbo.action.orchestrator.translation.batch.translator;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;

/**
 * This {@link BatchTranslator} passes through the input of the translation as its output and marks
 * all input actions as having been translated successfully.
 */
public class PassThroughBatchTranslator implements BatchTranslator {

    @Override
    public boolean appliesTo(@Nonnull final ActionView actionView) {
        return true;
    }

    /**
     * Pass through the input of the translation as its output.
     * Marks all input actions as having been translated successfully.
     *
     * @param actionsToTranslate The actions to be translated.
     * @param snapshot A snapshot of all the entities and settings involved in the actions
     *
     * @return A stream of the translated actions.
     */
    @Override
    public <T extends ActionView> Stream<T> translate(
            @Nonnull final List<T> actionsToTranslate,
            @Nonnull final EntitiesAndSettingsSnapshot snapshot) {
        return actionsToTranslate.stream()
                .peek(action -> action.getActionTranslation().setPassthroughTranslationSuccess())
                .collect(Collectors.toList()).stream();
    }
}
