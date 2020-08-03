package com.vmturbo.action.orchestrator.translation.batch.translator;

import java.util.List;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.action.ActionTranslation.TranslationStatus;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;

/**
 * This {@link BatchTranslator} skips action translation (e.g. because it has already been
 * translated).
 */
public class SkipBatchTranslator implements BatchTranslator {

    /**
     * Checks if {@code SkipBatchTranslator} should be applied to the action.
     * This {@code BatchTranslator} is applied to any action with "succeeded" translation status.
     *
     * @param actionView Action to check.
     * @return True if action translation has already happened.
     */
    @Override
    public boolean appliesTo(@Nonnull final ActionView actionView) {
        return actionView.getTranslationStatus() == TranslationStatus.TRANSLATION_SUCCEEDED;
    }

    /**
     * Do not translate the action because, for example, it has already been translated.
     *
     * @param actionsToTranslate The action to translate.
     * @param snapshot A snapshot of all the entities and settings involved in the actions
     *
     * @return A stream containing the input actions.
     */
    @Override
    public <T extends ActionView> Stream<T> translate(
            @Nonnull final List<T> actionsToTranslate,
            @Nonnull final EntitiesAndSettingsSnapshot snapshot) {
        return actionsToTranslate.stream();
    }
}
