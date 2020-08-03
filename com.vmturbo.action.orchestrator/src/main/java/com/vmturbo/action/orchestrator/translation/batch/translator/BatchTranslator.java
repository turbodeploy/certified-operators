package com.vmturbo.action.orchestrator.translation.batch.translator;

import java.util.List;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;

/**
 * An interface for translating a batch of actions at a time.
 */
public interface BatchTranslator {

    /**
     * Checks whether {@code BatchTranslator} should be applied to the given action.
     *
     * @param actionView Action to check.
     * @param <T> Action type.
     * @return True if {@code BatchTranslator} should be applied.
     */
    <T extends ActionView> boolean appliesTo(@Nonnull ActionView actionView);

    /**
     * Translate a batch of actions at a time.
     * Although it returns a stream, currently it must finish translation of all actions in the
     * stream.
     *
     * @param actionsToTranslate The action to translate.
     * @param snapshot A snapshot of all the entities and settings involved in the actions.
     * @param <T> Action type.
     * @return A stream of translated actions.
     */
    <T extends ActionView> Stream<T> translate(@Nonnull List<T> actionsToTranslate,
                                               @Nonnull EntitiesAndSettingsSnapshot snapshot);
}
