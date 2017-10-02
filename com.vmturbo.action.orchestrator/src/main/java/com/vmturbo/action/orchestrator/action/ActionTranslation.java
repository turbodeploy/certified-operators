package com.vmturbo.action.orchestrator.action;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.vmturbo.common.protobuf.action.ActionDTO;

/**
 * The result of translating from the market's domain-agnostic actions into real-world domain-specific actions.
 *
 * This translation happens lazily. That is, actions are not translated until such a time as the translation is
 * needed. A translation may be performed for the purposes of either display or execution.
 *
 * While an {@link ActionTranslation}'s {@link TranslationStatus} state machine is not enforced, the state
 * transitions that make sense are:
 *
 * UNTRANSLATED -> TRANSLATION_FAILED
 * UNTRANSLATED -> TRANSLATION_SUCCEEDED
 * TRANSLATION_FAILED -> TRANSLATION_FAILED
 * TRANSLATION_FAILED -> TRANSLATION_SUCCEEDED
 *
 * and UNTRANSLATED is always the initial state of a translation.
 */
@ThreadSafe
public class ActionTranslation {

    /**
     * The status of the translation.
     */
    public enum TranslationStatus {
        /**
         * Translation is lazy. If translation is not yet complete, status will be untranslated.
         */
        UNTRANSLATED,

        /**
         * Translation was successfully performed.
         */
        TRANSLATION_SUCCEEDED,

        /**
         * Translation was unsuccessful. It may be retried again at some point.
         */
        TRANSLATION_FAILED
    }

    /**
     * The status of the translation.
     *
     * Mark as volatile so that writes to this field are immediately visible to other threads.
     */
    private volatile TranslationStatus translationStatus;

    /**
     * The result of the translation, or if translation has not succeeded,
     * the original action recommendation.
     *
     * Mark as volatile so that writes to this field are immediately visible to other threads.
     */
    @Nonnull
    @GuardedBy("this")
    private volatile ActionDTO.Action translationResultOrOriginal;

    /**
     * Create a new, untranslated action translation.
     */
    public ActionTranslation(@Nonnull final ActionDTO.Action originalRecommendation) {
        translationStatus = TranslationStatus.UNTRANSLATED;
        translationResultOrOriginal = originalRecommendation;
    }

    public TranslationStatus getTranslationStatus() {
        return translationStatus;
    }

    /**
     * Get the translated recommendation if translation has succeeded.
     *
     * @return If translation succeeded, returns the result of translation. If translation has not yet succeded,
     *         returns {@link Optional#empty()}
     */
    public Optional<ActionDTO.Action> getTranslatedRecommendation() {
        return translationStatus == TranslationStatus.TRANSLATION_SUCCEEDED ?
            Optional.of(translationResultOrOriginal) : Optional.empty();
    }


    /**
     * If translation has succeeded, get the result of translation. If translation has not yet succeeded,
     * return the untranslated recommendation.
     *
     * @return Either the result of the translation or the untranslated recommendation depending on
     *         whether translation has succeeded.
     */
    @Nonnull
    public ActionDTO.Action getTranslationResultOrOriginal() {
        return translationResultOrOriginal;
    }

    /**
     * Mark the translation as having succeeded, setting its result to the input.
     *
     * @param translationResult The result of the translation.
     */
    public synchronized void setTranslationSuccess(@Nonnull final ActionDTO.Action translationResult) {
        // Synchronize this method because in order for both set operations to happen atomically we need
        // to perform them both in the same synchronized block. See
        // https://www.ibm.com/developerworks/java/library/j-jtp06197/ for guidelines on how to effectively
        // use the volatile keyword and when it is insufficient.
        //
        // This particular case falls into the category of variables participating in invariants with other
        // variables, which causes the protection that volatile affords to be insufficient.
        this.translationStatus = TranslationStatus.TRANSLATION_SUCCEEDED;
        this.translationResultOrOriginal = Objects.requireNonNull(translationResult);
    }

    /**
     * Mark the translation as having succeeded because no translation was necessary.
     */
    public synchronized void setPassthroughTranslationSuccess() {
        this.translationStatus = TranslationStatus.TRANSLATION_SUCCEEDED;
    }

    /**
     * Mark the translation as having failed.
     */
    public synchronized void setTranslationFailure() {
        this.translationStatus = TranslationStatus.TRANSLATION_FAILED;
    }
}
