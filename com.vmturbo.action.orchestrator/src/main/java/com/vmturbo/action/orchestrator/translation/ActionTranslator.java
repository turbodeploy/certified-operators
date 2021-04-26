package com.vmturbo.action.orchestrator.translation;

import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import io.grpc.Channel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.ActionTranslation;
import com.vmturbo.action.orchestrator.action.ActionTranslation.TranslationStatus;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.ExecutableStep;
import com.vmturbo.action.orchestrator.action.ExplanationComposer;
import com.vmturbo.action.orchestrator.action.PrerequisiteDescriptionComposer;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.store.PlanActionStore;
import com.vmturbo.action.orchestrator.topology.ActionTopologyStore;
import com.vmturbo.action.orchestrator.translation.batch.translator.BatchTranslator;
import com.vmturbo.action.orchestrator.translation.batch.translator.PassThroughBatchTranslator;
import com.vmturbo.action.orchestrator.translation.batch.translator.SkipBatchTranslator;
import com.vmturbo.action.orchestrator.translation.batch.translator.VCpuResizeBatchTranslator;
import com.vmturbo.auth.api.authorization.UserContextUtils;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Compliance;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.ListSettingPoliciesRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.topology.graph.util.BaseTopology;

/**
 * Translates actions from the market's domain-agnostic actions into real-world domain-specific actions.
 *
 * <p>Some actions may require no translation at all. For those actions, the translator functions purely
 * as a passthrough. Some actions may require translation of execution and/or display related properties.
 * Some examples of actions that need to be translated:</p>
 *
 * <p>1. VCPU. the unit is converted from Mhz to number of VCPU's (when resizing the VCPU commodity on a VM).
 * This translation should happen for both execution and display.
 * 2. UCS NetThroughput. In UCS Switches, we want to show resize in terms of ports, and not in terms on kbps.
 * this is done by: (net usage) / (net increment), where network increment specifies the kbps of a single port.
 * (Not yet supported). Not sure if this impacts both execution and display or just display.</p>
 *
 * <p>Both info and explanation for a recommendation may need to be translated.</p>
 *
 * <p>Note that Action information is communicated to other components in the form of {@link ActionSpec} objects.
 * Components outside the action orchestrator should receive information about those actions in terms of the
 * real-world domain. That is, actions should be translated before their details are sent to other components.
 * In order to consistently enforce that all actions are first translated before being sent to other components,
 * the {@link ActionTranslator} serves as the sole gateway for generating {@link ActionSpec} objects from
 * {@link ActionView} objects.</p>
 *
 * <p>A note on thread-safety: Because both {@link ActionView}s and {@link ActionTranslation} objects are thread-safe,
 * it is safe to attempt to translate the same action multiple times concurrently. Which translation result takes
 * precedence is not guaranteed, but information on the action will be consistent.</p>
 */
public class ActionTranslator {

    private static final Logger logger = LogManager.getLogger();

    private static final Set<String> ROLES_WITH_ABILITY_TO_APPLY_ACTIONS =
            ImmutableSet.of(SecurityConstant.ADMINISTRATOR.toUpperCase(),
                    SecurityConstant.SITE_ADMIN, SecurityConstant.AUTOMATOR);

    /**
     * An interface for actually executing action translation. Usually it is unnecessary to specify
     * the specific {@link TranslationExecutor} to use except when mocking for testing.
     */
    public interface TranslationExecutor {
        /**
         * Translate a stream of actions from the market's domain-agnostic form to the domain-specific form
         * relevant for execution and display in the real world.
         *
         * @param <T> the type of the action being translated.
         * @param actionStream A stream of actions whose info should be translated from the market's
         *                     domain-agnostic variant.
         * @param snapshot A snapshot of all the entities and settings involved in the actions
         * @return A stream of the translated actions.
         *         Note, like all Java streams, a terminal operation must be applied on it to force evaluation.
         *         Note that the order of the actions in the output stream is not guaranteed to be the same as they were
         *         on the input. If this is important, consider applying an {@link Stream#sorted(Comparator)} operation.
         */
        <T extends ActionView> Stream<T> translate(@Nonnull Stream<T> actionStream,
                                                   @Nonnull EntitiesAndSettingsSnapshot snapshot);
    }

    private final TranslationExecutor translationExecutor;

    private final SettingPolicyServiceBlockingStub settingPolicyService;

    private final ActionTopologyStore actionTopologyStore;

    /**
     * Create a new {@link ActionTranslator} for translating actions from the market's domain-agnostic
     * action recommendations into actions that can be executed and understood in real-world domain-specific
     * terms.
     *
     * @param repoChannel The searchServiceRpc to the repository component.
     * @param groupChannel Channel to use for creating a blocking stub to query the Group Service.
     * @param actionTopologyStore Store for minimal topology to look up entity information.
     */
    @VisibleForTesting
    public ActionTranslator(@Nonnull final Channel repoChannel,
                            @Nonnull final Channel groupChannel,
                            @Nonnull final ActionTopologyStore actionTopologyStore) {
        translationExecutor = new ActionTranslationExecutor(
            RepositoryServiceGrpc.newBlockingStub(repoChannel), actionTopologyStore);
        this.settingPolicyService =
            SettingPolicyServiceGrpc.newBlockingStub(Objects.requireNonNull(groupChannel));
        this.actionTopologyStore = Objects.requireNonNull(actionTopologyStore);
    }

    /**
     * Create a new {@link ActionTranslator} for translating actions from the market's domain-agnostic
     * action recommendations into actions that can be executed and understood in real-world domain-specific
     * terms.
     *
     * <p>This method is exposed to permit the generation of action specs in tests without having to supply
     * all the information necessary to perform real translation.</p>
     *
     * <p>This method should NOT be used in production code.</p>
     *
     * @param translationExecutor The object that will perform translation of actions.
     * @param groupChannel Channel to use for creating a blocking stub to query the Group Service.
     * @param actionTopologyStore Store for minimal topology to look up entity information.
     */
    @VisibleForTesting
    public ActionTranslator(@Nonnull final TranslationExecutor translationExecutor,
                            @Nonnull final Channel groupChannel,
                            @Nonnull final ActionTopologyStore actionTopologyStore) {
        this.translationExecutor = Objects.requireNonNull(translationExecutor);
        this.settingPolicyService =
            SettingPolicyServiceGrpc.newBlockingStub(Objects.requireNonNull(groupChannel));
        this.actionTopologyStore = Objects.requireNonNull(actionTopologyStore);
    }

    /**
     * Get an {@link ActionSpec} describing the action.
     *
     * @param sourceAction The action to be translated and whose spec should be generated.
     * @return The {@link ActionSpec} description of the input action.
     */
    @Nonnull
    public ActionSpec translateToSpec(@Nonnull final ActionView sourceAction) {
        return translateToSpecs(Collections.singletonList(sourceAction)).findFirst().get();
    }

    /**
     * Generate an {@link ActionSpec} describing each of the actions.
     *
     * @param actionViews the actions to be translated and whose specs should be generated
     * @return The {@link ActionSpec} descriptions of the input actions.
     */
    @Nonnull
    public Stream<ActionSpec> translateToSpecs(
            @Nonnull final List<? extends ActionView> actionViews) {
        return translateToSpecs(actionViews, null);
    }

    /**
     * Generates action spec, with option to override explanation in case of a cloud migration
     * plan. TopologyInfo in ActionStore is used to check if it is a cloud migration case.
     *
     * @param actionViews Actions to be translated.
     * @param actionStore Store used to check plan topology info.
     * @return ActionSpec info for input actions.
     */
    public Stream<ActionSpec> translateToSpecs(
            @Nonnull final List<? extends ActionView> actionViews,
            @Nullable final ActionStore actionStore) {
        final boolean isUserAbleToApplyActions = isUserAbleToApplyActions();
        final Map<Long, String> settingPolicyIdToSettingPolicyName =
                getReasonSettingPolicyIdToSettingPolicyNameMap(actionViews);
        return actionViews.stream()
                .map(actionView -> toSpec(actionView, settingPolicyIdToSettingPolicyName,
                        isUserAbleToApplyActions, actionStore));
    }

    /**
     * Get all reason settings from all actionViews and
     * construct a map from settingPolicyId to settingPolicyName.
     *
     * @param actionViews the actions from where we extract reason settings
     * @return a map from settingPolicyId to settingPolicyName
     */
    @Nonnull
    @VisibleForTesting
    Map<Long, String> getReasonSettingPolicyIdToSettingPolicyNameMap(
            @Nonnull final List<? extends ActionView> actionViews) {
        final Set<Long> reasonSettings = new HashSet<>();
        for (ActionView actionView : actionViews) {
            final Explanation explanation = actionView.getTranslationResultOrOriginal()
                    .getExplanation();
            switch (explanation.getActionExplanationTypeCase()) {
                case MOVE:
                case SCALE:
                    ActionDTOUtil.getChangeProviderExplanationList(explanation).stream()
                        .filter(ChangeProviderExplanation::getIsPrimaryChangeProviderExplanation)
                        .filter(ChangeProviderExplanation::hasCompliance)
                        .map(ChangeProviderExplanation::getCompliance)
                        .map(Compliance::getReasonSettingsList)
                        .forEach(reasonSettings::addAll);
                    break;
                case RECONFIGURE:
                    reasonSettings.addAll(explanation.getReconfigure().getReasonSettingsList());
                    break;
                default:
                    break;
            }
        }

        // Make a RPC to get raw settingPolicies for all reason setting ids.
        // We only need the displayName of the settingPolicy.
        final Map<Long, String> settingPolicyIdToSettingPolicyName = new HashMap<>();
        if (!reasonSettings.isEmpty()) {
            settingPolicyService.listSettingPolicies(ListSettingPoliciesRequest.newBuilder()
                .addAllIdFilter(reasonSettings).build())
                .forEachRemaining(settingPolicy -> settingPolicyIdToSettingPolicyName.put(
                    settingPolicy.getId(), settingPolicy.getInfo().getDisplayName()));
        }

        return settingPolicyIdToSettingPolicyName;
    }

    /**
     * Translate a stream of actions from the market's domain-agnostic form to the domain-specific form
     * relevant for execution and display in the real world.
     *
     * <p>If an action has already been translated, no attempt will be made to re-translate it.</p>
     *
     * <p>Note that the portion of this call that requires information from other services is currently blocking.
     * Actions whose translation fail will be given a translation status of
     * {@link TranslationStatus#TRANSLATION_FAILED}, and translations that succeed will be given a translation status
     * of {@link TranslationStatus#TRANSLATION_SUCCEEDED}.</p>
     *
     * <p>This method is blocking until translation of all actions completes.</p>
     *
     * <p>TODO: DavidBlinn 6/20/2017 - consider making the inner operation of this method asynch so that multiple
     * translation groups can be processed simultaneously. Right now only VCPU is translated so it would not yet
     * help.</p>
     *
     * @param <T> the type of the action being translated.
     * @param actionStream A stream of actions whose info should be translated from the market's
     *                     domain-agnostic variant.
     * @param snapshot A snapshot of all the entities and settings involved in the actions
     * @return A stream of the translated actions.
     *         Note, like all Java streams, a terminal operation must be applied on it to force evaluation.
     *         Note that the order of the actions in the output stream is not guaranteed to be the same as they were
     *         on the input. If this is important, consider applying an {@link Stream#sorted(Comparator)} operation.
     */
    public <T extends ActionView> Stream<T> translate(@Nonnull final Stream<T> actionStream,
                                                      @Nonnull EntitiesAndSettingsSnapshot snapshot) {
        return translationExecutor.translate(actionStream, snapshot);
    }

    /**
     * Convert the {@link ActionView} to an {@link ActionSpec}.
     *
     * <p>In the event that an action has been successfully translated, the recommendation provided will be the
     * result of translating the market's domain-agnostic action recommendation into a domain-specific recommendation.
     * In the event that an action cannot be translated, the included recommendation is the one originally provided
     * by the market which may not make sense for the user.</p>
     *
     * @param actionView the actions to be translated and whose specs should be generated
     * @param settingPolicyIdToSettingPolicyName a map from settingPolicyId to settingPolicyName
     * @param isUserAbleToApplyActions the current user is able to apply actions
     * @param actionStore Store to check plan topology for modifying explanation.
     * @return the {@link ActionSpec} representation of this action
     */
    @Nonnull
    private ActionSpec toSpec(@Nonnull final ActionView actionView,
            @Nonnull final Map<Long, String> settingPolicyIdToSettingPolicyName,
            boolean isUserAbleToApplyActions,
            @Nullable final ActionStore actionStore) {
        final ActionDTO.Action recommendationForDisplay = actionView
                .getTranslationResultOrOriginal();

        final TopologyInfo topologyInfo = actionStore instanceof PlanActionStore
                ? ((PlanActionStore)actionStore).getTopologyInfo() : null;
        ActionSpec.Builder specBuilder = ActionSpec.newBuilder()
            .setRecommendation(recommendationForDisplay)
            .setRecommendationId(actionView.getRecommendationOid())
            .setActionPlanId(actionView.getActionPlanId())
            .setRecommendationTime(actionView.getRecommendationTime()
                .toInstant(ZoneOffset.ofTotalSeconds(0)).toEpochMilli())
            .setActionState(actionView.getState())
            .setIsExecutable(actionView.determineExecutability())
            .setExplanation(ExplanationComposer.composeExplanation(
                recommendationForDisplay, settingPolicyIdToSettingPolicyName,
                actionTopologyStore.getSourceTopology().map(BaseTopology::entityGraph),
                topologyInfo))
            .setCategory(actionView.getActionCategory())
            .setSeverity(actionView.getActionSeverity())
            .setDescription(actionView.getDescription());

        // If the user has an observer role, all actions should be dropped to RECOMMEND
        // as the highest mode (DISABLED are generally filtered out,
        // but if they are returned from AO, their mode can stay DISABLED).
        if (!isUserAbleToApplyActions && actionView.getMode() != ActionMode.DISABLED) {
            specBuilder.setActionMode(ActionMode.RECOMMEND);
        } else {
            specBuilder.setActionMode(actionView.getMode());
        }

        if (actionView.getSchedule().isPresent()) {
            specBuilder.setActionSchedule(actionView.getSchedule().get().getTranslation());
        }

        actionView.getExternalActionName().ifPresent(specBuilder::setExternalActionName);
        actionView.getExternalActionUrl().ifPresent(specBuilder::setExternalActionUrl);

        // Compose pre-requisite description if action has any pre-requisite.
        if (!recommendationForDisplay.getPrerequisiteList().isEmpty()) {
            specBuilder.addAllPrerequisiteDescription(
                PrerequisiteDescriptionComposer.composePrerequisiteDescription(recommendationForDisplay));
        }

        actionView.getDecision()
            .ifPresent(specBuilder::setDecision);
        actionView.getCurrentExecutableStep()
            .map(ExecutableStep::getExecutionStep)
            .ifPresent(specBuilder::setExecutionStep);

        return specBuilder.build();
    }

    /**
     * Returns {@code true} if the current user is able to apply actions. It checks the spring
     * or GRPC context to find the role of the user making the request. If no user found, return
     * true for now, since it must be from a component other than api (since api always pass a JWT),
     * which we want original action mode.
     * Todo: this logic need to change when we finish the jwt token on all components.
     *
     * @return {@code true} if the current user is able to apply actions.
     */
    private static boolean isUserAbleToApplyActions() {
        return UserContextUtils.getCurrentUserRoles()
                .map(roles -> roles.stream()
                        .map(String::toUpperCase)
                        .anyMatch(ROLES_WITH_ABILITY_TO_APPLY_ACTIONS::contains))
                .orElse(true);
    }

    /**
     * Basic translation implementation.
     */
    private static class ActionTranslationExecutor implements TranslationExecutor {
        private final List<BatchTranslator> batchTranslatorList;

        /**
         * Create a new {@link ActionTranslationExecutor} for translating actions from the market's domain-agnostic
         * action recommendations into actions that can be executed and understood in real-world domain-specific
         * terms.
         *
         * @param repoService The repService which can be used to fetch entity information useful
         *                    for action translation.
         * @param actionTopologyStore Store for minimal topology to look up entity information.
         */
        ActionTranslationExecutor(
                @Nonnull final RepositoryServiceBlockingStub repoService,
                @Nonnull final ActionTopologyStore actionTopologyStore) {
            // The order is important. Matching BatchTranslator is searched for from the beginning
            // to the end of the list.
            batchTranslatorList = ImmutableList.of(
                new SkipBatchTranslator(),
                new VCpuResizeBatchTranslator(Objects.requireNonNull(repoService), actionTopologyStore),
                new PassThroughBatchTranslator());
        }

        /**
         * See comments for {@link ActionTranslator#translate(Stream, EntitiesAndSettingsSnapshot)}.
         *
         * @param <T> the type of the action being translated.
         * @param actionStream A stream of actions whose info should be translated from the market's
         *                     domain-agnostic variant.
         * @param snapshot A snapshot of all the entities and settings involved in the actions.
         *
         * @return A stream of the translated actions.
         *         Note, like all Java streams, a terminal operation must be applied on it to force evaluation.
         *         Note that the order of the actions in the output stream is not guaranteed to be the same as they were
         *         on the input. If this is important, consider applying an {@link Stream#sorted(Comparator)} operation.
         */
        public <T extends ActionView> Stream<T> translate(@Nonnull final Stream<T> actionStream,
                                                          @Nonnull final EntitiesAndSettingsSnapshot snapshot) {
            final Map<BatchTranslator, List<T>> actionsByTranslationMethod = actionStream
                .collect(Collectors.groupingBy(this::getBatchTranslator));

            return actionsByTranslationMethod.entrySet().stream()
                .map(entry -> translate(entry.getKey(), entry.getValue(), snapshot))
                .reduce(Stream.empty(), Stream::concat);
        }

        private <T extends ActionView> Stream<T> translate(@Nonnull final BatchTranslator batchTranslator,
                                                           @Nonnull final List<T> actionsToTranslate,
                                                           @Nonnull final EntitiesAndSettingsSnapshot snapshot) {
            try {
                return batchTranslator.translate(actionsToTranslate, snapshot);
            } catch (RuntimeException e) {
                logger.error("Error applying " + batchTranslator.getClass().getSimpleName(), e);
                // Fail the translations for all actions that we were attempting to translate.
                actionsToTranslate.forEach(
                    action -> action.getActionTranslation().setTranslationFailure());
                return Stream.empty();
            }
        }

        /**
         * Get the method for use in translating the action in question.
         * Will return a skipTranslation method if no translation should be performed.
         * Will return a passthroughTranslation method when the output of a translation should
         * be the same as the input.
         *
         * @param actionView The action for which an appropriate translation method will be fetched.
         * @return A method appropriate for use in translating a batch of actions of the sort
         *         passed in.
         */
        private BatchTranslator getBatchTranslator(@Nonnull final ActionView actionView) {
            for (BatchTranslator batchTranslator : batchTranslatorList) {
                if (batchTranslator.appliesTo(actionView)) {
                    return batchTranslator;
                }
            }
            // This should never happen. The last BatchTranslator in batchTranslatorList is
            // always applied.
            throw new IllegalStateException("Cannot find BatchTranslator for " + actionView);
        }
    }
}
