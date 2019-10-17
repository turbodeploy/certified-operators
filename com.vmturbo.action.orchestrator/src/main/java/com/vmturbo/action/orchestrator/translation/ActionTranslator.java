package com.vmturbo.action.orchestrator.translation;

import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.grpc.Channel;

import com.vmturbo.action.orchestrator.action.ActionTranslation;
import com.vmturbo.action.orchestrator.action.ActionTranslation.TranslationStatus;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.ExplanationComposer;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Compliance;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.ListSettingPoliciesRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.TypeSpecificPartialEntity;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.proactivesupport.DataMetricCounter;


/**
 * Translates actions from the market's domain-agnostic actions into real-world domain-specific actions.
 *
 * Some actions may require no translation at all. For those actions, the translator functions purely
 * as a passthrough. Some actions may require translation of execution and/or display related properties.
 * Some examples of actions that need to be translated:
 *
 * 1. VCPU. the unit is converted from Mhz to number of VCPU's (when resizing the VCPU commodity on a VM).
 * This translation should happen for both execution and display.
 * 2. UCS NetThroughput. In UCS Switches, we want to show resize in terms of ports, and not in terms on kbps.
 * this is done by: (net usage) / (net increment), where network increment specifies the kbps of a single port.
 * (Not yet supported). Not sure if this impacts both execution and display or just display.
 *
 * Both info and explanation for a recommendation may need to be translated.
 *
 * Note that Action information is communicated to other components in the form of {@link ActionSpec} objects.
 * Components outside the action orchestrator should receive information about those actions in terms of the
 * real-world domain. That is, actions should be translated before their details are sent to other components.
 * In order to consistently enforce that all actions are first translated before being sent to other components,
 * the {@link ActionTranslator} serves as the sole gateway for generating {@link ActionSpec} objects from
 * {@link ActionView} objects.
 *
 * A note on thread-safety: Because both {@link ActionView}s and {@link ActionTranslation} objects are thread-safe,
 * it is safe to attempt to translate the same action multiple times concurrently. Which translation result takes
 * precedence is not guaranteed, but information on the action will be consistent.
 */
public class ActionTranslator {

    private static final Logger logger = LogManager.getLogger();

    /**
     * An interface for actually executing action translation. Usually it is unnecessary to specify
     * the specific {@link TranslationExecutor} to use except when mocking for testing.
     */
    public interface TranslationExecutor {
        /**
         * Translate a stream of actions from the market's domain-agnostic form to the domain-specific form
         * relevant for execution and display in the real world.
         *
         * @param actionStream A stream of actions whose info should be translated from the market's
         *                     domain-agnostic variant.
         * @param snapshot A snapshot of all the entities and settings involved in the actions
         * @return A stream of the translated actions.
         *         Note, like all Java streams, a terminal operation must be applied on it to force evaluation.
         *         Note that the order of the actions in the output stream is not guaranteed to be the same as they were
         *         on the input. If this is important, consider applying an {@link Stream#sorted(Comparator)} operation.
         */
        <T extends ActionView> Stream<T> translate(@Nonnull final Stream<T> actionStream,
                                                   @Nonnull final EntitiesAndSettingsSnapshot snapshot);
    }

    private final TranslationExecutor translationExecutor;

    private final SettingPolicyServiceBlockingStub settingPolicyService;

    /**
     * Create a new {@link ActionTranslator} for translating actions from the market's domain-agnostic
     * action recommendations into actions that can be executed and understood in real-world domain-specific
     * terms.
     *
     * @param repoChannel The searchServiceRpc to the repository component.
     * @param groupChannel Channel to use for creating a blocking stub to query the Group Service.
     */
    public ActionTranslator(@Nonnull final Channel repoChannel, @Nonnull final Channel groupChannel) {
        translationExecutor = new ActionTranslationExecutor(RepositoryServiceGrpc.newBlockingStub(repoChannel));
        this.settingPolicyService =
            SettingPolicyServiceGrpc.newBlockingStub(Objects.requireNonNull(groupChannel));
    }

    /**
     * Create a new {@link ActionTranslator} for translating actions from the market's domain-agnostic
     * action recommendations into actions that can be executed and understood in real-world domain-specific
     * terms.
     *
     * This method is exposed to permit the generation of action specs in tests without having to supply
     * all the information necessary to perform real translation.
     *
     * This method should NOT be used in production code.
     *
     * @param translationExecutor The object that will perform translation of actions.
     */
    @VisibleForTesting
    public ActionTranslator(@Nonnull final TranslationExecutor translationExecutor) {
        this.translationExecutor = Objects.requireNonNull(translationExecutor);
        this.settingPolicyService = null;
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
    public Stream<ActionSpec> translateToSpecs(@Nonnull final List<ActionView> actionViews) {
        // Get all reason setting ids.
        final Set<Long> reasonSettings = getAllReasonSettings(actionViews);

        // Make a RPC to get raw settingPolicies for all reason setting ids.
        // We only need the displayName of the settingPolicy.
        final Map<Long, String> settingPolicyIdToSettingPolicyName = new HashMap<>();
        if (!reasonSettings.isEmpty()) {
            settingPolicyService.listSettingPolicies(ListSettingPoliciesRequest.newBuilder()
                .addAllIdFilter(reasonSettings).build())
                .forEachRemaining(settingPolicy -> settingPolicyIdToSettingPolicyName.put(
                    settingPolicy.getId(), settingPolicy.getInfo().getDisplayName()));
        }

        return actionViews.stream().map(actionView -> toSpec(actionView, settingPolicyIdToSettingPolicyName));
    }

    /**
     * Get all reason settings from all actionViews.
     *
     * @param actionViews the actions from where we extract reason settings
     * @return a set of reason settings
     */
    @Nonnull
    @VisibleForTesting
    public Set<Long> getAllReasonSettings(@Nonnull final List<ActionView> actionViews) {
        final Set<Long> reasonSettings = new HashSet<>();
        for (ActionView actionView : actionViews) {
            Explanation explanation = actionView.getRecommendation().getExplanation();
            switch (explanation.getActionExplanationTypeCase()) {
                case MOVE:
                    actionView.getRecommendation().getExplanation().getMove()
                        .getChangeProviderExplanationList().stream()
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
        return reasonSettings;
    }

    /**
     * Translate a stream of actions from the market's domain-agnostic form to the domain-specific form
     * relevant for execution and display in the real world.
     *
     * If an action has already been translated, no attempt will be made to re-translate it.
     *
     * Note that the portion of this call that requires information from other services is currently blocking.
     * Actions whose translation fail will be given a translation status of
     * {@link TranslationStatus#TRANSLATION_FAILED}, and translations that succeed will be given a translation status
     * of {@link TranslationStatus#TRANSLATION_SUCCEEDED}.
     *
     * This method is blocking until translation of all actions completes.
     *
     * TODO: DavidBlinn 6/20/2017 - consider making the inner operation of this method asynch so that multiple
     * translation groups can be processed simultaneously. Right now only VCPU is translated so it would not yet
     * help.
     *
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
     * In the event that an action has been successfully translated, the recommendation provided will be the
     * result of translating the market's domain-agnostic action recommendation into a domain-specific recommendation.
     * In the event that an action cannot be translated, the included recommendation is the one originally provided
     * by the market which may not make sense for the user.
     *
     * @param actionView the actions to be translated and whose specs should be generated
     * @param settingPolicyIdToSettingPolicyName a map from settingPolicyId to settingPolicyName
     * @return the {@link ActionSpec} representation of this action
     */
    @Nonnull
    private ActionSpec toSpec(@Nonnull final ActionView actionView,
                              @Nonnull final Map<Long, String> settingPolicyIdToSettingPolicyName) {
        final ActionDTO.Action recommendationForDisplay = actionView
            .getActionTranslation()
            .getTranslationResultOrOriginal();

        ActionSpec.Builder specBuilder = ActionSpec.newBuilder()
            .setRecommendation(recommendationForDisplay)
            .setActionPlanId(actionView.getActionPlanId())
            .setRecommendationTime(actionView.getRecommendationTime()
                .toInstant(ZoneOffset.ofTotalSeconds(0)).toEpochMilli())
            .setActionMode(actionView.getMode())
            .setActionState(actionView.getState())
            .setIsExecutable(actionView.determineExecutability())
            .setExplanation(ExplanationComposer.composeExplanation(
                recommendationForDisplay, settingPolicyIdToSettingPolicyName))
            .setCategory(actionView.getActionCategory())
            .setDescription(actionView.getDescription());

        actionView.getDecision()
            .ifPresent(specBuilder::setDecision);
        actionView.getCurrentExecutableStep()
            .ifPresent(step -> specBuilder.setExecutionStep(step.getExecutionStep()));

        return specBuilder.build();
    }

    /**
     * Basic translation implementation.
     */
    private static class ActionTranslationExecutor implements TranslationExecutor {
        /**
         * A client for making remote calls to the Repository service to retrieve entity data
         */
        private final RepositoryServiceBlockingStub repoService;
        /**
         * An interface for translating a batch of actions at a time.
         *
         * Although a {@link BatchTranslator} returns a stream, currently it must finish translation
         * of all actions in the stream.
         */
        @FunctionalInterface
        public interface BatchTranslator {
            <T extends ActionView> Stream<T> translate(@Nonnull final List<T> actionsToTranslate,
                                                       @Nonnull EntitiesAndSettingsSnapshot snapshot);
        }

        /**
         * Note that we take a single reference to each of our translation methods.
         * Capturing a reference results in the generation of a lambda object, with a distinct object
         * generated each time a method reference is taken. It is important that we have a single
         * lambda for each method, so store the lambda for later use so that methods can be
         * meaningfully compared with themselves.
         */
        private final BatchTranslator skipTranslationMethod = this::skipTranslation;
        private final BatchTranslator passthroughTranslationMethod = this::passthroughTranslation;
        private final BatchTranslator vcpuResizeTranslationMethod = this::translateVcpuResizes;

        /**
         * Create a new {@link ActionTranslationExecutor} for translating actions from the market's domain-agnostic
         * action recommendations into actions that can be executed and understood in real-world domain-specific
         * terms.
         *
         * @param repoService The repService which can be used to fetch entity information useful
         *                    for action translation.
         */
        public ActionTranslationExecutor(@Nonnull final RepositoryServiceBlockingStub repoService) {
            this.repoService = Objects.requireNonNull(repoService);
        }

        /**
         * See comments for {@link ActionTranslator#translate(Stream, EntitiesAndSettingsSnapshot)}
         *
         * @param actionStream A stream of actions whose info should be translated from the market's
         *                     domain-agnostic variant.
         *
         * @return A stream of the translated actions.
         *         Note, like all Java streams, a terminal operation must be applied on it to force evaluation.
         *         Note that the order of the actions in the output stream is not guaranteed to be the same as they were
         *         on the input. If this is important, consider applying an {@link Stream#sorted(Comparator)} operation.
         */
        public <T extends ActionView> Stream<T> translate(@Nonnull final Stream<T> actionStream,
                                                          @Nonnull EntitiesAndSettingsSnapshot snapshot) {
            final Map<BatchTranslator, List<T>> actionsByTranslationMethod = actionStream
                .collect(Collectors.groupingBy(this::getTranslationMethod));

            return actionsByTranslationMethod.entrySet().stream()
                .map(entry -> entry.getKey().translate(entry.getValue(), snapshot))
                .reduce(Stream.empty(), Stream::concat);
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
        private BatchTranslator getTranslationMethod(@Nonnull final ActionView actionView) {
            if (actionView.getTranslationStatus() == TranslationStatus.TRANSLATION_SUCCEEDED) {
                return this.skipTranslationMethod; // No translation necessary.
            }

            final ActionInfo actionInfo = actionView.getRecommendation().getInfo();
            switch (actionInfo.getActionTypeCase()) {
                case RESIZE:
                    return (actionInfo.getResize().getTarget().getType() == EntityType.VIRTUAL_MACHINE_VALUE &&
                            actionInfo.getResize().getCommodityType().getType() == CommodityType.VCPU_VALUE) ?
                        this.vcpuResizeTranslationMethod :
                        this.passthroughTranslationMethod;
                default:
                    return this.passthroughTranslationMethod;
            }
        }

        /**
         * Do not translate the action because, for example, it has already been translated.
         *
         * @param actionsToTranslate The action to translate.
         * @param snapshot A snapshot of all the entities and settings involved in the actions
         *
         * @return A stream containing the input actions.
         */
        private <T extends ActionView> Stream<T> skipTranslation(@Nonnull final List<T> actionsToTranslate,
                                                                 @Nonnull EntitiesAndSettingsSnapshot snapshot) {
            return actionsToTranslate.stream();
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
        private <T extends ActionView> Stream<T> passthroughTranslation(@Nonnull final List<T> actionsToTranslate,
                                                                        @Nonnull EntitiesAndSettingsSnapshot snapshot) {
            return actionsToTranslate.stream()
                .map(action -> {
                    action.getActionTranslation().setPassthroughTranslationSuccess();
                    return action;
                });
        }

        /**
         * Trnaslate vCPU resize actions.
         * vCPU resizes are translated from MHz to number of vCPUs.
         *
         * @param resizeActions The actions to be translated.
         * @param snapshot A snapshot of all the entities and settings involved in the actions
         *
         * @return A stream of translated vCPU actions.
         */
        private <T extends ActionView> Stream<T> translateVcpuResizes(@Nonnull final List<T> resizeActions,
                                                                      @Nonnull EntitiesAndSettingsSnapshot snapshot) {
            final Map<Long, List<T>> resizeActionsByVmTargetId = resizeActions.stream()
                .collect(Collectors.groupingBy(action ->
                    action.getRecommendation().getInfo().getResize().getTarget().getId()));
            Map <Long, Long> targetIdToPrimaryProviderId = Maps.newHashMap();
            Set<Long> entitiesToRetrieve = Sets.newHashSet();
            try {
                for (T action : resizeActions) {
                    long targetId = action.getRecommendation().getInfo().getResize().getTarget().getId();
                    Optional<ActionPartialEntity> targetEntity = snapshot.getEntityFromOid(targetId);
                    targetEntity.ifPresent(entity -> {
                        targetIdToPrimaryProviderId.put(entity.getOid(), entity.getPrimaryProviderId());
                        entitiesToRetrieve.add(entity.getPrimaryProviderId());
                    });
                }
                // Note: It is important to force evaluation of the gRPC stream here in order
                // to trigger any potential exceptions in this method where they can be handled
                // properly. Generating a lazy stream of gRPC results that is not evaluated until
                // after the method return causes any potential gRPC exception not to be thrown
                // until it is too late to be handled.
                Map<Long, TypeSpecificPartialEntity> hostInfoMap = RepositoryDTOUtil.topologyEntityStream(
                    repoService.retrieveTopologyEntities(
                        RetrieveTopologyEntitiesRequest.newBuilder()
                        .setTopologyContextId(snapshot.getToologyContextId())
                        .addAllEntityOids(entitiesToRetrieve)
                        .setReturnType(Type.TYPE_SPECIFIC)
                        .setTopologyType(TopologyType.PROJECTED)
                        .build()))
                    .map(PartialEntity::getTypeSpecific)
                    .collect(Collectors.toMap(TypeSpecificPartialEntity::getOid, Function.identity()));

                return resizeActionsByVmTargetId.entrySet().stream().flatMap(
                    entry -> translateVcpuResizes(
                        entry.getKey(), targetIdToPrimaryProviderId.get(entry.getKey()),
                        hostInfoMap, entry.getValue()));
            } catch (RuntimeException e) {
                logger.error("Error attempting to translate VCPU resize actions: ", e);
                // Fail the translations for all actions that we were attempting to translate.
                resizeActions
                    .forEach(action -> action.getActionTranslation().setTranslationFailure());
                return Stream.empty();
            }
        }

        /**
         * Apply HostInfo about hosts of VMs hosting the VMs being resized in the actions in order
         * to translate the vCPU actions from MHz to number of vCPUs.
         *
         * @param targetId      The target id (for ex. the VM id)
         * @param providerId    The provider id (for ex. the host id)
         * @param hostInfoMap   The host info for the various resize actions.
         * @param resizeActions The resize actions to be translated.
         * @return A stream of the translated resize actions.
         */
        private <T extends ActionView> Stream<T> translateVcpuResizes(long targetId,
                                                                      Long providerId,
                                                                      @Nonnull final Map<Long, TypeSpecificPartialEntity> hostInfoMap,
                                                                      @Nonnull List<T> resizeActions) {
            TypeSpecificPartialEntity hostInfo = hostInfoMap.get(providerId);
            if (providerId == null || hostInfo == null || !hostInfo.hasTypeSpecificInfo()
                || !hostInfo.getTypeSpecificInfo().hasPhysicalMachine()
                || !hostInfo.getTypeSpecificInfo().getPhysicalMachine().hasCpuCoreMhz()) {
                logger.warn("Host info not found for VCPU resize on entity {}. Skipping translation",
                    targetId);
                // No host info found, fail the translation and return the originals.
                return resizeActions.stream()
                    .map(action -> {
                        action.getActionTranslation().setTranslationFailure();
                        return action;
                    });
            }
            return resizeActions.stream()
                .map(action -> {
                    final Resize newResize =
                        translateVcpuResizeInfo(action.getRecommendation().getInfo().getResize(), hostInfo);

                    // Float comparision should apply epsilon. But in this case both capacities are
                    // result of Math.round and Math.ceil (see translateVcpuResizeInfo method),
                    // so the values are actually integers.
                    if (Float.compare(newResize.getOldCapacity(), newResize.getNewCapacity()) == 0) {
                        action.getActionTranslation().setTranslationFailure();
                        logger.debug("VCPU resize (action: {}, entity: {}) has same from and to value ({}).",
                            action.getId(), newResize.getTarget().getId(), newResize.getOldCapacity());
                        Metrics.VCPU_SAME_TO_FROM.increment();
                        return action;
                    }
                    // Resize explanation does not need to be translated because the explanation is in terms
                    // of utilization which is normalized so translating units will not affect the values.

                    action.getActionTranslation().setTranslationSuccess(
                        action.getRecommendation().toBuilder().setInfo(
                            ActionInfo.newBuilder(action.getRecommendation().getInfo())
                                .setResize(newResize).build())
                            .build());
                    return action;
                });
        }

        /**
         * Apply a translation for an individual vCPU resize action given its corresponding host info.
         *
         * @param originalResize The info for the original resize action (in MHz).
         * @param hostInfo The host info for the host of the VM being resized.
         * @return The translated resize information (in # of vCPU).
         */
        private Resize translateVcpuResizeInfo(@Nonnull final Resize originalResize,
                                               @Nonnull final TypeSpecificPartialEntity hostInfo) {
            // don't apply the mhz translation for limit and reserved commodity attributes
            if (originalResize.getCommodityAttribute() == CommodityAttribute.LIMIT
                || originalResize.getCommodityAttribute() == CommodityAttribute.RESERVED) {
                return originalResize;
            }
            int cpuCoreMhz = hostInfo.getTypeSpecificInfo().getPhysicalMachine().getCpuCoreMhz();
            final Resize newResize = originalResize.toBuilder()
                .setOldCapacity(Math.round(originalResize.getOldCapacity() / cpuCoreMhz))
                .setNewCapacity((float)Math.ceil(originalResize.getNewCapacity() / cpuCoreMhz))
                .build();

            logger.debug("Translated VCPU resize from {} to {} for host with info {}",
                originalResize, newResize, hostInfo);

            return newResize;
        }
    }

    private static class Metrics {

        private static final DataMetricCounter VCPU_SAME_TO_FROM = DataMetricCounter.builder()
            .withName("ao_vcpu_translate_same_to_from_count")
            .withHelp("The number of VCPU translates where the to and from VCPU counts were the same.")
            .build()
            .register();

    }
}
