package com.vmturbo.market.topology.conversions;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup.SettingPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.ListSettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValue;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.setting.SettingDTOUtil;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO.ActionTypeCase;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
/** This class reads the tier exclusion settings from group-component and keeps a track of the
 * commodities bought that need to be added to consumer and commodities sold that need to be
 * added to tiers. When TopologyConverter::convertToMarket executes, we use this class' instance
 * to add the relevant commodities to the trader.
 * If two settings have the same set of excluded templates, we create only one commodity. So the
 * instance of this class also keeps track of the mapping between segmentation commodity types
 * created and exclusion setting ids. This mapping is used while interpreting
 * actions - find the relevant setting which is the reason for move/reconfigure actions and put this
 * reason setting onto the action.
 *
 * <p>Example:
 *  Raw policies (as created by user or probe):
 *  Setting policy 1 -> Excludes tier 1 for VM1, VM2.
 *  Setting policy 2 -> Excludes tier 2 for VM1, VM3.
 *  Setting policy 3 -> Excludes tier 2 for VM2</p>
 *
 *  <p>This gets resolved in Topology-processor as:
 *  VM1 -> excludes tier 1, 2 and the responsible setting policies are 1 and 2.
 *  VM2 -> excludes tier 1, 2 and the responsible setting policies are 1 and 3.
 *  VM3 -> excludes tier 2 and the responsible setting policy is 2.</p>
 *
 *  <p>We make the call to group-component's getEntitySettings. This groups by the setting and
 *  the policy. So we recieve it like below:
 *  EntitySettingGroup 1:
 *       Setting - tier 1, tier 2
 *           -> Setting policy 1 and 2 are in the policyIds
 *           -> VM1 is in the EntityOidsList
 *  EntitySettingGroup 2:
 *       Setting - tier 1, tier 2
 *           -> Setting policy 1 and 3 are in the policyIds
 *           -> VM2 is in the EntityOidsList
 *  EntitySettingGroup 3:
 *       Setting - tier 2
 *           -> Setting policy 2
 *           -> VM3 is in the EntityOidsList</p>
 */

public class TierExcluder {

    @VisibleForTesting
    static final String TIER_EXCLUSION_KEY_PREFIX = "TIER_EXCLUSION_";
    // Map of family to tiers
    private final Map<String, Set<Long>> familyToTiers = Maps.newHashMap();
    // Map of tiers to the commodity types they need to sell
    private final Map<Long, Set<CommodityType>> tierToCommTypeSold = Maps.newHashMap();
    // Map of consumer to the commodity types they need to buy
    private final Map<Long, Set<CommodityType>> consumerToCommTypeBought = Maps.newHashMap();
    private static final Logger logger = LogManager.getLogger();
    private final Map<Long, ShoppingListInfo> shoppingListOidToInfos;
    // The set of commodity types created by this class
    private final Set<CommodityType> tierExclusionCommodityTypes = Sets.newHashSet();
    // Map of consumer oid to the ids of all the tier exclusion settings which have the consumer
    // in its scope.
    // In the example above, we would store the below mapping:
    // VM1 -> Setting policies 1 and 2.
    // VM2 -> Setting policy 1 and 3
    // VM3 -> Setting policy 2
    private final Map<Long, Set<Long>> consumerOidToTierExclusionSettings = Maps.newHashMap();
    private final SettingPolicyServiceBlockingStub settingPolicyService;
    private final TopologyInfo topologyInfo;
    private final Map<ActionTO, Set<Long>> m2ActionsToReasonSettings = Maps.newHashMap();
    private long keyCounter = 0;
    // isInitialized will be set to true after the initialize method has been called.
    private boolean isInitialized;
    private final CommodityConverter commodityConverter;

    /**
     * This constructor accepts all the parameters needed by this class. If the object is
     * constructed using that, then isInstanceValid will be true.
     *
     * @param topologyInfo Topology info
     * @param settingPolicyService setting policy service client (used to fetch the tier
     *                             exclusion settings from group component)
     * @param commodityConverter commodity converter
     * @param shoppingListOidToInfos shopping list oid to infos
     */
    private TierExcluder(
        @Nonnull final TopologyInfo topologyInfo,
        @Nonnull final SettingPolicyServiceBlockingStub settingPolicyService,
        @NonNull final CommodityConverter commodityConverter,
        @NonNull Map<Long, ShoppingListInfo> shoppingListOidToInfos) {
        this.topologyInfo = topologyInfo;
        this.settingPolicyService = settingPolicyService;
        this.commodityConverter = commodityConverter;
        this.shoppingListOidToInfos = shoppingListOidToInfos;
    }

    /**
     * Initializes the tier exclusion applicator.
     * 1. Fetches the tier exclusion settings from group component
     * 2. Parses this information, and finds out
     *      a. what consumers need to buy what commodity types
     *      b. what tiers need to sell what commodity types
     * The commodities are then added to the entity when TopologyConverter converts
     * TopologyEntityDTO to TraderTO.
     * We store the mapping between consumer
     * @param topology the topology containing all the topology entity DTOs
     */
    public void initialize(@Nonnull final Map<Long, TopologyDTO.TopologyEntityDTO> topology) {
        Map<Set<Long>, CommodityType> excludedTiersToCommodityType = Maps.newHashMap();
        Stream<EntitySettingGroup> entitySettingGroups = fetchTierExclusionSettings();
        entitySettingGroups.forEach(entitySettingGroup -> {
            List<Long> excludedTiersList = entitySettingGroup.getSetting()
                .getSortedSetOfOidSettingValue().getOidsList();
            List<Long> consumers = entitySettingGroup.getEntityOidsList();
            // Ensure at least one consumer is in the topology. In case of optimize cloud plans,
            // we get a scoped topology and it is possible that none of the consumers
            // are present in the scoped topology
            if (consumers.stream().anyMatch(consumer -> topology.containsKey(consumer))) {
                // Also make sure that at least one excluded tier is present in the topology
                Optional<Long> singleExcludedTierId = excludedTiersList.stream().filter(
                    entity -> topology.containsKey(entity)).findFirst();
                if (singleExcludedTierId.isPresent()) {
                    Set<Long> excludedTiers = Sets.newHashSet(excludedTiersList);
                    // If it is the first time that we are seeing this excluded set, then create
                    // a new commodity type for it and make the included tiers sell it.
                    if (!excludedTiersToCommodityType.containsKey(excludedTiers)) {
                        // TODO: Is there a better way to look for entity type?
                        int entityType = topology.get(singleExcludedTierId.get()).getEntityType();
                        Set<Long> includedTiers = topology.values().stream()
                            .filter(t -> t.getEntityType() == entityType
                                && !excludedTiers.contains(t.getOid()))
                            .map(TopologyEntityDTO::getOid)
                            .collect(Collectors.toSet());

                        CommodityType commodityType = CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.SEGMENTATION_VALUE)
                            .setKey(TIER_EXCLUSION_KEY_PREFIX + keyCounter++)
                            .build();
                        tierExclusionCommodityTypes.add(commodityType);
                        excludedTiersToCommodityType.put(excludedTiers, commodityType);

                        for (Long tierId : includedTiers) {
                            TopologyEntityDTO tier = topology.get(tierId);
                            String family = tier.getTypeSpecificInfo().getComputeTier().getFamily();
                            tierToCommTypeSold.computeIfAbsent(
                                tierId, k -> new HashSet<>()).add(commodityType);
                            familyToTiers.computeIfAbsent(
                                family, k -> new HashSet<>()).add(tierId);
                        }
                    }
                    // Get the comm type for this excluded set, and make the consumers buy it
                    CommodityType commType = excludedTiersToCommodityType.get(excludedTiers);
                    consumers.forEach(consumer -> consumerToCommTypeBought.computeIfAbsent(
                        consumer, k -> new HashSet<>()).add(commType));

                    List<Long> responsibleSettingPolicies = entitySettingGroup.getPolicyIdList().stream()
                        .map(SettingPolicyId::getPolicyId)
                        .collect(Collectors.toList());
                    consumers.forEach(consumer -> consumerOidToTierExclusionSettings.computeIfAbsent(
                        consumer, k -> new HashSet<>()).addAll(responsibleSettingPolicies));
                }
            }
        });
        isInitialized = true;
    }

    /**
     * Computes the reason settings for all actions which were caused by tier exclusion.
     * If an action was caused by tier exclusion, we find the setting policy responsible and
     * store it in m2ActionsToReasonSettings.
     *
     * @param m2Actions the list of actions generated by market
     * @param originalCloudTopology the original cloud topology
     */
    void computeReasonSettings(List<ActionTO> m2Actions,
                                  CloudTopology<TopologyEntityDTO> originalCloudTopology) {
        // Filter out the tier exclusion actions and find the setting policies to fetch
        Set<ActionTO> tierExclusionActions = Sets.newHashSet();
        Set<Long> settingPoliciesToFetch = Sets.newHashSet();
        m2Actions.stream().filter(this::isTierExclusionAction)
            .peek(tierExclusionActions::add)
            .filter(m2Action -> m2Action.getActionTypeCase() != ActionTypeCase.RECONFIGURE)
            .map(this::getActionTarget)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .map(consumerOidToTierExclusionSettings::get)
            .forEach(settingPoliciesToFetch::addAll);

        // Fetch the setting policies
        Map<Long, Set<Long>> settingPolicyToExcludedTemplates = Maps.newHashMap();
        settingPolicyService.listSettingPolicies(ListSettingPoliciesRequest.newBuilder()
            .addAllIdFilter(settingPoliciesToFetch).build())
            .forEachRemaining(sp -> {
                Optional<List<Long>> excludedTiers = sp.getInfo().getSettingsList().stream()
                    .filter(s -> s.getSettingSpecName().equals(EntitySettingSpecs.ExcludedTemplates.getSettingName()))
                    .findFirst()
                    .map(Setting::getSortedSetOfOidSettingValue)
                    .map(SortedSetOfOidSettingValue::getOidsList);
                if (excludedTiers.isPresent() && !excludedTiers.get().isEmpty()) {
                    settingPolicyToExcludedTemplates.put(sp.getId(), Sets.newHashSet(excludedTiers.get()));
                }
            });
        // Find the reason setting for all the tier exclusion actions
        for (ActionTO tierExclusionAction : tierExclusionActions) {
            Optional<Long> actionTarget = getActionTarget(tierExclusionAction);
            if (actionTarget.isPresent()) {
                Optional<TopologyEntityDTO> primaryTier = originalCloudTopology.getPrimaryTier(actionTarget.get());
                if (primaryTier.isPresent()) {
                    Set<Long> candidateReasonSettings = consumerOidToTierExclusionSettings.get(actionTarget.get());
                    // The candidateReasonSettings which exclude the source tier of the consumer
                    // are the reason settings we want.
                    Set<Long> reasonSettings;
                    if (tierExclusionAction.getActionTypeCase() == ActionTypeCase.RECONFIGURE) {
                        reasonSettings = candidateReasonSettings;
                    } else {
                        reasonSettings = candidateReasonSettings.stream()
                            .filter(setting -> {
                                Set<Long> tiersExcludedByCandidateReasonSetting =
                                    settingPolicyToExcludedTemplates.get(setting);
                                return tiersExcludedByCandidateReasonSetting != null &&
                                    tiersExcludedByCandidateReasonSetting.contains(primaryTier.get().getOid());
                            }).collect(Collectors.toSet());
                    }

                    if (!reasonSettings.isEmpty()) {
                        m2ActionsToReasonSettings.put(tierExclusionAction, reasonSettings);
                    }
                }
            }
        }
    }

    /**
     * Gets the reason tier exclusion setting oids for an action, if an action was caused by
     * tier exclusion.
     * @param m2Action the action
     * @return Optional of the set of reason settings for an action. If the action was not
     * caused by tier exclusion, Optional.empty is returned.
     */
    Optional<Set<Long>> getReasonSettings(ActionTO m2Action) {
        return Optional.ofNullable(m2ActionsToReasonSettings.get(m2Action));
    }

    /**
     * After Traders are created in TopologyConverter::convertToMarket, we don't need some state.
     * So we clear that.
     */
    void clearStateNeededForConvertToMarket() {
        familyToTiers.clear();
        tierToCommTypeSold.clear();
        consumerToCommTypeBought.clear();
    }

    /**
     * After actions have been interpreted, we don't need some state.
     * So we clear that.
     */
    void clearStateNeededForActionInterpretation() {
        tierExclusionCommodityTypes.clear();
        consumerOidToTierExclusionSettings.clear();
        m2ActionsToReasonSettings.clear();
    }

    /**
     * Get the tier exclusion commodityTypes to sell for a tier.
     *
     * @param tierOid the tier's oid
     * @return Set of commodity types to sell for a tier id
     */
    @NonNull
    Set<CommodityType> getTierExclusionCommoditiesToSell(long tierOid) {
        if (isInitialized) {
            if (tierToCommTypeSold.containsKey(tierOid)) {
                return tierToCommTypeSold.get(tierOid);
            }
        }
        return Collections.emptySet();
    }

    /**
     * Get the tier exclusion commodityTypes to sell for a family of tiers.
     * RIs of a family will sell a union of the commodityTypes which the individual tiers sell.
     * For ex. m4 family instance size flexible RIs will sell all commodity types which all the
     * tiers of m4 family (nano, micro, small etc) sell.
     *
     * @param family the family
     * @return the set of commodities to sell for the family of CBTP
     */
    @NonNull
    Set<CommodityType> getTierExclusionCommoditiesToSell(@Nonnull String family) {
        Set<CommodityType> commoditySoldTOs = Sets.newHashSet();
        if (isInitialized) {
            if (familyToTiers.containsKey(family)) {
                Set<Long> tiers = familyToTiers.get(family);
                for (Long tierId : tiers) {
                    if (tierToCommTypeSold.containsKey(tierId)) {
                        commoditySoldTOs.addAll(tierToCommTypeSold.get(tierId));
                    }
                }
            }
        }
        return commoditySoldTOs;
    }

    /**
     * Get the tier exclusion commodities to buy for a consumer.
     *
     * @param consumerOid the oid of the consumer
     * @return the set of commodities to buy for a consumer
     */
    @NonNull
    Set<CommodityType> getTierExclusionCommoditiesToBuy(long consumerOid) {
        if (isInitialized) {
            if (consumerToCommTypeBought.containsKey(consumerOid)) {
                return consumerToCommTypeBought.get(consumerOid);
            }
        }
        return Collections.emptySet();
    }

    /**
     * Fetch the tier exclusion settings from group component.
     *
     * @return the stream of entity setting group which have the template exclusion settings
     */
    private Stream<EntitySettingGroup> fetchTierExclusionSettings() {
        EntitySettingFilter.Builder entitySettingFilter = EntitySettingFilter.newBuilder()
            .addSettingName(EntitySettingSpecs.ExcludedTemplates.getSettingName());
        // Do not set topology selection in GetEntitySettingsRequest because resolved settings are
        // not uplodaded to group component for plans. So pick the real time settings always.
        // This is fine because we cannot set template exclusion exclusively for plans today
        GetEntitySettingsRequest request = GetEntitySettingsRequest.newBuilder()
            .setSettingFilter(entitySettingFilter)
            .setIncludeSettingPolicies(true).build();
        return SettingDTOUtil.flattenEntitySettings(settingPolicyService.getEntitySettings(request));
    }

    /**
     * Get the target oid of an action.
     * @param m2Action the action.
     * @return the target oid of the action.
     */
    private Optional<Long> getActionTarget(ActionTO m2Action) {
        long targetSlOid;
        switch (m2Action.getActionTypeCase()) {
            case MOVE:
                targetSlOid = m2Action.getMove().getShoppingListToMove();
                break;
            case RECONFIGURE:
                targetSlOid = m2Action.getReconfigure().getShoppingListToReconfigure();
                break;
            case COMPOUND_MOVE:
                targetSlOid = m2Action.getCompoundMove().getMovesList().get(0).getShoppingListToMove();
                break;
            default:
                logger.error("Trying to find target of action. Action type {} not supported.", m2Action.getActionTypeCase());
                return Optional.empty();
        }
        ShoppingListInfo slInfo = shoppingListOidToInfos.get(targetSlOid);
        if (slInfo == null) {
            logger.error("Cannot find target of action as it has no associated slInfo. Action -> {}.", m2Action);
            return Optional.empty();
        }
        return Optional.of(slInfo.getBuyerId());
    }

    /**
     * Is m2Action a tier exclusion action?
     * @param m2Action the action generated by market 2
     * @return true if it was caused by tier exclusion, false otherwise
     */
    private boolean isTierExclusionAction(ActionTO m2Action) {
        switch (m2Action.getActionTypeCase()) {
            case MOVE:
                return isMoveForTierExclusion(m2Action.getMove());
            case RECONFIGURE:
                return m2Action.getReconfigure().getCommodityToReconfigureList().stream()
                    .anyMatch(this::isCommSpecTypeForTierExclusion);
            case COMPOUND_MOVE:
                return m2Action.getCompoundMove().getMovesList().stream()
                    .anyMatch(this::isMoveForTierExclusion);
            default:
                return false;
        }
    }

    /**
     * Is the move for tier exclusion?
     * @param moveTO the moveTO
     * @return true if the move was caused by tier exclusion
     */
    private boolean isMoveForTierExclusion(MoveTO moveTO) {
        return moveTO.getMoveExplanation().hasCompliance() &&
            moveTO.getMoveExplanation().getCompliance().getMissingCommoditiesList().stream()
                .anyMatch(this::isCommSpecTypeForTierExclusion);
    }

    /**
     * Is the commodity type for tier exclusion?
     * @param commType the commodity type
     * @return true if the commodity type was created for tier exclusion
     */
    boolean isCommodityTypeForTierExclusion(CommodityType commType) {
        return tierExclusionCommodityTypes.contains(commType);
    }

    /**
     * Is the comm spec for tier exclusion?
     * @param commSpecType the commSpecType
     * @return true if the comm spec was created for tier exclusion
     */
    boolean isCommSpecTypeForTierExclusion(int commSpecType) {
        return tierExclusionCommodityTypes.contains(
            commodityConverter.commodityIdToCommodityType(commSpecType));
    }

    /**
     * Factory for instances of {@link TierExcluder}.
     */
    public interface TierExcluderFactory {

        /**
         * Create a new {@link TierExcluder}.
         * @param topologyInfo information about the topology
         * @param commodityConverter the commodity converter
         * @param shoppingListOidToInfos the map of shopping list oids to infos
         * @return new instance of{@link TierExcluder}
         */
        @Nonnull
        TierExcluder newExcluder(TopologyInfo topologyInfo,
                                 CommodityConverter commodityConverter,
                                 Map<Long, ShoppingListInfo> shoppingListOidToInfos);

        /**
         * The default implementation of {@link TierExcluderFactory}, for use in "real" code.
         */
        class DefaultTierExcluderFactory implements TierExcluderFactory {

            private final SettingPolicyServiceBlockingStub settingPolicyService;

            public DefaultTierExcluderFactory(@Nonnull final SettingPolicyServiceBlockingStub settingPolicyService) {
                this.settingPolicyService = settingPolicyService;
            }

            /**
             * Returns a new {@link TierExcluder}.
             * @param topologyInfo the topologyInfo
             * @param commodityConverter the commodity converter
             * @param shoppingListOidToInfos the map of shopping list oids to infos
             * @return a new {@link TierExcluder}
             */
            @Nonnull
            @Override
            public TierExcluder newExcluder(@NonNull final TopologyInfo topologyInfo,
                                            @NonNull CommodityConverter commodityConverter,
                                            @NonNull Map<Long, ShoppingListInfo> shoppingListOidToInfos) {
                return new TierExcluder(topologyInfo,
                    settingPolicyService,
                    commodityConverter,
                    shoppingListOidToInfos);
            }
        }
    }
}