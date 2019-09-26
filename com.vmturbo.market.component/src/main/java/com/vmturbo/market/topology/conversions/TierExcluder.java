package com.vmturbo.market.topology.conversions;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.TopologySelection;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.setting.SettingDTOUtil;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * This class reads the tier exclusion settings from group-component and keeps a track of the
 * commodities bought that need to be added to consumer and commodities sold that need to be
 * added to tiers. When TopologyConverter::convertToMarket executes, we use this class' instance
 * to add the relevant commodities to the trader.
 * If two settings have the same set of excluded templates, we create only one commodity. So the
 * instance of this class also keeps track of the mapping between segmentation commodity types
 * created and exclusion setting ids. This mapping is used while interpreting
 * actions - find the relevant setting in violation for move/reconfigure actions and put it onto
 * the action.
 */
public class TierExcluder {

    private final Map<String, Set<Long>> familyToTiers = Maps.newHashMap();
    private final Map<Long, Set<CommodityType>> tierToCommTypeSold = Maps.newHashMap();
    private final Map<Long, Set<CommodityType>> consumerToCommTypeBought = Maps.newHashMap();
    private final Map<CommodityType, Set<Long>> commTypeToResponsibleSettings = Maps.newHashMap();

    private final SettingPolicyServiceBlockingStub settingPolicyService;
    private final TopologyInfo topologyInfo;
    private static final String TIER_EXCLUSION_KEY_PREFIX = "TIER_EXCLUSION_";
    private long keyCounter = 0;
    // isInitialized will be set to true after the initialize method has been called.
    private boolean isInitialized;

    /**
     * This constructor accepts all the parameters needed by this class. If the object is
     * constructed using that, then isInstanceValid will be true.
     *
     * @param topologyInfo Topology info
     * @param settingPolicyService setting policy service client (used to fetch the tier
     *                             exclusion settings from group component)
     */
    private TierExcluder(
        @Nonnull final TopologyInfo topologyInfo,
        @Nonnull final SettingPolicyServiceBlockingStub settingPolicyService) {
        this.topologyInfo = topologyInfo;
        this.settingPolicyService = settingPolicyService;
    }

    /**
     * Initializes the tier exclusion applicator.
     * 1. Fetches the tier exclusion settings from group component
     * 2. Parses this information, and finds out
     *      a. what consumers need to buy what commodity types
     *      b. what tiers need to sell what commodity types
     *
     * @param topology the topology containing all the topology entity DTOs
     */
    public void initialize(@Nonnull final Map<Long, TopologyDTO.TopologyEntityDTO> topology) {
        Map<Set<Long>, CommodityType> excludedTiersToCommodityType = Maps.newHashMap();
        Stream<EntitySettingGroup> entitySettingGroups = fetchTierExclusionSettings();
        entitySettingGroups.forEach(entitySettingGroup -> {
            List<Long> excludedTiersList = entitySettingGroup.getSetting().getListOfOidSettingValue().getOidsList();
            List<Long> consumers = entitySettingGroup.getEntityOidsList();
            // Make sure that the setting has some consumer and some excluded tiers
            // Also make sure that the topology contains the excluded tier
            if (!consumers.isEmpty() && !excludedTiersList.isEmpty()
                && topology.containsKey(excludedTiersList.iterator().next())) {
                Set<Long> excludedTiers = Sets.newHashSet(excludedTiersList);
                // If it is the first time that we are seeing this excluded set, then create
                // a new commodity type for it and make the included tiers sell it.
                if (!excludedTiersToCommodityType.containsKey(excludedTiers)) {
                    // TODO: Is there a better way to look for entity type?
                    int entityType = topology.get(excludedTiers.iterator().next()).getEntityType();
                    Set<Long> includedTiers = topology.values().stream()
                        .filter(t -> t.getEntityType() == entityType
                            && !excludedTiers.contains(t.getOid()))
                        .map(TopologyEntityDTO::getOid)
                        .collect(Collectors.toSet());

                    CommodityType commodityType = CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.SEGMENTATION_VALUE)
                        .setKey(TIER_EXCLUSION_KEY_PREFIX + keyCounter++)
                        .build();
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

                // TODO: Change this to get the list of policy ids directly from the setting
                // policy id once settings framework changes are done
                Set<Long> responsibleSettingPolicies = Sets.newHashSet(entitySettingGroup.getPolicyId().getPolicyId());
                commTypeToResponsibleSettings.computeIfAbsent(commType, k -> new HashSet<>()).addAll(responsibleSettingPolicies);
            }
        });
        isInitialized = true;
    }

    /**
     * Get the tier exclusion commodityTypes to sell for a tier.
     *
     * @param tierOid the tier's oid
     * @return Set of commodity types to sell for a tier id
     */
    @NonNull
    public Set<CommodityType> getTierExclusionCommoditiesToSell(long tierOid) {
        if (isInitialized) {
            if (tierToCommTypeSold.containsKey(tierOid)) {
                return tierToCommTypeSold.get(tierOid);
            }
        }
        return Collections.EMPTY_SET;
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
    public Set<CommodityType> getTierExclusionCommoditiesToSell(@Nonnull String family) {
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
    public Set<CommodityType> getTierExclusionCommoditiesToBuy(long consumerOid) {
        if (isInitialized) {
            if (consumerToCommTypeBought.containsKey(consumerOid)) {
                return consumerToCommTypeBought.get(consumerOid);
            }
        }
        return Collections.EMPTY_SET;
    }

    /**
     * Fetch the tier exclusion settings from group component.
     *
     * @return the stream of entity setting group which have the template exclusion settings
     */
    private Stream<EntitySettingGroup> fetchTierExclusionSettings() {
        TopologySelection.Builder topologySelection = TopologySelection.newBuilder()
            .setTopologyContextId(topologyInfo.getTopologyContextId())
            .setTopologyId(topologyInfo.getTopologyId());
        EntitySettingFilter.Builder entitySettingFilter = EntitySettingFilter.newBuilder()
            .addSettingName(EntitySettingSpecs.ExcludedTemplates.getSettingName());
        GetEntitySettingsRequest request = GetEntitySettingsRequest.newBuilder()
            .setTopologySelection(topologySelection)
            .setSettingFilter(entitySettingFilter)
            .setIncludeSettingPolicies(true).build();

        return SettingDTOUtil.flattenEntitySettings(settingPolicyService.getEntitySettings(request));
    }

    /**
     * Factory for instances of {@link TierExcluder}.
     */
    public interface TierExcluderFactory {

        /**
         * Create a new {@link TierExcluder}.
         * @param topologyInfo information about the topology
         * @return new instance of{@link TierExcluder}
         */
        @Nonnull
        TierExcluder newExcluder(TopologyInfo topologyInfo);

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
             * @return a new {@link TierExcluder}
             */
            @Nonnull
            @Override
            public TierExcluder newExcluder(@NonNull final TopologyInfo topologyInfo) {
                return new TierExcluder(topologyInfo, settingPolicyService);
            }
        }
    }
}