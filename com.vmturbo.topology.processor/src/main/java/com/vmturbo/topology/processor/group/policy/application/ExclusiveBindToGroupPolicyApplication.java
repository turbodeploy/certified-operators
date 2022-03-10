package com.vmturbo.topology.processor.group.policy.application;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.PolicyDetailCase;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderView;
import com.vmturbo.commons.analysis.InvertedIndex;
import com.vmturbo.platform.common.builders.SDKConstants;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.topology.TopologyInvertedIndexFactory;

/**
 * Applies a collection of Exclusive BindToGroup policy.
 */
public class ExclusiveBindToGroupPolicyApplication extends BindToGroupPolicyApplication {

    private static final String NON_CONTAINING_CONSUMERS_KEY_POSTFIX = "_out";

    protected ExclusiveBindToGroupPolicyApplication(GroupResolver groupResolver,
            TopologyGraph<TopologyEntity> topologyGraph,
            TopologyInvertedIndexFactory invertedIndexFactory) {
        super(groupResolver, topologyGraph, invertedIndexFactory);
    }

    @Override
    protected void applyExclusivePolicy(
            InvertedIndex<TopologyEntity, CommoditiesBoughtFromProviderView> invertedIndex,
            BindToGroupPolicy policy, PolicyDetailCase policyDetailCase,
            ApiEntityType consumerEntityType, Set<Long> providers, Set<Long> consumers,
            int providerType) throws PolicyApplicationException {
        final Set<Long> nonContainingConsumers = topologyGraph.entities().filter(
                e -> e.getEntityType() == consumerEntityType.typeNumber()).map(
                TopologyEntity::getOid).filter(oid -> !consumers.contains(oid)).collect(
                Collectors.toSet());
        final CommoditySoldView commoditySoldDTO = new CommoditySoldImpl().setCommodityType(
                new CommodityTypeImpl()
                        .setType(CommodityDTO.CommodityType.SEGMENTATION.getNumber())
                        .setKey(policy.getPolicyDefinition().getId()
                                + NON_CONTAINING_CONSUMERS_KEY_POSTFIX))
                .setCapacity(SDKConstants.ACCESS_COMMODITY_CAPACITY);
        addCommoditySoldToComplementaryProviders(nonContainingConsumers, providers, providerType,
                invertedIndex, commoditySoldDTO);
        final CommodityBoughtView commodityBoughtDTO =
                new CommodityBoughtImpl().setCommodityType(new CommodityTypeImpl()
                        .setType(CommodityDTO.CommodityType.SEGMENTATION.getNumber())
                        .setKey(policy.getPolicyDefinition().getId()
                                + NON_CONTAINING_CONSUMERS_KEY_POSTFIX))
                        .setUsed(SEGM_BOUGHT_USED_VALUE);
        addCommodityBought(nonContainingConsumers, providerType, commodityBoughtDTO);
    }

    @Override
    protected Map<PlacementPolicy, PolicyApplicationException> applyInternal(
            List<BindToGroupPolicy> policies) {
        // Build up an inverted index for the provider types targetted by these policies.
        // We use this inverted index to find which providers we can put segmentation commodities
        // onto (to avoid putting segmentation commodities on ALL providers outside the target
        // provider group).
        final Set<ApiEntityType> providerTypes = policies.stream().flatMap(
                policy -> GroupProtoUtil.getEntityTypes(
                        policy.getProviderPolicyEntities().getGroup()).stream()).collect(
                Collectors.toSet());
        final InvertedIndex<TopologyEntity, CommoditiesBoughtFromProviderView> invertedIndex =
                invertedIndexFactory.typeInvertedIndex(topologyGraph, providerTypes,
                        TopologyInvertedIndexFactory.DEFAULT_MINIMAL_SCAN_STOP_THRESHOLD);
        final Map<PlacementPolicy, PolicyApplicationException> errors = new HashMap<>();
        policies.forEach(policy -> applyPlacePolicy(errors, invertedIndex, policy));
        return errors;
    }
}
