package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.vmturbo.api.dto.action.RelatedActionApiDTO;
import com.vmturbo.api.enums.ActionRelationType;
import com.vmturbo.api.enums.ActionType;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.AtomicResize;
import com.vmturbo.common.protobuf.action.ActionDTO.BlockedByRelation;
import com.vmturbo.common.protobuf.action.ActionDTO.BlockedByRelation.BlockedByResize;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.AtomicResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.RelatedAction;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test {@link RelatedActionMapper}.
 */
public class RelatedActionMapperTest {

    private static final long NAMESPACE_ID = 11L;
    private static final long NS_RESIZE_ACTION_ID = 111L;
    private static final String NS_RESIZE_DESCRIPTION1 = "Resize up VCPU Limit Quota for Namespace quotatest from 100 mCores to 110 mCores";
    private static final String NS_RESIZE_DESCRIPTION2 = "Resize up VMEM Limit Quota for Namespace quotatest from 100 MB to 228 MB";
    private static final String NAMESPACE_NAME = "ns_name";

    /**
     * Test {@link RelatedActionMapper#mapXlRelatedActionsToApi}.
     */
    @Test
    public void testMapXlRelatedActionsToApi() {
        final ActionSpec actionSpec = ActionSpec.newBuilder()
                .setRecommendation(getWCResizeAction())
                .addRelatedActions(getBlockedByNSResizeRelatedAction(NS_RESIZE_ACTION_ID,
                        NS_RESIZE_DESCRIPTION1, NAMESPACE_ID, NAMESPACE_NAME,
                        CommodityDTO.CommodityType.VCPU_LIMIT_QUOTA_VALUE))
                .build();

        List<RelatedActionApiDTO> relatedActionApiDTOs = RelatedActionMapper.mapXlRelatedActionsToApi(actionSpec);
        assertEquals(1, relatedActionApiDTOs.size());
        RelatedActionApiDTO relatedActionApiDTO = relatedActionApiDTOs.get(0);
        assertEquals(ActionRelationType.BLOCKED_BY, relatedActionApiDTO.getActionRelationType());
        assertNotNull(relatedActionApiDTO.getAction());
        assertEquals(NS_RESIZE_ACTION_ID, (long)relatedActionApiDTO.getAction().getActionID());
        assertEquals(ActionType.RESIZE, relatedActionApiDTO.getAction().getActionType());
        assertEquals(NS_RESIZE_DESCRIPTION1, relatedActionApiDTO.getAction().getDetails());
        assertEquals(ApiEntityType.NAMESPACE.apiStr(), relatedActionApiDTO.getAction().getTarget().getClassName());
        assertEquals(String.valueOf(NAMESPACE_ID), relatedActionApiDTO.getAction().getTarget().getUuid());
        assertEquals(NAMESPACE_NAME, relatedActionApiDTO.getAction().getTarget().getDisplayName());
        assertEquals(EnvironmentType.HYBRID, relatedActionApiDTO.getAction().getTarget().getEnvironmentType());
    }

    /**
     * Test {@link RelatedActionMapper#countRelatedActionsByType}.
     */
    @Test
    public void testCountRelatedActionsByType() {
        final ActionSpec actionSpec = ActionSpec.newBuilder()
                .setRecommendation(getWCResizeAction())
                .addRelatedActions(getBlockedByNSResizeRelatedAction(NS_RESIZE_ACTION_ID,
                        NS_RESIZE_DESCRIPTION1, NAMESPACE_ID, NAMESPACE_NAME,
                        CommodityDTO.CommodityType.VCPU_LIMIT_QUOTA_VALUE))
                .addRelatedActions(getBlockedByNSResizeRelatedAction(NS_RESIZE_ACTION_ID,
                        NS_RESIZE_DESCRIPTION2, NAMESPACE_ID, NAMESPACE_NAME,
                        CommodityDTO.CommodityType.VMEM_LIMIT_QUOTA_VALUE))
                .build();
        Map<ActionRelationType, Integer> countMap = RelatedActionMapper.countRelatedActionsByType(actionSpec);
        assertEquals(2, (int)countMap.get(ActionRelationType.BLOCKED_BY));
    }

    private Action getWCResizeAction() {
        Explanation explanation = Explanation.newBuilder()
                .setAtomicResize(AtomicResizeExplanation.newBuilder()
                        .setMergeGroupId("container-group"))
                .build();
        return buildAction(ActionInfo.newBuilder()
                        .setAtomicResize(AtomicResize.newBuilder()
                                .setExecutionTarget(ActionEntity.newBuilder()
                                        .setId(11L)
                                        .setType(EntityType.WORKLOAD_CONTROLLER_VALUE))).build(),
                explanation);
    }

    private RelatedAction getBlockedByNSResizeRelatedAction(final long relatedActionId, final String actionDescription,
                                                            final long relatedActionEntityId, final String relatedActionEntityName,
                                                            final int commodityType) {
        return RelatedAction.newBuilder()
                .setRecommendationId(relatedActionId)
                .setDescription(actionDescription)
                .setActionEntity(ActionEntity.newBuilder()
                        .setId(relatedActionEntityId)
                        .setType(EntityType.NAMESPACE_VALUE)
                        .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.HYBRID))
                .setActionEntityDisplayName(relatedActionEntityName)
                .setBlockedByRelation(BlockedByRelation.newBuilder()
                        .setResize(BlockedByResize.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(commodityType))))
                .build();
    }

    private Action buildAction(ActionInfo actionInfo, Explanation explanation) {
        return Action.newBuilder()
                .setDeprecatedImportance(0)
                .setId(1234)
                .setInfo(actionInfo)
                .setExplanation(explanation)
                .setDisruptive(false)
                .setReversible(true)
                .build();
    }
}