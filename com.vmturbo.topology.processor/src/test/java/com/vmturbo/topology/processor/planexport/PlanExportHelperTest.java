package com.vmturbo.topology.processor.planexport;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanExportDTO;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestination;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO.NonMarketEntityType;
import com.vmturbo.platform.common.dto.PlanExport;
import com.vmturbo.platform.sdk.common.util.SDKUtil;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;

/**
 * Unit tests for {@link PlanExportHelper}.
 */
public class PlanExportHelperTest {
    private PlanExportHelper uploadPlanHelper;

    /**
     * Create the GRPC stub.
     */
    @Before
    public void before() {
        uploadPlanHelper = spy(new PlanExportHelper(mock(EntityRetriever.class)));
    }

    /**
     * Test the conversion for an {@link ActionPlan} to a {@link PlanExportDTO}.
     *
     * @throws ActionDTOConversionException actionDTO conversion exception.
     */
    @Test
    public void testBuildPlanExportDTO() throws ActionDTOConversionException {
        long planId = 1000001L;
        String name = "Migrate to cloud plan 1";
        long actionId = 1L;
        List<Action> actions = new ArrayList<>();

        actions.add(Action.newBuilder().setId(actionId).setInfo(ActionInfo.newBuilder().setMove(
                        Move.newBuilder().setTarget(ActionEntity.newBuilder().setId(777L)
                                .setType(EntityType.VIRTUAL_MACHINE_VALUE))))
                        .setExplanation(Explanation.getDefaultInstance()).setDeprecatedImportance(2.0)
            .build());

        PlanInstance plan = PlanInstance.newBuilder().setPlanId(planId).setName(name).setStatus(
            PlanStatus.SUCCEEDED).build();

        PlanExport.PlanExportDTO exportDTO = uploadPlanHelper.buildPlanExportDTO(plan, actions);
        assertEquals(planId, Long.parseLong(exportDTO.getMarketId()));
        assertEquals(name, exportDTO.getPlanName());
        assertEquals(1, exportDTO.getActionsCount());
        assertEquals(actionId, exportDTO.getActions(0).getActionOid());
        assertEquals(ActionType.MOVE, exportDTO.getActions(0).getActionType());
    }

    /**
     * Test the conversion for a given {@link PlanDestination}.
     */
    @Test
    public void testBuildPlanDestinationNonMarketEntityDTO() {
        long externalId = 1234567L;
        String displayname = "TEST";
        String localName = "LocalName";
        String path = "migrate_project_path";
        PlanDestination pd = PlanDestination.newBuilder()
                .setExternalId(String.valueOf(externalId)).setDisplayName(displayname)
                .setHasExportedData(true).putPropertyMap(localName, path)
                .build();
        NonMarketEntityDTO pdEntityDTO = uploadPlanHelper.buildPlanDestinationNonMarketEntityDTO(pd);
        assertEquals(externalId, Long.parseLong(pdEntityDTO.getId()));
        assertEquals(NonMarketEntityType.PLAN_DESTINATION, pdEntityDTO.getEntityType());
        assertEquals(displayname, pdEntityDTO.getDisplayName());
        assertEquals(1, pdEntityDTO.getEntityPropertiesCount());
        assertEquals(SDKUtil.DEFAULT_NAMESPACE, pdEntityDTO.getEntityProperties(0).getNamespace());
        assertEquals(localName, pdEntityDTO.getEntityProperties(0).getName());
        assertEquals(path, pdEntityDTO.getEntityProperties(0).getValue());
    }
}
