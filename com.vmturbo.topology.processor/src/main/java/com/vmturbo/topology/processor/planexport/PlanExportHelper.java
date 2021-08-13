package com.vmturbo.topology.processor.planexport;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestination;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO.NonMarketEntityType;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO.PlanDestinationData;
import com.vmturbo.platform.common.dto.PlanExport.PlanExportDTO;
import com.vmturbo.platform.sdk.common.util.SDKUtil;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;

/**
 * The class to build {@link com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO} and
 * the {@link PlanExportDTO} used in plan upload request.
 */
public class PlanExportHelper {

    private static final Logger logger = LogManager.getLogger();

    private EntityRetriever entityRetriever;

    /**
     * Constructor.
     *
     * @param entityRetriever the object retrieves and converts an entity.
     */
    public PlanExportHelper(@Nonnull EntityRetriever entityRetriever) {
        this.entityRetriever = entityRetriever;
    }

    /**
     * Construct the {@link PlanExportDTO} with a list of {@link ActionExecutionDTO}s.
     *
     * @param plan the plan instance to export
     * @param actions a list of market actions generated for the plan.
     * @return {@link PlanExportDTO}.
     * @throws ActionDTOConversionException exception when converting the {@link ActionExecutionDTO}.
     */
    public PlanExportDTO buildPlanExportDTO(@Nonnull PlanInstance plan, List<Action> actions)
        throws ActionDTOConversionException {
        PlanExportDTO.Builder builder = PlanExportDTO.newBuilder()
                .setMarketId(String.valueOf(plan.getPlanId()))
                .setPlanName(plan.getName());

        for (Action action : actions) {
            convertToActionExecutionDTO(action).ifPresent(builder::addActions);
        }

        return builder.build();
    }

    /**
     * Convert an {@link Action} to an {@link ActionExecutionDTO}.
     *
     * @param action the given {@link Action}.
     * @return {@link ActionExecutionDTO} the actionExecutionDTO converted from actionDTO, or
     * an empty optional if the action type is not supported.
     * @throws ActionDTOConversionException the exception occurs during actionDTO conversion.
     */
    private Optional<ActionExecutionDTO> convertToActionExecutionDTO(Action action) throws
            ActionDTOConversionException {
        // NOTE: currently only move conversion is supported, once other action conversions are
        // implemented, it can be changed to a switch case block.
        if (action.getInfo().getActionTypeCase() == ActionTypeCase.MOVE) {
            ExecutionDTOConverter converter = new MoveExecutionDTOConverter(entityRetriever);
            ActionExecutionDTO executionDTO = converter.convert(action);
            return Optional.of(executionDTO);
        }
        return Optional.empty();
    }

    /**
     * Construct the {@link NonMarketEntityDTO} to represent the {@link PlanDestination}.
     *
     * @param planDestination the given {@link PlanDestination}.
     * @return {@link NonMarketEntityDTO}.
     */
    public NonMarketEntityDTO buildPlanDestinationNonMarketEntityDTO(PlanDestination planDestination) {
        NonMarketEntityDTO.Builder pdDTO = NonMarketEntityDTO.newBuilder().setEntityType(
                NonMarketEntityType.PLAN_DESTINATION).setId(planDestination.getExternalId())
                .setDisplayName(planDestination.getDisplayName());
        for (Map.Entry<String, String> e :  planDestination.getPropertyMapMap().entrySet()) {
            // Note: DiscoveredPlanDestinationUploader populates only the default namespace in entity
            // property map.
            pdDTO.addEntityProperties(EntityProperty.newBuilder().setNamespace(SDKUtil.DEFAULT_NAMESPACE)
                    .setName(e.getKey()).setValue(e.getValue()).build());
        }
        if (planDestination.hasHasExportedData()) {
            pdDTO.setPlanDestinationData(PlanDestinationData.newBuilder()
                    .setHasExportedData(planDestination.getHasExportedData())).build();
        }
        return pdDTO.build();
    }
}
