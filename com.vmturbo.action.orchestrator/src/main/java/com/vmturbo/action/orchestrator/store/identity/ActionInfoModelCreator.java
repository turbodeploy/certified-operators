package com.vmturbo.action.orchestrator.store.identity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.annotation.Nonnull;

import com.google.gson.Gson;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.components.api.ComponentGsonFactory;

/**
 * A function to convert {@link ActionInfo} to the corresponding immutable model object {@link
 * ActionInfoModel}.
 */
public class ActionInfoModelCreator implements Function<ActionInfo, ActionInfoModel> {

    private static final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();
    private static final Map<ActionTypeCase, Function<ActionInfo, ActionInfoModel>>
            fieldsCalculators;

    static {
        final Map<ActionTypeCase, Function<ActionInfo, ActionInfoModel>> map =
                new EnumMap<>(ActionTypeCase.class);
        map.put(ActionTypeCase.MOVE, ActionInfoModelCreator::getMove);
        map.put(ActionTypeCase.RECONFIGURE, ActionInfoModelCreator::getReconfigure);
        map.put(ActionTypeCase.PROVISION, ActionInfoModelCreator::getProvision);
        map.put(ActionTypeCase.ACTIVATE, ActionInfoModelCreator::getActivate);
        map.put(ActionTypeCase.DEACTIVATE, ActionInfoModelCreator::getDeactivate);
        map.put(ActionTypeCase.RESIZE, ActionInfoModelCreator::getResize);
        map.put(ActionTypeCase.DELETE, ActionInfoModelCreator::getDelete);
        fieldsCalculators = Collections.unmodifiableMap(map);
    }

    /**
     * Converts {@link ActionInfo} into an immutable model - {@link ActionInfoModel}.
     *
     * @param action action to convert
     * @return model used to distinguish between instances of {@link ActionInfo}
     */
    @Nonnull
    public ActionInfoModel apply(@Nonnull ActionInfo action) {
        final Function<ActionInfo, ActionInfoModel> extractor =
                fieldsCalculators.get(action.getActionTypeCase());
        if (extractor == null) {
            throw new IllegalArgumentException(String.format(
                    "Could not find a suitable field extractor for action type \"%s\": %s",
                    action.getActionTypeCase(), action));
        }
        return extractor.apply(action);
    }

    @Nonnull
    private static ActionInfoModel getMove(@Nonnull ActionInfo action) {
        final Move move = action.getMove();
        final List<MoveChange> changes = new ArrayList<>(move.getChangesCount());
        for (ChangeProvider changeProvider : move.getChangesList()) {
            final MoveChange change = new MoveChange(changeProvider.getSource().getId(),
                    changeProvider.getDestination().getId(),
                    changeProvider.hasResource() ? changeProvider.getResource().getId() : null);
            changes.add(change);
        }
        final String changesString = gson.toJson(changes);
        return new ActionInfoModel(ActionTypeCase.MOVE, move.getTarget().getId(), changesString);
    }

    @Nonnull
    private static ActionInfoModel getReconfigure(@Nonnull ActionInfo action) {
        final Reconfigure reconfigure = action.getReconfigure();
        final String changesString =
                reconfigure.hasSource() ? Long.toString(reconfigure.getSource().getId()) : null;
        return new ActionInfoModel(ActionTypeCase.RECONFIGURE, reconfigure.getTarget().getId(),
                changesString);
    }

    @Nonnull
    private static ActionInfoModel getProvision(@Nonnull ActionInfo action) {
        final Provision provision = action.getProvision();
        return new ActionInfoModel(ActionTypeCase.PROVISION, provision.getEntityToClone().getId(),
                Long.toString(provision.getProvisionedSeller()));
    }

    @Nonnull
    private static ActionInfoModel getResize(@Nonnull ActionInfo action) {
        final Resize resize = action.getResize();
        final ResizeModel model =
                new ResizeModel(resize.getCommodityType(), resize.getCommodityAttribute(),
                        resize.getOldCapacity(), resize.getNewCapacity(),
                        resize.getHotAddSupported(), resize.getHotRemoveSupported());
        final String modelString = gson.toJson(model);
        return new ActionInfoModel(ActionTypeCase.RESIZE, resize.getTarget().getId(), modelString);
    }

    @Nonnull
    private static ActionInfoModel getActivate(@Nonnull ActionInfo action) {
        final Activate activate = action.getActivate();
        return new ActionInfoModel(ActionTypeCase.ACTIVATE, activate.getTarget().getId(), null);
    }

    @Nonnull
    private static ActionInfoModel getDeactivate(@Nonnull ActionInfo action) {
        final Deactivate deactivate = action.getDeactivate();
        return new ActionInfoModel(ActionTypeCase.DEACTIVATE, deactivate.getTarget().getId(), null);
    }

    @Nonnull
    private static ActionInfoModel getDelete(@Nonnull ActionInfo action) {
        final Delete delete = action.getDelete();
        return new ActionInfoModel(ActionTypeCase.DELETE, delete.getTarget().getId(), null);
    }

    /**
     * JSON schema to store a single move change.
     */
    private static class MoveChange {
        private final long sourceId;
        private final long destinationId;
        private final Long resourceId;

        MoveChange(long sourceId, long destinationId, Long resourceId) {
            this.sourceId = sourceId;
            this.destinationId = destinationId;
            this.resourceId = resourceId;
        }
    }

    /**
     * JSON schema to store resize actions model.
     */
    private static class ResizeModel {
        private final CommodityType commodityType;
        private final CommodityAttribute commodityAttribute;
        private final float oldCapacity;
        private final float newCapacity;
        private final boolean hotAddSupported;
        private final boolean hotRemoveSupported;

        ResizeModel(CommodityType commodityType, CommodityAttribute commodityAttribute,
                float oldCapacity, float newCapacity, boolean hotAddSupported,
                boolean hotRemoveSupported) {
            this.commodityType = commodityType;
            this.commodityAttribute = commodityAttribute;
            this.oldCapacity = oldCapacity;
            this.newCapacity = newCapacity;
            this.hotAddSupported = hotAddSupported;
            this.hotRemoveSupported = hotRemoveSupported;
        }
    }
}
