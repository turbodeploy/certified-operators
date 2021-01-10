package com.vmturbo.action.orchestrator.store.identity;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.Allocate;
import com.vmturbo.common.protobuf.action.ActionDTO.AtomicResize;
import com.vmturbo.common.protobuf.action.ActionDTO.BuyRI;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.components.api.ComponentGsonFactory;

/**
 * A function to convert {@link ActionInfo} to the corresponding immutable model object {@link
 * ActionInfoModel}.
 */
public class ActionInfoModelCreator implements Function<ActionInfo, ActionInfoModel> {

    private static Logger logger = LogManager.getLogger();
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
        map.put(ActionTypeCase.BUYRI, ActionInfoModelCreator::getBuyRi);
        map.put(ActionTypeCase.ALLOCATE, ActionInfoModelCreator::getAllocate);
        map.put(ActionTypeCase.SCALE, ActionInfoModelCreator::getScale);
        map.put(ActionTypeCase.ATOMICRESIZE, ActionInfoModelCreator::getAtomicResize);
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
        final Set<String> changes = createChangesJson(move.getChangesList());
        return new ActionInfoModel(ActionTypeCase.MOVE, move.getTarget().getId(), null, changes);
    }

    @Nonnull
    private static ActionInfoModel getReconfigure(@Nonnull ActionInfo action) {
        final Reconfigure reconfigure = action.getReconfigure();
        final String changesString = String.valueOf(reconfigure.getIsProvider())
            + (reconfigure.hasSource() ? Long.toString(reconfigure.getSource().getId()) : null)
            + (reconfigure.hasIsAddition() ? String.valueOf(reconfigure.getIsAddition()) : null);
        return new ActionInfoModel(ActionTypeCase.RECONFIGURE, reconfigure.getTarget().getId(),
                changesString, null);
    }

    @Nonnull
    private static ActionInfoModel getProvision(@Nonnull ActionInfo action) {
        final Provision provision = action.getProvision();
        final ProvisionModel model = new ProvisionModel(
            provision.getEntityToClone().getId(),
            provision.hasProvisionIndex() ? provision.getProvisionIndex() : null);
        return new ActionInfoModel(ActionTypeCase.PROVISION, provision.getEntityToClone().getId(),
                gson.toJson(model), null);
    }

    @Nonnull
    private static ActionInfoModel getResize(@Nonnull ActionInfo action) {
        final Resize resize = action.getResize();
        final ResizeModel model =
                new ResizeModel(resize.getCommodityType(), resize.getCommodityAttribute(),
                        resize.getOldCapacity(), resize.getNewCapacity(),
                        resize.getHotAddSupported(), resize.getHotRemoveSupported());
        final String modelString = gson.toJson(model);
        return new ActionInfoModel(ActionTypeCase.RESIZE, resize.getTarget().getId(), modelString,
                null);
    }

    @Nonnull
    private static ActionInfoModel getAtomicResize(@Nonnull ActionInfo action) {
        final AtomicResize atomicResize = action.getAtomicResize();

        // When creating the changeString, it is important that we sort the resize changes
        // because the order of the actions being merged doesn't matter, just their contents.
        // Sorting will ensure that the same ActionInfoModel is generated even when order
        // of the resize actions is different during different market recommendation plans
        final List<ResizeChangeByTarget> resizeChangeByTarget
                = atomicResize.getResizesList()
                        .stream()
                        .collect(Collectors.groupingBy(resize -> resize.getTarget().getId()))
                        .entrySet()
                        .stream()
                        .sorted(Map.Entry.comparingByKey())
                        .map(entry -> new ResizeChangeByTarget(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toList());

        AtomicResizeChange resizeChange = new AtomicResizeChange(resizeChangeByTarget);

        final String changesString = gson.toJson(resizeChange);

        return new ActionInfoModel(ActionTypeCase.ATOMICRESIZE,
                                    atomicResize.getExecutionTarget().getId(),
                                    changesString, null);
    }

    /**
     * Model for atomic resize action details per de-duplication target.
     * The mode is of the form : {"id": "OID1[commodity list],OID2[commodity list]"}
     *
     * <p>Example:
     * {"id":"73617269445249[26, 53, 100, 101],73617269445251[26, 53, 100, 101]"}
     *
     */
    @VisibleForTesting
    static class ResizeChangeByTarget {
        private final long sourceId;
        private final List<Integer> commTypes;

        ResizeChangeByTarget(long sourceId, List<ResizeInfo> resizeChange) {
            // Sorting the commodity type will ensure that the same ActionInfoModel is generated
            // even when order of the resize actions is different during different market recommendation plans
            this.commTypes = resizeChange.stream()
                                .map(resize -> resize.getCommodityType().getType())
                                .sorted()
                                .collect(Collectors.toList());
            this.sourceId = sourceId;
        }

        public String toString() {
            return "" + sourceId + commTypes;
        }
    }

    /**
     * Model for all the atomic resize action details.
     */
    @VisibleForTesting
    static class AtomicResizeChange {

        private final String id;

        AtomicResizeChange(List<ResizeChangeByTarget> resizeChangeByTarget) {

            List<String> changeStringList = resizeChangeByTarget.stream()
                    .map(change -> change.toString())
                    .collect(Collectors.toList());
            String allChangeString = Strings.join(changeStringList, ',');
            this.id = allChangeString;
        }
    }

    @Nonnull
    private static ActionInfoModel getActivate(@Nonnull ActionInfo action) {
        final Activate activate = action.getActivate();
        return new ActionInfoModel(ActionTypeCase.ACTIVATE, activate.getTarget().getId(), null,
                null);
    }

    @Nonnull
    private static ActionInfoModel getDeactivate(@Nonnull ActionInfo action) {
        final Deactivate deactivate = action.getDeactivate();
        return new ActionInfoModel(ActionTypeCase.DEACTIVATE, deactivate.getTarget().getId(), null,
                null);
    }

    @Nonnull
    private static ActionInfoModel getDelete(@Nonnull ActionInfo action) {
        final Delete delete = action.getDelete();
        final DeleteModel model = new DeleteModel(
                delete.getTarget().getId(),
                delete.getFilePath());

        return new ActionInfoModel(ActionTypeCase.DELETE, delete.getTarget().getId(),
                gson.toJson(model), null);
    }

    @Nonnull
    private static ActionInfoModel getBuyRi(@Nonnull ActionInfo action) {
        final BuyRI buyRi = action.getBuyRi();
        final BuyRiModel model = new BuyRiModel(
                buyRi.hasComputeTier() ? buyRi.getComputeTier().getId() : null,
                buyRi.hasMasterAccount() ? buyRi.getMasterAccount().getId() : null,
                buyRi.hasRegion() ? buyRi.getRegion().getId() : null);
        return new ActionInfoModel(ActionTypeCase.BUYRI, buyRi.getBuyRiId(), gson.toJson(model),
                null);
    }

    @Nonnull
    private static ActionInfoModel getAllocate(@Nonnull ActionInfo action) {
        final Allocate allocate = action.getAllocate();
        return new ActionInfoModel(ActionTypeCase.ALLOCATE, allocate.getTarget().getId(),
                allocate.hasWorkloadTier()
                        ? Long.toString(allocate.getWorkloadTier().getId())
                        : null, null);
    }

    @Nonnull
    private static ActionInfoModel getScale(@Nonnull ActionInfo action) {
        final Scale scale = action.getScale();
        final Set<String> changes = createChangesJson(scale.getChangesList());
        return new ActionInfoModel(ActionTypeCase.SCALE, scale.getTarget().getId(), null, changes);
    }

    /**
     * JSON schema to store a single move change.
     *
     * @param providers change providers to serialize to a string.
     * @return JSON serialized string
     */
    @Nonnull
    private static Set<String> createChangesJson(@Nonnull List<ChangeProvider> providers) {
        final Set<String> changes = new HashSet<>(providers.size());
        for (ChangeProvider changeProvider : providers) {
            final MoveChange change = new MoveChange(changeProvider.getSource().getId(),
                    changeProvider.getDestination().getId(),
                    changeProvider.hasResource() ? changeProvider.getResource().getId() : null);
            changes.add(gson.toJson(change));
        }
        return changes;
    }

    /**
     * Corrects positive floating point value to the values suitable for JSON.
     *
     * @param src positive floating point value to convert
     * @return fixed float value suitable for JSON
     */
    private static float fixPositiveValue(final float src) {
        if (Float.isNaN(src)) {
            return -1;
        } else if (Float.isInfinite(src)) {
            return -2;
        } else {
            return src;
        }
    }

    /**
     * Move change object. Used for JSON serialization. It is just a composition of fields.
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
            this.oldCapacity = fixPositiveValue(oldCapacity);
            this.newCapacity = fixPositiveValue(newCapacity);
            this.hotAddSupported = hotAddSupported;
            this.hotRemoveSupported = hotRemoveSupported;
        }
    }

    /**
     * Model for BuyRi action details.
     */
    private static class BuyRiModel {
        private final Long computeTier;
        private final Long masterAccount;
        private final Long region;

        BuyRiModel(@Nullable Long computeTier, @Nullable Long masterAccount,
                @Nullable Long region) {
            this.computeTier = computeTier;
            this.masterAccount = masterAccount;
            this.region = region;
        }
    }

    /**
     * Model for Provision action details.
     */
    private static class ProvisionModel {
        private final long sourceEntityId;
        private final Integer provisionIndex;


        private ProvisionModel(long sourceEntityId, Integer provisionIndex) {
            this.sourceEntityId = sourceEntityId;
            this.provisionIndex = provisionIndex;
        }
    }

    /**
     * Model for Delete action details.
     */
    private static class DeleteModel {
        private final long targetEntityId;
        private final String filePath;

        private DeleteModel(long targetEntityId, String filePath) {
            this.targetEntityId = targetEntityId;
            this.filePath = filePath;
        }
    }

}
