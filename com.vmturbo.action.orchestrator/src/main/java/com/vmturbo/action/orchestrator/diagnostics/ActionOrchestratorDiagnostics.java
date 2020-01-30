package com.vmturbo.action.orchestrator.diagnostics;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.Action.SerializationState;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.IActionStoreFactory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan.ActionPlanType;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;
import com.vmturbo.components.common.diagnostics.DiagsZipReader;

/**
 * Represents the diagnostics of the action
 * orchestrator. It only has two functions.
 */
public class ActionOrchestratorDiagnostics implements DiagsRestorable {

    private static final String SERIALIZED_FILE_NAME = "serializedActions";

    private static final Gson GSON = ComponentGsonFactory.createGson();

    private final ActionStorehouse actionStorehouse;

    private final ActionModeCalculator actionModeCalculator;

    public ActionOrchestratorDiagnostics(@Nonnull final ActionStorehouse actionStorehouse,
                                         @Nonnull final ActionModeCalculator actionModeCalculator) {
        this.actionStorehouse = Objects.requireNonNull(actionStorehouse);
        this.actionModeCalculator = Objects.requireNonNull(actionModeCalculator);
    }

    @Override
    public void collectDiags(@Nonnull DiagnosticsAppender appender) throws DiagnosticsException {
        final ActionStorehouseData storehouseData = new ActionStorehouseData(
                actionStorehouse.getAllStores().entrySet().stream()
                        .map(entry -> {
                            final Long topologyContextId = entry.getKey();
                            final ActionStore actionStore = entry.getValue();
                            return new ActionStoreData(topologyContextId, actionStore);
                        }).collect(Collectors.toList())
        );
        appender.appendString(GSON.toJson(storehouseData));
    }

    @Nonnull
    @Override
    public String getFileName() {
        return SERIALIZED_FILE_NAME;
    }

    @Override
    public void restoreDiags(@Nonnull List<String> collectedDiags) throws DiagnosticsException {
        if (collectedDiags.size() != 1) {
            throw new DiagnosticsException(
                    "Wrong number of values. Expected: 1 found: " + collectedDiags.size());
        }
        final ActionStorehouseData storehouseData;
        try {
            storehouseData =
                    GSON.fromJson(collectedDiags.get(0), ActionStorehouseData.class);
        } catch (JsonSyntaxException e) {
            throw new DiagnosticsException(
                    "Invalid JSON syntax exception while reading diags: " + e.getLocalizedMessage(),
                    e);
        }
        if (storehouseData.getStoreData() == null) {
            throw new DiagnosticsException("Unable to parse the input zip file.");
        }
        final Map<Long, ActionStore> deserializedStores = generateActionStores(storehouseData);
        actionStorehouse.restoreStorehouse(deserializedStores);
    }

    private Map<Long, ActionStore> generateActionStores(@Nonnull final ActionStorehouseData storehouseData) {
        return storehouseData.getStoreData().stream().collect(Collectors.toMap(
            ActionStoreData::getTopologyContextId,
            storeData -> {
                final IActionStoreFactory actionStoreFactory = Objects.requireNonNull(actionStorehouse.getActionStoreFactory());
                final Map<ActionPlanType, List<Action>> actions = storeData.getActions().entrySet().stream()
                    .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().stream()
                            .map(action -> new Action(action, actionModeCalculator))
                            .collect(Collectors.toList())));
                final ActionStore store = actionStorehouse
                    .getStore(storeData.getTopologyContextId())
                    .orElse(actionStoreFactory.newStore(storeData.getTopologyContextId()));

                // Note that EntitySeverityCaches are not saved, but are rebuilt from the saved actions.
                store.overwriteActions(actions);
                store.getEntitySeverityCache().refresh(store);
                return store;
            }
        ));
    }

    @Immutable
    public class ActionStorehouseData {
        public final Collection<ActionStoreData> storeData;

        public ActionStorehouseData(@Nonnull final Collection<ActionStoreData> storeData) {
            this.storeData = storeData;
        }

        public Collection<ActionStoreData> getStoreData() {
            return storeData;
        }
    }

    /**
     * A private helper class that exists solely for serialization.
     */
    @Immutable
    public class ActionStoreData {
        public final long topologyContextId;

        public final Map<ActionPlanType, List<SerializationState>> actionsByActionPlanType;

        public ActionStoreData(final long topologyContextId, @Nonnull final ActionStore actionStore) {
            this.topologyContextId = topologyContextId;
            this.actionsByActionPlanType = actionStore.getActionsByActionPlanType().entrySet().stream()
                    .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().stream()
                            .map(Action::toSerializationState).collect(Collectors.toList())));
        }

        public long getTopologyContextId() {
            return topologyContextId;
        }

        public Map<ActionPlanType, List<SerializationState>> getActions() {
            return actionsByActionPlanType;
        }
    }
}
