package com.vmturbo.action.orchestrator.diagnostics;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;

import io.prometheus.client.CollectorRegistry;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.Action.SerializationState;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.IActionFactory;
import com.vmturbo.action.orchestrator.store.IActionStoreFactory;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.DiagnosticsWriter;
import com.vmturbo.components.common.InvalidRestoreInputException;

/**
 * Represents the diagnostics of the action
 * orchestrator. It only has two functions.
 */
public class ActionOrchestratorDiagnostics {

    @VisibleForTesting
    public static final String SERIALIZED_FILE_NAME = "serializedActions.json";

    private static final int DESERIALIZATION_BUFFER_SIZE = 1024;

    private static final Gson GSON = ComponentGsonFactory.createGson();

    private final Logger logger = LogManager.getLogger();

    private final ActionStorehouse actionStorehouse;

    private final IActionFactory actionFactory;

    private final DiagnosticsWriter diagnosticsWriter;

    public ActionOrchestratorDiagnostics(@Nonnull final ActionStorehouse actionStorehouse,
                                         @Nonnull final IActionFactory actionFactory,
                                         @Nonnull final DiagnosticsWriter diagnosticsWriter) {
        this.actionStorehouse = Objects.requireNonNull(actionStorehouse);
        this.actionFactory = Objects.requireNonNull(actionFactory);
        this.diagnosticsWriter = Objects.requireNonNull(diagnosticsWriter);
    }

    public void dump(@Nonnull final ZipOutputStream zipOutputStream) {
        final ActionStorehouseData storehouseData = new ActionStorehouseData(
            actionStorehouse.getAllStores().entrySet().stream()
                .map(entry -> {
                    final Long topologyContextId = entry.getKey();
                    final ActionStore actionStore = entry.getValue();

                    return new ActionStoreData(topologyContextId, actionStore);
                }).collect(Collectors.toList())
        );

        diagnosticsWriter.writeZipEntry(SERIALIZED_FILE_NAME,
                Collections.singletonList(GSON.toJson(storehouseData)),
                zipOutputStream);

        diagnosticsWriter.writePrometheusMetrics(CollectorRegistry.defaultRegistry, zipOutputStream);
    }

    public void restore(@Nonnull final ZipInputStream zipInputStream)
            throws InvalidRestoreInputException {
        try {
            final ZipEntry entry = zipInputStream.getNextEntry();
            if (entry == null) {
                throw new InvalidRestoreInputException("The input is either not a zip file, or is empty.");
            } else if (!entry.getName().equals(SERIALIZED_FILE_NAME)) {
                throw new InvalidRestoreInputException("The input zip file does not contain the expected JSON file.");
            }

            final ActionStorehouseData storehouseData = readActionStorehouseData(zipInputStream);
            if (storehouseData == null || storehouseData.getStoreData() == null) {
                throw new InvalidRestoreInputException("Unable to parse the input zip file.");
            }

            final Map<Long, ActionStore> deserializedStores = generateActionStores(storehouseData);
            actionStorehouse.restoreStorehouse(deserializedStores);
        } catch (IOException e) {
            throw new InvalidRestoreInputException("Failed to read from the zip input stream!", e);
        } catch (JsonParseException e) {
            throw new InvalidRestoreInputException("Failed to parse the JSON file in the archive", e);
        }
    }

    private Map<Long, ActionStore> generateActionStores(@Nonnull final ActionStorehouseData storehouseData) {
        return storehouseData.getStoreData().stream().collect(Collectors.toMap(
            ActionStoreData::getTopologyContextId,
            storeData -> {
                final IActionStoreFactory actionStoreFactory = Objects.requireNonNull(actionStorehouse.getActionStoreFactory());
                final List<Action> actions = storeData.getActions().stream()
                    .map(Action::new)
                    .collect(Collectors.toList());
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

    private ActionStorehouseData readActionStorehouseData(@Nonnull final ZipInputStream zipInputStream)
        throws IOException {

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final byte[] buffer = new byte[DESERIALIZATION_BUFFER_SIZE];
        int len = 0;
        while ((len = zipInputStream.read(buffer)) > 0) {
            outputStream.write(buffer, 0, len);
        }
        final String serializedStr = outputStream.toString();
        return GSON.fromJson(serializedStr, ActionStorehouseData.class);
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

        public final List<SerializationState> actions;

        public ActionStoreData(final long topologyContextId, @Nonnull final ActionStore actionStore) {
            this.topologyContextId = topologyContextId;
            this.actions = actionStore.getActions().values()
                .stream()
                .map(Action::toSerializationState)
                .collect(Collectors.toList());
        }

        public long getTopologyContextId() {
            return topologyContextId;
        }

        public List<SerializationState> getActions() {
            return actions;
        }
    }
}
