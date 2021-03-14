package com.vmturbo.action.orchestrator.action;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.AtomicActionSpec;

/**
 * Cache to store the {@link AtomicActionSpec} messages received from the topology processor.
 * {@code AtomicActionSpecsRpcService} receives the message with the specs.
 */
public class AtomicActionSpecsCache {
    private static final Logger logger = LogManager.getLogger();

    // Map of atomic action specs by action type and entity OID
    private static volatile Map<ActionType, Map<Long, AtomicActionSpec>> atomicActionSpecsStores = new HashMap<>();

    /**
     * Update atomic action spec stores with the received list of {@link AtomicActionSpec}.
     *
     * @param atomicActionSpecsMap  map of list of atomic action specs by action type
     */
    public void updateAtomicActionSpecsInfo(
            @Nonnull final Map<ActionType, List<AtomicActionSpec>> atomicActionSpecsMap) {

        for (ActionType actionType : atomicActionSpecsMap.keySet()) {
            logger.info("received {} atomic action specs for {}",
                        atomicActionSpecsMap.get(actionType).size(), actionType);
        }

        // The action specs are organized by action type and entity oid
        Map<ActionType, Map<Long, AtomicActionSpec>> newSpecsStores = new HashMap<>();
        atomicActionSpecsMap.entrySet().stream()
                .forEach( e -> {
                    Map<Long, AtomicActionSpec> specsByOid
                            = newSpecsStores.computeIfAbsent(e.getKey(), v -> new  HashMap<>());
                    for (AtomicActionSpec actionMergeInfo : e.getValue()) {
                          for (Long entityId : actionMergeInfo.getEntityIdsList()) {
                              specsByOid.put(entityId, actionMergeInfo);
                          }
                    }
                });

        // Replace the specs store map
        atomicActionSpecsStores = newSpecsStores;
    }

    /**
     * Check if there are any atomic action specs.
     *
     * @return true if there are atomic action specs, false otherwise
     */
    public boolean isEmpty() {
        return atomicActionSpecsStores.isEmpty();
    }

    /**
     * Return a copy of the atomic action specs for an action type.
     *
     * @param actionType  action type
     * @return map containing the entity oid and the action merge spec
     */
    public Map<Long, AtomicActionSpec> getAtomicActionsSpec(ActionType actionType) {
        return atomicActionSpecsStores.get(actionType);
    }
}
