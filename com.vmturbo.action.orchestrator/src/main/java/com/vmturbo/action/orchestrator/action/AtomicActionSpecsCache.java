package com.vmturbo.action.orchestrator.action;

import java.util.Collections;
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
 */
public class AtomicActionSpecsCache {
    private static final Logger logger = LogManager.getLogger();

    // Map of atomic action specs by action type and entity OID
    private static final Map<ActionType, Map<Long, AtomicActionSpec>> atomicActionSpecsStores = new HashMap<>();

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

        atomicActionSpecsStores.clear();
        atomicActionSpecsMap.entrySet().stream()
                .forEach( e -> {
                    Map<Long, AtomicActionSpec> specsByOid
                            = atomicActionSpecsStores.computeIfAbsent(e.getKey(), v -> new  HashMap<>());
                    for (AtomicActionSpec actionMergeInfo : e.getValue()) {
                          for (Long entityId : actionMergeInfo.getEntityIdsList()) {
                              specsByOid.put(entityId, actionMergeInfo);
                          }
                    }
                });
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
     * Check if there is an atomic action merge spec for an entity.
     *
     * @param actionType action type
     * @param entityOid entity OID
     *
     * @return  true if atomic action spec is found
     */
    public boolean hasAtomicActionSpec(ActionType actionType, Long entityOid) {
        return atomicActionSpecsStores.getOrDefault(actionType, Collections.emptyMap())
                .containsKey(entityOid);
    }

    /**
     * Return the atomic resize action spec for an entity.
     * @param entityOid entity oid
     * @return  resize action merge spec
     */
    public AtomicActionSpec getAtomicResizeSpec(Long entityOid) {
        return atomicActionSpecsStores.get(ActionType.RESIZE).get(entityOid);
    }
}
