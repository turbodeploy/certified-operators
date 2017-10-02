package com.vmturbo.history.topology;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.history.utils.TopologyOrganizer;

/**
 * Keep an in-memory copy of a topology during processing, keyed by TopologyContextId.
 *
 * <p>Processing a topology may span a long time-scale, as the priceIndex information, derived by the Market,
 * must be stored as part of the history of the TopologyContextId from which the priceIndex information is derived.
 *
 * <p>We keep the entire topology, not just the ID and snapshot_time, as the priceIndex processing requires the Entity Type
 * for each ServiceEntity in the Topology be available - the Entity Type determines which _stats_latest table the
 * priceIndex information should be written.
 *
 * <p>The two pieces of information, topology and priceIndex, may arrive in any order. If the topology arrives first, it
 * must be saved (in topologySnapshotMap) until the priceIndex has been received and processed.
 * At that time, the saved copy of the topology may be released.
 * <p>Topology arrives before PriceIndex:
 * <ol>
 *     <li>receive topology, call registerTopologySnapshot()
 *     <li>check pendingPriceIndexLists for this topologyContextId; since not found:
 *     <ul>
 *          <li>save topology in topologySnapshotMap
 *     </ul>
 *     <p>some time later...
 *     <li>receive priceIndexList, call registerPriceIndexInfo()
 *     <li>check topologySnapshotMap for the given context id; since found
 *     <ul>
 *         <li>remove topology from topologySnapshotMap
 *         <li>process priceIndexlist using the saved topologySnapshot
 *     </ul>
 * </ol>
 *
 * <p>On the other hand, if the priceIndex arrives first, it must be saved (in pendingPriceIndexLists) without processing,
 * until the topology has been received and processed.
 * <p>PriceIndex arrives first, then Topology:
 * <ol>
 *     <li>receive priceIndex list, call registerPriceIndexInfo()
 *     <li>check topologySnapshotMap for the given context id; since not found
 *     <ul>
 *         <li>add this priceIndex list to the pendingPriceIndexLists map
 *     </ul>
 *     <p>some time later...
 *     <li>receive topology, call registerTopologySnapshot()
 *     <li>check pendingPriceIndexLists for this topologyContextId; since found:
 *     <ul>
 *         <li>remove priceIndexList from pendingPriceIndexLists
 *         <li>process priceIndexlist using the newly received topologySnapshot
 *     </ul>
 * </ol>
 *
 * <p>Note that topologies are saved based on the topologyContextId. In other words, successive topologies for the same
 * topologyContextId will replace earlier saved copies, if any. For the live topology, there will only ever be one
 * saved topology. For planning topologies, the history component will need to be notified when a plan has been dropped
 * so any saved topologies or priceIndex lists may be cleared.
 * TODO: History component must be notified when a plan context is dropped so that the cache may be cleaned up. OM-12795
 *
 * <p>Note that the priceIndex for any given topologyId (toplogy-id-2) may NEVER be received; the Market will discard
 * incoming topologies for the same context received while still processing the previous topology (topology-id-1).
 * The sequence looks like:
 * <ol>
 *     <li>History receives topology-id-1; info in topology-id-1 is persisted to RDB; topology-id-1 is saved in
 *     topologySnapshotMap
 *     <li>History receives topology-id-2; info in topology-id-2 is persisted to RDB;
 *     since the topologyContextId is the same, topology-id-2 replaces topology-id-1 in the topologySnapshotMap
 *     <li>History receives priceIndex-for-topology-id-1; priceIndex-for-topology-id-1 is processed with
 *     the data saved from topology-id-2
 * </ol>
 * We may need to store ALL topologies, in order, and discard topologies < n when priceIndex-n is received.
 **/
public class TopologySnapshotRegistry {

    private final Logger logger = LogManager.getLogger();

    // map from topologyContextId to TopologySnapshot for that context
    private final Map<Long, TopologyOrganizer> topologySnapshotMap = new HashMap<>();
    // map from topologyContextId to a closure which will process a saved priceIndexList given a topology snapshot
    private final Map<Long, Consumer<TopologyOrganizer>> pendingPriceIndexLists = new HashMap<>();
    // set of topologyId's that are "invalid"; priceIndex info for these should be discarded
    private final Set<Long> invalidTopologyIds = new HashSet<>();

    /**
     * Record a new TopologySnapshot represented by a utility {@link TopologyOrganizer}, and indexed by the
     * topologyContextId. The current time is recorded as the snapshot_time.
     *
     * Note that new topologies for the same context will overwrite previous topologies.
     *
     * @param topologyContextId the context ID to which this topology belongs, e.g. real time vs a plan context
     * @param snapshotInfo a {@link TopologyOrganizer} representing this topology
     */
    public void registerTopologySnapshot(long topologyContextId, @Nonnull TopologyOrganizer snapshotInfo) {
        synchronized (this) {
            if (invalidTopologyIds.contains(snapshotInfo.getTopologyId())) {
                logger.warn("Inconsistent state - registering previously failed topology " +
                        "snapshot for {} {}; failure marker ignored.",
                        topologyContextId, snapshotInfo.getTopologyId());
                invalidTopologyIds.remove(snapshotInfo.getTopologyId());
            }
            Consumer<TopologyOrganizer> priceIndexHandler = pendingPriceIndexLists.get(topologyContextId);
            if (priceIndexHandler == null) {
                if (topologySnapshotMap.containsKey(topologyContextId)) {
                    logger.warn("replacing topo context {}, prior topology {}, new topology {}:",
                            topologyContextId,
                            topologySnapshotMap.get(topologyContextId).getTopologyId(),
                            snapshotInfo.getTopologyId());
                }
                // no priceIndex handler yet received; save this topologySnapshot
                topologySnapshotMap.put(topologyContextId, snapshotInfo);
            } else {
                // priceIndex handler already received; remove it from the list and call it
                pendingPriceIndexLists.remove(topologyContextId);
                priceIndexHandler.accept(snapshotInfo);
            }
        }
    }

    /**
     * Record a handler for a priceIndex list. If the corresponding topology has been already processed,
     * then dispatch the handler immediately. If the corresponding topology is not available,
     * then record the handler for later.
     *  @param topologyContextId the topology context ID for this priceIndex information
     * @param topologyId
     * @param priceIndexHandler a handler to be called when the topology is available; the handler closure
     */
    public void registerPriceIndexInfo(long topologyContextId, long topologyId,
                                       @Nonnull Consumer<TopologyOrganizer> priceIndexHandler) {
        final TopologyOrganizer topologyOrganizer;
        synchronized (this) {
            if (invalidTopologyIds.contains(topologyId)) {
                // the corresponding topology failed, we cannot persist this priceIndex info
                logger.warn("Discarding priceIndex info for invalid topologyContext Id: {}" +
                        " topologyId {}", topologyContextId, topologyId);
                invalidTopologyIds.remove(topologyId);
                return;
            }
            topologyOrganizer = topologySnapshotMap.get(topologyContextId);
            if (topologyOrganizer != null) {
                topologySnapshotMap.remove(topologyContextId);
            } else {
                logger.info("PriceIndexInfo pending topology context ID: {}, topology id: {}",
                        topologyContextId, topologyId);
                if (pendingPriceIndexLists.containsKey(topologyContextId)) {
                    logger.warn("replacing existing priceIndex info, toplogy {} ",
                            topologyId);
                }
                pendingPriceIndexLists.put(topologyContextId, priceIndexHandler);
            }
        }
        if (topologyOrganizer != null) {
            priceIndexHandler.accept(topologyOrganizer);
        }
    }

    /**
     * Record that an invalid topology has been received and should not be processed further.
     * This will typically occur if there's a transmission error, e.g.
     *   CommunicationException | TimeoutException | InterruptedException
     * receiving the topology chunks. In that case service entities will be missing and so the
     * topology cannot be processed.
     *
     * @param topologyContextId the contextId of the invalid topology
     * @param topologyId the unique topologyId of the invalid topology
     */
    public void registerInvalidTopology(long topologyContextId, long topologyId) {
        synchronized (this) {
            if (pendingPriceIndexLists.containsKey(topologyContextId)) {
                // the priceIndex info was waiting for this topology - must be discarded
                logger.info("Discarded pending priceIndex info, topologyContextId {} topologyId {}",
                        topologyContextId, topologyId);
                pendingPriceIndexLists.remove(topologyContextId);
            } else {
                logger.info("TopologyContextId {} topologyId {} marked invalid.",
                        topologyContextId, topologyId);
                invalidTopologyIds.add(topologyId);
            }
        }
    }

}
