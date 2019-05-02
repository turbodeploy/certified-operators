package com.vmturbo.repository.topology;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import javaslang.control.Option;

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.GlobalSupplyChainProviderRelsException;
import com.vmturbo.repository.graph.executor.ArangoDBExecutor;
import com.vmturbo.repository.graph.executor.GraphDBExecutor;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyEntitiesException;

/**
 * Record the provider relationships in the topology graph.
 *
 */
public class TopologyRelationshipRecorder implements Diagnosable {
    private final GraphDBExecutor executor;
    private static final Logger logger = LogManager.getLogger();
    private volatile Multimap<String, String> globalSupplyChainProviderRels;
    private final TopologyLifecycleManager topologyManager;

    /**
     * A multimap that records the "provide" relationship.
     *
     * The key is an entity type, and the value is a list of providers for that entity type.
     *
     * For example:
     * <pre>
     *     {
     *         "VM": ["PM", "ST"]
     *     }
     * </pre>
     * The above example represents virtual machine entity type has two providers, physical machine and storage.
     */
    @Nonnull
    public TopologyRelationshipRecorder(final GraphDBExecutor executor,
                                        final TopologyLifecycleManager topologyManager) {

        this.executor = Preconditions.checkNotNull(executor);
        this.topologyManager = Preconditions.checkNotNull(topologyManager);
        this.globalSupplyChainProviderRels  = HashMultimap.create();

        final Optional<TopologyID> targetTopologyId = topologyManager.getRealtimeTopologyId();

        final Option<TopologyDatabase> databaseToUse =
            Option.ofOptional(targetTopologyId.map(TopologyID::database));
        try {
            GlobalSupplyChainRelationships supplyChainRels =
                executor.getSupplyChainRels(databaseToUse.get());
            this.setGlobalSupplyChainProviderRels(supplyChainRels.getRelationships());
        } catch (Exception e) {
            String errorMsg = "Failed to retrieve relationships of the supply chain";
            logger.error(errorMsg, e);
        }
    }

    public Multimap<String, String> getGlobalSupplyChainProviderStructures() {
            return globalSupplyChainProviderRels;
    }

    public void setGlobalSupplyChainProviderRels(final Multimap<String, String> providerRels){
        this.globalSupplyChainProviderRels = providerRels;
    }

    public void createGlobalSupplyChainProviderRels(final Multimap<String, String> providerRels,
                                                 TopologyID topologyId)
        throws TopologyEntitiesException {
        this.setGlobalSupplyChainProviderRels(providerRels);
        GlobalSupplyChainRelationships supplyChainRels =
            new GlobalSupplyChainRelationships(providerRels);
        try {
            executor.insertNewDocument(supplyChainRels.convertToDocument(),
                ArangoDBExecutor.SUPPLY_CHAIN_RELS_COLLECTION,
                TopologyDatabases.getDbName(topologyId.database()));
        } catch (GlobalSupplyChainProviderRelsException e) {
            throw new TopologyEntitiesException(e);
        }
    }

    @Nonnull
    @Override
    public List<String> collectDiags() throws DiagnosticsException {
        return Collections.singletonList(ComponentGsonFactory.createGsonNoPrettyPrint()
            .toJson(globalSupplyChainProviderRels.asMap()));
    }

    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags) throws DiagnosticsException {
        Map<String, Collection<String>> map =
            ComponentGsonFactory.createGsonNoPrettyPrint().fromJson(collectedDiags.get(0),
                new TypeToken<Map<String, Collection<String>>>(){}.getType());
        Multimap<String, String> multimap = HashMultimap.create();
        map.forEach(multimap::putAll);
        setGlobalSupplyChainProviderRels(multimap);
    }
}
