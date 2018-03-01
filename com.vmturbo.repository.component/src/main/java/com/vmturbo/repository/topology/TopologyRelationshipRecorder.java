package com.vmturbo.repository.topology;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.Diagnosable;

/**
 * Record the provider relationships in the topology graph.
 *
 */
public class TopologyRelationshipRecorder implements Diagnosable {
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
    private volatile Multimap<String, String> globalSupplyChainProviderRels;

    public TopologyRelationshipRecorder() {
        this.globalSupplyChainProviderRels = HashMultimap.create();
    }

    public Multimap<String, String> getGlobalSupplyChainProviderStructures() {
        return globalSupplyChainProviderRels;
    }

    public void setGlobalSupplyChainProviderRels(final Multimap<String, String> providerRels) {
        this.globalSupplyChainProviderRels = providerRels;
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
