package com.vmturbo.repository.topology;

import com.vmturbo.repository.topology.TopologyID.TopologyType;

/**
 * Factory class to create {@link TopologyID}.
 */
public class TopologyIDFactory {

    /**
     * ArangoDB namespace prefix to be used as prefix of database names.
     */
    private final String arangoDBNamespacePrefix;

    /**
     * Create a TopologyIDFactory object.
     *
     * @param arangoDBNamespacePrefix ArangoDB namespace prefix to be prepended to database names,
     *                                e.g. "turbonomic-".
     */
    public TopologyIDFactory(String arangoDBNamespacePrefix) {
        this.arangoDBNamespacePrefix = arangoDBNamespacePrefix;
    }

    /**
     * Create {@link TopologyID} from given context ID, topology ID and topology type.
     *
     * @param contextId  For a plan it is the plan ID, for real time topology it is a number that
     *                   uniquely identifies the source topology processor.
     * @param topologyId Topology ID.
     * @param type       Topology type, either SOURCE or PROJECTED.
     * @return {@link TopologyID}.
     */
    public TopologyID createTopologyID(final long contextId, final long topologyId, final TopologyType type) {
        return new TopologyID(contextId, topologyId, type, arangoDBNamespacePrefix);
    }
}
