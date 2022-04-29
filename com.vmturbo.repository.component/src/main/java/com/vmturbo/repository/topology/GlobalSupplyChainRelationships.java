package com.vmturbo.repository.topology;


import com.google.common.collect.Multimap;


/**
 * Handler for the supply chain relationship coming from the topology processor. This object
 * contains the logic to convert into and from BaseDocument, a format that allows to query ArangoDb
 */
public class GlobalSupplyChainRelationships {
    Multimap<String, String> relationships;

    public Multimap<String, String> getRelationships() {
       return this.relationships;
    }

    public GlobalSupplyChainRelationships(final Multimap<String, String> providerRels) {
        this.relationships = providerRels;
    }
}
