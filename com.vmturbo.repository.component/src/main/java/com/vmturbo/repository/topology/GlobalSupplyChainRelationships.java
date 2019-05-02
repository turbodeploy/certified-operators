package com.vmturbo.repository.topology;


import java.util.List;
import java.util.Map.Entry;

import com.arangodb.entity.BaseDocument;
import com.google.common.collect.HashMultimap;
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

    /**
     * Convert into a BaseDocument for ArangoDB
     */
    public BaseDocument convertToDocument() {
        BaseDocument providerRelsDocument = new BaseDocument();
        for (String key : this.relationships.keySet()) {
            providerRelsDocument.addAttribute(key, this.relationships.get(key));
        }
        return providerRelsDocument;
    }

    public GlobalSupplyChainRelationships(List<BaseDocument> results) {
        this.relationships = HashMultimap.create();
        for (BaseDocument result: results) {
            for (Entry<String, Object> e : result.getProperties().entrySet()) {
                relationships.putAll(e.getKey(), (List<String>) e.getValue());
            }
        }

    }
    public GlobalSupplyChainRelationships(final Multimap<String, String> providerRels) {
        this.relationships = providerRels;
    }
}
