package com.vmturbo.platform.analysis.utilities;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import java.util.Map.Entry;

// TODO: take out the prefixes.
/**
 * A (non thread safe) biclique builder. Used to compute a biclique cover of a bipartite graph.
 * A bipartite graph is a graph with two types of nodes: TYPE1 and TYPE2.
 * A biclique cover is a set of complete bipartite subgraphs (bicliques) that cover all the edges in the underlying bipartite graph.
 * A complete bipartite graph (a biclique) is a bipartite graph where each TYPE1 node is connected to every TYPE2 node.
 */
public class BiCliquer {

    private boolean computed = false;

    private String type1Prefix;
    private String type2Prefix;

    // A map representing the underlying bipartite graph.
    // edges.get(nid) is a list of all TYPE2 nodes that are connected to the TYPE1 node nid
    // in the underlying graph.
    private final Map<String, Set<String>> edges = new TreeMap<>();
    // The key and value in each entry in this map are two sets of nodes (TYPE1 and TYPE2) that form one biclique
    private final Map<Set<String>, Set<String>> bicliques = new LinkedHashMap<>();
    // map(nid1, nid2) is the biclique number of the biclique that contains the edge between nodes nid1 and nid2
    private final Map<String, Map<String, Integer>> nids2bcNumber = new HashMap<>();
    // Map from node id to the set of all biclique keys associated with the node
    private final Map<String, Set<String>> nid2bcKeys = new HashMap<>();
    private final Map<String, Set<Integer>> nid2bcNumbers = new HashMap<>();

    /**
     * Constructs a new {@code BiCliquer} with biclique-key prefixes "T1-" and "T2-".
     */
    public BiCliquer() {
        this("BC-T1-", "BC-T2-");
    }

    /**
     * Constructs a new {@code BiCliquer} with biclique-key prefixes {@code prefix1} and {@code prefix2}.
     */
    public BiCliquer(String prefix1, String prefix2) {
        super();
        type1Prefix = prefix1;
        type2Prefix = prefix2;
    }

    /**
     * Compute the biclique cover based on the edges added so far.
     * Lock {@code this} instance from accepting new edges.
     * @throws IllegalStateException when invoked more than once.
     */
    public void compute() {
        if (computed) {
            throw new IllegalStateException("Bicliques already computed");
        }
        computed = true;

        Map<Set<String>, Set<String>> bicliques_ = new LinkedHashMap<>();
        // The bicliques are computed in this one line
        edges.forEach((node1, setNode2s) -> bicliques_.compute(setNode2s, (key, val) -> val == null ? new TreeSet<>() : val).add(node1));

        bicliques_.forEach((k, v) -> bicliques.put(Collections.unmodifiableSet(k), Collections.unmodifiableSet(v)));

        // The rest of this method is helper maps
        int cliqueNum = 0;
        for (Entry<Set<String>, Set<String>> clique : bicliques.entrySet()) {
            for (String nid2 : clique.getValue()) {
                nid2bcKeys.compute(nid2, (key, val) -> val == null ? new HashSet<>() : val).add(type1Prefix + cliqueNum);
                nid2bcNumbers.compute(nid2, (key, val) -> val == null ? new HashSet<>() : val).add(cliqueNum);
            }
            for (String nid1 : clique.getKey()) {
                nid2bcKeys.compute(nid1, (key, val) -> val == null ? new HashSet<>() : val).add(type2Prefix + cliqueNum);
                nid2bcNumbers.compute(nid1, (key, val) -> val == null ? new HashSet<>() : val).add(cliqueNum);
                nids2bcNumber.putIfAbsent(nid1, new HashMap<>());
                for (String nid2 : clique.getValue()) {
                    nids2bcNumber.get(nid1).putIfAbsent(nid2, cliqueNum);
                }
            }
            cliqueNum++;
        }
    }

    /**
     * Add an edge between nodes {@code nid1} and {@code nid2} in the underlying graph.
     * @param nid1 ID of TYPE1 node
     * @param nid2 ID of TYPE2 node
     * @throws IllegalStateException when invoked after the biclqiues were computed
     * @see #compute
     */
    public void edge(String nid1, String nid2) {
        if (computed) {
            throw new IllegalStateException("Bicliques already computed");
        }
        edges.compute(nid1, (node, set) -> set == null ? new LinkedHashSet<>() : set).add(nid2);
    }

    /**
     * Get the biclque keys that a node is associated with.
     * @param nid ID of a node (either TYPE1 or TYPE2)
     * @return an unmodifiable set of biclique keys
     * @throws IllegalStateException when invoked before the biclqiues were computed
     */
    public Set<String> getBcKeys(String nid) {
        if (!computed) {
            throw new IllegalStateException("Bicliques not computed yet");
        }
        Set<String> bcKeys = nid2bcKeys.get(nid);
        return bcKeys == null ? null : Collections.unmodifiableSet(bcKeys);
    }

    /**
     * The key of the biclique that covers the edge between the nodes {@code nid1} and {@code nid2}.
     * The key is a prefix prepended by the biclique ID for the two nodes. The prefix is either type1
     * prefix or type2 prefix, depending on whether the first argument is a type1 or type2 node.
     * @param nid1 ID of one node (either TYPE1 or TYPE2)
     * @param nid2 ID of the other node (either TYPE2 or TYPE1)
     * @return a biclique key, which is the correct prefix prepended by biclique ID
     * (only expected if the two nodes are not connected in the underlying graph)..
     * @throws IllegalStateException when invoked before the biclqiues were computed
     * @see #getBcID(String, String)
     */
    public String getBcKey(String nid1, String nid2) {
        Integer bcNumber = -1;
        String bcPrefix = null;
        Map<String, Integer> map = nids2bcNumber.get(nid1);
        if (map != null) {
            bcNumber = map.get(nid2);
            bcPrefix = type2Prefix;
        } else {
            map = nids2bcNumber.get(nid2);
            if (map != null) {
                bcNumber = map.get(nid1);
                bcPrefix = type1Prefix;
            }
        }
        return bcNumber == null || bcNumber == -1 ? null : bcPrefix + bcNumber;
    }

    /**
     * Get the biclique IDs that the node is associated with.
     * A node is associated with a biclique if the biclique covers an edge
     * that is connected to the node.
     * @param nid ID of a node
     * @return an unmodifiable set of biclique numbers
     * @throws IllegalStateException when invoked before the biclqiues were computed
     */
    public Set<Integer> getBcIDs(String nid) {
        if (!computed) {
            throw new IllegalStateException("Bicliques not computed yet");
        }
        Set<Integer> bcNumbers = nid2bcNumbers.get(nid);
        return bcNumbers == null ? null : Collections.unmodifiableSet(bcNumbers);
    }

    /**
     * The identifier of the biclique that covers the edge between the nodes {@code nid1} and {@code nid2}.
     * @param nid1 ID of one node (either TYPE1 or TYPE2)
     * @param nid2 ID of the other node (either TYPE2 or TYPE1)
     * @return a biclique number, or -1 if there is no biclique that covers the edge between the two nodes
     * (only expected if the two nodes are not connected in the underlying graph)..
     * @throws IllegalStateException when invoked before the biclqiues were computed
     */
    public int getBcID(String nid1, String nid2) {
        if (!computed) {
            throw new IllegalStateException("Bicliques not computed yet");
        }
        Integer bcNumber = -1;
        Map<String, Integer> map = nids2bcNumber.get(nid1);
        if (map != null) {
            bcNumber = map.get(nid2);
        } else {
            map = nids2bcNumber.get(nid2);
            if (map != null) {
                bcNumber = map.get(nid1);
            }
        }
        return bcNumber != null ? bcNumber : -1;
    }

    /**
     * @return the number of bicliques
     * @throws IllegalStateException when invoked before the biclqiues were computed
     */
    public int size() {
        if (!computed) {
            throw new IllegalStateException("Bicliques not computed yet");
        }
        return bicliques.size();
    }

    /**
     * @return an unmodifiable view of the bicliques
     * @throws IllegalStateException when invoked before the biclqiues were computed
     */
    public Map<Set<String>, Set<String>> getBicliques() {
        if (!computed) {
            throw new IllegalStateException("Bicliques not computed yet");
        }
        return Collections.unmodifiableMap(bicliques);
    }
}
