package com.vmturbo.platform.analysis.utilities;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.vmturbo.platform.analysis.topology.Topology;

public class M2Utils {
    public static void main(String... args) {
        loadFile(args[0]);
    }

    /**
     * Load the specified file. The file is an EMF repository file saved by an Operations Manager.
     * This method uses a static {@link Logger} which name is the {@link EMF2MarketHandler} class name.
     * @param fileName the name of the file to load
     * @return a {@link TopologyMapping} that contains a market2 representation of the entities in the file
     */
    public static TopologyMapping loadFile(String fileName) {
        return loadFile(fileName, Logger.getLogger(EMF2MarketHandler.class));
    }

    /**
     * Load the specified file. The file is an EMF repository file saved by an Operations Manager.
     * @param fileName the name of the file to load
     * @param logger the {@link Logger} to be used when parsing the file by the {@link EMF2MarketHandler}
     * SAX handler.
     * @return a {@link TopologyMapping} that contains a market2 representation of the entities in the file
     */
    public static TopologyMapping loadFile(String fileName, Logger logger) {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        try {
            SAXParser parser = factory.newSAXParser();
            EMF2MarketHandler handler = new EMF2MarketHandler(logger);
            parser.parse(fileName, handler);
            return handler.getTopologyMapping();
        } catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Mapping of a trader identity/name to/from an index. The trader identity has
     * to be unique. It could be, for example, the trader uuid.
     * The index is assigned by the {@link Topology topology}. It is the index of the trader in
     * the list of traders in the {@link Economy}. The identity is assigned by the
     * client (e.g. mediation) when creating the {@link Trader} instance. It is the client's
     * responsibility to populate the mapping and to guarantee uniqueness.
     */
    public static class TopologyMapping {
        private BiMap<Integer, String> trader_identity;
        private Topology topology;

        TopologyMapping(Topology topo) {
            super();
            topology = topo;
            trader_identity = HashBiMap.create();
        }

        /**
         * @return the {@Topology} that the traders in the mapping belong to.
         */
        public Topology getTopology() {
            return topology;
        }

        /**
         * @param traderIndex the index of the {@link Trader} in the list of traders in the {@link Economy}
         * @return the identity/name of the {@link Trader} with the given index
         */
        public String getTraderName(int traderIndex) {
            return trader_identity.get(traderIndex);
        }

        /**
         * Allows reverse lookup of trader index by name
         * @param traderName the identity/name of the {@link Trader}
         * @return the index of the {@Trader} with the given name
         */
        public int getTraderIndex(String traderName) {
            return trader_identity.inverse().get(traderName);
        }

        /**
         * Add a mapping between trader name and trader index.
         * @param traderId the index of the {@link Trader trader}, assigned by the topology.
         * @param traderName the unique name of the trader
         */
        void addTraderMapping(int traderId, String traderName) {
            trader_identity.put(traderId,  traderName);
        }
    }
}
