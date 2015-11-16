package com.vmturbo.platform.analysis.utilities;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.BuyerParticipation;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.pricefunction.PFUtility;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.topology.Topology;

/**
 * A SAX handler that loads EMF topology files and creates a Market2 topology
 */
public class EMF2MarketHandler extends DefaultHandler {

    private static final List<String> COMM_REFS =
            Arrays.asList(new String[]{"Commodities", "CommoditiesBought"});

    private static final String XSITYPE = "xsi:type";

    Logger logger = Logger.getLogger(this.getClass().getCanonicalName());

    Topology topology;
    Economy economy;

    // Map from trader type string (e.g. "Abstraction:PhysicalMachine") to trader type number
    TypeMap traderTypes = new TypeMap();
    // Map from commodity type string (class + key) to commodity specification number
    TypeMap commoditySpecs = new TypeMap();

    // Stacks are used to keep track of the parent of an xml element
    Deque<String> elementsStack;
    Deque<Attributes> attributesStack;

    // The loaded entities. In all these maps the key is the object UUID
    // Traders and commodities are kept as Attributes, which are key/value pairs
    Map<String, Attributes> traders;
    Map<String, Attributes> commodities;
    Map<String, Attributes> commoditySold2trader;
    Map<String, List<Attributes>> trader2commoditiesBought;
    // Basket is a set of commodity type strings
    Map<String, Set<String>> trader2basketSold;
    // Used to log commodities bought that consume more than one commodity sold
    Map<String, String> multipleConsumes;
    // Commodities sold which "SoldBy" reference points to the UUID of a trader not persent in the file
    List<Attributes> noSeller;
    // Commodities bought which "Consumes" reference points to the UUID of a commodity not present in the file
    List<Attributes> noConsumes;
    // First key is the types of buyer and seller that are skipped
    // Second key is their uuids. The value is how many commodities were skipped for this specific pair.
    Map<String, Set<String>> skippedBaskets;
    Set<String> skippedPairs;

    long startTime;
    long elementCount;

    public EMF2MarketHandler() {
        super();
        topology = new Topology();
        economy = topology.getEconomy();
    }

    public Topology getTopology() {
        return topology;
    }

    @Override
    public void startElement(String uri, String localName, String qName, org.xml.sax.Attributes attr) throws SAXException {
        elementCount++;
        if (elementCount % 100000 == 0) {
            logger.info(String.format("%(,d elemets, %(,d traders, %(,d commodities", elementCount, traders.size(), commodities.size()));
        }
        // attributes is an instance of AbstractSAXParser$AttributesProxy, which is always the same instance.
        // So creating a new copy.
        Attributes attributes = new Attributes(qName, attr);
        Attributes parent = attributesStack.peek();
        elementsStack.push(qName);
        attributesStack.push(attributes);
        // Ignore shadow entities
        String name = attributes.getValue("name");
        if (name != null && name.endsWith("_shadow")) return;
        // Ignore templates
        if ("true".equals(attributes.getValue("isTemplate"))) return;
        // This version only parses service entities that are contained in another object, e.g. a Market.
        // Otherwise there is no xsi:type and instead the qName is the type. These are currently skipped.
        if (parent != null && parent.getValue(XSITYPE) == null) return;
        if (COMM_REFS.contains(qName)) {
            printAttributes("Start Element :", attributes, Level.TRACE);
            trader(parent);
            commodity(attributes);
            if (qName.equals("CommoditiesBought")) {
                trader2commoditiesBought.get(uuid(parent)).add(attributes);
            } else {
                commoditySold2trader.put(uuid(attributes), parent);
                trader2basketSold.get(uuid(parent)).add(commoditySpec(attributes));
            }
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        String expected = elementsStack.pop();
        if (!expected.equals(qName)) {
            throw new SAXParseException("Expected " + expected + " but got " + qName, null);
        }
        attributesStack.pop();
        // TODO: Create the trader here. We have the basket sold.
    }

    private void printAttributes(String prefix, Attributes attributes, Level level) {
        if (!logger.isEnabledFor(level)) return;

        StringBuffer sb = new StringBuffer(prefix);
        String xsiType = attributes.get(XSITYPE);
        if (xsiType != null) {
            sb.append(XSITYPE).append("=").append(xsiType).append(" ");
        }
        attributes.keySet().stream()
            .filter(k -> !XSITYPE.equals(k))
            .sorted()
            .forEach(k -> sb.append(k).append("=").append(attributes.get(k)).append(" "));
        logger.log(level, sb);
    }

    String uuid(Attributes attr) {
        return attr.getValue("uuid");
    }

    // return the value (as double) of the first attribute in the the list of properties
    double value(Attributes attr, String...props) {
        for (String prop : props) {
            String sProp = attr.get(prop);
            if (sProp != null) {
                return Double.valueOf(sProp);
            }
        }
        return 0.0;
    }

    private void trader(Attributes trader) {
        String uuid = uuid(trader);
        if (!traders.containsKey(uuid)) {
            traders.put(uuid, trader);
            trader2commoditiesBought.put(uuid, new ArrayList<Attributes>());
            trader2basketSold.put(uuid, new HashSet<String>());
            traderTypes.insert(trader.getValue(XSITYPE));
        }
    }

    private void commodity(Attributes comm) {
        String uuid = uuid(comm);
        if (!commodities.containsKey(uuid)) {
            commodities.put(uuid, comm);
            commoditySpecs.insert(commoditySpec(comm));
        }
    }

    private String commoditySpec(Attributes commodity) {
        String commType = commodity.getValue(XSITYPE);
        String key = commodity.getValue("key");
        return commType + (key != null ? "[" + key + "]" : "");
    }

    @Override
    public void startDocument() throws SAXException {
        elementsStack = new ArrayDeque<String>();
        attributesStack = new ArrayDeque<Attributes>();
        /* Use LinkedHashMap when the order of iteration matters,
         * HashMap otherwise.
         * It matters if we want to get exactly the same results
         * when loading the same file in different runs or on
         * different machines.
         */
        traders = new LinkedHashMap<String, Attributes>();
        commodities = new HashMap<String, Attributes>();
        trader2commoditiesBought = new LinkedHashMap<String, List<Attributes>>();
        trader2basketSold = new LinkedHashMap<String, Set<String>>();
        commoditySold2trader = new LinkedHashMap<String, Attributes>();
        multipleConsumes = new HashMap<String, String>();
        noSeller = new ArrayList<Attributes>();
        noConsumes = new ArrayList<Attributes>();
        skippedBaskets = Maps.newHashMap();
        skippedPairs = Sets.newHashSet();

        startTime = System.currentTimeMillis();
        elementCount = 0;

        logger.info("Start reading file");
    }

    @Override
    public void endDocument() throws SAXException {
        logger.info("Done reading file");

        // Just for counting purposes
        Set<Basket> allBasketsBought = new HashSet<>();
        Set<Basket> allBasketsSold = new HashSet<>();

        // From basket bought to trader uuid
        Map<Basket, String> placement = Maps.newLinkedHashMap();
        Map<String, Trader> uuid2trader = Maps.newHashMap();
        Map<Basket, Trader> shopper = Maps.newHashMap();

        logger.info("Start creating traders");
        for (String traderUuid : traders.keySet()) {
            logger.trace("==========================");
            Attributes traderAttr = traders.get(traderUuid);
            printAttributes("Trader ", traderAttr, Level.DEBUG);
            Set<String> keysSold = trader2basketSold.get(traderUuid);
            Basket basketSold = keysToBasket(keysSold, commoditySpecs);
            allBasketsSold.add(basketSold);
            int traderType = traderTypes.get(traderAttr.getValue(XSITYPE));
            Trader aSeller = economy.addTrader(traderType, TraderState.ACTIVE, basketSold);
            uuid2trader.put(traderUuid, aSeller);

            // Baskets bought
            // Keys are the sellers that this buyer is buying from
            // Values are the commodities sold by this seller
            Map<Attributes, List<Attributes>> sellerAttr2commsSoldAttr = Maps.newLinkedHashMap();
            // Keys are the same as above
            // Values are the commodities that consume commodities from this seller
            Map<Attributes, List<Attributes>> sellerAttr2commsBoughtAttr = Maps.newLinkedHashMap();
            for (Attributes commBoughtAttr : trader2commoditiesBought.get(traderUuid)) {
                printAttributes("    Buys ", commBoughtAttr, Level.TRACE);
                String consumes = commBoughtAttr.getValue("Consumes");
                if (consumes == null) {
                    continue;
                }
                if (consumes.contains(" ")) {
                    multipleConsumes.put(uuid(commBoughtAttr), consumes);
                    // TODO: how do we handle this?
                    continue;
                }
                Attributes commSoldAttr = commodities.get(consumes);
                if (commSoldAttr == null) {
                    printAttributes("Cannot find commodity sold consumed by ", commBoughtAttr, Level.WARN);
                    noConsumes.add(commBoughtAttr);
                    continue;
                }
                printAttributes("        Consumes ", commSoldAttr, Level.TRACE);
                Attributes sellerAttr = commoditySold2trader.get(consumes);
                if (sellerAttr == null) {
                    logger.warn("No seller");
                    noSeller.add(commSoldAttr);
                    continue;
                }
                printAttributes("            Sold by ", sellerAttr, Level.TRACE);

                if (skip(traderAttr, sellerAttr)) {
                    printAttributes("Skipping ", traderAttr, Level.TRACE);
                    printAttributes("   buying from ", sellerAttr, Level.TRACE);
                    continue;
                }

                // if key doesn't exist then create one, otherwise return the existing value,
                // then add the entry to the list
                sellerAttr2commsSoldAttr
                    .compute(sellerAttr, (k, v) -> v == null ? new ArrayList<Attributes>() : v)
                        .add(commSoldAttr);
                sellerAttr2commsBoughtAttr
                    .compute(sellerAttr, (k, v) -> v == null ? new ArrayList<Attributes>() : v)
                        .add(commBoughtAttr);
            }
            List<Basket> basketsBoughtByTrader = new ArrayList<>();
            for (Entry<Attributes, List<Attributes>> entry : sellerAttr2commsSoldAttr.entrySet()) {
                Attributes sellerAttrs = entry.getKey();
                printAttributes("    Buys from ", sellerAttrs, Level.DEBUG);
                Set<String> keysBought = new HashSet<>();
                for (Attributes commSold : entry.getValue()) {
                    printAttributes("      - ", commSold, Level.TRACE);
                    keysBought.add(commoditySpec(commSold));
                }
                logger.debug("    Basket : " + keysBought);
                Basket basketBought = keysToBasket(keysBought, commoditySpecs);
                economy.addBasketBought(aSeller, basketBought);

                for (Attributes commBought : sellerAttr2commsBoughtAttr.get(sellerAttrs)) {
                    CommoditySpecification specification = commSpec(commoditySpecs.get(commoditySpec(commBought)));
                    BuyerParticipation participation = economy.getMarketsAsBuyer(aSeller).get(economy.getMarket(basketBought)).get(0);
                    double used = value(commBought, "used");
                    economy.getCommodityBought(participation, specification).setQuantity(used);
                }

                placement.put(basketBought, uuid(entry.getKey()));
                shopper.put(basketBought,  aSeller);

                basketsBoughtByTrader.add(basketBought);
                allBasketsBought.add(basketBought);
            }

            logger.debug("Created trader " + traderAttr.getValue(XSITYPE) + " (type " + traderType + ")");
            logger.debug("    Sells " + basketSold);
            Set<Basket> baskets_ = new HashSet<>();
            for (Basket basket : basketsBoughtByTrader) {
                boolean dup = false;
                for (Basket basket_ : baskets_) {
                    // TODO: Add equals to Basket
                    if (basket_.compareTo(basket) == 0) {
                         dup = true;
                         break;
                    }
                }
                logger.debug("    Buys " + basket + (dup ? " (duplicate)" : ""));
                baskets_.add(basket);
            }
        }

        // Set various properties of commodity sold capacities
        logger.info("Set commodity properties");
        for (Entry<String, Attributes> entry : commoditySold2trader.entrySet()) {
            Attributes commSoldAttr = commodities.get(entry.getKey());
            printAttributes("Commodity sold ",  commSoldAttr, Level.TRACE);
            Double capacity = value(commSoldAttr, "capacity", "startCapacity");
            Double used = value(commSoldAttr, "used");
            Double peakUtil = value(commSoldAttr, "peakUtilization");
            CommoditySpecification specification = commSpec(commoditySpecs.get(commoditySpec(commSoldAttr)));
            Trader trader = uuid2trader.get(uuid(entry.getValue()));
            CommoditySold commSold = trader.getCommoditySold(specification);
            if (used > capacity) {
                printAttributes("used > capacity ", commSoldAttr, Level.WARN);
                used = capacity;
            }
            if (peakUtil > 1.0) {
                printAttributes("peakUtilization > 1.0 ", commSoldAttr, Level.WARN);
                peakUtil = 1.0;
            }
            commSold.setCapacity(capacity);
            commSold.setQuantity(used);
            commSold.setPeakQuantity(peakUtil * capacity);
            PriceFunction pf = priceFunction(commSoldAttr);
            commSold.getSettings().setPriceFunction(pf);
        }

        // Assume baskets are not reused
        logger.info("Processing placement");
        for (Entry<Basket, String> entry : placement.entrySet()) {
            Basket basket = entry.getKey();
            Trader placeTrader = shopper.get(basket);
            Trader onTrader = uuid2trader.get(entry.getValue());
            economy.moveTrader(economy.getMarketsAsBuyer(placeTrader).get(economy.getMarket(basket)).get(0), onTrader);
        }

        if (logger.isTraceEnabled()) {
            verify();
        }

        // Commodities consuming more than one commodity (skipped)
        logger.log(warning(!multipleConsumes.isEmpty()), multipleConsumes.size() + " Multiple Consumes");
        if (logger.isDebugEnabled()) {
	        for (Entry<String, String> mcEntry : multipleConsumes.entrySet()) {
	            printAttributes("", commodities.get(mcEntry.getKey()), Level.WARN);
	            for (String uuid : mcEntry.getValue().split(" ")) {
	                printAttributes("    Consumes ", commodities.get(uuid), Level.WARN);
	            }
	        }
        }

        logger.log(warning(!noConsumes.isEmpty()) , noConsumes.size() + " No Consumes");
        noConsumes.forEach(a -> printAttributes("", a, Level.WARN));

        logger.log(warning(!noSeller.isEmpty()), noSeller.size() + " No seller");
        noSeller.forEach(a -> printAttributes("", a, Level.WARN));

        skippedBaskets.forEach((k, v) -> logger.info("Skipped " + v.size() + " " + k));

        logger.info(traders.size() + " traders");
        logger.info(commodities.size() + " commodities (bought and sold)");
        logger.info(allBasketsBought.size() + " baskets bought");
        logger.info(allBasketsSold.size() + " baskets sold");
        logger.info(traderTypes.size() + " trader types");
        if (logger.isTraceEnabled()) traderTypes.entrySet().stream().forEach(logger::trace);
        logger.info(commoditySpecs.size() + " commodity types");
        if (logger.isTraceEnabled()) commoditySpecs.entrySet().stream().forEach(logger::trace);
        logger.info((System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    }

    Level warning(boolean warning) {
        return warning ? Level.WARN : Level.INFO;
    }

    /**
     * print the traders and their placements
     */
    private void verify() {
        for (Trader trader : economy.getTraders()) {
            logger.trace(traderTypes.getByValue(trader.getType()) + " @" + trader.hashCode());
            logger.trace("    Sells " + trader.getBasketSold());
            if (!trader.getCommoditiesSold().isEmpty()) {
                logger.trace("       " + basketAsStrings(trader.getBasketSold()));
                logger.trace("        with capacities "
                        + trader.getCommoditiesSold()
                        .stream().map(CommoditySold::getCapacity)
                        .collect(Collectors.toList()));
                logger.trace("         and quantities "
                        + trader.getCommoditiesSold()
                        .stream().map(CommoditySold::getQuantity)
                        .collect(Collectors.toList()));
            }
            //TODO: placement
            logger.trace("    Buys from "
                    + economy.getSuppliers(trader)
                    .stream().map(Trader::getType)
                    .map(traderTypes::getByValue)
                    .collect(Collectors.toList()));
            logger.trace(economy.getMarketsAsBuyer(trader).size() + " participations ");
            // Print "P" x number of participations (e.g. PPPPP for 5 participations). Makes search easier.
            logger.trace(Strings.repeat("P", economy.getMarketsAsBuyer(trader).size()));
            for (@NonNull Entry<@NonNull Market, @NonNull BuyerParticipation> entry : economy.getMarketsAsBuyer(trader).entries()) {
                BuyerParticipation participation = entry.getValue();
                logger.trace("    -- participation @" + participation.hashCode());
                logger.trace("         basket: " + basketAsStrings(entry.getKey().getBasket()));
                logger.trace("         quantities: " + Arrays.toString(participation.getQuantities()));
                logger.trace("         peaks     : " + Arrays.toString(participation.getPeakQuantities()));
            }
        }
    }

    /**
     * Construct a Basket from a set of commodity type strings
     * @param keys commodty type strings
     * @param typeMap a mapping from commodity type string to commodity specification number
     * @return a Basket
     */
    Basket keysToBasket(Set<String> keys, TypeMap typeMap) {
        List<CommoditySpecification> list = Lists.newArrayList();
        keys.stream().mapToInt(key -> typeMap.get(key)).sorted().
            forEach(i -> list.add(commSpec(i)));
        // TODO: Reuse baskets?
        return new Basket(list);
    }

    List<String> basketAsStrings(Basket basket) {
        List<String> result = new ArrayList<>(basket.size());
        for (CommoditySpecification specification : basket) {
            result.add(commoditySpecs.getByValue(specification.getType()).toString());
        }

        return result;
    }

    CommoditySpecification commSpec(int i) {
        return new CommoditySpecification(i);
    }


    CommoditySpecification commSpec(Attributes commAttr) {
        return commSpec(commoditySpecs.get(commoditySpec(commAttr)));
    }

    private static final String PHYSICAL_MACHINE = "Abstraction:PhysicalMachine";
    private static final String VIRTUAL_MACHINE = "Abstraction:VirtualMachine";
    private static final String STORAGE = "Abstraction:Storage";
    private static final String APPLICATION = "Abstraction:Application";

    boolean skip(Attributes buyer, Attributes seller) {
        String buyerType = buyer.getValue(XSITYPE);
        String sellerType = seller.getValue(XSITYPE);
        if (
            (PHYSICAL_MACHINE.equals(buyerType) && STORAGE.equals(sellerType))
            || (APPLICATION.equals(buyerType) && VIRTUAL_MACHINE.equals(sellerType))) {
            String key = buyerType.split(":")[1] + " buying from " + sellerType.split(":")[1];
            String skippedPair = uuid(buyer)+"/"+uuid(seller);
            skippedBaskets.compute(key, (k, v) -> v == null ? Sets.newHashSet() : v).add(skippedPair);
            return true;
        }
        return false;
    }

    PriceFunction priceFunction(Attributes commodity) {
        String type = commodity.getValue(XSITYPE);
        switch(type) {
        case "Abstraction:StorageAmount":
        case "Abstraction:StorageProvisioned":
        case "Abstraction:VStorage":
            return PFUtility.createStepPriceFunction(value(commodity, "utilThreshold"), 0.0, 20000.0);
        case "Abstraction:Power":
        case "Abstraction:Cooling":
        case "Abstraction:Space":
            return PFUtility.createConstantPriceFunction(27.0);
        case "Abstraction:SegmentationCommodity":
        case "Abstraction:DrsSegmentationCommodity":
        case "Abstraction:ClusterCommodity":
        case "Abstraction:StorageClusterCommodity":
            return PFUtility.createConstantPriceFunction(0.0);
        default:
            return PFUtility.createStandardWeightedPriceFunction(1.0);
        }
    }

    /**
     * Used to allocate integer values to strings.
     */
    @SuppressWarnings("serial")
    static class TypeMap extends LinkedHashMap<Object, Integer> {
        // TODO(Shai): use BiMap
        // Not thread safe but we don't care
        int counter = 0;
        Map<Integer, Object> reverse = new LinkedHashMap<Integer, Object>();

        /**
         * If the key exists then return its type.
         * Otherwise increment the counter and allocate it to the key.
         * @param key either a new or an existing key
         * @return the integer type of the provided key.
         */
        Integer insert(Object key) {
            if (this.containsKey(key)) {
                return this.get(key);
            } else {
                put(key, ++counter);
                reverse.put(counter, key);
                return counter;
            }
        }

        Object getByValue(Integer val) {
            return reverse.get(val);
        }
    }

    class Attributes extends HashMap<String, String> {
        private static final long serialVersionUID = 1L;

        Attributes(String qName, org.xml.sax.Attributes attributes) {
            super();
            if (attributes.getValue(XSITYPE) == null) {
                // SE that is a child of the document root
                put(XSITYPE, qName);
            }
            for (int i = 0; i < attributes.getLength(); i++) {
                put(attributes.getLocalName(i), attributes.getValue(i));
            }
        }

        public String getValue(String key) {
            return get(key);
        }
    }
}
