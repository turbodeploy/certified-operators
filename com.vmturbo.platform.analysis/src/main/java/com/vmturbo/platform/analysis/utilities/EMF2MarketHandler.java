package com.vmturbo.platform.analysis.utilities;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.BuyerParticipation;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.topology.Topology;

/**
 * A SAX handler that loads EMF topology files and creates a Market2 topology
 */
public class EMF2MarketHandler extends DefaultHandler {

    private static final List<String> COMM_REFS =
            Arrays.asList(new String[]{"Commodities", "CommoditiesBought"});

    private static final String XSITYPE = "xsi:type";

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

    long startTime;

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
            printAttributes("Start Element :", attributes);
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

    private void printAttributes(String prefix, Attributes attributes) {
        System.out.print(prefix);
        String xsiType = attributes.get(XSITYPE);
        if (xsiType != null) {
            System.out.print(XSITYPE + "=" + xsiType + " ");
        }
        attributes.keySet().stream()
            .filter(k -> !XSITYPE.equals(k))
            .sorted()
            .forEach(k -> System.out.print(k + "=" + attributes.get(k) + " "));
        System.out.println();
    }

    String uuid(Attributes attr) {
        return attr.getValue("uuid");
    }

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
        traders = new HashMap<String, Attributes>();
        commodities = new HashMap<String, Attributes>();
        trader2commoditiesBought = new HashMap<String, List<Attributes>>();
        trader2basketSold = new HashMap<String, Set<String>>();
        commoditySold2trader = new HashMap<String, Attributes>();
        multipleConsumes = new HashMap<String, String>();
        startTime = System.currentTimeMillis();

        System.out.println("Start reading file");
    }

    @Override
    public void endDocument() throws SAXException {
        System.out.println("Done reading file");

        // Just for counting purposes
        Set<Basket> allBasketsBought = new HashSet<Basket>();
        Set<Basket> allBasketsSold = new HashSet<Basket>();

        // From basket bought to trader uuid
        Map<Basket, String> placement = Maps.newHashMap();
        Map<String, Trader> uuid2trader = Maps.newHashMap();
        Map<Basket, Trader> shopper = Maps.newHashMap();

        for (String traderUuid : traders.keySet()) {
            System.out.println("==========================");
            Attributes traderAttr = traders.get(traderUuid);
            printAttributes("Trader ", traderAttr);
            Set<String> keysSold = trader2basketSold.get(traderUuid);
            Basket basketSold = keysToBasket(keysSold, commoditySpecs);
            allBasketsSold.add(basketSold);
            int traderType = traderTypes.get(traderAttr.getValue(XSITYPE));
            Trader aSeller = economy.addTrader(traderType, TraderState.ACTIVE, basketSold);
            uuid2trader.put(traderUuid, aSeller);

            // Baskets bought
            // Keys are the sellers that this buyer is buying from
            // Values are the commodities sold by this sells
            Map<Attributes, List<Attributes>> sellerAttr2commsSoldAttr = Maps.newHashMap();
            // Keys are the same as above
            // Values are the commodities that consume commodities from this seller
            Map<Attributes, List<Attributes>> sellerAttr2commsBoughtAttr = Maps.newHashMap();
            for (Attributes commBoughtAttr : trader2commoditiesBought.get(traderUuid)) {
                printAttributes("    Buys ", commBoughtAttr);
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
                printAttributes("        Consumes ", commSoldAttr);
                Attributes seller = commoditySold2trader.get(consumes);
                if (seller == null) throw new IllegalArgumentException(consumes);
                printAttributes("            Sold by ", seller);
                if (!sellerAttr2commsSoldAttr.containsKey(seller)) {
                    sellerAttr2commsSoldAttr.put(seller, new ArrayList<Attributes>());
                }
                sellerAttr2commsSoldAttr.get(seller).add(commSoldAttr);
                if (!sellerAttr2commsBoughtAttr.containsKey(seller)) {
                    sellerAttr2commsBoughtAttr.put(seller, new ArrayList<Attributes>());
                }
                sellerAttr2commsBoughtAttr.get(seller).add(commBoughtAttr);
            }
            System.out.println("Trader Summary @" + aSeller.hashCode());
            List<Basket> basketsBoughtByTrader = new ArrayList<Basket>();
            for (Entry<Attributes, List<Attributes>> entry : sellerAttr2commsSoldAttr.entrySet()) {
                Attributes sellerAttrs = entry.getKey();
                printAttributes("    Buys from ", sellerAttrs);
                Set<String> keysBought = new HashSet<String>();
                for (Attributes commSold : entry.getValue()) {
                    printAttributes("      - ", commSold);
                    keysBought.add(commoditySpec(commSold));
                }
                System.out.println("    Basket : " + keysBought);
                Basket basketBought = keysToBasket(keysBought, commoditySpecs);
                economy.addBasketBought(aSeller, basketBought);

                for (Attributes commBought : sellerAttr2commsBoughtAttr.get(sellerAttrs)) {
                    double used = value(commBought, "used");
                    CommoditySpecification specification = commSpec(commoditySpecs.get(commoditySpec(commBought)));
                    BuyerParticipation participation = economy.getMarketsAsBuyer(aSeller).get(economy.getMarket(basketBought)).get(0);
                    economy.getCommodityBought(participation, specification).setQuantity(used);
                }

                placement.put(basketBought, uuid(entry.getKey()));
                shopper.put(basketBought,  aSeller);

                basketsBoughtByTrader.add(basketBought);
                allBasketsBought.add(basketBought);
            }

            System.out.println("Created trader " + traderAttr.getValue(XSITYPE) + " (type " + traderType + ")");
            System.out.println("    Sells " + basketSold);
            Set<Basket> baskets_ = new HashSet<Basket>();
            for (Basket basket : basketsBoughtByTrader) {
                boolean dup = false;
                for (Basket basket_ : baskets_) {
                    // TODO: Add equals to Basket
                    if (basket_.compareTo(basket) == 0) {
                         dup = true;
                         break;
                    }
                }
                System.out.println("    Buys " + basket + (dup ? " (duplicate)" : ""));
                baskets_.add(basket);
            }
        }

        // Set capacities
        for (Entry<String, Attributes> entry : commoditySold2trader.entrySet()) {
            Attributes commSoldAttr = commodities.get(entry.getKey());
            printAttributes("Commodity sold ",  commSoldAttr);
            Double capacity = value(commSoldAttr, "capacity", "startCapacity");
            Double used = value(commSoldAttr, "used");
            CommoditySpecification specification = commSpec(commoditySpecs.get(commoditySpec(commSoldAttr)));
            Trader trader = uuid2trader.get(uuid(entry.getValue()));
            CommoditySold commSold = trader.getCommoditySold(specification);
            if (used > capacity) {
                System.out.println("used > cxapacity");
                used = capacity;
            }
            commSold.setCapacity(capacity);
            commSold.setQuantity(used);
        }

        // Assume baskets are not reused
        System.out.println("Processing placement");
        for (Entry<Basket, String> foo : placement.entrySet()) {
            Basket basket = foo.getKey();
            Trader placeTrader = shopper.get(basket);
            Trader onTrader = uuid2trader.get(foo.getValue());
            economy.moveTrader(economy.getMarketsAsBuyer(placeTrader).get(economy.getMarket(basket)).get(0), onTrader);
        }

        // Commodities consuming more than one commodity (skipped)
        System.out.println("Multiple Consumes");
        for (Entry<String, String> mcEntry : multipleConsumes.entrySet()) {
            printAttributes("", commodities.get(mcEntry.getKey()));
            for (String uuid : mcEntry.getValue().split(" ")) {
                printAttributes("    Consumes ", commodities.get(uuid));
            }
        }

        System.out.println(traders.size() + " traders");
        System.out.println(commodities.size() + " commodities (bought and sold)");
        System.out.println(allBasketsBought.size() + " baskets bought");
        System.out.println(allBasketsSold.size() + " baskets sold");
        System.out.println(traderTypes.size() + " trader types : " + traderTypes);
        System.out.println(commoditySpecs.size() + " commodity types : " + commoditySpecs);
        System.out.println((System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        verify();
    }

    /**
     * print the traders and their placements
     */
    private void verify() {
        for (Trader trader : economy.getTraders()) {
            System.out.println(traderTypes.getByValue(trader.getType()) + " @" + trader.hashCode());
            System.out.println("    Sells " + trader.getBasketSold());
            if (!trader.getCommoditiesSold().isEmpty()) {
                System.out.println("       " + basketAsStrings(trader.getBasketSold()));
                System.out.println("        with capacities "
                        + trader.getCommoditiesSold()
                        .stream().map(c -> c.getCapacity())
                        .collect(Collectors.toList()));
                System.out.println("         and quantities "
                        + trader.getCommoditiesSold()
                        .stream().map(c -> c.getQuantity())
                        .collect(Collectors.toList()));
            }
            //TODO: placement
            System.out.println("    Buys from "
                    + economy.getSuppliers(trader)
                    .stream().map(t -> t.getType())
                    .map(k -> traderTypes.getByValue(k))
                    .collect(Collectors.toList()));
            for (@NonNull Entry<@NonNull Market, @NonNull BuyerParticipation> entry : economy.getMarketsAsBuyer(trader).entries()) {
                BuyerParticipation participation = entry.getValue();
                System.out.println("    -- participation @" + participation.hashCode());
                System.out.println("         basket: " + basketAsStrings(entry.getKey().getBasket()));
                System.out.println("         quantities: " + Arrays.toString(participation.getQuantities()));
                System.out.println("         peaks     : " + Arrays.toString(participation.getPeakQuantities()));
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
        return basket.getCommoditySpecifications()
                .stream().map(cs -> cs.getType())
                .map(k -> commoditySpecs.getByValue((int)k))
                .map(o -> o.toString())
                .collect(Collectors.toList());
    }

    CommoditySpecification commSpec(int i) {
        return new CommoditySpecification((short)i);
    }


    CommoditySpecification commSpec(Attributes commAttr) {
        return commSpec(commoditySpecs.get(commoditySpec(commAttr)));
    }


    /**
     * Used to allocate integer values to strings.
     */
    @SuppressWarnings("serial")
    class TypeMap extends HashMap<Object, Integer> {
        // Not thread safe but we don't care
        int counter = 0;
        Map<Integer, Object> reverse = new HashMap<Integer, Object>();

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
