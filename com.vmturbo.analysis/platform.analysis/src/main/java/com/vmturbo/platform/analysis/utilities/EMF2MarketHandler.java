package com.vmturbo.platform.analysis.utilities;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.helpers.DefaultHandler;

import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommodityBought;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderSettings;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.pricefunction.PriceFunctionFactory;
import com.vmturbo.platform.analysis.topology.LegacyTopology;

/**
 * A SAX handler that loads EMF topology files and creates a Market2 topology
 */
final public class EMF2MarketHandler extends DefaultHandler {

    /**
     * When true - replace DSPM and Datastore commodities with biclique commodities.
     * Used mostly for testing.
     */
    private static final boolean doBicliques = true;
    BiCliquer bicliquer;

    private static final List<String> COMM_REFS =
            Arrays.asList("Commodities", "CommoditiesBought");
    private static final String MARKET = "Analysis:Market";
    private static final String S_TRUE = "true";

    private static final String XSITYPE = "xsi:type";

    // Aggregated stats will be logged at info level every time ELEMENT_LOG more elements are loaded
    private static final long ELEMENT_LOG = 100000;

    // Prefix for biclique PM commodity
    private static final String BCPM_PREFIX = "BC-PM-";
    // Prefix for biclique DS commodity
    private static final String BCDS_PREFIX = "BC-DS-";

    private static final String DSPMAccess = "Abstraction:DSPMAccessCommodity";
    private static final String DatastoreCommodity = "Abstraction:DatastoreCommodity";
    private static final String StorageLatency = "Abstraction:StorageLatency";
    private static final String ACCESSES = "Accesses";

    private final Logger logger;
    private LegacyTopology topology; // the legacy topology that will be populated.

    // Stacks (using the Deque implementation) are used to keep track of the parent of an xml element
    private final Deque<Attributes> attributesStack = new ArrayDeque<>();

    /* Use LinkedHashMap when the order of iteration matters,
     * HashMap otherwise.
     * It matters if we want to get exactly the same results
     * when loading the same file in different runs or on
     * different machines.
     */
    // The loaded entities. In all these maps the key is the object UUID
    // Traders and commodities are kept as Attributes, which are key/value pairs
    private final Map<String, Attributes> traders = new LinkedHashMap<>();
    private final Map<String, Attributes> commodities = new LinkedHashMap<>();
    private final Map<String, Attributes> commoditySold2trader = new LinkedHashMap<>();
    private final Map<String, List<Attributes>> trader2commoditiesBought = new LinkedHashMap<>();
    private final Map<String, Attributes> traderSettings = new HashMap<>();
    // Basket is a set of commodity type strings
    private final Map<String, Set<String>> trader2basketSold = new LinkedHashMap<>();
    // Used to log commodities bought that consume more than one commodity sold
    private final Map<String, String> multipleConsumes = new HashMap<>();
    // Commodities sold which "SoldBy" reference points to the UUID of a trader not present in the file
    private final List<Attributes> noSeller = new ArrayList<>();
    // Commodities bought which "Consumes" reference points to the UUID of a commodity not present in the file
    private final List<Attributes> noConsumes = new ArrayList<>();
    // This map is used for logging.
    // First key is the types of buyer and seller that are skipped, separated with " buying from ",
    // for example "PhysicalMachine buying from Storage"
    // The entries in the set are UUIDs of pairs of skipped traders, separated with a forward slash.
    private final Map<String, Set<String>> skippedBaskets = Maps.newHashMap();

    private long startTime;
    private long elementCount;
    private boolean mainMarket = true;

    /**
     * A constructor that allows the client to specify which logger to use
     * @param logger the {@link Logger} to use for logging
     */
    public EMF2MarketHandler(Logger logger) {
        this.logger = logger;
    }

    /**
     * A constructor that uses the class logger for logging.
     * The class logger is LogManager.getLogger(EMF2MarketHandler.class)
     */
    public EMF2MarketHandler() {
        this(LogManager.getLogger(EMF2MarketHandler.class));
    }

    public LegacyTopology getTopology() {
        return topology;
    }

    @Override
    public void startElement(String uri, String localName, String qName, org.xml.sax.Attributes attr) throws SAXException {
        elementCount++;
        if (elementCount % ELEMENT_LOG == 0) {
            logger.info(String.format("%(,d elemets, %(,d traders, %(,d commodities", elementCount, traders.size(), commodities.size()));
        }
        // attributes is an instance of AbstractSAXParser$AttributesProxy, which is always the same instance.
        // So creating a new copy.
        Attributes attributes = new Attributes(qName, attr);
        Attributes parent = attributesStack.peek();
        attributesStack.push(attributes);
        if (MARKET.equals(qName)) {
            mainMarket = S_TRUE.equals(attributes.get("mainMarket"));
            logger.debug(mainMarket ? "Main market" : "Market : " + attributes.get("name"));
        }
        // ignore entities not in the main market
        if (!mainMarket) return;

        // Ignore shadow entities
        String name = attributes.get("name");
        if (name != null && name.endsWith("_shadow")) return;
        // Ignore templates
        if (S_TRUE.equals(attributes.get("isTemplate"))) return;
        if (parent != null
                && (parent.xsitype() == null
                || parent.xsitype().equals("Analysis:ServiceEntityTemplate")
                || S_TRUE.equals(parent.get("isTemplate"))
            )
        ) {
            return;
        }
        if (COMM_REFS.contains(qName)) {
            printAttributes("Start Element :", attributes, Level.TRACE);
            handleTraderElement(parent);
            handleCommodityElement(attributes);
            if (qName.equals("CommoditiesBought")) {
                trader2commoditiesBought.get(parent.uuid()).add(attributes);
            } else {
                commoditySold2trader.put(attributes.uuid(), parent);
                trader2basketSold.get(parent.uuid()).add(attributes.commoditySpecString());
                // Bicliques
                if (doBicliques && attributes.xsitype().equals(DSPMAccess)) {
                    String uuid1 = parent.uuid();
                    String uuid2 = attributes.get(ACCESSES);
                    bicliquer.edge(uuid1, uuid2);
                }
            }
        }
        if (qName.equals("settings")) {
            traderSettings.put(parent.uuid(), attributes);
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (MARKET.equals(qName)) {
            mainMarket = false; // don't handle entities in between markets
        }
        attributesStack.pop();
        // TODO: Create the trader here. We have the basket sold.
    }

    private void printAttributes(String prefix, Attributes attributes, Level level) {
        if (logger.isEnabled(level))
            return;
        if (attributes == null) {
            logger.log(level, prefix + null);
            return;
        }

        StringBuffer sb = new StringBuffer(prefix);
        String xsiType = attributes.get(XSITYPE);
        if (xsiType != null) {
            sb.append(XSITYPE).append("=").append(xsiType).append(" ");
        }
        attributes.keySet().stream()
            .filter(key -> !XSITYPE.equals(key))
            .sorted()
            .forEach(key -> sb.append(key).append("=").append(attributes.get(key)).append(" "));
        logger.log(level, sb);
    }

    /**
     * Handle an element that represents a trader. Check if it was already handled and if not then
     * add to maps for processing when done loading the document.
     * @param trader Attributes representing a service entity element in the loaded file
     */
    private void handleTraderElement(@NonNull Attributes trader) {
        String uuid = trader.uuid();
        if (!traders.containsKey(uuid)) {
            traders.put(uuid, trader);
            trader2commoditiesBought.put(uuid, new ArrayList<Attributes>());
            trader2basketSold.put(uuid, new HashSet<String>());
        }
    }

    /**
     * Handle an element that represents a commodity bought or sold. Check if it was already handled and
     * if not then add to maps for processing when done loading the document.
     * @param comm Attributes representing a commodity (bought or sold) element in the loaded file
     */
    private void handleCommodityElement(@NonNull Attributes comm) {
        String uuid = comm.uuid();
        if (!commodities.containsKey(uuid)) {
            commodities.put(uuid, comm);
        }
    }

    @Override
    public void startDocument() throws SAXException {
        topology = new LegacyTopology();

        // Clear all maps before starting to parse a new file.
        attributesStack.clear();
        traders.clear();
        commodities.clear();
        trader2commoditiesBought.clear();
        trader2basketSold.clear();
        commoditySold2trader.clear();
        multipleConsumes.clear();
        noSeller.clear();
        noConsumes.clear();
        skippedBaskets.clear();

        startTime = System.currentTimeMillis();
        elementCount = 0;

        logger.debug("Biclique mode is " + (doBicliques ? "on" : "off"));
        if (doBicliques) {
            bicliquer = new BiCliquer(BCDS_PREFIX, BCPM_PREFIX);
        }
        logger.info("Start reading file");
    }

    @Override
    public void endDocument() throws SAXException {
        logger.info("Done reading file in " + (System.currentTimeMillis() - startTime)/1000 + " sec");

        if (doBicliques) bicliquer.compute();
        // Just for counting purposes
        Set<Basket> allBasketsBought = new TreeSet<>();
        Set<Basket> allBasketsSold = new TreeSet<>();
        int nShoppingLists = 0;

        // From basket bought to trader uuid
        Map<ShoppingList, String> placement = Maps.newLinkedHashMap();

        logger.info("Start creating traders");
        for (String traderUuid : traders.keySet()) {
            Attributes traderAttr = traders.get(traderUuid);
            printAttributes("Trader ", traderAttr, Level.DEBUG);
            Set<String> keysSold = trader2basketSold.get(traderUuid)
                    .stream()
                    .filter(k -> !doBicliques || (!k.startsWith(DSPMAccess) && !k.startsWith(DatastoreCommodity)))
                    .collect(Collectors.toSet());;
            Set<String> bcKeys = bicliquer.getBcKeys(traderUuid);
            if (bcKeys != null) {
                keysSold.addAll(bcKeys);
            }

            logger.trace("Keys sold : " + keysSold);
            final @NonNull Trader aSeller = topology.addTrader(traderUuid, traderAttr.get("displayName"),
                                                traderAttr.xsitype(), TraderState.ACTIVE, keysSold);
            allBasketsSold.add(aSeller.getBasketSold());

            // parse settings
            Attributes traderSettingsAttr = traderSettings.get(traderUuid);
            if (traderSettingsAttr != null) {
                TraderSettings traderSett = aSeller.getSettings();
                double utilTarget = traderSettingsAttr.value("utilTarget");
                double targetBand = traderSettingsAttr.value("targetBand");
                traderSett.setMaxDesiredUtil(utilTarget + targetBand/2);
                traderSett.setMinDesiredUtil(utilTarget - targetBand/2);
                traderSett.setCloneable(S_TRUE.equals(traderSettingsAttr.get("ENABLE_PROVISION")));
                traderSett.setSuspendable(S_TRUE.equals(traderSettingsAttr.get("ENABLE_SUSPEND")));
            }

            // Baskets bought
            // Keys are the sellers that this buyer is buying from
            // Values are the commodities sold by this seller
            Map<Attributes, List<Attributes>> sellerAttr2commsSoldAttr = Maps.newLinkedHashMap();
            // Keys are the same as above
            // Values are the commodities that consume commodities from this seller
            Map<Attributes, List<Attributes>> sellerAttr2commsBoughtAttr = Maps.newLinkedHashMap();
            // Key is the uuid of the trader and value is the biclique keys bought from that trader
            Map<String ,String> traderUuid2bcKeysBought = new HashMap<>();
            for (Attributes commBoughtAttr : trader2commoditiesBought.get(traderUuid)) {
                printAttributes("    Buys ", commBoughtAttr, Level.TRACE);
                String consumes = commBoughtAttr.get("Consumes");
                if (consumes == null) {
                    continue;
                }
                if (consumes.contains(" ")) {
                    multipleConsumes.put(commBoughtAttr.uuid(), consumes);
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

                if (isDspmAccess(commBoughtAttr) || isDatastoreCommodity(commBoughtAttr)) {
                    String bcKey = bcKey(commSoldAttr);
                    if (bcKey != null) {
                        traderUuid2bcKeysBought.put(sellerAttr.uuid(), bcKey);
                        continue;
                    }
                }
                // if key doesn't exist then create one, otherwise return the existing value,
                // then add the entry to the list
                sellerAttr2commsSoldAttr
                    .compute(sellerAttr, (key, val) -> val == null ? new ArrayList<>() : val)
                        .add(commSoldAttr);
                sellerAttr2commsBoughtAttr
                    .compute(sellerAttr, (key, val) -> val == null ? new ArrayList<>() : val)
                        .add(commBoughtAttr);
            }
            printAttributes("", traderAttr, Level.DEBUG);
            for (Entry<Attributes, List<Attributes>> entry : sellerAttr2commsSoldAttr.entrySet()) {
                Attributes sellerAttrs = entry.getKey();
                printAttributes("    Buys from ", sellerAttrs, Level.DEBUG);
                Set<String> keysBought = new LinkedHashSet<>();
                for (Attributes commSold : entry.getValue()) {
                    printAttributes("      - ", commSold, Level.TRACE);
                    keysBought.add(commSold.commoditySpecString());
                }
                String bcKeysBought = traderUuid2bcKeysBought.get(sellerAttrs.uuid());
                if (bcKeysBought != null) {
                    keysBought.add(bcKeysBought);
                }
                logger.debug("    Basket : " + keysBought);
                ShoppingList shoppingList = topology.addBasketBought(aSeller, keysBought);
                Basket basketBought = shoppingList.getBasket();

                if (VIRTUAL_MACHINE.equals(traderAttr.xsitype())) // TODO: also check for containers
                    shoppingList.setMovable(true);

                for (Attributes commBought : sellerAttr2commsBoughtAttr.get(sellerAttrs)) {
                    double capacity = commBought.value("capacity", "startCapacity");
                    if (capacity < 0) {
                        printAttributes("capacity bought < 0 ", commBought, Level.WARN);
                        capacity = 0.0;
                    }
                    double used = commBought.value("used");
                    if (used > capacity) {
                        printAttributes("used bought > capacity bought", commBought, Level.WARN);
                        used = capacity;
                    }
                    double peak = commBought.value("peak", "startPeak");
                    if (peak > capacity) {
                        printAttributes("peak bought > capacity bought", commBought, Level.WARN);
                        peak = capacity;
                    }

                    CommodityBought commodity = topology.getEconomy().getCommodityBought(shoppingList,
                        new CommoditySpecification(topology.getCommodityTypes().getId(commBought.commoditySpecString())));
                    commodity.setQuantity(used);
                    commodity.setPeakQuantity(peak);
                }

                placement.put(shoppingList, entry.getKey().uuid());

                allBasketsBought.add(basketBought);
                ++nShoppingLists;
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Created trader " + traderAttr.xsitype() + " (type " + aSeller.getType() + ")");
                logger.debug("    Sells " + aSeller.getBasketSold());
                for (Entry<@NonNull Market, Collection<@NonNull ShoppingList>> entry
                        : Multimaps.invertFrom(Multimaps.forMap(topology.getEconomy().getMarketsAsBuyer(aSeller)),
                                               ArrayListMultimap.create()).asMap().entrySet()) {
                    logger.debug("    Buys " + entry.getKey().getBasket() + " " + entry.getValue().size() + " time(s)");
                }
            }
        }
        topology.populateMarketsWithSellersAndMergeConsumerCoverage();

        // Set various properties of commodity sold
        logger.info("Set commodity properties");
        for (Entry<String, Attributes> entry : commoditySold2trader.entrySet()) {
            Attributes commSoldAttr = commodities.get(entry.getKey());
            printAttributes("Commodity sold ",  commSoldAttr, Level.TRACE);
            // Bicliques - skip DSPMAccess and Datastore commodities
            if (isDspmAccess(commSoldAttr) || isDatastoreCommodity(commSoldAttr)) {
                continue;
            }

            double capacity = commSoldAttr.value("capacity", "startCapacity");
            double used = commSoldAttr.value("used");
            double peakUtil = commSoldAttr.value("peakUtilization");
            double utilThreshold = commSoldAttr.value("utilThreshold", 1.0);
            CommoditySpecification specification = new CommoditySpecification(
                topology.getCommodityTypes().getId(commSoldAttr.commoditySpecString()));
            Trader trader = topology.getUuids().inverse().get(entry.getValue().uuid());
            CommoditySold commSold = trader.getCommoditySold(specification);
            // The only known way to get a negative capacity is bug OM-3669 which is fixed, but we
            // check and recover from this error with only a warning, so that we have some tolerance
            // in case we have to work with servers that don't have the fix and/or have other bugs.
            if (capacity < 0) {
                printAttributes("capacity sold < 0 ", commSoldAttr, Level.WARN);
                capacity = 0.0;
            }
            if (used > capacity) {
                printAttributes("used sold > capacity sold", commSoldAttr, Level.WARN);
                used = capacity;
            }
            if (peakUtil > 1.0) {
                printAttributes("peakUtilization > 1.0 ", commSoldAttr, Level.WARN);
                peakUtil = 1.0;
            }
            commSold.setCapacity(capacity);
            commSold.setQuantity(used);
            commSold.setPeakQuantity(peakUtil * capacity);
            commSold.getSettings().setUtilizationUpperBound(utilThreshold);
            PriceFunction pf = priceFunction(commSoldAttr);
            commSold.getSettings().setPriceFunction(pf);
        }

        logger.info("Processing placement");
        for (Entry<ShoppingList, String> entry : placement.entrySet()) {
            entry.getKey().move(topology.getUuids().inverse().get(entry.getValue()));
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

        skippedBaskets.forEach((key, val) -> logger.info("Skipped " + val.size() + " " + key));

        logger.info(traders.size() + " traders");
        logger.info(commodities.size() + " commodities (bought and sold)");
        logger.info(allBasketsBought.size() + " unique baskets bought");
        logger.info(allBasketsSold.size() + " unique baskets sold");
        logger.info(nShoppingLists + " shopping lists");
        logger.info(topology.getTraderTypes().size() + " trader types");
        if (logger.isTraceEnabled()) topology.getTraderTypes().entrySet().stream().forEach(logger::trace);
        logger.info(topology.getCommodityTypes().size() + " commodity types");
        logger.info(bicliquer.size() + " bicliques");
        if (logger.isDebugEnabled()) bicliquer.getBicliques().forEach((k, v) -> logger.debug(names(k) + " = " + names(v)));
        if (logger.isTraceEnabled()) topology.getCommodityTypes().entrySet().stream().forEach(logger::trace);
        logger.info((System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }

    /**
     * The commodity key of the biclique commodity sold by {@code commSoldAttr}.
     * @param commSoldAttr the {@link Attributes} representing the sold commodity
     * @return the key of the biclique commodity sold by {@code commSoldAttr},
     * or null if there is no such biclique.
     */
    private String bcKey(Attributes commSoldAttr) {
        // These two uuids belong to two traders that are connected with an edge in the biclique
        Attributes sellerAttr = commoditySold2trader.get(commSoldAttr.uuid());
        String uuid1 = sellerAttr.uuid();
        String uuid2 = commSoldAttr.get(ACCESSES);
        return bicliquer.getBcKey(uuid1, uuid2);
    }

    /**
     * Convert a set of UUIDs to a set of trader names
     * @param uuids set of trader UUIDs
     * @return the names of the traders
     */
    private Collection<String> names(Set<String> uuids) {
        Set<String> foo = uuids.stream()
            .map(topology.getUuids().inverse()::get)
            // for testing, when a trader may be referenced by "Accesses" but is not part of the topology
            .filter(t -> t != null)
            .map(Trader::getEconomyIndex)
            .map(topology.getNames()::get)
            .collect(Collectors.toSet());
        return foo;
    }

    /**
     * @return True if processing bicliques ({@link #doBicliques} is true) and this is a DSPMAccessCommodity
     */
    private boolean isDspmAccess(Attributes commodity) {
        return doBicliques && commodity.xsitype().equals(DSPMAccess);
    }

    /**
     * @return True if processing bicliques ({@link #doBicliques} is true) and this is a DatastoreCommodity
     */
    private boolean isDatastoreCommodity(Attributes commodity) {
        return doBicliques && commodity.xsitype().equals(DatastoreCommodity);
    }

    Level warning(boolean warning) {
        return warning ? Level.WARN : Level.INFO;
    }

    /**
     * print the traders and their placements
     */
    private void verify() {
        for (Trader trader : topology.getEconomy().getTraders()) {
            logger.trace(topology.getTraderTypes().getName(trader.getType()) + " @" + trader.hashCode());
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
                    + topology.getEconomy().getSuppliers(trader)
                    .stream().map(Trader::getType)
                    .map(topology.getTraderTypes()::getName)
                    .collect(Collectors.toList()));
            logger.trace(topology.getEconomy().getMarketsAsBuyer(trader).size() + " shopping lists ");
            // Print "P" x number of shopping lists (e.g. PPPPP for 5 shopping lists). Makes search easier.
            logger.trace(Strings.repeat("P", topology.getEconomy().getMarketsAsBuyer(trader).size()));
            for (@NonNull Entry<@NonNull ShoppingList, @NonNull Market> entry
                            : topology.getEconomy().getMarketsAsBuyer(trader).entrySet()) {
                ShoppingList shoppingList = entry.getKey();
                logger.trace("    -- shopping lists @" + shoppingList.hashCode());
                logger.trace("         basket: " + basketAsStrings(entry.getValue().getBasket()));
                logger.trace("         quantities: " + Arrays.toString(shoppingList.getQuantities()));
                logger.trace("         peaks     : " + Arrays.toString(shoppingList.getPeakQuantities()));
            }
        }
    }

    List<String> basketAsStrings(Basket basket) {
        List<String> result = new ArrayList<>(basket.size());
        for (CommoditySpecification specification : basket) {
            result.add(topology.getCommodityTypes().getName(specification.getType()).toString());
        }

        return result;
    }

    private static final String PHYSICAL_MACHINE = "Abstraction:PhysicalMachine";
    private static final String VIRTUAL_MACHINE = "Abstraction:VirtualMachine";
    private static final String STORAGE = "Abstraction:Storage";
    private static final String APPLICATION = "Abstraction:Application";

    boolean skip(Attributes buyer, Attributes seller) {
        String buyerType = buyer.xsitype();
        String sellerType = seller.xsitype();
        if (
            (PHYSICAL_MACHINE.equals(buyerType) && STORAGE.equals(sellerType))
            || (APPLICATION.equals(buyerType) && VIRTUAL_MACHINE.equals(sellerType))) {
            String key = buyerType.split(":")[1] + " buying from " + sellerType.split(":")[1];
            String skippedPair = buyer.uuid()+"/"+seller.uuid();
            skippedBaskets.compute(key, (k, v) -> v == null ? Sets.newHashSet() : v).add(skippedPair);
            return true;
        }
        return false;
    }

    PriceFunction priceFunction(Attributes commodity) {
        String type = commodity.xsitype();
        switch(type) {
            case "Abstraction:StorageAmount":
            case "Abstraction:StorageProvisioned":
            case "Abstraction:VStorage":
                return PriceFunctionFactory.createStepPriceFunction(commodity.value("utilThreshold", 1.0), 0.0, 20000.0);
            case "Abstraction:Power":
            case "Abstraction:Cooling":
            case "Abstraction:Space":
                return PriceFunctionFactory.createConstantPriceFunction(27.0);
            case "Abstraction:SegmentationCommodity":
            case "Abstraction:DrsSegmentationCommodity":
            case "Abstraction:ClusterCommodity":
            case "Abstraction:StorageClusterCommodity":
                return PriceFunctionFactory.createConstantPriceFunction(0.0);
            default:
                return PriceFunctionFactory.createStandardWeightedPriceFunction(1.0);
        }
    }

    /**
     * A representation of an XML element from the file as a key-value map.
     */
    static class Attributes {
        final org.xml.sax.Attributes saxAttributes;
        final String xsiType;
        final List<String> keyset = new ArrayList<>();

        Attributes(@NonNull String qName, org.xml.sax.Attributes attributes) {
            saxAttributes = new AttributesImpl(attributes);
            // xsiType can be null if SE is a child of the document root
            xsiType = attributes.getValue(XSITYPE) == null ? qName : attributes.getValue(XSITYPE);
        }

        public Collection<String> keySet() {
            if (keyset.isEmpty()) {
                for (int i = 0; i < saxAttributes.getLength(); i++) {
                    keyset.add(saxAttributes.getLocalName(i));
                }
                if (!keyset.contains(XSITYPE)) {
                    keyset.add(XSITYPE);
                }
            }
            return keyset;
        }

        String get(String key) {
            if (XSITYPE.equals(key)) {
                return xsiType;
            } else {
                return saxAttributes.getValue(key);
            }
        }

        String uuid() {
            return get("uuid");
        }

        /**
         * @return the value (as double) of the first attribute in the the list of properties
         */
        double value(String...props) {
            for (String prop : props) {
                String sProp = get(prop);
                if (sProp != null) {
                    return Double.valueOf(sProp);
                }
            }
            return 0.0;
        }

        /**
         * Return the value (as double) of the property if it is set, or the default value otherwise.
         */
        private double value(String prop, double defaultValue) {
            String sValue = saxAttributes.getValue(prop);
            return sValue == null ? defaultValue : Double.valueOf(sValue);
        }

        /**
         * The string mapping of the commodity spec for the commodity represented by the attributes
         * @return xsitype when there is no key, and xsitype[key] when a key exists
         */
        private String commoditySpecString() {
            String commType = xsitype();
            String key = get("key");
            return commType + (key != null ? "[" + key + "]" : "");
        }

        /**
         * @return the xsi::type of the element
         */
        private String xsitype() {
            return xsiType;
        }
    }
}
