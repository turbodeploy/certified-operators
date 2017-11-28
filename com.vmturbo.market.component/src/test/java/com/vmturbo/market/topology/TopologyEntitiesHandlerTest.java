package com.vmturbo.market.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nonnull;

import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.util.JsonFormat;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.analysis.InvalidTopologyException;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.market.topology.conversions.TopologyConverter;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.DeactivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.topology.processor.conversions.Converter;

/**
 * Unit tests for {@link TopologyEntitiesHandler}.
 */
public class TopologyEntitiesHandlerTest {

    static {
     // allow running this test standalone (not part of a full build)
        IdentityGenerator.initPrefix(0);
    }

    private static final String DSPM_OR_DATASTORE_PATTERN = "DSPM.*|DATASTORE.*";
    private static final String BC_PATTERN = "BICLIQUE.*";

    /**
     * Test loading a file that was generated using the hyper-v probe.
     * Move the DTOs through the whole pipe and verify we get actions.
     * TODO: more specific tests for the generated actions
     * @throws IOException when the loaded file is missing
     * @throws InvalidTopologyException not supposed to happen here
     */
    @Test
    public void end2endTest() throws IOException, InvalidTopologyException {
        List<ActionTO> actionTOs = generateEnd2EndActions();
        assertFalse(actionTOs.isEmpty());
        assertTrue(actionTOs.stream().anyMatch(ActionTO::hasResize));
        assertTrue(actionTOs.stream().anyMatch(ActionTO::hasMove));
        assertTrue(actionTOs.stream().anyMatch(ActionTO::hasProvisionBySupply));
    }

    /**
     * Test loading a file that was generated using the hyper-v probe.
     * Move the DTOs through the whole pipe and verify actions are properly collapsed.
     * @throws IOException when the loaded file is missing
     * @throws InvalidTopologyException not supposed to happen here
     */
    @Ignore("Currently fails due to OM-21364. Please re-enable when that bug is fixed.")
    @Test
    public void end2endTestCollapsing() throws IOException, InvalidTopologyException {
        List<ActionTO> actionTOs = generateEnd2EndActions();

        // Verify that the actions are collapsed
        // Count Move actions - with collapsed actions there should be no more than one per shopping list
        Map<Long, Long> shoppingListMoveCount = actionTOs.stream()
                        .filter(ActionTO::hasMove)
                        .map(ActionTO::getMove)
                        .collect(Collectors.groupingBy(
                            MoveTO::getShoppingListToMove,
                            Collectors.counting()));
        long max = shoppingListMoveCount.values().stream().max(Long::compare).orElse(0L);
        assertTrue("Shopping lists are moved more than once", max <= 1);
        // In collapsed actions there should be no resize actions for traders that are then deactivated
        Set<Long> deactivated = actionTOs.stream()
                        .filter(ActionTO::hasDeactivate)
                        .map(ActionTO::getDeactivate)
                        .map(DeactivateTO::getTraderToDeactivate)
                        .collect(Collectors.toSet());
        Set<Long> resized = actionTOs.stream()
                        .filter(ActionTO::hasResize)
                        .map(ActionTO::getResize)
                        .map(ResizeTO::getSellingTrader)
                        .collect(Collectors.toSet());
        resized.retainAll(deactivated);
        assertTrue("Traders are resized but later deactivated", resized.isEmpty());
    }

    /**
     * Load a small topology with 2 PMs and 3 DSs that are fully meshed. We expect to get one biclique.
     * We assert that only the biclique keys BC-T1-0 and BC-T2-0 are used.
     * @throws IOException if the test file cannot be loaded
     * @throws InvalidTopologyException not supposed to happen here
     */
    @Test
    public void bicliquesTest() throws IOException, InvalidTopologyException {
        List<CommonDTO.EntityDTO> probeDTOs = messagesFromJsonFile("protobuf/messages/small.json",
            EntityDTO::newBuilder);

        Map<Long, CommonDTO.EntityDTO> map = Maps.newHashMap();
        IntStream.range(0, probeDTOs.size()).forEach(i -> map.put((long)i, probeDTOs.get(i)));

        List<TopologyEntityDTO.Builder> topoDTOs = Converter.convert(map);
        TopologyConverter topoConverter = new TopologyConverter(true, TopologyType.REALTIME);

        Set<TraderTO> traderDTOs = topoConverter.convertToMarket(
            topoDTOs.stream().map(TopologyEntityDTO.Builder::build).collect(Collectors.toList()));
        Set<String> debugInfos = traderDTOs.stream()
                .map(TraderTO::getCommoditiesSoldList)
                .flatMap(List::stream)
                .map(CommoditySoldTO::getSpecification)
                .map(CommoditySpecificationTO::getDebugInfoNeverUseInCode)
                .filter(key -> key.startsWith("BICLIQUE"))
                .collect(Collectors.toSet());
        assertEquals(Sets.newHashSet("BICLIQUE BC-T1-1", "BICLIQUE BC-T2-1"), debugInfos);

        // Verify that buyers buy commodities from sellers that indeed sell them (all, not only biclique)
        Map<Long, TraderTO> tradersMap = traderDTOs.stream()
                        .collect(Collectors.toMap(TraderTO::getOid, Function.identity()));
        for (TraderTO trader : traderDTOs) {
            for (ShoppingListTO shoppingList : trader.getShoppingListsList()) {
                TraderTO supplier = tradersMap.get(shoppingList.getSupplier());
                for (CommodityBoughtTO commBought : shoppingList.getCommoditiesBoughtList()) {
                    assertTrue(supplier.getCommoditiesSoldList().stream()
                        .anyMatch(commSold ->
                            commSold.getSpecification().equals(commBought.getSpecification())));
                }
            }
        }
    }

    /**
     * Load a bigger topology. Verify that each storage participates in exactly one biclique,
     * and that each VM buys a unique set of BC keys in each one of its shopping lists.
     * @throws IOException if the test file cannot be loaded
     * @throws InvalidTopologyException not supposed to happen here
     */
    @Test
    public void bcKeysTests() throws IOException, InvalidTopologyException {
        List<CommonDTO.EntityDTO> probeDTOs = messagesFromJsonFile("protobuf/messages/entities.json",
            EntityDTO::newBuilder);

        Map<Long, CommonDTO.EntityDTO> map = Maps.newHashMap();
        IntStream.range(0, probeDTOs.size()).forEach(i -> map.put((long)i, probeDTOs.get(i)));

        List<TopologyEntityDTO.Builder> topoDTOs = Converter.convert(map);

        Set<TraderTO> traderDTOs = new TopologyConverter(TopologyType.REALTIME).convertToMarket(
            topoDTOs.stream().map(TopologyEntityDTO.Builder::build).collect(Collectors.toList()));

        for (TraderTO traderTO : traderDTOs) {
            if (traderTO.getDebugInfoNeverUseInCode().startsWith("STORAGE")) {
                long numBcKeys = traderTO.getCommoditiesSoldList().stream()
                    .map(CommoditySoldTO::getSpecification)
                    .map(CommoditySpecificationTO::getDebugInfoNeverUseInCode)
                    .filter(key -> key.startsWith("BICLIQUE"))
                    .count();
                assertEquals(1, numBcKeys);
            }
        }

        // BC keys sold are unique
        for (TraderTO traderTO : traderDTOs) {
            if (traderTO.getDebugInfoNeverUseInCode().startsWith("VIRTUAL_MACHINE")) {
                for (ShoppingListTO shoppingList : traderTO.getShoppingListsList()) {
                    List<String> bcKeys = shoppingList.getCommoditiesBoughtList().stream()
                        .map(c -> c.getSpecification().getDebugInfoNeverUseInCode())
                        .filter(s -> s.startsWith("BICLIQUE"))
                        .collect(Collectors.toList());
                    assertEquals(Sets.newHashSet(bcKeys).size(), bcKeys.size());
                }
            }
        }
    }

    /**
     * Test that when shop-together is enabled, we don't create biclique commodities,
     * and instead we populate the 'cliques' property of the trader DTO.
     * Also test that regardless of shop-together, we remove all the DSPMAccess and
     * Datastore commodities.
     * @throws IOException if the test file cannot be loaded
     * @throws InvalidTopologyException not supposed to happen here
     */
    @Test
    public void shopTogetherTest() throws IOException, InvalidTopologyException {
        List<CommonDTO.EntityDTO> nonShopTogetherProbeDTOs = messagesFromJsonFile("protobuf/messages/nonShopTogetherEntities.json",
            EntityDTO::newBuilder);
        Map<Long, CommonDTO.EntityDTO> map = Maps.newHashMap();
        IntStream.range(0, nonShopTogetherProbeDTOs.size()).forEach(i -> map.put((long)i, nonShopTogetherProbeDTOs.get(i)));

        List<TopologyEntityDTO> nonShopTogetherTopoDTOs = Converter.convert(map).stream()
            .map(TopologyEntityDTO.Builder::build)
            .collect(Collectors.toList());

        TopologyConverter togetherConverter = new TopologyConverter(TopologyType.REALTIME);
        final Set<TraderTO> traderDTOs = togetherConverter.convertToMarket(nonShopTogetherTopoDTOs);

        // No DSPMAccess and Datastore commodities sold
        final long nspDdSoldShopTogether = countSoldCommodities(traderDTOs, DSPM_OR_DATASTORE_PATTERN);
        assertEquals(0, nspDdSoldShopTogether);

        // No DSPMAccess and Datastore commodities bought
        long nspDdBoughtShopTogether = countBoughtCommodities(traderDTOs, DSPM_OR_DATASTORE_PATTERN);
        assertEquals(0, nspDdBoughtShopTogether);

        // Some BiClique commodities sold
        long bcSoldNonShopTogether = countSoldCommodities(traderDTOs, BC_PATTERN);
        assertTrue(bcSoldNonShopTogether > 0);

        // Some BiClique commodities bought
        long bcBoughtNonShopTogether = countBoughtCommodities(traderDTOs, BC_PATTERN);
        assertTrue(bcBoughtNonShopTogether > 0);

        // check that bicliques are correct in the generated dtos
        checkBicliques(traderDTOs);

        // ====== shop together ======
        List<CommonDTO.EntityDTO> shopTogetherProbeDTOs = messagesFromJsonFile("protobuf/messages/shopTogetherEntities.json",
            EntityDTO::newBuilder);
        Map<Long, CommonDTO.EntityDTO> shopTogetherMap = Maps.newHashMap();
        IntStream.range(0, shopTogetherProbeDTOs.size()).forEach(i -> shopTogetherMap.put((long)i, shopTogetherProbeDTOs.get(i)));

        List<TopologyEntityDTO> shopTogetherTopoDTOs = Converter.convert(shopTogetherMap).stream()
            .map(TopologyEntityDTO.Builder::build)
            .collect(Collectors.toList());

        TopologyConverter shopTogetherConverter = new TopologyConverter(TopologyType.REALTIME);
        final Set<TraderTO> shopTogetherTraderDTOs = shopTogetherConverter.convertToMarket(shopTogetherTopoDTOs);

        // No DSPMAccess and Datastore commodities sold
        long ddSoldShopTogether = countSoldCommodities(shopTogetherTraderDTOs, DSPM_OR_DATASTORE_PATTERN);
        assertEquals(0, ddSoldShopTogether);

        // No DSPMAccess and Datastore commodities bought
        long ddBoughtNonShopTogether = countBoughtCommodities(shopTogetherTraderDTOs, DSPM_OR_DATASTORE_PATTERN);
        assertEquals(0, ddBoughtNonShopTogether);

        //No BiClique commodities sold
        final long bcSoldShopTogether = countSoldCommodities(shopTogetherTraderDTOs, BC_PATTERN);
        assertEquals(0, bcSoldShopTogether);

        // No BiClique commodities bought
        final long bcBoughtShopTogether = countBoughtCommodities(shopTogetherTraderDTOs, BC_PATTERN);
        assertEquals(0, bcBoughtShopTogether);

        // check that bicliques are correct in the generated dtos
        checkBicliques(shopTogetherTraderDTOs);
    }

    /**
     * Test that when used > capacity we throw InvalidTopologyException.
     * In the loaded file there are 3 such commodities.
     * @throws IOException if the test file cannot be loaded
     * @throws InvalidTopologyException when the topology is invalid
     */
    @Test(expected = InvalidTopologyException.class)
    public void testInvalidTopology() throws IOException, InvalidTopologyException {
        List<CommonDTO.EntityDTO> probeDTOs = messagesFromJsonFile("protobuf/messages/invalid-topology.json",
            EntityDTO::newBuilder);

        Map<Long, CommonDTO.EntityDTO> map = Maps.newHashMap();
        IntStream.range(0, probeDTOs.size()).forEach(i -> map.put((long)i, probeDTOs.get(i)));

        List<TopologyEntityDTO.Builder> topoDTOs = Converter.convert(map);
        new TopologyConverter(TopologyType.REALTIME).convertToMarket(
            topoDTOs.stream().map(TopologyEntityDTO.Builder::build).collect(Collectors.toList()));
    }

    /**
     * Load DTOs from a file generated using the hyper-v probe.
     * Move the DTOs through the whole pipe and return the actions generated.
     *
     * @return The actions generated from the end2endTest DTOs.
     * @throws IOException If the file load fails.
     * @throws InvalidTopologyException not supposed to happen here
     */
    private List<ActionTO> generateEnd2EndActions() throws IOException, InvalidTopologyException {
        List<CommonDTO.EntityDTO> probeDTOs =
            messagesFromJsonFile("protobuf/messages/discoveredEntities.json", EntityDTO::newBuilder);

        Map<Long, CommonDTO.EntityDTO> map = Maps.newHashMap();
        IntStream.range(0, probeDTOs.size()).forEach(i -> map.put((long)i, probeDTOs.get(i)));
        List<TopologyEntityDTO> topoDTOs = Converter.convert(map).stream()
            .map(TopologyEntityDTO.Builder::build)
            .collect(Collectors.toList());
        Set<TraderTO> economyDTOs = new TopologyConverter(true, TopologyType.REALTIME)
                        .convertToMarket(topoDTOs);
        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                .setTopologyContextId(7L)
                .setTopologyType(TopologyType.PLAN)
                .setTopologyId(1L)
                .build();

        AnalysisResults results =
            TopologyEntitiesHandler.performAnalysis(economyDTOs, topologyInfo);
        return results.getActionsList();
    }

    private long countSoldCommodities(Set<TraderTO> traderDTOs, String pattern) {
        return traderDTOs.stream()
                        .map(TraderTO::getCommoditiesSoldList)
                        .flatMap(List::stream)
                        .map(CommoditySoldTO::getSpecification)
                        .map(CommoditySpecificationTO::getDebugInfoNeverUseInCode)
                        .filter(key -> key.matches(pattern))
                        .count();
    }

    private long countBoughtCommodities(Set<TraderTO> traderDTOs, String pattern) {
        return traderDTOs.stream()
                        .map(TraderTO::getShoppingListsList)
                        .flatMap(List::stream)
                        .map(ShoppingListTO:: getCommoditiesBoughtList)
                        .flatMap(List::stream)
                        .map(CommodityBoughtTO::getSpecification)
                        .map(CommoditySpecificationTO::getDebugInfoNeverUseInCode)
                        .filter(key -> key.matches(pattern))
                        .count();
    }

    /**
     * Load a json file that contains multiple DTOs, one per line.
     *
     * @param fileName the name of the file to load
     * @param builderSupplier The method to create a builder for Msg.
     * @param <Msg> The type of DTO.
     * @return A list of DTOs represented by the file
     * @throws IOException when the file is not found
     */
    public static <Msg extends AbstractMessage> List<Msg> messagesFromJsonFile(
            @Nonnull final String fileName,
            @Nonnull final Supplier<AbstractMessage.Builder> builderSupplier) throws IOException {
        URL fileUrl = TopologyEntitiesHandlerTest.class.getClassLoader().getResources(fileName).nextElement();
        File file = new File(fileUrl.getFile());
        LineProcessor<List<Msg>> callback = new LineProcessor<List<Msg>>() {
            List<Msg> list = Lists.newArrayList();
            @Override
            public boolean processLine(@Nonnull String line) throws IOException {
                AbstractMessage.Builder builder = builderSupplier.get();
                JsonFormat.parser().merge(line, builder);
                return list.add((Msg)builder.build());
            }

            @Override
            public List<Msg> getResult() {
                // TODO Auto-generated method stub
                return list;
            }
        };
        return Files.readLines(file, Charset.defaultCharset(), callback);
    }

    /**
     * Helper method to check that bicliques are created correctly on the {@link TraderTO}
     *
     * @param shopTogetherTraderDTOs {@link TraderTO} to check
     */
    private void checkBicliques(final Set<TraderTO> shopTogetherTraderDTOs) {

        // Each storage is member of exactly one biclique (check using the 'cliques' property)
        final Set<Integer> stCliqueCounts = shopTogetherTraderDTOs.stream()
                .filter(trader -> trader.getDebugInfoNeverUseInCode().startsWith("STORAGE"))
                .map(TraderTO::getCliquesCount)
                .collect(Collectors.toSet());
        assertEquals(Sets.newHashSet(1), stCliqueCounts);

        // Each PM is member of one or more bicliques (check using the 'cliques' property)
        final Set<Integer> pmCliqueCounts = shopTogetherTraderDTOs.stream()
                .filter(trader -> trader.getDebugInfoNeverUseInCode().startsWith("PHYSICAL_MACHINE"))
                .map(TraderTO::getCliquesCount)
                .collect(Collectors.toSet());
        assertFalse(pmCliqueCounts.contains(0));

        // All other traders don't have bicliques (check using the 'cliques' property
        final Set<Integer> otherCliqueCounts = shopTogetherTraderDTOs.stream()
                .filter(trader -> !trader.getDebugInfoNeverUseInCode().startsWith("PHYSICAL_MACHINE")
                        && !trader.getDebugInfoNeverUseInCode().startsWith("STORAGE"))
                .map(TraderTO::getCliquesCount)
                .collect(Collectors.toSet());
        assertEquals(Sets.newHashSet(0), otherCliqueCounts);
    }
}
