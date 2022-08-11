package com.vmturbo.platform.analysis.economy;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import com.vmturbo.commons.analysis.InvertedIndex;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class InvertedIndexTest {
    final Economy economy = new Economy();
    final InvertedIndex<Trader, Basket> invertedIndex = new InvertedIndex(1, new InvertedIndexTranslator());
    private static final CommoditySpecification CPU = new CommoditySpecification(0);
    private static final CommoditySpecification MEM = new CommoditySpecification(1);

    private static final Basket EMPTY_BASKET = new Basket();
    private static final Basket CPU_BASKET = new Basket(CPU);
    private static final Basket MEM_BASKET = new Basket(MEM);
    private static final Basket CPU_MEM_BASKET = new Basket(CPU, MEM);

    private final Trader cpuTrader = new TraderWithSettings(0, 1, TraderState.ACTIVE, CPU_BASKET);
    private final Trader memTrader1 = new TraderWithSettings(0, 1, TraderState.ACTIVE, MEM_BASKET);
    private final Trader memTrader2 = new TraderWithSettings(0, 1, TraderState.ACTIVE, MEM_BASKET);
    private final Trader cpuMemTrader = new TraderWithSettings(0, 1, TraderState.ACTIVE, CPU_MEM_BASKET);

    @Test
    public void testAddCpuGetsCpu() {
        invertedIndex.add(cpuTrader);
        assertTrue(invertedIndex.getSatisfyingSellers(CPU_BASKET)
            .collect(Collectors.toList())
            .contains(cpuTrader));
    }

    @Test
    public void testAddCpuDoesNotGetMem() {
        invertedIndex.add(cpuTrader);
        assertFalse(invertedIndex.getSatisfyingSellers(MEM_BASKET)
            .collect(Collectors.toList())
            .contains(cpuTrader));
    }

    @Test
    public void testAddCpuMemGetsCpuAndMem() {
        invertedIndex.add(cpuMemTrader);
        assertTrue(invertedIndex.getSatisfyingSellers(CPU_BASKET)
            .collect(Collectors.toList())
            .contains(cpuMemTrader));
        assertTrue(invertedIndex.getSatisfyingSellers(MEM_BASKET)
            .collect(Collectors.toList())
            .contains(cpuMemTrader));
    }

    @Test
    public void testAddMultipleGetsMultiple() {
        invertedIndex.add(cpuTrader);
        invertedIndex.add(cpuMemTrader);

        assertTrue(invertedIndex.getSatisfyingSellers(CPU_BASKET)
            .collect(Collectors.toList())
            .contains(cpuTrader));
        assertTrue(invertedIndex.getSatisfyingSellers(CPU_BASKET)
            .collect(Collectors.toList())
            .contains(cpuMemTrader));
    }

    @Test
    public void testGetsCorrectSatisfyingTradersWhenOverThreshold() {
        invertedIndex.add(cpuTrader);
        invertedIndex.add(cpuMemTrader);
        invertedIndex.add(memTrader1);
        invertedIndex.add(memTrader2);

        assertEquals(1, invertedIndex.getSatisfyingSellers(CPU_MEM_BASKET).count());
        assertEquals(cpuMemTrader,
            invertedIndex.getSatisfyingSellers(CPU_MEM_BASKET).findFirst().get()
        );
    }

    @Test
    public void testIndexSizeEmpty() {
        assertEquals(0, invertedIndex.indexSize());
    }

    @Test
    public void testIndexSizeOne() {
        invertedIndex.add(memTrader1);
        invertedIndex.add(memTrader2);

        assertEquals(1, invertedIndex.indexSize());
    }

    @Test
    public void testIndexSizeMultipleCommoditiesAddedMultipleTimes() {
        invertedIndex.add(cpuMemTrader);

        assertEquals(2, invertedIndex.indexSize());
    }

    @Test
    public void testValueCountEmpty() {
        assertEquals(0, invertedIndex.valueCount());
    }

    @Test
    public void testValueCountOne() {
        invertedIndex.add(cpuTrader);

        assertEquals(1, invertedIndex.valueCount());
    }

    @Test
    public void testValueCountMultiple() {
        invertedIndex.add(memTrader1);
        invertedIndex.add(memTrader2);
        invertedIndex.add(cpuMemTrader);

        assertEquals(4, invertedIndex.valueCount());
    }

    @Test
    public void testGetMinimalScanStopThreshold() {
        final InvertedIndex<Trader, Basket> invertedIndex = new InvertedIndex(12, new InvertedIndexTranslator());
        assertEquals(12, invertedIndex.getMinimalScanStopThreshold());
    }

    @Test
    public void testClear() {
        invertedIndex.add(cpuTrader);
        invertedIndex.clear();

        assertTrue(invertedIndex.indexSize() == 0);
        assertFalse(invertedIndex.getSatisfyingSellers(CPU_BASKET)
                .collect(Collectors.toList())
                .contains(cpuTrader));
    }

    @Test
    public void testRemove() {
        invertedIndex.add(cpuTrader);
        assertEquals(1, invertedIndex.remove(cpuTrader));

        assertFalse(invertedIndex.getSatisfyingSellers(CPU_BASKET)
                .collect(Collectors.toList())
                .contains(cpuTrader));
    }

    @Test
    public void testRemoveMultipleCommodityTrader() {
        invertedIndex.add(cpuMemTrader);
        assertEquals(2, invertedIndex.remove(cpuMemTrader));

        assertFalse(invertedIndex.getSatisfyingSellers(CPU_BASKET)
            .collect(Collectors.toList())
            .contains(cpuMemTrader));
        assertFalse(invertedIndex.getSatisfyingSellers(MEM_BASKET)
            .collect(Collectors.toList())
            .contains(cpuMemTrader));
    }

    @Test
    public void testRemoveDoesNotRemoveOthers() {
        invertedIndex.add(cpuMemTrader);
        invertedIndex.add(cpuTrader);
        invertedIndex.add(memTrader1);
        assertEquals(2, invertedIndex.remove(cpuMemTrader));

        assertTrue(invertedIndex.getSatisfyingSellers(CPU_BASKET)
            .collect(Collectors.toList())
            .contains(cpuTrader));
        assertTrue(invertedIndex.getSatisfyingSellers(MEM_BASKET)
            .collect(Collectors.toList())
            .contains(memTrader1));
    }

    @Test
    public void testRemoveWhenNotContained() {
        assertEquals(0, invertedIndex.remove(cpuMemTrader));
    }

    @Test
    public void testAddThenRemoveOther() {
        invertedIndex.add(cpuTrader);

        assertEquals(0, invertedIndex.remove(cpuMemTrader));
    }

    @Test
    public void testEmptyBasket() throws Exception {
        economy.addTrader(0, TraderState.ACTIVE, EMPTY_BASKET);
        economy.addTrader(0, TraderState.ACTIVE, CPU_BASKET);
        economy.addTrader(0, TraderState.ACTIVE, MEM_BASKET);
        economy.addTrader(0, TraderState.ACTIVE, MEM_BASKET);
        economy.addTrader(0, TraderState.ACTIVE, CPU_MEM_BASKET);

        assertEquals(
            Collections.EMPTY_LIST,
            invertedIndex.getSatisfyingSellers(EMPTY_BASKET).collect(Collectors.toList())
        );
    }
}