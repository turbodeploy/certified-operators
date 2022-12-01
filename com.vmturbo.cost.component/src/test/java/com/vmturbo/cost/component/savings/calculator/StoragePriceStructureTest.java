package com.vmturbo.cost.component.savings.calculator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.Message.Builder;

import org.junit.Test;

import com.vmturbo.cost.component.pricing.BusinessAccountPriceTableKeyStore;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.util.TestUtils;
import com.vmturbo.platform.sdk.common.PricingDTO.StorageTierPriceList;

/**
 * Test the use of StorageAmountResolver to adjust storage amount values using the price table.
 */
public class StoragePriceStructureTest {
    private final BusinessAccountPriceTableKeyStore priceTableKeyStore = mock(BusinessAccountPriceTableKeyStore.class);
    private final PriceTableStore priceTableStore = mock(PriceTableStore.class);
    private final StoragePriceStructure storagePriceStructure = spy(new StoragePriceStructure(priceTableKeyStore, priceTableStore));

    private static final long STANDARD_HDD_DISK_TIER_OID = 11111L;
    private static final long TEST_DISK_TIER_OID = 22222L;

    /**
     * Test the conversion of storage amount to end ranges.
     */
    @Test
    public void testGetEndRange() {
        Map<Long, StorageTierPriceList> priceListMap = createPriceListMap();
        doReturn(priceListMap).when(storagePriceStructure).getStoragePriceTiers(anyLong(), anyLong());
        double adjustedAmount = storagePriceStructure.getEndRangeInPriceTier(33.0, 1L, 2L, STANDARD_HDD_DISK_TIER_OID);
        assertEquals(64, adjustedAmount, 0.001);
        adjustedAmount = storagePriceStructure.getEndRangeInPriceTier(4096.0, 1L, 2L, STANDARD_HDD_DISK_TIER_OID);
        assertEquals(4096, adjustedAmount, 0.001);
        // Return the input storage amount if there is no end-range. set.
        adjustedAmount = storagePriceStructure.getEndRangeInPriceTier(18.0, 1L, 2L, TEST_DISK_TIER_OID);
        assertEquals(18.0, adjustedAmount, 0.001);
        // If price table is not available, return the given storage amount.
        adjustedAmount = storagePriceStructure.getEndRangeInPriceTier(17.0, 1L, 2L, 1111L);
        assertEquals(17, adjustedAmount, 0.001);
    }

    public Map<Long, StorageTierPriceList> createPriceListMap() {
        Map<Long, StorageTierPriceList> priceListMap = new HashMap<>();
        final Builder standHdd = StorageTierPriceList.newBuilder();
        assertTrue(TestUtils.loadProtobufBuilder("/savings/storageTierPriceStandardHDD.json",
                standHdd, null));
        priceListMap.put(STANDARD_HDD_DISK_TIER_OID, (StorageTierPriceList)(standHdd.build()));
        final Builder ultra = StorageTierPriceList.newBuilder();
        assertTrue(TestUtils.loadProtobufBuilder("/savings/storageTierPriceNoEndRange.json",
                ultra, null));
        priceListMap.put(TEST_DISK_TIER_OID, (StorageTierPriceList)(ultra.build()));
        return priceListMap;
    }
}
