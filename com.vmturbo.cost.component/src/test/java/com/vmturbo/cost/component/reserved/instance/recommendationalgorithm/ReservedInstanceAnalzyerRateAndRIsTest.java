package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Table;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstancePriceTable;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalyzerRateAndRIs.ReservedInstanceSpecKey;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.PricingDTO;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList.ComputeTierConfigPrice;

/**
 * Test the methods of the ReservedInstanceAnalzyerRateAndRIs class.
 */
public class ReservedInstanceAnalzyerRateAndRIsTest {

    static final PriceTableStore priceTableStoreArg = Mockito.mock(PriceTableStore.class);
    static final ReservedInstanceSpecStore riSpecStoreArg = Mockito.mock(ReservedInstanceSpecStore.class);
    static final ReservedInstanceBoughtStore riBoughtStoreArg = Mockito.mock(ReservedInstanceBoughtStore.class);
    static final ReservedInstanceAnalyzerRateAndRIs rateAndRIsProvider = Mockito.spy(ReservedInstanceAnalyzerRateAndRIs.class);

    static final String logTag = "testLogTag";

    /**
     * Test ReservedInstanceSpecInfo constructor, hashCode and Equals.
     */
    @Test
    public void testReservedInstanceSpecInfo() {
        ReservedInstanceSpecInfo specInfo = ReservedInstanceSpecInfo.newBuilder()
            .setOs(OSType.LINUX)
            .setTenancy(Tenancy.DEFAULT)
            .setTierId(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_M5_LARGE_OID)
            .setRegionId(ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OHIO_OID)
            .setType(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_TYPE_1)
            .setSizeFlexible(ReservedInstanceAnalyzerConstantsTest.SIZE_FLEXIBLE_TRUE)
            .build();
        int hashCode = specInfo.hashCode();
        int hashCode1 = ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_1.hashCode();
        Assert.assertTrue(hashCode == hashCode1);
        Assert.assertTrue(specInfo.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_1));
        Assert.assertTrue(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_1.equals(specInfo));

        int hashCode2 = ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_2.hashCode();
        Assert.assertFalse(hashCode == hashCode2);
        Assert.assertFalse(specInfo.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_2));
        Assert.assertFalse(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_2.equals(specInfo));

        int hashCode3 = ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_3.hashCode();
        Assert.assertFalse(hashCode == hashCode3);
        Assert.assertFalse(specInfo.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_3));
        Assert.assertFalse(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_3.equals(specInfo));

        int hashCode4 = ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_4.hashCode();
        Assert.assertFalse(hashCode == hashCode4);
        Assert.assertFalse(specInfo.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_4));
        Assert.assertFalse(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_4.equals(specInfo));
    }

    /**
     * Test ReservedInstanceSpec constructor, hashCode and Equals.
     */
    @Test
    public void testReservedInstanceSpec() {
        ReservedInstanceSpec spec = ReservedInstanceSpec.newBuilder()
            .setId(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_SPEC_ID_1)
            .setReservedInstanceSpecInfo(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_INFO_1).build();
        int hashCode = spec.hashCode();
        int hashCode1 = ReservedInstanceAnalyzerConstantsTest.RI_SPEC_1.hashCode();
        Assert.assertTrue(hashCode == hashCode1);
        Assert.assertTrue(spec.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_1));
        Assert.assertTrue(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_1.equals(spec));

        int hashCode2 = ReservedInstanceAnalyzerConstantsTest.RI_SPEC_2.hashCode();
        Assert.assertFalse(hashCode == hashCode2);
        Assert.assertFalse(spec.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_2));
        Assert.assertFalse(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_2.equals(spec));

        int hashCode3 = ReservedInstanceAnalyzerConstantsTest.RI_SPEC_3.hashCode();
        Assert.assertFalse(hashCode1 == hashCode3);
        Assert.assertFalse(spec.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_3));
        Assert.assertFalse(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_3.equals(spec));

        int hashCode4 = ReservedInstanceAnalyzerConstantsTest.RI_SPEC_4.hashCode();
        Assert.assertFalse(hashCode1 == hashCode4);
        Assert.assertFalse(spec.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_4));
        Assert.assertFalse(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_4.equals(spec));
    }

    /**
     * Test ReservedInstanceSpecKey constructors, hashCode and equals methods.
     */
    @Test
    public void testReservedInstanceSpecKey() {
        ReservedInstanceSpecKey key = new ReservedInstanceSpecKey(ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OHIO_OID,
            ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_M5_LARGE_OID,
            OSType.LINUX,
            Tenancy.DEFAULT,
            true,
            ReservedInstanceAnalyzerConstantsTest.PURCHASE_CONSTRAINTS_1);
        int hashCode = key.hashCode();
        int hashCode1 = ReservedInstanceAnalyzerConstantsTest.RI_SPEC_KEY_1.hashCode();
        Assert.assertTrue(hashCode == hashCode1);
        Assert.assertTrue(key.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_KEY_1));
        Assert.assertTrue(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_KEY_1.equals(key));

        Assert.assertFalse(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_KEY_1
            .equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_KEY_2));
        Assert.assertFalse(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_KEY_1
            .equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_KEY_3));
        Assert.assertFalse(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_KEY_1
            .equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_KEY_4));

        Assert.assertFalse(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_KEY_2
            .equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_KEY_3));
        Assert.assertFalse(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_KEY_2
            .equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_KEY_4));

        Assert.assertFalse(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_KEY_3
            .equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_KEY_4));
    }

    /**
     * test populateReservedInstanceSpecKeyMap and lookupReservedInstanceSpec for a single store entry.
     */
    @Test
    public void testReservedInstanceSpecKeyMapForOneEntry() {

        List<ReservedInstanceSpec> specs = new ArrayList<>();
        specs.add(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_1);
        ReservedInstanceSpecStore store = Mockito.spy(ReservedInstanceSpecStore.class);
        Mockito.doReturn(specs).when(store).getAllReservedInstanceSpec();

        // populate
        rateAndRIsProvider.populateReservedInstanceSpecKeyMap(store);

        // lookup
        ReservedInstanceSpec spec = rateAndRIsProvider.lookupReservedInstanceSpec(ReservedInstanceAnalyzerConstantsTest.REGIONAL_CONTEXT_1,
            ReservedInstanceAnalyzerConstantsTest.PURCHASE_CONSTRAINTS_1);
        Assert.assertTrue(spec != null);
        Assert.assertTrue(spec.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_1));
    }

    /**
     * test populateReservedInstanceSpecKeyMap and lookupReservedInstanceSpec for multiple store entries.
     */
    @Test
    public void testReservedInstanceSpecKeyMapForMultipleEntries() {
        /*
         * Build store
         */
        List<ReservedInstanceSpec> specs = new ArrayList<>();
        specs.add(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_1);
        specs.add(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_2);
        specs.add(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_3);
        specs.add(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_4);
        ReservedInstanceSpecStore store = Mockito.spy(ReservedInstanceSpecStore.class);
        Mockito.doReturn(specs).when(store).getAllReservedInstanceSpec();

        /*
         * populate
         */
        rateAndRIsProvider.populateReservedInstanceSpecKeyMap(store);

        // lookup
        ReservedInstanceSpec spec = rateAndRIsProvider.lookupReservedInstanceSpec(ReservedInstanceAnalyzerConstantsTest.REGIONAL_CONTEXT_1,
            ReservedInstanceAnalyzerConstantsTest.PURCHASE_CONSTRAINTS_1);
        Assert.assertTrue(spec != null);
        Assert.assertTrue(spec.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_1));
        spec = rateAndRIsProvider.lookupReservedInstanceSpec(ReservedInstanceAnalyzerConstantsTest.REGIONAL_CONTEXT_1,
            ReservedInstanceAnalyzerConstantsTest.PURCHASE_CONSTRAINTS_2);
        Assert.assertTrue(spec != null);
        Assert.assertTrue(spec.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_2));
        spec = rateAndRIsProvider.lookupReservedInstanceSpec(ReservedInstanceAnalyzerConstantsTest.REGIONAL_CONTEXT_2,
            ReservedInstanceAnalyzerConstantsTest.PURCHASE_CONSTRAINTS_1);
        Assert.assertTrue(spec != null);
        Assert.assertTrue(spec.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_3));
        spec = rateAndRIsProvider.lookupReservedInstanceSpec(ReservedInstanceAnalyzerConstantsTest.REGIONAL_CONTEXT_2,
            ReservedInstanceAnalyzerConstantsTest.PURCHASE_CONSTRAINTS_2);
        Assert.assertTrue(spec != null);
        Assert.assertTrue(spec.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_4));
    }

    /**
     * test populateReservedInstanceSpecIdMap and lookupReservedInstanceSpec for a single store entry.
     */
    @Test
    public void testReservedInstanceSpecIdMapForOneEntry() {

        List<ReservedInstanceSpec> specs = new ArrayList<>();
        specs.add(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_1);
        ReservedInstanceSpecStore store = Mockito.spy(ReservedInstanceSpecStore.class);
        Mockito.doReturn(specs).when(store).getAllReservedInstanceSpec();

        // populate
        rateAndRIsProvider.populateReservedInstanceSpecIdMap(store);

        // lookup
        ReservedInstanceSpec spec = rateAndRIsProvider.lookupReservedInstanceSpecWithId(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_SPEC_ID_1);
        Assert.assertTrue(spec != null);
        Assert.assertTrue(spec.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_1));
        spec = rateAndRIsProvider.lookupReservedInstanceSpecWithId(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_1.getId());
        Assert.assertTrue(spec != null);
        Assert.assertTrue(spec.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_1));
    }

    /**
     * test populateReservedInstanceSpecIdMap and lookupReservedInstanceSpec for multiple store entries.
     */
    @Test
    public void testReservedInstanceSpecIdMapForMultipleEntries() {

        List<ReservedInstanceSpec> specs = new ArrayList<>();
        specs.add(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_1);
        specs.add(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_2);
        specs.add(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_3);
        specs.add(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_4);
        ReservedInstanceSpecStore store = Mockito.spy(ReservedInstanceSpecStore.class);
        Mockito.doReturn(specs).when(store).getAllReservedInstanceSpec();

        // populate
        rateAndRIsProvider.populateReservedInstanceSpecIdMap(store);

        // lookup
        ReservedInstanceSpec spec = rateAndRIsProvider.lookupReservedInstanceSpecWithId(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_1.getId());
        Assert.assertTrue(spec != null);
        Assert.assertTrue(spec.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_1));
        spec = rateAndRIsProvider.lookupReservedInstanceSpecWithId(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_SPEC_ID_1);
        Assert.assertTrue(spec != null);
        Assert.assertTrue(spec.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_1));

        spec = rateAndRIsProvider.lookupReservedInstanceSpecWithId(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_2.getId());
        Assert.assertTrue(spec != null);
        Assert.assertTrue(spec.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_2));
        spec = rateAndRIsProvider.lookupReservedInstanceSpecWithId(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_SPEC_ID_2);
        Assert.assertTrue(spec != null);
        Assert.assertTrue(spec.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_2));

        spec = rateAndRIsProvider.lookupReservedInstanceSpecWithId(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_3.getId());
        Assert.assertTrue(spec != null);
        Assert.assertTrue(spec.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_3));
        spec = rateAndRIsProvider.lookupReservedInstanceSpecWithId(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_SPEC_ID_3);
        Assert.assertTrue(spec != null);
        Assert.assertTrue(spec.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_3));

        spec = rateAndRIsProvider.lookupReservedInstanceSpecWithId(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_4.getId());
        Assert.assertTrue(spec != null);
        Assert.assertTrue(spec.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_4));
        spec = rateAndRIsProvider.lookupReservedInstanceSpecWithId(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_SPEC_ID_4);
        Assert.assertTrue(spec != null);
        Assert.assertTrue(spec.equals(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_4));

    }

    /**
     * test populateReservedInstanceBoughtInfoTable, lookupRegionalReservedInstancesBoughtInfo, and
     * and lookupReservedInstanceBoughtInfos for one store entry and zonal RI.
     */
    @Test
    public void testReservedInstanceBoughtInfoTableForOneEntryWithZonalRI() {
        /*
         * build store
         */
        List<ReservedInstanceBought> list = new ArrayList<>();
        list.add(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_1);
        ReservedInstanceBoughtStore store = Mockito.mock(ReservedInstanceBoughtStore.class);
        Mockito.doReturn(list).when(store)
            .getReservedInstanceBoughtByFilter(Mockito.any(ReservedInstanceBoughtFilter.class));

        // populate
        rateAndRIsProvider.populateReservedInstanceBoughtInfoTable(store);

        /*
         * lookup by zone
         */
        List<ReservedInstanceBoughtInfo> infoList2 =
            rateAndRIsProvider.lookupReservedInstanceBoughtInfos(
                ReservedInstanceAnalyzerConstantsTest.MASTER_ACCOUNT_1_OID,
                ReservedInstanceAnalyzerConstantsTest.ZONE_AWS_OHIO_1_OID, logTag);
        Assert.assertTrue(infoList2 != null);
        Assert.assertTrue(infoList2.size() == 1);
        Assert.assertTrue(infoList2.contains(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_INFO_1));
    }

    /**
     * test populateReservedInstanceBoughtInfoTable, lookupRegionalReservedInstancesBoughtInfo, and
     * and lookupReservedInstanceBoughtInfos for one store entry and Regional RI.
     * TODO: figure out how to specify Regional RI in the table; that is, zonal ID is undefined.
     */
    @Test
    public void testReservedInstanceBoughtInfoTableForOneEntryWithRegionalRI() {
        /*
         * Build ReservedInstanceSpecStore.
         */
        List<ReservedInstanceSpec> specs = new ArrayList<>();
        specs.add(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_1);
        specs.add(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_2);
        specs.add(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_3);
        specs.add(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_4);
        ReservedInstanceSpecStore specStore = Mockito.spy(ReservedInstanceSpecStore.class);
        Mockito.doReturn(specs).when(specStore).getAllReservedInstanceSpec();

        // populate reservedInstanceSpecIdMap.
        rateAndRIsProvider.populateReservedInstanceSpecIdMap(specStore);

        /*
         * populate
         */
        List<ReservedInstanceBought> list = new ArrayList<>();
        list.add(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_REGIONAL_1);
        ReservedInstanceBoughtStore store = Mockito.mock(ReservedInstanceBoughtStore.class);
        Mockito.doReturn(list).when(store)
            .getReservedInstanceBoughtByFilter(Mockito.any(ReservedInstanceBoughtFilter.class));

        // populate reservedInstanceBoughtInfoTable
        rateAndRIsProvider.populateReservedInstanceBoughtInfoTable(store);

        /*
         * lookup by regional context
         */
        List<ReservedInstanceBoughtInfo> infoList3 =
            rateAndRIsProvider.lookupReservedInstancesBoughtInfos(ReservedInstanceAnalyzerConstantsTest.REGIONAL_CONTEXT_1, logTag);
        Assert.assertTrue(infoList3 != null);
        Assert.assertTrue(infoList3.size() == 1);
        Assert.assertTrue(infoList3.contains(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_INFO_REGIONAL_1));
    }

    /**
     * test populateReservedInstanceBoughtInfoTable, lookupRegionalReservedInstancesBoughtInfo, and
     * and lookupReservedInstanceBoughtInfos for multiple store entries and zonal RI.
     */
    @Test
    public void testReservedInstanceBoughtInfoTableForMultipleEntriesWithZonalRI() {
        /*
         * Build up ReservedInstanceBought objects.
         */
        List<ReservedInstanceBought> list = new ArrayList<>();
        list.add(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_1);
        list.add(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_2);
        list.add(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_3);
        list.add(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_4);
        ReservedInstanceBoughtStore store = Mockito.mock(ReservedInstanceBoughtStore.class);
        Mockito.doReturn(list).when(store)
            .getReservedInstanceBoughtByFilter(Mockito.any(ReservedInstanceBoughtFilter.class));

        /*
         * populate
         */
        rateAndRIsProvider.populateReservedInstanceBoughtInfoTable(store);

        /*
         * lookup by zone
         */
         List<ReservedInstanceBoughtInfo> lookupList1 =
            rateAndRIsProvider.lookupReservedInstanceBoughtInfos(
                ReservedInstanceAnalyzerConstantsTest.MASTER_ACCOUNT_1_OID,
                ReservedInstanceAnalyzerConstantsTest.ZONE_AWS_OHIO_1_OID, logTag);
        Assert.assertTrue(lookupList1 != null);
        Assert.assertTrue(lookupList1.size() == 1);
        Assert.assertTrue(lookupList1.contains(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_INFO_1));

        List<ReservedInstanceBoughtInfo> lookupList2 =
            rateAndRIsProvider.lookupReservedInstanceBoughtInfos(
                ReservedInstanceAnalyzerConstantsTest.MASTER_ACCOUNT_1_OID,
                ReservedInstanceAnalyzerConstantsTest.ZONE_AWS_OHIO_2_OID, logTag);
        Assert.assertTrue(lookupList2 != null);
        Assert.assertTrue(lookupList2.size() == 1);
        Assert.assertTrue(lookupList2.contains(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_INFO_2));

        List<ReservedInstanceBoughtInfo> lookupList3 =
            rateAndRIsProvider.lookupReservedInstanceBoughtInfos(
                ReservedInstanceAnalyzerConstantsTest.MASTER_ACCOUNT_2_OID,
                ReservedInstanceAnalyzerConstantsTest.ZONE_AWS_OREGON_1_OID, logTag);
        Assert.assertTrue(lookupList3 != null);
        Assert.assertTrue(lookupList3.size() == 2);
        Assert.assertTrue(lookupList3.contains(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_INFO_3));
        Assert.assertTrue(lookupList3.contains(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_INFO_4));
    }

    /**
     * test populateReservedInstanceBoughtInfoTable, lookupRegionalReservedInstancesBoughtInfo, and
     * and lookupReservedInstanceBoughtInfos for multiple store entries and regional RI.
     */
    @Test
    public void testReservedInstanceBoughtInfoTableForMultipleEntriesWithRegionalRI() {
        /*
         * Build ReservedInstanceSpecStore.
         */
        List<ReservedInstanceSpec> specs = new ArrayList<>();
        specs.add(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_1);
        specs.add(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_2);
        specs.add(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_3);
        specs.add(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_4);
        ReservedInstanceSpecStore specStore = Mockito.spy(ReservedInstanceSpecStore.class);
        Mockito.doReturn(specs).when(specStore).getAllReservedInstanceSpec();

        // populate reservedInstanceSpecIdMap.
        rateAndRIsProvider.populateReservedInstanceSpecIdMap(specStore);

        /*
         * Build ReservedInstanceBoughtStore.
         */
        List<ReservedInstanceBought> list = new ArrayList<>();
        list.add(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_REGIONAL_1);
        list.add(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_REGIONAL_2);
        list.add(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_REGIONAL_3);
        list.add(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_REGIONAL_4);
        ReservedInstanceBoughtStore store = Mockito.mock(ReservedInstanceBoughtStore.class);
        Mockito.doReturn(list).when(store)
            .getReservedInstanceBoughtByFilter(Mockito.any(ReservedInstanceBoughtFilter.class));

        /*
         * populate
         */
        rateAndRIsProvider.populateReservedInstanceBoughtInfoTable(store);

        /*
         * Lookup
         */
        List<ReservedInstanceBoughtInfo> infoList3 =
            rateAndRIsProvider.lookupReservedInstancesBoughtInfos(ReservedInstanceAnalyzerConstantsTest.REGIONAL_CONTEXT_1, logTag);
        Assert.assertTrue(infoList3 != null);
        Assert.assertTrue(infoList3.size() == 2);
        Assert.assertTrue(infoList3.contains(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_INFO_REGIONAL_1));
        Assert.assertTrue(infoList3.contains(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_INFO_REGIONAL_2));
        List<ReservedInstanceBoughtInfo> infoList4 =
            rateAndRIsProvider.lookupReservedInstancesBoughtInfos(ReservedInstanceAnalyzerConstantsTest.REGIONAL_CONTEXT_2, logTag);
        Assert.assertTrue(infoList4 != null);
        Assert.assertTrue(infoList4.size() == 2);
        Assert.assertTrue(infoList4.contains(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_INFO_REGIONAL_3));
        Assert.assertTrue(infoList4.contains(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_BOUGHT_INFO_REGIONAL_4));

    }


    /**
     * Tests methods to populate and lookup values in the ReservedInstanceRateMap.
     * This test depends on the reservedInstanceSpecKeyMap to look up the ReservedInstanceSpec and
     * the reservedInstancePriceMap to get the rate.
     */
    @Test
    public void  testReservedInstanceRateMap() {
        /*
         * Set up reservedInstanceSpecKeyMap
         */
        List<ReservedInstanceSpec> specs = new ArrayList<>();
        specs.add(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_1);
        specs.add(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_2);
        specs.add(ReservedInstanceAnalyzerConstantsTest.RI_SPEC_3);
        ReservedInstanceSpecStore specStore = Mockito.spy(ReservedInstanceSpecStore.class);
        Mockito.doReturn(specs).when(specStore).getAllReservedInstanceSpec();
        rateAndRIsProvider.populateReservedInstanceSpecKeyMap(specStore);

        /*
         * build reservedInstancePriceMap
         */
        // map from ReservedInstanceSpec to ReservedInstancePrice.
        Map<Long, PricingDTO.ReservedInstancePrice> map = new HashMap<>();
        PricingDTO.ReservedInstancePrice price1 = PricingDTO.ReservedInstancePrice.newBuilder()
            .setUpfrontPrice(ReservedInstanceAnalyzerConstantsTest.PRICE_UPFRONT_1)
            .setRecurringPrice(ReservedInstanceAnalyzerConstantsTest.PRICE_HOURLY_1).build();
        map.put(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_SPEC_ID_1, price1);
        PricingDTO.ReservedInstancePrice price2 = PricingDTO.ReservedInstancePrice.newBuilder()
            .setUpfrontPrice(ReservedInstanceAnalyzerConstantsTest.PRICE_UPFRONT_2)
            .setRecurringPrice(ReservedInstanceAnalyzerConstantsTest.PRICE_HOURLY_2).build();
        map.put(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_SPEC_ID_2, price2);
        PricingDTO.ReservedInstancePrice price3 = PricingDTO.ReservedInstancePrice.newBuilder()
            .setUpfrontPrice(ReservedInstanceAnalyzerConstantsTest.PRICE_UPFRONT_1)
            .setRecurringPrice(ReservedInstanceAnalyzerConstantsTest.PRICE_HOURLY_2).build();
        map.put(ReservedInstanceAnalyzerConstantsTest.RESERVED_INSTANCE_SPEC_ID_3, price3);

        ReservedInstancePriceTable riPriceTable = ReservedInstancePriceTable.newBuilder()
            .putAllRiPricesBySpecId(map).build();
        PriceTableStore store = Mockito.spy(PriceTableStore.class);
        Mockito.doReturn(riPriceTable).when(store).getMergedRiPriceTable();

        /*
         * populate
         */
        // Map: ReservedInstanceSpec ID to ReservedInstancePrice
        rateAndRIsProvider.populateReservedInstanceRateMap(store);

        /*
         * lookup
         */
        // RESERVED_INSTANCE_SPEC_ID_1
        Pair<Float, Float> riRate =
            rateAndRIsProvider.lookupReservedInstanceRate(ReservedInstanceAnalyzerConstantsTest.REGIONAL_CONTEXT_1,
                ReservedInstanceAnalyzerConstantsTest.PURCHASE_CONSTRAINTS_1,
                "RILT0001");
        Assert.assertTrue(riRate.getKey() != Float.MAX_VALUE);
        Assert.assertTrue(riRate.getValue() != Float.MAX_VALUE);
        // test for upfront cost.
        Assert.assertEquals(riRate.getKey(), 0.011415f, 0.0001f);
        // test for recurring cost.
        Assert.assertEquals(riRate.getValue(), 1f, 0.0001f);

        // RESERVED_INSTANCE_SPEC_ID_2
        riRate =
            rateAndRIsProvider.lookupReservedInstanceRate(ReservedInstanceAnalyzerConstantsTest.REGIONAL_CONTEXT_1,
                ReservedInstanceAnalyzerConstantsTest.PURCHASE_CONSTRAINTS_2,
                "RILT0002");
        Assert.assertTrue(riRate.getKey() != Float.MAX_VALUE);
        Assert.assertTrue(riRate.getValue() != Float.MAX_VALUE);
        Assert.assertEquals(riRate.getKey(), 0.00761D, 0.0001f);
        Assert.assertEquals(riRate.getValue(), 2D, 0.0001f);

        // RESERVED_INSTANCE_SPEC_ID_3
        riRate =
            rateAndRIsProvider.lookupReservedInstanceRate(ReservedInstanceAnalyzerConstantsTest.REGIONAL_CONTEXT_2,
                ReservedInstanceAnalyzerConstantsTest.PURCHASE_CONSTRAINTS_1,
                "RILT0003");
        Assert.assertTrue(riRate.getKey() != Float.MAX_VALUE);
        Assert.assertTrue(riRate.getValue() != Float.MAX_VALUE);
        Assert.assertEquals(riRate.getKey(), 0.0114155f, 0.0001f);
        Assert.assertEquals(riRate.getValue(), 2f, 0.0001f);
    }

    /**
     * Tests methods to populate and lookup values in the onDemandRateMap.
     *
     */
    @Test
    public void testOnDemandRateMap() {
        /*
         * build PriceTable
         */
        ComputeTierConfigPrice configPriceBase1 = ComputeTierConfigPrice.newBuilder()
            .setGuestOsType(OSType.LINUX)
            .setTenancy(Tenancy.DEFAULT)
            .addPrices(ReservedInstanceAnalyzerConstantsTest.PRICE_HOURLY_1)
            .build();
        ComputeTierConfigPrice configPriceAdjustment11 = ComputeTierConfigPrice.newBuilder()
            .setGuestOsType(OSType.RHEL)
            .setTenancy(Tenancy.DEFAULT)
            .addPrices(ReservedInstanceAnalyzerConstantsTest.PRICE_HOURLY_1)
            .build();
        ComputeTierConfigPrice configPriceAdjustment12 = ComputeTierConfigPrice.newBuilder()
            .setGuestOsType(OSType.WINDOWS)
            .setTenancy(Tenancy.DEFAULT)
            .addPrices(ReservedInstanceAnalyzerConstantsTest.PRICE_HOURLY_2)
            .build();
        ComputeTierConfigPrice configPriceAdjustment13 = ComputeTierConfigPrice.newBuilder()
            .setGuestOsType(OSType.SUSE)
            .setTenancy(Tenancy.DEFAULT)
            .addPrices(ReservedInstanceAnalyzerConstantsTest.PRICE_HOURLY_3)
            .build();
        List<ComputeTierConfigPrice> configPriceAdjustments1 =
            Arrays.asList(configPriceAdjustment11, configPriceAdjustment12, configPriceAdjustment13);

        ComputeTierConfigPrice configPriceBase2 = ComputeTierConfigPrice.newBuilder()
            .setGuestOsType(OSType.LINUX)
            .setTenancy(Tenancy.DEFAULT)
            .addPrices(ReservedInstanceAnalyzerConstantsTest.PRICE_HOURLY_2)
            .build();
        ComputeTierConfigPrice configPriceAdjustment21 = ComputeTierConfigPrice.newBuilder()
            .setGuestOsType(OSType.RHEL)
            .setTenancy(Tenancy.DEFAULT)
            .addPrices(ReservedInstanceAnalyzerConstantsTest.PRICE_HOURLY_2)
            .build();
        ComputeTierConfigPrice configPriceAdjustment22 = ComputeTierConfigPrice.newBuilder()
            .setGuestOsType(OSType.WINDOWS)
            .setTenancy(Tenancy.DEFAULT)
            .addPrices(ReservedInstanceAnalyzerConstantsTest.PRICE_HOURLY_3)
            .build();
        List<ComputeTierConfigPrice> configPriceAdjustments2 =
            Arrays.asList(configPriceAdjustment21, configPriceAdjustment22);

        ComputeTierConfigPrice configPriceBase3 = ComputeTierConfigPrice.newBuilder()
            .setGuestOsType(OSType.LINUX)
            .setTenancy(Tenancy.DEDICATED)
            .addPrices(ReservedInstanceAnalyzerConstantsTest.PRICE_HOURLY_3)
            .build();
        ComputeTierConfigPrice configPriceAdjustment31 = ComputeTierConfigPrice.newBuilder()
            .setGuestOsType(OSType.RHEL)
            .setTenancy(Tenancy.DEDICATED)
            .addPrices(ReservedInstanceAnalyzerConstantsTest.PRICE_HOURLY_3)
            .build();
        ComputeTierConfigPrice configPriceAdjustment32 = ComputeTierConfigPrice.newBuilder()
            .setGuestOsType(OSType.WINDOWS)
            .setTenancy(Tenancy.DEDICATED)
            .addPrices(ReservedInstanceAnalyzerConstantsTest.PRICE_HOURLY_4)
            .build();
        List<ComputeTierConfigPrice> configPriceAdjustments3 =
            Arrays.asList(configPriceAdjustment31, configPriceAdjustment32);

        ComputeTierPriceList computePriceList1 =  ComputeTierPriceList.newBuilder()
            .setBasePrice(configPriceBase1)
            .addAllPerConfigurationPriceAdjustments(configPriceAdjustments1).build();
        ComputeTierPriceList computePriceList2 =  ComputeTierPriceList.newBuilder()
            .setBasePrice(configPriceBase2)
            .addAllPerConfigurationPriceAdjustments(configPriceAdjustments2).build();
        ComputeTierPriceList computePriceList3 =  ComputeTierPriceList.newBuilder()
            .setBasePrice(configPriceBase3)
            .addAllPerConfigurationPriceAdjustments(configPriceAdjustments3).build();

        PriceTable priceTable = PriceTable.newBuilder()
            .putOnDemandPriceByRegionId(ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OHIO_OID,
                OnDemandPriceTable.newBuilder()
                .putComputePricesByTierId(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_T2_MICRO_OID,
                    computePriceList1)
                .putComputePricesByTierId(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_M5_LARGE_OID,
                    computePriceList2).build())
            .putOnDemandPriceByRegionId(ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OREGON_OID,
                OnDemandPriceTable.newBuilder()
                    .putComputePricesByTierId(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_T2_MICRO_OID,
                        computePriceList3)
                    .putComputePricesByTierId(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_M5_LARGE_OID,
                        computePriceList2).build())
            .build();

        /*
         * populate
         */
        PriceTableStore store = Mockito.spy(PriceTableStore.class);
        Mockito.doReturn(priceTable).when(store).getMergedPriceTable();
        rateAndRIsProvider.populateOnDemandRateMap(store);

        /*
         * lookup
         */
        // Ohio, m5.large, LINUX, DEFAULT
        float rate = rateAndRIsProvider.lookupOnDemandRate(
                        ReservedInstanceAnalyzerConstantsTest.REGIONAL_CONTEXT_1, "RILT0006");
        Assert.assertEquals(rate,
            new Double(ReservedInstanceAnalyzerConstantsTest.PRICE_HOURLY_2.getPriceAmount().getAmount()).floatValue(),
            0.001f);

        // Oregon, t2.micro, WINDOWS, DEDICATED
        rate = rateAndRIsProvider.lookupOnDemandRate(
                        ReservedInstanceAnalyzerConstantsTest.REGIONAL_CONTEXT_2, "RILT0007");
        // lINUX/DEDICATED is PRICE_HOURLY_3 and WINDOWS/DEDICATED is PRICE_HOURLY_4
        float result = new Double(ReservedInstanceAnalyzerConstantsTest.PRICE_HOURLY_3.getPriceAmount().getAmount()
            + ReservedInstanceAnalyzerConstantsTest.PRICE_HOURLY_4.getPriceAmount().getAmount()).floatValue();
        Assert.assertEquals(rate, result, 0.001f);

        // Oregon, m5.large, RHEL, DEFAULT
        rate = rateAndRIsProvider.lookupOnDemandRate(
                        ReservedInstanceAnalyzerConstantsTest.REGIONAL_CONTEXT_4, "RILT0008");
        // lINUX/DEDICATED is PRICE_HOURLY_3 and WINDOWS/DEDICATED is PRICE_HOURLY_4
        result = new Double(ReservedInstanceAnalyzerConstantsTest.PRICE_HOURLY_2.getPriceAmount()
                        .getAmount() * 2).floatValue();
        Assert.assertEquals(rate, result, 0.001f);

        // Ohio, t2.micro, SUSE, DEFAULT
        rate = rateAndRIsProvider.lookupOnDemandRate(
                        ReservedInstanceAnalyzerConstantsTest.REGIONAL_CONTEXT_5, "RILT0009");
        // lINUX/DEDICATED is PRICE_HOURLY_3 and WINDOWS/DEDICATED is PRICE_HOURLY_4
        result = new Double(ReservedInstanceAnalyzerConstantsTest.PRICE_HOURLY_3.getPriceAmount().getAmount()
            + ReservedInstanceAnalyzerConstantsTest.PRICE_HOURLY_1.getPriceAmount().getAmount()).floatValue();
        Assert.assertEquals(rate, result, 0.001f);



    }
}
