package com.vmturbo.mediation.azure.pricing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import com.vmturbo.mediation.azure.pricing.resolver.ResolvedMeter;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor.MeterType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.PricingDTO.IpPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.IpPriceList.IpConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.Builder;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable.OnDemandPriceTableByRegionEntry;

/**
 * Unit tests for PricingWorkspace.
 */
public class PricingWorkspaceTest {

    private static final String EASTUS_ENTITY_ID = "azure::eastus::DC::eastus";
    private static final String WESTUS_ENTITY_ID = "azure::westus::DC::westus";

    /**
     * Test the constructor.
     */
    @Test
    public void testConstructor() {
        Map<MeterType, List<ResolvedMeter>> resolvedMeters = new HashMap<>();
        PricingWorkspace workspace = new PricingWorkspace(resolvedMeters);
        assertSame(resolvedMeters, workspace.getResolvedMeterByMeterType());
        assertEquals(0, workspace.getResolvedMeterByMeterType().size());
        Map<String, PriceTable> result = workspace.build();
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    /**
     * Test the builder by plan related functionality.
     */
    @Test
    public void testGetPriceTableBuilderByPlanId() {
        PricingWorkspace workspace = new PricingWorkspace(Collections.emptyMap());

        // If we ask for a plans that haven't been added yet, builders are created

        final Builder foo = workspace.getPriceTableBuilderForPlan("foo");
        final Builder bar = workspace.getPriceTableBuilderForPlan("bar");

        // If we ask for plans that have already been added, we get the existing ones

        final Builder foo2 = workspace.getPriceTableBuilderForPlan("foo");
        final Builder bar2 = workspace.getPriceTableBuilderForPlan("bar");
        assertSame(foo, foo2);
        assertSame(bar, bar);

        // Verify the expected plans are present

        Map<String, PriceTable> result = workspace.build();

        List<String> plans = result.keySet().stream().sorted().collect(Collectors.toList());
        assertEquals(ImmutableList.of("bar", "foo"), plans);
    }

    /**
     * Test the getOnDemandBuilder method.
     */
    @Test
    public void testGetOnDemandBuilder() {
        PricingWorkspace workspace = new PricingWorkspace(Collections.emptyMap());

        // Get builders for some regions for some plans

        OnDemandPriceTableByRegionEntry.Builder p1EastUS = workspace.getOnDemandBuilder("plan1", "eastus");
        OnDemandPriceTableByRegionEntry.Builder p1WestUS = workspace.getOnDemandBuilder("plan1", "westus");
        OnDemandPriceTableByRegionEntry.Builder p2EastUS = workspace.getOnDemandBuilder("plan2", "eastus");
        OnDemandPriceTableByRegionEntry.Builder p2WestUS = workspace.getOnDemandBuilder("plan2", "westus");

        assertNotSame(p1EastUS, p2EastUS);
        assertNotSame(p1WestUS, p2WestUS);
        assertNotSame(p1EastUS, p1WestUS);
        assertNotSame(p2EastUS, p2WestUS);
        assertNotSame(p1EastUS, p2WestUS);
        assertNotSame(p2EastUS, p1WestUS);

        p1EastUS.setIpPrices(makeIpPriceList(1));
        p1WestUS.setIpPrices(makeIpPriceList(2));
        p2EastUS.setIpPrices(makeIpPriceList(3));
        p2WestUS.setIpPrices(makeIpPriceList(4));

        // Verify that if we get them again, we get the same builders, not new ones

        final OnDemandPriceTableByRegionEntry.Builder p1EastUS2 = workspace.getOnDemandBuilder("plan1", "eastus");
        final OnDemandPriceTableByRegionEntry.Builder p1WestUS2 = workspace.getOnDemandBuilder("plan1", "westus");
        final OnDemandPriceTableByRegionEntry.Builder p2EastUS2 = workspace.getOnDemandBuilder("plan2", "eastus");
        final OnDemandPriceTableByRegionEntry.Builder p2WestUS2 = workspace.getOnDemandBuilder("plan2", "westus");

        assertSame(p1EastUS, p1EastUS2);
        assertSame(p1WestUS, p1WestUS2);
        assertSame(p2EastUS, p2EastUS2);
        assertSame(p2WestUS, p2WestUS2);

        // Build the price tables and verify that the on demand data was included

        Map<String, PriceTable> result = workspace.build();

        PriceTable plan1 = result.get("plan1");
        PriceTable plan2 = result.get("plan2");

        assertNotNull(plan1);
        assertNotNull(plan2);
        assertNotSame(plan1, plan2);

        // Check the regions have been populated in the builders

        List<EntityDTO> p1regions = plan1.getOnDemandPriceTableList().stream()
            .map(OnDemandPriceTableByRegionEntry::getRelatedRegion).collect(Collectors.toList());
        List<EntityDTO> p2regions = plan2.getOnDemandPriceTableList().stream()
                .map(OnDemandPriceTableByRegionEntry::getRelatedRegion).collect(Collectors.toList());

        assertTrue(p1regions.stream().map(EntityDTO::getEntityType).allMatch(EntityType.REGION::equals));
        assertTrue(p2regions.stream().map(EntityDTO::getEntityType).allMatch(EntityType.REGION::equals));

        assertEquals(EASTUS_ENTITY_ID + "," + WESTUS_ENTITY_ID, p1regions.stream()
                .map(EntityDTO::getId).sorted().collect(Collectors.joining(",")));
        assertEquals(EASTUS_ENTITY_ID + "," + WESTUS_ENTITY_ID, p2regions.stream()
                .map(EntityDTO::getId).sorted().collect(Collectors.joining(",")));

        // Check the On Demand price tables are present

        List<OnDemandPriceTableByRegionEntry> onDemand1 = plan1.getOnDemandPriceTableList();
        List<OnDemandPriceTableByRegionEntry> onDemand2 = plan2.getOnDemandPriceTableList();

        assertEquals(2, onDemand1.size());
        assertEquals(2, onDemand2.size());

        // Check that the changes to the builders were applied

        assertEquals( "1,2",
            onDemand1.stream().map(OnDemandPriceTableByRegionEntry::getIpPrices)
                .map(prices -> prices.getIpPrice(0).getFreeIpCount())
                .map(String::valueOf)
                .sorted()
                .collect(Collectors.joining(",")));

        assertEquals( "3,4",
            onDemand2.stream().map(OnDemandPriceTableByRegionEntry::getIpPrices)
                .map(prices -> prices.getIpPrice(0).getFreeIpCount())
                .map(String::valueOf)
                .sorted()
                .collect(Collectors.joining(",")));
    }

    @NotNull
    private IpPriceList makeIpPriceList(int freeIps) {
        return IpPriceList.newBuilder().addIpPrice(
            IpConfigPrice.newBuilder()
                .setFreeIpCount(freeIps)
                .build()
            ).build();
    }
}
