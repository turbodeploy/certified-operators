package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.search.Search.PropertyFilter;

/**
 * Test cases for {@link SearchMapper}.
 * @author shai
 *
 */
public class SearchMapperTest {
    /**
     * This test verifies that changes in the types specified in {@link ServiceEntityMapper}
     * will not affect {@link SearchMapper#SEARCH_ALL_TYPES} (and if they do affect then the
     * test will fail).
     */
    @Test
    public void testSearchAllTypes() {
        ImmutableList<String> EXPECTED_TYPES = ImmutableList.of(
            "VirtualMachine", "PhysicalMachine", "Storage", "DiskArray", "DataCenter", "VirtualDataCenter",
            "BusinessApplication", "ApplicationServer", "Application", "VirtualApplication",
            "Container", "ContainerPod", "VPod", "DPod", "StorageController", "IOModule", "Switch", "Chassis",
            "Network", "LogicalPool", "Database", "DatabaseServer", "LoadBalancer",
            "BusinessAccount", "CloudService", "ComputeTier", "StorageTier", "DatabaseTier",
            "DatabaseServerTier", "AvailabilityZone", "Region", "VirtualVolume", "ProcessorPool",
            "ViewPod", "DesktopPool", "BusinessUser");
        assertThat(SearchMapper.SEARCH_ALL_TYPES, containsInAnyOrder(EXPECTED_TYPES.toArray()));
    }

    /**
     * This test verifies that the expression value that comes from the UI is translated properly
     * into a map filter, in various cases.
     */
    @Test
    public void testMapFilter() {
        final PropertyFilter filter1 = SearchMapper.mapPropertyFilterForMultimapsExact("Prop", "AA=B", true);
        assertTrue(filter1.hasMapFilter());
        assertEquals("Prop", filter1.getPropertyName());
        assertEquals("AA", filter1.getMapFilter().getKey());
        assertEquals(1, filter1.getMapFilter().getValuesCount());
        assertEquals("B", filter1.getMapFilter().getValues(0));
        assertTrue(filter1.getMapFilter().getPositiveMatch());
        assertFalse(filter1.getMapFilter().hasRegex());

        final PropertyFilter filter2 =
                SearchMapper.mapPropertyFilterForMultimapsExact("Prop", "AA=BB|AA=CC", false);
        assertTrue(filter2.hasMapFilter());
        assertEquals("Prop", filter2.getPropertyName());
        assertEquals("AA", filter2.getMapFilter().getKey());
        assertEquals(2, filter2.getMapFilter().getValuesCount());
        assertTrue(filter2.getMapFilter().getValuesList().contains("BB"));
        assertTrue(filter2.getMapFilter().getValuesList().contains("CC"));
        assertFalse(filter2.getMapFilter().getPositiveMatch());
        assertFalse(filter2.getMapFilter().hasRegex());

        final PropertyFilter filter3 = SearchMapper.mapPropertyFilterForMultimapsExact("Prop", "AA=", true);
        assertTrue(filter3.hasMapFilter());
        assertEquals("Prop", filter3.getPropertyName());
        assertEquals("AA", filter3.getMapFilter().getKey());
        assertEquals(0, filter3.getMapFilter().getValuesCount());
        assertFalse(filter3.getMapFilter().hasRegex());

        final PropertyFilter filter4 =
                SearchMapper.mapPropertyFilterForMultimapsRegex("Prop", "k", ".*", false);
        assertTrue(filter4.hasMapFilter());
        assertEquals("Prop", filter4.getPropertyName());
        assertEquals("k", filter4.getMapFilter().getKey());
        assertEquals(0, filter4.getMapFilter().getValuesCount());
        assertEquals("^.*$", filter4.getMapFilter().getRegex());
        assertFalse(filter4.getMapFilter().getPositiveMatch());

        final PropertyFilter filter5 =
                SearchMapper.mapPropertyFilterForMultimapsRegex("Prop", "k", ".*", true);
        assertTrue(filter5.hasMapFilter());
        assertEquals("Prop", filter5.getPropertyName());
        assertEquals("k", filter5.getMapFilter().getKey());
        assertEquals(0, filter5.getMapFilter().getValuesCount());
        assertEquals("^.*$", filter5.getMapFilter().getRegex());
        assertTrue(filter5.getMapFilter().getPositiveMatch());

        assertIsEmptyMapFilter(SearchMapper.mapPropertyFilterForMultimapsExact("Prop", "AA=B|DD", false));
        assertIsEmptyMapFilter(SearchMapper.mapPropertyFilterForMultimapsExact("Prop", "AA=BB|C=D", false));
        assertIsEmptyMapFilter(SearchMapper.mapPropertyFilterForMultimapsExact("Prop", "=B|foo=DD", false));
    }

    private void assertIsEmptyMapFilter(PropertyFilter filter) {
        assertTrue(filter.hasMapFilter());
        assertEquals("Prop", filter.getPropertyName());
        assertEquals("", filter.getMapFilter().getKey());
        assertEquals(0, filter.getMapFilter().getValuesCount());
    }
}
