package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.Entity;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test cases for {@link SearchMapper}.
 * @author shai
 *
 */
public class SearchMapperTest {

    static final String FOO = "foo";
    static final String BAR = "bar";
    static final String VM = "VirtualMachine";

    @Test
    public void testNameFilter() {
        PropertyFilter fooSearch = SearchMapper.nameFilter(FOO);
        assertEquals("displayName", fooSearch.getPropertyName());
        assertEquals("^" + FOO + "$", fooSearch.getStringFilter().getStringPropertyRegex());
    }

    @Test
    public void testNameFilterWithMatch() {
        PropertyFilter fooSearch = SearchMapper.nameFilter(FOO, false, false);
        assertEquals("displayName", fooSearch.getPropertyName());
        assertEquals("^" + FOO + "$", fooSearch.getStringFilter().getStringPropertyRegex());
        assertFalse(fooSearch.getStringFilter().getMatch());
    }

    @Test
    public void testEntityFilter() {
        PropertyFilter vmSearch = SearchMapper.entityTypeFilter(VM);
        assertEquals("entityType", vmSearch.getPropertyName());
        assertEquals(ServiceEntityMapper.fromUIEntityType(VM),
            vmSearch.getNumericFilter().getValue());
    }

    @Test
    public void testPropertyFilter() {
        PropertyFilter fooBar = SearchMapper.stringPropertyFilter(BAR, FOO);
        assertEquals(BAR, fooBar.getPropertyName());
        assertEquals("^" + FOO + "$", fooBar.getStringFilter().getStringPropertyRegex());
    }

    @Test
    public void testStringFilterNoDoublePrefix() {
        assertThat(
            SearchMapper.stringFilter("^val", true, false).getStringPropertyRegex(),
            // No extra "^" prefix.
            is("^val$"));
    }

    @Test
    public void testStringFilterNoDoubleSuffix() {
        assertThat(
            SearchMapper.stringFilter("val$", true, false).getStringPropertyRegex(),
            // No extra "$" suffix.
            is("^val$"));
    }

    @Test
    public void testTraverseToType() {
        TraversalFilter traversalFilter =
                        SearchMapper.traverseToType(TraversalDirection.CONSUMES, VM);
        assertEquals(TraversalDirection.CONSUMES, traversalFilter.getTraversalDirection());
        assertEquals(SearchMapper.numericPropertyFilter("entityType",
            ServiceEntityMapper.fromUIEntityType(VM), ComparisonOperator.EQ),
            traversalFilter.getStoppingCondition().getStoppingPropertyFilter());
    }

    @Test
    public void testSearchProperty() {
        PropertyFilter fooBar = SearchMapper.stringPropertyFilter(BAR, FOO);
        SearchFilter searchFilter = SearchMapper.searchFilterProperty(fooBar);
        assertEquals(fooBar, searchFilter.getPropertyFilter());
    }

    @Test
    public void testSearchTraversal() {
        TraversalFilter traversalFilter =
                        SearchMapper.traverseToType(TraversalDirection.CONSUMES, VM);
        SearchFilter searchFilter = SearchMapper.searchFilterTraversal(traversalFilter);
        assertEquals(traversalFilter, searchFilter.getTraversalFilter());

    }

    private static long oid = 123456;
    private static int state = EntityState.MAINTENANCE_VALUE; // MAINTENANCE
    private static String displayName = "foo";
    private static int vmType = EntityType.VIRTUAL_MACHINE_VALUE;

    @Test
    public void testSeDTO() {
        Entity entity = Entity.newBuilder()
                    .setOid(oid)
                    .setState(state)
                    .setDisplayName(displayName)
                    .setType(vmType)
                    .build();
        ServiceEntityApiDTO seDTO = SearchMapper.seDTO(entity, Collections.emptyMap());
        assertEquals(displayName, seDTO.getDisplayName());
        assertEquals(String.valueOf(oid), seDTO.getUuid());
        assertEquals("MAINTENANCE", seDTO.getState());
        assertEquals(ServiceEntityMapper.UIEntityType.VIRTUAL_MACHINE.getValue(), seDTO.getClassName());
    }

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
            "Container", "ContainerPod", "StorageController", "IOModule", "Switch", "Chassis",
            "Network", "LogicalPool", "Database", "DatabaseServer", "LoadBalancer",
            "BusinessAccount", "CloudService", "ComputeTier", "StorageTier", "DatabaseTier",
            "DatabaseServerTier", "AvailabilityZone", "Region", "VirtualVolume", "ProcessorPool");
        assertEquals(EXPECTED_TYPES, SearchMapper.SEARCH_ALL_TYPES);
    }

    /**
     * This test verifies that the expression value that comes from the UI is translated properly
     * into a map filter, in various cases.
     */
    @Test
    public void testMapFilter() {
        final PropertyFilter filter1 = SearchMapper.mapPropertyFilterForMultimaps("Prop", "AA=B");
        assertTrue(filter1.hasMapFilter());
        assertEquals("Prop", filter1.getPropertyName());
        assertEquals("AA", filter1.getMapFilter().getKey());
        assertEquals(1, filter1.getMapFilter().getValuesCount());
        assertEquals("B", filter1.getMapFilter().getValues(0));

        final PropertyFilter filter2 =
                SearchMapper.mapPropertyFilterForMultimaps("Prop", "AA=BB|AA=CC");
        assertTrue(filter2.hasMapFilter());
        assertEquals("Prop", filter2.getPropertyName());
        assertEquals("AA", filter2.getMapFilter().getKey());
        assertEquals(2, filter2.getMapFilter().getValuesCount());
        assertTrue(filter2.getMapFilter().getValuesList().contains("BB"));
        assertTrue(filter2.getMapFilter().getValuesList().contains("CC"));

        final PropertyFilter filter3 = SearchMapper.mapPropertyFilterForMultimaps("Prop", "AA=");
        assertTrue(filter3.hasMapFilter());
        assertEquals("Prop", filter3.getPropertyName());
        assertEquals("AA", filter3.getMapFilter().getKey());
        assertEquals(0, filter3.getMapFilter().getValuesCount());

        assertIsEmptyMapFilter(SearchMapper.mapPropertyFilterForMultimaps("Prop", "AA=B|DD"));
        assertIsEmptyMapFilter(SearchMapper.mapPropertyFilterForMultimaps("Prop", "AA=BB|C=D"));
        assertIsEmptyMapFilter(SearchMapper.mapPropertyFilterForMultimaps("Prop", "=B|foo=DD"));
    }

    private void assertIsEmptyMapFilter(PropertyFilter filter) {
        assertTrue(filter.hasMapFilter());
        assertEquals("Prop", filter.getPropertyName());
        assertEquals("", filter.getMapFilter().getKey());
        assertEquals(0, filter.getMapFilter().getValuesCount());
    }
}
