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
        assertEquals(0, fooSearch.getStringFilter().getOptionsCount());
    }

    @Test
    public void testNameFilterWithMatch() {
        PropertyFilter fooSearch = SearchMapper.nameFilter(FOO, false, false);
        assertEquals("displayName", fooSearch.getPropertyName());
        assertEquals("^" + FOO + "$", fooSearch.getStringFilter().getStringPropertyRegex());
        assertEquals(0, fooSearch.getStringFilter().getOptionsCount());
    }

    @Test
    public void testEntityFilter() {
        PropertyFilter vmSearch = SearchMapper.entityTypeFilter(VM);
        assertEquals("entityType", vmSearch.getPropertyName());
        assertEquals(ServiceEntityMapper.fromUIEntityType(VM),
            vmSearch.getNumericFilter().getValue());
    }

    @Test
    public void testRegexPropertyFilter() {
        PropertyFilter fooBar = SearchMapper.stringPropertyFilterRegex(BAR, FOO);
        assertEquals(BAR, fooBar.getPropertyName());
        assertEquals("^" + FOO + "$", fooBar.getStringFilter().getStringPropertyRegex());
    }

    @Test
    public void testPropertyFilter() {
        PropertyFilter fooBar = SearchMapper.stringPropertyFilterExact(BAR, Collections.singletonList(FOO));
        assertEquals(BAR, fooBar.getPropertyName());
        assertEquals(1, fooBar.getStringFilter().getOptionsCount());
        assertEquals(FOO, fooBar.getStringFilter().getOptions(0));
        assertFalse(fooBar.getStringFilter().hasStringPropertyRegex());
    }

    @Test
    public void testStringFilterNoDoublePrefix() {
        assertThat(
            SearchMapper.stringFilterRegex("^val", true, false).getStringPropertyRegex(),
            // No extra "^" prefix.
            is("^val$"));
    }

    @Test
    public void testStringFilterNoDoubleSuffix() {
        assertThat(
            SearchMapper.stringFilterRegex("val$", true, false).getStringPropertyRegex(),
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
        PropertyFilter fooBar = SearchMapper.stringPropertyFilterRegex(BAR, FOO);
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

    @Test
    public void testStateFilter() {
        final PropertyFilter propertyFilter = SearchMapper.stateFilter("a|b|c", true);
        assertEquals(SearchMapper.STATE_PROPERTY, propertyFilter.getPropertyName());
        assertFalse(propertyFilter.getStringFilter().hasStringPropertyRegex());
        assertEquals(3, propertyFilter.getStringFilter().getOptionsCount());
        assertEquals("a", propertyFilter.getStringFilter().getOptions(0));
        assertEquals("b", propertyFilter.getStringFilter().getOptions(1));
        assertEquals("c", propertyFilter.getStringFilter().getOptions(2));
    }
}
