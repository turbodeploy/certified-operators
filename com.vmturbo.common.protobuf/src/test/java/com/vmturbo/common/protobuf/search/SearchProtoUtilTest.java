package com.vmturbo.common.protobuf.search;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import java.util.Collections;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.topology.ApiEntityType;

public class SearchProtoUtilTest {

    private static final String FOO = "foo";
    private static final String BAR = "bar";
    private static final String VM = "VirtualMachine";
    private static final ApiEntityType VM_TYPE = ApiEntityType.fromString(VM);

    /**
     * Check the value of {@link SearchProtoUtil#SEARCH_ALL_TYPES}.
     */
    @Test
    public void testSearchAllTypes() {
        ImmutableList<String> expectedTypes = ImmutableList.of(
                "VirtualMachine", "PhysicalMachine", "Storage", "DiskArray", "DataCenter", "VirtualDataCenter",
                "BusinessApplication", "ApplicationServer", "Application", "ApplicationComponent", "VirtualApplication",
                "Container", "ContainerPod", "VPod", "DPod", "StorageController", "IOModule", "Switch", "Chassis",
                "Network", "LogicalPool", "Database", "DatabaseServer", "LoadBalancer",
                "BusinessAccount", "CloudService", "ComputeTier", "StorageTier", "DatabaseTier",
                "DatabaseServerTier", "AvailabilityZone", "Region", "VirtualVolume", "ProcessorPool",
                "Service", "BusinessTransaction",
                "ViewPod", "DesktopPool", "BusinessUser", "ServiceProvider", "HCIPhysicalMachine",
                "Namespace", "WorkloadController", "ContainerSpec", "VMSpec",
                "ContainerPlatformCluster");
        assertThat(SearchProtoUtil.SEARCH_ALL_TYPES, containsInAnyOrder(expectedTypes.toArray()));
    }

    /**
     * Test name filters.
     */
    @Test
    public void testNameFilter() {
        PropertyFilter fooSearch = SearchProtoUtil.nameFilterRegex(FOO);
        assertEquals("displayName", fooSearch.getPropertyName());
        assertEquals("^" + FOO + "$", fooSearch.getStringFilter().getStringPropertyRegex());
        assertEquals(0, fooSearch.getStringFilter().getOptionsCount());
    }

    /**
     * Test name filters.
     */
    @Test
    public void testNameFilterWithMatch() {
        PropertyFilter fooSearch = SearchProtoUtil.nameFilterRegex(FOO, false, false);
        assertEquals("displayName", fooSearch.getPropertyName());
        assertEquals("^" + FOO + "$", fooSearch.getStringFilter().getStringPropertyRegex());
        assertEquals(0, fooSearch.getStringFilter().getOptionsCount());
    }

    /**
     * Test entity filters.
     */
    @Test
    public void testEntityFilter() {
        PropertyFilter vmSearch = SearchProtoUtil.entityTypeFilter(VM);
        assertEquals("entityType", vmSearch.getPropertyName());
        assertEquals(VM_TYPE.typeNumber(),
            vmSearch.getNumericFilter().getValue());
    }

    /**
     * Test string regex filters.
     */
    @Test
    public void testRegexPropertyFilter() {
        PropertyFilter fooBar = SearchProtoUtil.stringPropertyFilterRegex(BAR, FOO);
        assertEquals(BAR, fooBar.getPropertyName());
        assertEquals("^" + FOO + "$", fooBar.getStringFilter().getStringPropertyRegex());
    }

    /**
     * Test exact string matching filters.
     */
    @Test
    public void testPropertyFilter() {
        PropertyFilter fooBar = SearchProtoUtil.stringPropertyFilterExact(BAR, Collections.singletonList(FOO));
        assertEquals(BAR, fooBar.getPropertyName());
        assertEquals(1, fooBar.getStringFilter().getOptionsCount());
        assertEquals(FOO, fooBar.getStringFilter().getOptions(0));
        assertFalse(fooBar.getStringFilter().hasStringPropertyRegex());
    }

    /**
     * Test that there is no double {@code ^} in the beginning of a regex in a filter.
     */
    @Test
    public void testStringFilterNoDoublePrefix() {
        assertThat(
            SearchProtoUtil.stringFilterRegex("^val", true, false).getStringPropertyRegex(),
            // No extra "^" prefix.
            is("^val$"));
    }

    /**
     * Test that there is no double {@code $} in the end of a regex in a filter.
     */
    @Test
    public void testStringFilterNoDoubleSuffix() {
        assertThat(
            SearchProtoUtil.stringFilterRegex("val$", true, false).getStringPropertyRegex(),
            // No extra "$" suffix.
            is("^val$"));
    }

    /**
     * Test traversal to type filter.
     */
    @Test
    public void testTraverseToType() {
        TraversalFilter traversalFilter =
            SearchProtoUtil.traverseToType(TraversalDirection.CONSUMES, VM);
        assertEquals(TraversalDirection.CONSUMES, traversalFilter.getTraversalDirection());
        assertEquals(SearchProtoUtil.numericPropertyFilter("entityType",
            VM_TYPE.typeNumber(), ComparisonOperator.EQ),
            traversalFilter.getStoppingCondition().getStoppingPropertyFilter());
    }

    /**
     * Test search property filter creation.
     */
    @Test
    public void testSearchProperty() {
        PropertyFilter fooBar = SearchProtoUtil.stringPropertyFilterRegex(BAR, FOO);
        SearchFilter searchFilter = SearchProtoUtil.searchFilterProperty(fooBar);
        assertEquals(fooBar, searchFilter.getPropertyFilter());
    }

    /**
     * Test search traversal filter creation.
     */
    @Test
    public void testSearchTraversal() {
        TraversalFilter traversalFilter =
            SearchProtoUtil.traverseToType(TraversalDirection.CONSUMES, VM);
        SearchFilter searchFilter = SearchProtoUtil.searchFilterTraversal(traversalFilter);
        assertEquals(traversalFilter, searchFilter.getTraversalFilter());
    }

    /**
     * Test state filter.
     */
    @Test
    public void testStateFilter() {
        final PropertyFilter propertyFilter = SearchProtoUtil.stateFilter("a|b|c");
        assertEquals(SearchableProperties.ENTITY_STATE, propertyFilter.getPropertyName());
        assertFalse(propertyFilter.getStringFilter().hasStringPropertyRegex());
        assertEquals(3, propertyFilter.getStringFilter().getOptionsCount());
        assertEquals("a", propertyFilter.getStringFilter().getOptions(0));
        assertEquals("b", propertyFilter.getStringFilter().getOptions(1));
        assertEquals("c", propertyFilter.getStringFilter().getOptions(2));
    }

    /**
     * Test regex stripping.
     */
    @Test
    public void testStripFullRegex() {
        assertEquals("ACTIVE", SearchProtoUtil.stripFullRegex("^ACTIVE$"));
        assertEquals("ACTIVE", SearchProtoUtil.stripFullRegex("ACTIVE"));
        assertEquals("", SearchProtoUtil.stripFullRegex(""));
    }
}
