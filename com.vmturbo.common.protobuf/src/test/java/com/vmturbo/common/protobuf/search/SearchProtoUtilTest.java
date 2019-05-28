package com.vmturbo.common.protobuf.search;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import java.util.Collections;

import org.junit.Test;

import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.topology.UIEntityType;

public class SearchProtoUtilTest {

    static final String FOO = "foo";
    static final String BAR = "bar";
    static final String VM = "VirtualMachine";
    static final UIEntityType VM_TYPE = UIEntityType.fromString(VM);

    @Test
    public void testNameFilter() {
        PropertyFilter fooSearch = SearchProtoUtil.nameFilterRegex(FOO);
        assertEquals("displayName", fooSearch.getPropertyName());
        assertEquals("^" + FOO + "$", fooSearch.getStringFilter().getStringPropertyRegex());
        assertEquals(0, fooSearch.getStringFilter().getOptionsCount());
    }

    @Test
    public void testNameFilterWithMatch() {
        PropertyFilter fooSearch = SearchProtoUtil.nameFilterRegex(FOO, false, false);
        assertEquals("displayName", fooSearch.getPropertyName());
        assertEquals("^" + FOO + "$", fooSearch.getStringFilter().getStringPropertyRegex());
        assertEquals(0, fooSearch.getStringFilter().getOptionsCount());
    }

    @Test
    public void testEntityFilter() {
        PropertyFilter vmSearch = SearchProtoUtil.entityTypeFilter(VM);
        assertEquals("entityType", vmSearch.getPropertyName());
        assertEquals(VM_TYPE.typeNumber(),
            vmSearch.getNumericFilter().getValue());
    }

    @Test
    public void testRegexPropertyFilter() {
        PropertyFilter fooBar = SearchProtoUtil.stringPropertyFilterRegex(BAR, FOO);
        assertEquals(BAR, fooBar.getPropertyName());
        assertEquals("^" + FOO + "$", fooBar.getStringFilter().getStringPropertyRegex());
    }

    @Test
    public void testPropertyFilter() {
        PropertyFilter fooBar = SearchProtoUtil.stringPropertyFilterExact(BAR, Collections.singletonList(FOO));
        assertEquals(BAR, fooBar.getPropertyName());
        assertEquals(1, fooBar.getStringFilter().getOptionsCount());
        assertEquals(FOO, fooBar.getStringFilter().getOptions(0));
        assertFalse(fooBar.getStringFilter().hasStringPropertyRegex());
    }

    @Test
    public void testStringFilterNoDoublePrefix() {
        assertThat(
            SearchProtoUtil.stringFilterRegex("^val", true, false).getStringPropertyRegex(),
            // No extra "^" prefix.
            is("^val$"));
    }

    @Test
    public void testStringFilterNoDoubleSuffix() {
        assertThat(
            SearchProtoUtil.stringFilterRegex("val$", true, false).getStringPropertyRegex(),
            // No extra "$" suffix.
            is("^val$"));
    }

    @Test
    public void testTraverseToType() {
        TraversalFilter traversalFilter =
            SearchProtoUtil.traverseToType(TraversalDirection.CONSUMES, VM);
        assertEquals(TraversalDirection.CONSUMES, traversalFilter.getTraversalDirection());
        assertEquals(SearchProtoUtil.numericPropertyFilter("entityType",
            VM_TYPE.typeNumber(), ComparisonOperator.EQ),
            traversalFilter.getStoppingCondition().getStoppingPropertyFilter());
    }

    @Test
    public void testSearchProperty() {
        PropertyFilter fooBar = SearchProtoUtil.stringPropertyFilterRegex(BAR, FOO);
        SearchFilter searchFilter = SearchProtoUtil.searchFilterProperty(fooBar);
        assertEquals(fooBar, searchFilter.getPropertyFilter());
    }

    @Test
    public void testSearchTraversal() {
        TraversalFilter traversalFilter =
            SearchProtoUtil.traverseToType(TraversalDirection.CONSUMES, VM);
        SearchFilter searchFilter = SearchProtoUtil.searchFilterTraversal(traversalFilter);
        assertEquals(traversalFilter, searchFilter.getTraversalFilter());
    }


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
}