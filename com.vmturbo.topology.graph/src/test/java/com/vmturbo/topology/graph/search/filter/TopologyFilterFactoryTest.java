package com.vmturbo.topology.graph.search.filter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.ListFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.MapFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.ObjectFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.HotResizeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.HotResizeInfo.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessAccountInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CustomControllerInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DeploymentInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.StatefulSetInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.StorageInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.WorkloadControllerInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.WorkloadControllerInfo.ControllerTypeCase;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.topology.graph.TestGraphEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.search.filter.TraversalFilter.TraversalToDepthFilter;
import com.vmturbo.topology.graph.search.filter.TraversalFilter.TraversalToPropertyFilter;

public class TopologyFilterFactoryTest {

    private static final String VENDOR_ID1 = "test-service1";
    private static final String VENDOR_ID2 = "test-service2";
    private static final long TARGET_ID1 = 123L;
    private static final long UDT_OID = 543L;
    private static final TestGraphEntity DISCOVERED_SERVICE =
            TestGraphEntity.newBuilder(1234L, ApiEntityType.SERVICE)
                    .addTargetIdentity(TARGET_ID1, VENDOR_ID1)
                    .build();
    private static final TestGraphEntity USER_DEFINED_TOPOLOGY_SERVICE =
            TestGraphEntity.newBuilder(2345L, ApiEntityType.SERVICE)
                    .addTargetIdentity(UDT_OID, VENDOR_ID2)
                    .build();

    private static final TestGraphEntity DISCOVERED_SERVICE_BY_TWO_TARGETS =
            TestGraphEntity.newBuilder(765L, ApiEntityType.SERVICE)
                    .addTargetIdentity(UDT_OID, VENDOR_ID2)
                    .addTargetIdentity(TARGET_ID1, VENDOR_ID1)
                    .build();

    private final TopologyFilterFactory<TestGraphEntity> filterFactory = new TopologyFilterFactory<>();
    private final TopologyGraph<TestGraphEntity> graph = mock(TopologyGraph.class);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testSearchFilterForOid() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName("oid")
                .setNumericFilter(NumericFilter.newBuilder()
                    .setValue(1234L)
                    .setComparisonOperator(ComparisonOperator.EQ)
                ))
            .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_MACHINE).build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(2345L, ApiEntityType.VIRTUAL_MACHINE).build();
        assertTrue(propertyFilter.test(entity1, graph));
        assertFalse(propertyFilter.test(entity2, graph));
    }

    @Test
    public void testSearchFilterForOidRegex() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName("oid")
                .setStringFilter(StringFilter.newBuilder()
                    .addOptions("1234")
                    .addOptions("4321")))
            .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_MACHINE).build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(4321L, ApiEntityType.VIRTUAL_MACHINE).build();
        final TestGraphEntity entity3 = TestGraphEntity.newBuilder(2345L, ApiEntityType.VIRTUAL_MACHINE).build();
        assertTrue(propertyFilter.test(entity1, graph));
        assertTrue(propertyFilter.test(entity2, graph));
        assertFalse(propertyFilter.test(entity3, graph));
    }

    @Test
    public void testSearchFilterForOidOption() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName("oid")
                .setStringFilter(StringFilter.newBuilder()
                    .setStringPropertyRegex("1234")))
            .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_MACHINE).build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(2345L, ApiEntityType.VIRTUAL_MACHINE).build();
        assertTrue(propertyFilter.test(entity1, graph));
        assertFalse(propertyFilter.test(entity2, graph));
    }

    @Test
    public void testSearchFilterForEntityType() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName("entityType")
                .setNumericFilter(NumericFilter.newBuilder()
                        .setValue(EntityType.VIRTUAL_MACHINE.getNumber())
                        .setComparisonOperator(ComparisonOperator.EQ)
                ))
            .build();

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_MACHINE).build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(2345L, ApiEntityType.PHYSICAL_MACHINE).build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        assertTrue(propertyFilter.test(entity1, graph));
        assertFalse(propertyFilter.test(entity2, graph));
    }

    @Test
    public void testSearchFilterForDisplayNameEquality() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName("displayName")
                .setStringFilter(StringFilter.newBuilder()
                        .setStringPropertyRegex("foo")
                ))
            .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_MACHINE)
            .setName("foo")
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(2345L, ApiEntityType.VIRTUAL_MACHINE)
            .setName("bar")
            .build();

        assertTrue(propertyFilter.test(entity1, graph));
        assertFalse(propertyFilter.test(entity2, graph));
    }

    @Test
    public void testSearchFilterForDisplayCaseSensitive() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
                .setPropertyFilter(Search.PropertyFilter.newBuilder()
                        .setPropertyName("displayName")
                        .setStringFilter(StringFilter.newBuilder()
                                .setStringPropertyRegex("MyEntity")
                                .setCaseSensitive(true)
                        ))
                .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_MACHINE)
            .setName("MyEntity")
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(2345L, ApiEntityType.VIRTUAL_MACHINE)
            .setName("myentity")
            .build();


        assertTrue(propertyFilter.test(entity1, graph));
        assertFalse(propertyFilter.test(entity2, graph));
    }

    @Test
    public void testSearchFilterForDisplayCaseInsensitive() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
                .setPropertyFilter(Search.PropertyFilter.newBuilder()
                        .setPropertyName("displayName")
                        .setStringFilter(StringFilter.newBuilder()
                                .setStringPropertyRegex("myentity")
                                .setCaseSensitive(false)
                        ))
                .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_MACHINE)
            .setName("MyEntity")
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(2345L, ApiEntityType.VIRTUAL_MACHINE)
            .setName("myentity")
            .build();

        assertTrue(propertyFilter.test(entity1, graph));
        assertTrue(propertyFilter.test(entity2, graph));
    }

    @Test
    public void testSearchFilterForDisplayNameRegex() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName("displayName")
                .setStringFilter(StringFilter.newBuilder()
                        .setStringPropertyRegex("f.*")
                ))
            .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_MACHINE)
            .setName("foo")
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(2345L, ApiEntityType.VIRTUAL_MACHINE)
            .setName("bar")
            .build();


        assertTrue(propertyFilter.test(entity1, graph));
        assertFalse(propertyFilter.test(entity2, graph));
    }

    @Test
    public void testSearchFilterForDisplayNameRegexNegated() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName("displayName")
                .setStringFilter(StringFilter.newBuilder()
                        .setPositiveMatch(false)
                        .setStringPropertyRegex("f.*")
                ))
            .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_MACHINE)
            .setName("foo")
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(2345L, ApiEntityType.VIRTUAL_MACHINE)
            .setName("bar")
            .build();


        assertFalse(propertyFilter.test(entity1, graph));
        assertTrue(propertyFilter.test(entity2, graph));
    }

    @Test
    public void testSearchFilterForEntityStateOptions() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName("state")
                .setStringFilter(StringFilter.newBuilder()
                    .addOptions(UIEntityState.ACTIVE.apiStr())
                    .addOptions(UIEntityState.IDLE.apiStr())
                    .setPositiveMatch(true)))
            .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_MACHINE)
            .setState(EntityState.POWERED_ON)
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(4321L, ApiEntityType.VIRTUAL_MACHINE)
            .setState(EntityState.POWERED_OFF)
            .build();
        final TestGraphEntity entity3 = TestGraphEntity.newBuilder(2345L, ApiEntityType.VIRTUAL_MACHINE)
            .setState(EntityState.FAILOVER)
            .build();

        assertTrue(propertyFilter.test(entity1, graph));
        assertTrue(propertyFilter.test(entity2, graph));
        assertFalse(propertyFilter.test(entity3, graph));
    }

    @Test
    public void testSearchFilterForEntityStateMatch() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
                .setPropertyFilter(Search.PropertyFilter.newBuilder()
                        .setPropertyName("state")
                        .setStringFilter(StringFilter.newBuilder()
                                .addOptions("ACTIVE")
                                .setPositiveMatch(true)
                                .setStringPropertyRegex("ACTIVE")))
                .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_MACHINE)
            .setState(EntityState.POWERED_ON)
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(2345L, ApiEntityType.VIRTUAL_MACHINE)
            .setState(EntityState.POWERED_OFF)
            .build();

        assertTrue(propertyFilter.test(entity1, graph));
        assertFalse(propertyFilter.test(entity2, graph));
    }

    @Test
    public void testSearchFilterForEntityStateNoMatch() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
                .setPropertyFilter(Search.PropertyFilter.newBuilder()
                        .setPropertyName("state")
                        .setStringFilter(StringFilter.newBuilder()
                                .addOptions("ACTIVE")
                                // Match set to false, so "ACTIVE" entities shouldn't match.
                                .setPositiveMatch(false)
                                .setStringPropertyRegex("ACTIVE")))
                .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_MACHINE)
            .setState(EntityState.POWERED_ON)
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(2345L, ApiEntityType.VIRTUAL_MACHINE)
            .setState(EntityState.POWERED_OFF)
            .build();
        final TestGraphEntity entity3 = TestGraphEntity.newBuilder(5678L, ApiEntityType.VIRTUAL_MACHINE)
            .setState(EntityState.FAILOVER)
            .build();

        assertFalse(propertyFilter.test(entity1, graph));
        assertTrue(propertyFilter.test(entity2, graph));
        assertTrue(propertyFilter.test(entity3, graph));
    }

    @Test
    public void testNumericSearchFilterForEntityState() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
                .setPropertyFilter(Search.PropertyFilter.newBuilder()
                        .setPropertyName("state")
                        .setNumericFilter(NumericFilter.newBuilder()
                                .setValue(EntityState.POWERED_ON_VALUE)
                                .setComparisonOperator(ComparisonOperator.EQ)
                                .build())
                        .build())
                .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_MACHINE)
                .setState(EntityState.POWERED_ON)
                .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(2345L, ApiEntityType.VIRTUAL_MACHINE)
                .setState(EntityState.POWERED_OFF)
                .build();

        assertTrue(propertyFilter.test(entity1, graph));
        assertFalse(propertyFilter.test(entity2, graph));
    }

    /**
     * Test environment type matching.
     */
    @Test
    public void testSearchFilterForEnvironmentTypeMatch() {
        testSearchFilterForEnvironmentTypeMatch("CLOUD", true, true, false, false);
        testSearchFilterForEnvironmentTypeMatch("ONPREM", true, false, true, false);
        testSearchFilterForEnvironmentTypeMatch("HYBRID", true, true, true, true);
        testSearchFilterForEnvironmentTypeMatch("UNKNOWN", false, false, false, true);
    }

    private void testSearchFilterForEnvironmentTypeMatch(
            @Nonnull String envType, boolean hybridShouldMatch, boolean cloudShouldMatch,
            boolean onpremShouldMatch, boolean unknownShouldMatch) {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
                .setPropertyFilter(Search.PropertyFilter.newBuilder()
                        .setPropertyName("environmentType")
                        .setStringFilter(StringFilter.newBuilder()
                                .setPositiveMatch(true)
                                .setStringPropertyRegex(envType)))
                .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_MACHINE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(2345L, ApiEntityType.VIRTUAL_MACHINE)
                .setEnvironmentType(EnvironmentType.ON_PREM)
                .build();
        final TestGraphEntity entity3 = TestGraphEntity.newBuilder(3456L, ApiEntityType.VIRTUAL_MACHINE)
                .setEnvironmentType(EnvironmentType.HYBRID)
                .build();
        final TestGraphEntity entity4 = TestGraphEntity.newBuilder(4567L, ApiEntityType.VIRTUAL_MACHINE)
                .setEnvironmentType(EnvironmentType.UNKNOWN_ENV)
                .build();

        assertEquals(cloudShouldMatch, propertyFilter.test(entity1, graph));
        assertEquals(onpremShouldMatch, propertyFilter.test(entity2, graph));
        assertEquals(hybridShouldMatch, propertyFilter.test(entity3, graph));
        assertEquals(unknownShouldMatch, propertyFilter.test(entity4, graph));
    }

    @Test
    public void testSearchFilterForStringEntityTypeNumericMatch() {
        final SearchFilter searchFilter = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.ENTITY_TYPE)
                .setNumericFilter(NumericFilter.newBuilder()
                    .setValue(ApiEntityType.VIRTUAL_MACHINE.typeNumber())))
            .build();
        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchFilter);

        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_MACHINE)
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(1234L, ApiEntityType.PHYSICAL_MACHINE)
            .build();

        assertTrue(propertyFilter.test(entity1, graph));
        assertFalse(propertyFilter.test(entity2, graph));
    }

    @Test
    public void testSearchFilterForStringEntityTypeNumericNoMatch() {
        final SearchFilter searchFilter = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.ENTITY_TYPE)
                .setNumericFilter(NumericFilter.newBuilder()
                    .setComparisonOperator(ComparisonOperator.NE)
                    .setValue(ApiEntityType.VIRTUAL_MACHINE.typeNumber())))
            .build();
        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchFilter);

        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_MACHINE)
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(1234L, ApiEntityType.PHYSICAL_MACHINE)
            .build();

        assertFalse(propertyFilter.test(entity1, graph));
        assertTrue(propertyFilter.test(entity2, graph));
    }

    @Test
    public void testSearchFilterForStringEntityTypeRegexMatch() {
        final SearchFilter searchFilter = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.ENTITY_TYPE)
                .setStringFilter(StringFilter.newBuilder()
                    .setStringPropertyRegex(ApiEntityType.VIRTUAL_MACHINE.apiStr())))
            .build();
        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchFilter);

        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_MACHINE)
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(1234L, ApiEntityType.PHYSICAL_MACHINE)
            .build();

        assertTrue(propertyFilter.test(entity1, graph));
        assertFalse(propertyFilter.test(entity2, graph));
    }

    @Test
    public void testSearchFilterForEntityTypeRegexNoMatch() {
        SearchFilter searchFilter = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.ENTITY_TYPE)
                .setStringFilter(StringFilter.newBuilder()
                    .setStringPropertyRegex(ApiEntityType.VIRTUAL_MACHINE.apiStr())
                    .setPositiveMatch(false)))
            .build();
        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchFilter);

        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_MACHINE)
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(1234L, ApiEntityType.PHYSICAL_MACHINE)
            .build();

        assertFalse(propertyFilter.test(entity1, graph));
        assertTrue(propertyFilter.test(entity2, graph));
    }

    @Test
    public void testSearchFilterForEntityTypeOptions() {
        SearchFilter searchFilter = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.ENTITY_TYPE)
                .setStringFilter(StringFilter.newBuilder()
                    .addOptions(ApiEntityType.VIRTUAL_MACHINE.apiStr())
                    .addOptions(ApiEntityType.PHYSICAL_MACHINE.apiStr())))
            .build();
        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchFilter);

        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_MACHINE)
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(1234L, ApiEntityType.PHYSICAL_MACHINE)
            .build();
        final TestGraphEntity entity3 = TestGraphEntity.newBuilder(1234L, ApiEntityType.STORAGE)
            .build();

        assertTrue(propertyFilter.test(entity1, graph));
        assertTrue(propertyFilter.test(entity2, graph));
        assertFalse(propertyFilter.test(entity3, graph));
    }

    @Test
    public void testNumericDisplayNameIllegal() {
        expectedException.expect(IllegalArgumentException.class);

        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                    .setPropertyName("displayName")
                    .setNumericFilter(NumericFilter.getDefaultInstance())
            ).build();
        filterFactory.filterFor(searchCriteria);
    }

    @Test
    public void testStringOid() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                    .setPropertyName("oid")
                    .setStringFilter(StringFilter.newBuilder()
                        .setStringPropertyRegex("1234")
                    )
            ).build();

        final TestGraphEntity entity = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_MACHINE).build();;

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertThat(
            filter.apply(Stream.of(entity), graph).collect(Collectors.toList()),
            contains(entity));
    }

    @Test
    public void testStringOidNegation() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                    .setPropertyName("oid")
                    .setStringFilter(StringFilter.newBuilder()
                            .setPositiveMatch(false)
                            .setStringPropertyRegex("1234")
                    )
            ).build();

        final TestGraphEntity entity = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_MACHINE).build();;

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter.apply(Stream.of(entity), graph).collect(Collectors.toList()).isEmpty());
    }

    /**
     * Test the hot add memory enabled filter for vms.
     */
    @Test
    public void testSearchFilterHotAddMemory() {
        testAllCaseHotFiltersForVms(SearchableProperties.HOT_ADD_MEMORY, 53,
                HotResizeInfo.Builder::setHotAddSupported);
    }

    /**
     * Test the hot add cpu enabled filter for vms.
     */
    @Test
    public void testSearchFilterHotAddCPU() {
        testAllCaseHotFiltersForVms(SearchableProperties.HOT_ADD_CPU, 26,
                HotResizeInfo.Builder::setHotAddSupported);
    }

    /**
     * Test the hot remove cpu enabled filter for vms.
     */
    @Test
    public void testSearchFilterHotRemoveCPU() {
        testAllCaseHotFiltersForVms(SearchableProperties.HOT_REMOVE_CPU, 26,
                HotResizeInfo.Builder::setHotRemoveSupported);
    }

    private void testAllCaseHotFiltersForVms(String filterName, int commodityTypeNumber,
            BiFunction<Builder, Boolean, Builder> function) {
        testHotFiltersForVms(filterName, commodityTypeNumber, function, true, true);
        testHotFiltersForVms(filterName, commodityTypeNumber, function, true, false);
        testHotFiltersForVms(filterName, commodityTypeNumber, function, false, true);
        testHotFiltersForVms(filterName, commodityTypeNumber, function, false, false);
    }

    private void testHotFiltersForVms(String filterName, int commodityTypeNumber,
            BiFunction<Builder, Boolean, Builder> function, boolean hotSupport,
            boolean positiveMatch) {
        final SearchFilter searchFilter = SearchFilter.newBuilder().setPropertyFilter(
                Search.PropertyFilter.newBuilder()
                        .setPropertyName(filterName)
                        .setStringFilter(StringFilter.newBuilder()
                                .addOptions(String.valueOf(hotSupport))
                                .setPositiveMatch(positiveMatch)
                                .setCaseSensitive(false))).build();
        final TestGraphEntity vm1 = createVm(commodityTypeNumber,
                function.apply(HotResizeInfo.newBuilder(), true), 1L);
        final TestGraphEntity vm2 = createVm(commodityTypeNumber,
                function.apply(HotResizeInfo.newBuilder(), false), 2L);
        final TestGraphEntity vm3 = TestGraphEntity.newBuilder(3L, ApiEntityType.VIRTUAL_MACHINE)
                .addCommSold(CommoditySoldDTO.newBuilder()
                        .setCommodityType(
                                CommodityType.newBuilder().setType(commodityTypeNumber).build())
                        .build())
                .build();
        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchFilter);
        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter =
                (PropertyFilter<TestGraphEntity>)filter;
        assertEquals(hotSupport != positiveMatch, propertyFilter.test(vm3, graph));
        assertEquals(hotSupport == positiveMatch, propertyFilter.test(vm1, graph));
        assertEquals(hotSupport != positiveMatch, propertyFilter.test(vm2, graph));
    }

    private TestGraphEntity createVm(int commodityTypeNumber, Builder vmHotResizeInfo, long oid) {
        return TestGraphEntity.newBuilder(oid, ApiEntityType.VIRTUAL_MACHINE)
                .addCommSold(CommoditySoldDTO.newBuilder()
                        .setCommodityType(
                                CommodityType.newBuilder().setType(commodityTypeNumber).build())
                        .setHotResizeInfo(vmHotResizeInfo.build())
                        .build())
                .build();
    }

    private class NumericFilterTest {
        private final ComparisonOperator operator;
        private final Collection<TestGraphEntity> expectedMatches;

        public NumericFilterTest(final ComparisonOperator operator,
                                 @Nonnull final Collection<TestGraphEntity> expectedMatches) {
            this.operator = operator;
            this.expectedMatches = expectedMatches;
        }

        public ComparisonOperator getOperator() {
            return operator;
        }

        public Collection<TestGraphEntity> getExpectedMatches() {
            return expectedMatches;
        }
    }

    @Test
    public void testIntNumericComparisons() {
        final TestGraphEntity entity1 = mock(TestGraphEntity.class);
        final TestGraphEntity entity2 = mock(TestGraphEntity.class);
        final TestGraphEntity entity3 = mock(TestGraphEntity.class);

        final List<TestGraphEntity> testCases = Arrays.asList(entity1, entity2, entity3);
        when(entity1.getEntityType()).thenReturn(1);
        when(entity2.getEntityType()).thenReturn(2);
        when(entity3.getEntityType()).thenReturn(3);

        final NumericFilter.Builder numericBuilder = NumericFilter.newBuilder()
            .setValue(2);
        final Search.PropertyFilter.Builder propertyBuilder = Search.PropertyFilter.newBuilder()
            .setPropertyName("entityType");

        // Execute the comparison on entities with entityType 1,2,3 with the value 2 passed to the filter.
        Stream.of(
            new NumericFilterTest(ComparisonOperator.EQ, Collections.singletonList(entity2)),
            new NumericFilterTest(ComparisonOperator.NE, Arrays.asList(entity1, entity3)),
            new NumericFilterTest(ComparisonOperator.GT, Collections.singletonList(entity3)),
            new NumericFilterTest(ComparisonOperator.GTE, Arrays.asList(entity2, entity3)),
            new NumericFilterTest(ComparisonOperator.LT, Collections.singletonList(entity1)),
            new NumericFilterTest(ComparisonOperator.LTE, Arrays.asList(entity1, entity2))
        ).forEach(testCase -> {
            final SearchFilter searchCriteria = SearchFilter.newBuilder()
                .setPropertyFilter(propertyBuilder.setNumericFilter(
                    numericBuilder.setComparisonOperator(testCase.getOperator())))
                .build();

            final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);

            assertThat(
                "Test for: " + testCase.getOperator().toString(),
                filter.apply(testCases.stream(), graph).collect(Collectors.toList()),
                containsInAnyOrder(testCase.getExpectedMatches().toArray())
            );
        });
    }

    @Test
    public void testLongNumericComparisons() {
        final TestGraphEntity entity1 = mock(TestGraphEntity.class);
        final TestGraphEntity entity2 = mock(TestGraphEntity.class);
        final TestGraphEntity entity3 = mock(TestGraphEntity.class);

        final List<TestGraphEntity> testCases = Arrays.asList(entity1, entity2, entity3);
        when(entity1.getOid()).thenReturn(1L);
        when(entity2.getOid()).thenReturn(2L);
        when(entity3.getOid()).thenReturn(3L);

        final NumericFilter.Builder numericBuilder = NumericFilter.newBuilder()
            .setValue(2L);
        final Search.PropertyFilter.Builder propertyBuilder = Search.PropertyFilter.newBuilder()
            .setPropertyName("oid");

        // Execute the comparison on entities with oid 1,2,3 with the value 2 passed to the filter.
        Stream.of(
            new NumericFilterTest(ComparisonOperator.EQ, Collections.singletonList(entity2)),
            new NumericFilterTest(ComparisonOperator.NE, Arrays.asList(entity1, entity3)),
            new NumericFilterTest(ComparisonOperator.GT, Collections.singletonList(entity3)),
            new NumericFilterTest(ComparisonOperator.GTE, Arrays.asList(entity2, entity3)),
            new NumericFilterTest(ComparisonOperator.LT, Collections.singletonList(entity1)),
            new NumericFilterTest(ComparisonOperator.LTE, Arrays.asList(entity1, entity2))
        ).forEach(testCase -> {
            final SearchFilter searchCriteria = SearchFilter.newBuilder()
                .setPropertyFilter(propertyBuilder.setNumericFilter(
                    numericBuilder.setComparisonOperator(testCase.getOperator())))
                .build();

            final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);

            assertThat(
                "Test for: " + testCase.getOperator().toString(),
                filter.apply(testCases.stream(), graph).collect(Collectors.toList()),
                containsInAnyOrder(testCase.getExpectedMatches().toArray())
            );
        });
    }

    @Test
    public void testTraversalToDepthFilter() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setTraversalFilter(Search.TraversalFilter.newBuilder()
                .setTraversalDirection(TraversalDirection.CONSUMES)
                .setStoppingCondition(StoppingCondition.newBuilder().setNumberHops(3)))
            .build();

        final TopologyFilter filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof TraversalToDepthFilter);
        final TraversalToDepthFilter depthFilter = (TraversalToDepthFilter)filter;
        assertEquals(TraversalDirection.CONSUMES, depthFilter.getTraversalDirection());
    }

    @Test
    public void testTraversalToPropertyFilter() {
        final Search.PropertyFilter stoppingFilter = Search.PropertyFilter.newBuilder()
            .setPropertyName("displayName")
            .setStringFilter(StringFilter.newBuilder().setStringPropertyRegex("foo"))
            .build();

        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setTraversalFilter(Search.TraversalFilter.newBuilder()
                .setTraversalDirection(TraversalDirection.PRODUCES)
                .setStoppingCondition(StoppingCondition.newBuilder()
                    .setStoppingPropertyFilter(stoppingFilter)))
            .build();

        final TopologyFilter filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof TraversalToPropertyFilter);
        final TraversalToPropertyFilter propertyFilter = (TraversalToPropertyFilter)filter;
        assertEquals(TraversalDirection.PRODUCES, propertyFilter.getTraversalDirection());
    }

    @Test
    public void testRegexContainsMatch() {
        final PropertyFilter<TestGraphEntity> displayNameFilter =
            filterFactory.filterFor(Search.PropertyFilter.newBuilder()
                .setPropertyName("displayName")
                .setStringFilter(StringFilter.newBuilder()
                        .setStringPropertyRegex("in-group")
                ).build());


        final TestGraphEntity entity = TestGraphEntity.newBuilder(123L, ApiEntityType.VIRTUAL_MACHINE)
            .setName("entity-in-group-1")
            .build();

        assertThat(
            displayNameFilter.apply(Stream.of(entity), graph).collect(Collectors.toList()),
            contains(entity));
    }

    @Test
    public void testRegexAnchorMatch() {
        final PropertyFilter<TestGraphEntity> displayNameFilter =
            filterFactory.filterFor(Search.PropertyFilter.newBuilder()
                .setPropertyName("displayName")
                .setStringFilter(StringFilter.newBuilder()
                        .setStringPropertyRegex("^entity")
                ).build());

        final TestGraphEntity entity = TestGraphEntity.newBuilder(123L, ApiEntityType.VIRTUAL_MACHINE)
            .setName("entity-in-group-1")
            .build();

        assertThat(
            displayNameFilter.apply(Stream.of(entity), graph).collect(Collectors.toList()),
            contains(entity));
    }

    @Test
    public void testRegexWildcardMatch() {
        final PropertyFilter<TestGraphEntity> displayNameFilter =
            filterFactory.filterFor(Search.PropertyFilter.newBuilder()
                .setPropertyName("displayName")
                .setStringFilter(StringFilter.newBuilder()
                        .setStringPropertyRegex(".*group.+")
                ).build());

        final TestGraphEntity entity = TestGraphEntity.newBuilder(123L, ApiEntityType.VIRTUAL_MACHINE)
            .setName("entity-in-group-1")
            .build();

        assertThat(
            displayNameFilter.apply(Stream.of(entity), graph).collect(Collectors.toList()),
            contains(entity));
    }

    @Test
    public void testStringStateActiveOption() {
        final PropertyFilter<TestGraphEntity> stringFilter =
            filterFactory.filterFor(Search.PropertyFilter.newBuilder()
                .setPropertyName("state")
                .setStringFilter(StringFilter.newBuilder()
                    .setStringPropertyRegex("")
                    .addOptions("ACTIVE")
                ).build());

        final TestGraphEntity entity = TestGraphEntity.newBuilder(123L, ApiEntityType.VIRTUAL_MACHINE)
            .setState(EntityState.POWERED_ON)
            .build();

        assertThat(
            stringFilter.apply(Stream.of(entity), graph).collect(Collectors.toList()),
            contains(entity));
    }

    @Test
    public void testSearchFilterForUserDefinedEntitiesNotMatch() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
                    .setPropertyFilter(Search.PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.EXCLUSIVE_DISCOVERING_TARGET)
                        .setStringFilter(StringFilter.newBuilder()
                                .addOptions(String.valueOf(UDT_OID))
                                .setPositiveMatch(false)
                        ))
                .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter =
                (PropertyFilter<TestGraphEntity>)filter;

        assertTrue(propertyFilter.test(DISCOVERED_SERVICE, graph));
        assertFalse(propertyFilter.test(USER_DEFINED_TOPOLOGY_SERVICE, graph));
        assertTrue(propertyFilter.test(DISCOVERED_SERVICE_BY_TWO_TARGETS, graph));
    }

    @Test
    public void testSearchFilterForUserDefinedEntitiesMatch() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
                    .setPropertyFilter(Search.PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.EXCLUSIVE_DISCOVERING_TARGET)
                        .setStringFilter(StringFilter.newBuilder()
                                .addOptions(String.valueOf(UDT_OID))
                                .setPositiveMatch(true)
                        ))
                .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter =
                (PropertyFilter<TestGraphEntity>)filter;

        assertTrue(propertyFilter.test(USER_DEFINED_TOPOLOGY_SERVICE, graph));
        assertFalse(propertyFilter.test(DISCOVERED_SERVICE, graph));
        assertFalse(propertyFilter.test(DISCOVERED_SERVICE_BY_TWO_TARGETS, graph));
    }

    /**
     * Checks various cases of the tags-related filter.
     */
    @Test
    public void testMapFilter() {
        final Search.PropertyFilter positiveSearchFilter = Search.PropertyFilter
            .newBuilder()
            .setPropertyName(SearchableProperties.TAGS_TYPE_PROPERTY_NAME)
            .setMapFilter(
                MapFilter
                    .newBuilder()
                    .setKey("KEY")
                    .addValues("VALUE1")
                    .addValues("VALUE2")
            ).build();

        final Search.PropertyFilter negativeSearchFilter;
        {
            final Search.PropertyFilter.Builder negativeSearchFilterBldr = positiveSearchFilter.toBuilder();
            negativeSearchFilterBldr.getMapFilterBuilder().setPositiveMatch(false);
            negativeSearchFilter = negativeSearchFilterBldr.build();
        }

        final Search.PropertyFilter searchFilterWithRegex =
                Search.PropertyFilter.newBuilder()
                    .setPropertyName(SearchableProperties.TAGS_TYPE_PROPERTY_NAME)
                    .setMapFilter(MapFilter.newBuilder()
                                           .setRegex("O.*=.*A.*"))
                    .build();

        final PropertyFilter<TestGraphEntity> positiveFilter = filterFactory.filterFor(positiveSearchFilter);
        final PropertyFilter<TestGraphEntity> negativeFilter = filterFactory.filterFor(negativeSearchFilter);
        final PropertyFilter<TestGraphEntity> filterWithRegex =
                filterFactory.filterFor(searchFilterWithRegex);

        // entity has no tags
        final TestGraphEntity noTagsEntity = TestGraphEntity.newBuilder(120L, ApiEntityType.VIRTUAL_MACHINE)
            .build();
        assertFalse(positiveFilter.test(noTagsEntity, graph));
        assertTrue(negativeFilter.test(noTagsEntity, graph));
        assertFalse(filterWithRegex.test(noTagsEntity, graph));

        // entity does not have the key
        final TestGraphEntity noKeyEntity = TestGraphEntity.newBuilder(121L, ApiEntityType.VIRTUAL_MACHINE)
            .addTag("OTHERKEY", Collections.singletonList("VALUE1"))
            .build();
        assertFalse(positiveFilter.test(noKeyEntity, graph));
        assertTrue(negativeFilter.test(noKeyEntity, graph));
        assertTrue(filterWithRegex.test(noKeyEntity, graph));

        // entity has the key, but not one of the values
        final TestGraphEntity wrongValueEntity = TestGraphEntity.newBuilder(122L, ApiEntityType.VIRTUAL_MACHINE)
            .addTag("OTHERKEY", Arrays.asList("VALUE1"))
            .addTag("KEY", Arrays.asList("VALUE3", "VALUE4"))
            .build();
        assertFalse(positiveFilter.test(wrongValueEntity, graph));
        assertTrue(negativeFilter.test(wrongValueEntity, graph));
        assertTrue(filterWithRegex.test(wrongValueEntity, graph));

        // entity has the key, and one of the values
        final TestGraphEntity rightValueEntity = TestGraphEntity.newBuilder(123L, ApiEntityType.VIRTUAL_MACHINE)
            .addTag("KEY", Arrays.asList("VALUE2", "VALUE4"))
            .build();
        assertTrue(positiveFilter.test(rightValueEntity, graph));
        assertFalse(negativeFilter.test(rightValueEntity, graph));
        assertFalse(filterWithRegex.test(rightValueEntity, graph));
    }

    @Test
    public void testVmemCapacityFilter() {
        final SearchFilter searchFilter = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.COMMODITY_SOLD_LIST_PROPERTY_NAME)
                .setListFilter(ListFilter.newBuilder()
                    .setObjectFilter(ObjectFilter.newBuilder()
                        .addFilters(Search.PropertyFilter.newBuilder()
                            .setPropertyName(SearchableProperties.COMMODITY_TYPE_PROPERTY_NAME)
                            .setStringFilter(StringFilter.newBuilder()
                                .setStringPropertyRegex("^" + UICommodityType.VMEM.apiStr() + "$")
                                .build()))
                        .addFilters(Search.PropertyFilter.newBuilder()
                            .setPropertyName(SearchableProperties.COMMODITY_CAPACITY_PROPERTY_NAME)
                            .setNumericFilter(NumericFilter.newBuilder()
                                .setComparisonOperator(ComparisonOperator.GT)
                                .setValue(5))))))
            .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchFilter);

        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_MACHINE)
            .addCommSold(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(UICommodityType.VMEM.typeNumber()))
                .setCapacity(10)
                .build())
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_MACHINE)
            .addCommSold(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(UICommodityType.VMEM.typeNumber()))
                .setCapacity(1)
                .build())
            .build();

        assertTrue(propertyFilter.test(entity1, graph));
        assertFalse(propertyFilter.test(entity2, graph));
    }

    @Test
    public void testMemCapacityFilter() {
        final SearchFilter searchFilter = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.COMMODITY_SOLD_LIST_PROPERTY_NAME)
                .setListFilter(ListFilter.newBuilder()
                    .setObjectFilter(ObjectFilter.newBuilder()
                        .addFilters(Search.PropertyFilter.newBuilder()
                            .setPropertyName(SearchableProperties.COMMODITY_TYPE_PROPERTY_NAME)
                            .setStringFilter(StringFilter.newBuilder()
                                .setStringPropertyRegex("^" + UICommodityType.MEM.apiStr() + "$")
                                .build()))
                        .addFilters(Search.PropertyFilter.newBuilder()
                            .setPropertyName(SearchableProperties.COMMODITY_CAPACITY_PROPERTY_NAME)
                            .setNumericFilter(NumericFilter.newBuilder()
                                .setComparisonOperator(ComparisonOperator.GT)
                                .setValue(5))))))
            .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchFilter);

        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_MACHINE)
            .addCommSold(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(UICommodityType.MEM.typeNumber()))
                .setCapacity(10)
                .build())
            .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_MACHINE)
            .addCommSold(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(UICommodityType.MEM.typeNumber()))
                .setCapacity(1)
                .build())
            .build();

        assertTrue(propertyFilter.test(entity1, graph));
        assertFalse(propertyFilter.test(entity2, graph));
    }

    @Test
    public void testDiscoveringTargetFilter() {
        final TestGraphEntity entity = TestGraphEntity.newBuilder(1L, ApiEntityType.APPLICATION)
                                           .addTargetIdentity(1L, "")
                                           .build();
        final PropertyFilter<TestGraphEntity> filter1 =
            makeDiscoveringTargetFilter(StringFilter.newBuilder()
                                            .setStringPropertyRegex(".*")
                                            .build());
        final PropertyFilter<TestGraphEntity> filter2 =
            makeDiscoveringTargetFilter(StringFilter.newBuilder()
                                            .setStringPropertyRegex(".*")
                                            .setPositiveMatch(false)
                                            .build());
        final PropertyFilter<TestGraphEntity> filter3 =
            makeDiscoveringTargetFilter(StringFilter.newBuilder()
                                            .setStringPropertyRegex(".*")
                                            .setCaseSensitive(true)
                                            .build());
        final PropertyFilter<TestGraphEntity> filter4 =
            makeDiscoveringTargetFilter(StringFilter.newBuilder()
                                            .addOptions("2")
                                            .addOptions("3")
                                            .build());
        final PropertyFilter<TestGraphEntity> filter5 =
            makeDiscoveringTargetFilter(StringFilter.newBuilder()
                                            .addOptions("2")
                                            .addOptions("1")
                                            .build());
        final PropertyFilter<TestGraphEntity> filter6 =
            makeDiscoveringTargetFilter(StringFilter.newBuilder()
                                            .addOptions("1")
                                            .setPositiveMatch(false)
                                            .build());
        final PropertyFilter<TestGraphEntity> filter7 =
            makeDiscoveringTargetFilter(StringFilter.newBuilder()
                                            .addOptions("2")
                                            .build());
        final PropertyFilter<TestGraphEntity> filter8 =
            makeDiscoveringTargetFilter(StringFilter.newBuilder()
                                            .addOptions("1")
                                            .build());
        final PropertyFilter<TestGraphEntity> filter9 =
            makeDiscoveringTargetFilter(NumericFilter.newBuilder()
                                            .setValue(2)
                                            .setComparisonOperator(ComparisonOperator.EQ)
                                            .build());
        final PropertyFilter<TestGraphEntity> filter10 =
            makeDiscoveringTargetFilter(NumericFilter.newBuilder()
                                            .setValue(1)
                                            .setComparisonOperator(ComparisonOperator.EQ)
                                            .build());

        assertTrue(filter1.test(entity, graph));
        assertFalse(filter2.test(entity, graph));
        assertTrue(filter3.test(entity, graph));
        assertFalse(filter4.test(entity, graph));
        assertTrue(filter5.test(entity, graph));
        assertFalse(filter6.test(entity, graph));
        assertFalse(filter7.test(entity, graph));
        assertTrue(filter8.test(entity, graph));
        assertFalse(filter9.test(entity, graph));
        assertTrue(filter10.test(entity, graph));
    }

    @Test
    public void testSearchFilterForStorageLocalSupport() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.DS_INFO_REPO_DTO_PROPERTY_NAME)
                .setObjectFilter(ObjectFilter.newBuilder()
                    .addFilters(Search.PropertyFilter.newBuilder()
                        .setPropertyName("local")
                        .setStringFilter(StringFilter.newBuilder()
                            .setStringPropertyRegex("true")))))
            .build();

        final TestGraphEntity vmEntity =
            TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_MACHINE).build();
        final TestGraphEntity pmEntity =
            TestGraphEntity.newBuilder(2345L, ApiEntityType.PHYSICAL_MACHINE).build();
        final TestGraphEntity storageEntityMatching =
            TestGraphEntity.newBuilder(3456L, ApiEntityType.STORAGE)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setStorage(StorageInfo.newBuilder().setIsLocal(true))
                    .build())
                .build();
        final TestGraphEntity storageEntityNotMatching =
            TestGraphEntity.newBuilder(4567L, ApiEntityType.STORAGE)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setStorage(StorageInfo.newBuilder().setIsLocal(false))
                    .build())
                .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        assertFalse(propertyFilter.test(vmEntity, graph));
        assertFalse(propertyFilter.test(pmEntity, graph));
        assertTrue(propertyFilter.test(storageEntityMatching, graph));
        assertFalse(propertyFilter.test(storageEntityNotMatching, graph));
    }

    /**
     * Test that the search for finding a BusinessAccount by subscription ID works properly.
     */
    @Test
    public void testSearchFilterForBusinessAccountId() {
        final String subscriptionId = "subId22";
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.BUSINESS_ACCOUNT_INFO_REPO_DTO_PROPERTY_NAME)
                .setObjectFilter(ObjectFilter.newBuilder()
                    .addFilters(Search.PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.BUSINESS_ACCOUNT_INFO_ACCOUNT_ID)
                        .setStringFilter(StringFilter.newBuilder()
                            .addOptions(subscriptionId)))))
            .build();

        final TestGraphEntity vmEntity =
            TestGraphEntity.newBuilder(11111L, ApiEntityType.VIRTUAL_MACHINE).build();
        final TestGraphEntity pmEntity =
            TestGraphEntity.newBuilder(22222L, ApiEntityType.PHYSICAL_MACHINE).build();
        final TestGraphEntity businessEntityMatching =
            TestGraphEntity.newBuilder(33333L, ApiEntityType.BUSINESS_ACCOUNT)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setBusinessAccount(BusinessAccountInfo.newBuilder()
                        .setAccountId(subscriptionId))
                    .build())
                .build();
        final TestGraphEntity businessEntityNotMatching =
            TestGraphEntity.newBuilder(44444L, ApiEntityType.BUSINESS_ACCOUNT)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setBusinessAccount(BusinessAccountInfo.newBuilder().setAccountId("Id22"))
                    .build())
                .build();

        final TestGraphEntity businessEntityNoAccountId =
            TestGraphEntity.newBuilder(44444L, ApiEntityType.BUSINESS_ACCOUNT)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setBusinessAccount(BusinessAccountInfo.newBuilder())
                    .build())
                .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchCriteria);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        assertFalse(propertyFilter.test(vmEntity, graph));
        assertFalse(propertyFilter.test(pmEntity, graph));
        assertTrue(propertyFilter.test(businessEntityMatching, graph));
        assertFalse(propertyFilter.test(businessEntityNotMatching, graph));
        assertFalse(propertyFilter.test(businessEntityNoAccountId, graph));
    }

    /**
     * Test the filter generated from a VMs-connected-to-Network filter
     */
    @Test
    public void testVMsConnectedtoNetworksFilter() {
        final String regex = "^a.*$";
        final String matchingString = "axx";
        final String nonMatchingString = "xaa";
        final SearchFilter searchFilter =
                SearchFilter.newBuilder()
                    .setPropertyFilter(
                            Search.PropertyFilter.newBuilder()
                                .setPropertyName(SearchableProperties.VM_CONNECTED_NETWORKS)
                                .setListFilter(
                                        ListFilter.newBuilder()
                                            .setStringFilter(
                                                    StringFilter.newBuilder()
                                                        .setStringPropertyRegex(regex))))
                .build();
        final PropertyFilter<TestGraphEntity> filter = (PropertyFilter)filterFactory.filterFor(searchFilter);

        final TestGraphEntity vm1 = makeVmWithConnectedNetworks(1);
        final TestGraphEntity vm2 = makeVmWithConnectedNetworks(2, matchingString);
        final TestGraphEntity vm3 = makeVmWithConnectedNetworks(3, nonMatchingString);
        final TestGraphEntity vm4 = makeVmWithConnectedNetworks(4, nonMatchingString, matchingString);

        assertFalse(filter.test(vm1, graph));
        assertTrue(filter.test(vm2, graph));
        assertFalse(filter.test(vm3, graph));
        assertTrue(filter.test(vm4, graph));
    }

    /**
     * Test the filter for a storage volume's attachment state.
     */
    @Test
    public void testSearchFilterVolumeAttachmentState() {
        final SearchFilter searchFilter = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.VOLUME_REPO_DTO)
                .setObjectFilter(ObjectFilter.newBuilder()
                    .addFilters(Search.PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.VOLUME_ATTACHMENT_STATE)
                        .setStringFilter(StringFilter.newBuilder().addOptions(AttachmentState.UNATTACHED.name()).build())
                    )
                )
            ).build();
        final TestGraphEntity vmEntity =
            TestGraphEntity.newBuilder(1L, ApiEntityType.VIRTUAL_MACHINE).build();
        final TestGraphEntity pmEntity =
            TestGraphEntity.newBuilder(2L, ApiEntityType.PHYSICAL_MACHINE).build();

        final TestGraphEntity volumeEntityMatching =
            TestGraphEntity.newBuilder(3L, ApiEntityType.VIRTUAL_VOLUME)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setVirtualVolume(VirtualVolumeInfo.newBuilder()
                        .setAttachmentState(AttachmentState.UNATTACHED))
                    .build())
                .build();
        final TestGraphEntity volumeEntityNotMatching =
            TestGraphEntity.newBuilder(4L, ApiEntityType.VIRTUAL_VOLUME)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setVirtualVolume(VirtualVolumeInfo.newBuilder()
                        .setAttachmentState(AttachmentState.ATTACHED))
                    .build())
                .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchFilter);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        assertFalse(propertyFilter.test(vmEntity, graph));
        assertFalse(propertyFilter.test(pmEntity, graph));
        assertTrue(propertyFilter.test(volumeEntityMatching, graph));
        assertFalse(propertyFilter.test(volumeEntityNotMatching, graph));
    }

    /**
     * Test the filter for a storage volume's deletable property.
     */
    @Test
    public void testSearchFilterVolumeDeletableTrue() {
        final SearchFilter searchFilter = SearchFilter.newBuilder()
                .setPropertyFilter(Search.PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.DELETABLE)
                        .setStringFilter(StringFilter.newBuilder().addOptions("true").build()))
                .build();
        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchFilter);

        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_VOLUME)
                .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_VOLUME)
                .build();
        final TestGraphEntity entity3 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_VOLUME)
                .setDeletable(false)
                .build();

        assertTrue(propertyFilter.test(entity1, graph));
        assertTrue(propertyFilter.test(entity2, graph));
        assertFalse(propertyFilter.test(entity3, graph));
    }

    /**
     * Test the filter for a storage volume's deletable property.
     */
    @Test
    public void testSearchFilterVolumeDeletableFalse() {
        final SearchFilter searchFilter = SearchFilter.newBuilder()
                .setPropertyFilter(Search.PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.DELETABLE)
                        .setStringFilter(StringFilter.newBuilder().addOptions("false").build()))
                .build();
        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchFilter);

        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_VOLUME)
                .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_VOLUME)
                .build();
        final TestGraphEntity entity3 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_VOLUME)
                .setDeletable(false)
                .build();

        assertFalse(propertyFilter.test(entity1, graph));
        assertFalse(propertyFilter.test(entity2, graph));
        assertTrue(propertyFilter.test(entity3, graph));
    }

    /**
     * Test the filter for a storage volume's deletable property.
     */
    @Test
    public void testSearchFilterVolumeDeletableTrueAndFalse() {
        final SearchFilter searchFilter = SearchFilter.newBuilder()
                .setPropertyFilter(Search.PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.DELETABLE)
                        .setStringFilter(StringFilter.newBuilder().addOptions("true").addOptions("false").build()))
                .build();
        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchFilter);

        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_VOLUME)
                .build();
        final TestGraphEntity entity2 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_VOLUME)
                .build();
        final TestGraphEntity entity3 = TestGraphEntity.newBuilder(1234L, ApiEntityType.VIRTUAL_VOLUME)
                .setDeletable(false)
                .build();

        assertTrue(propertyFilter.test(entity1, graph));
        assertTrue(propertyFilter.test(entity2, graph));
        assertTrue(propertyFilter.test(entity3, graph));
    }

    /**
     * Test the filter for an entity's vendor ID.
     */
    @Test
    public void testSearchFilterVendorID() {
        final SearchFilter vendorIdFilter = SearchProtoUtil.searchFilterProperty(
            Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.VENDOR_ID)
                .setListFilter(ListFilter.newBuilder()
                    .setStringFilter(StringFilter.newBuilder().setStringPropertyRegex("id-1.*"))
                ).build());

        final TestGraphEntity entityWithMatchingId =
            TestGraphEntity.newBuilder(1L, ApiEntityType.VIRTUAL_VOLUME)
                .addTargetIdentity(333L, "id-123")
                .build();
        final TestGraphEntity entityNotMatching =
            TestGraphEntity.newBuilder(2L, ApiEntityType.VIRTUAL_VOLUME)
                .addTargetIdentity(333L, "id-456")
                .build();
        final TestGraphEntity entityWithOneMatchingOneUnmatching =
            TestGraphEntity.newBuilder(1L, ApiEntityType.VIRTUAL_VOLUME)
                .addTargetIdentity(333L, "id-789")
                .addTargetIdentity(444L, "id-100")
                .build();
        final TestGraphEntity entityNoVendorId =
            TestGraphEntity.newBuilder(3L, ApiEntityType.VIRTUAL_VOLUME).build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(vendorIdFilter);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        assertTrue(propertyFilter.test(entityWithMatchingId, graph));
        assertTrue(propertyFilter.test(entityWithOneMatchingOneUnmatching, graph));
        assertFalse(propertyFilter.test(entityNotMatching, graph));
        assertFalse(propertyFilter.test(entityNoVendorId, graph));
    }

    /**
     * Test that this filter filters out business accounts which have no associated targets.
     */
    @Test
    public void testAssociatedTargetFilter() {
        final long account1Id = 1L;
        final long account2Id = 2L;
        final long account3Id = 3L;
        final long account4Id = 4L;
        final long target1Id = 11L;
        final long target2Id = 22L;

        final PropertyFilter<TestGraphEntity> associatedTargetFilter = filterFactory.filterFor(
                Search.PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.ASSOCIATED_TARGET_ID)
                        .setNumericFilter(NumericFilter.getDefaultInstance())
                        .build());

        final TestGraphEntity account1 =
                TestGraphEntity.newBuilder(account1Id, ApiEntityType.BUSINESS_ACCOUNT)
                        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                                .setBusinessAccount(BusinessAccountInfo.newBuilder()
                                        .setAssociatedTargetId(target1Id)
                                        .build())
                                .build())
                        .build();
        final TestGraphEntity account2 =
                TestGraphEntity.newBuilder(account2Id, ApiEntityType.BUSINESS_ACCOUNT)
                        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                                .setBusinessAccount(BusinessAccountInfo.newBuilder()
                                        .setAssociatedTargetId(target2Id)
                                        .build())
                                .build())
                        .build();
        final TestGraphEntity account3 =
                TestGraphEntity.newBuilder(account3Id, ApiEntityType.BUSINESS_ACCOUNT)
                        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                                .setBusinessAccount(BusinessAccountInfo.getDefaultInstance())
                                .build())
                        .build();
        final TestGraphEntity account4 =
                TestGraphEntity.newBuilder(account4Id, ApiEntityType.BUSINESS_ACCOUNT).build();

        assertThat(associatedTargetFilter.apply(Stream.of(account1, account2, account3, account4),
                graph).collect(Collectors.toList()), contains(account1, account2));
    }

    /**
     * Test exception in case if we try to filter entity which has not this property.
     */
    @Test
    public void testAssociatedTargetFilterInvalidEntity() {
        final PropertyFilter<TestGraphEntity> associatedTargetFilter = filterFactory.filterFor(
                Search.PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.ASSOCIATED_TARGET_ID)
                        .setNumericFilter(NumericFilter.getDefaultInstance())
                        .build());

        final TestGraphEntity notSupportedEntity =
                TestGraphEntity.newBuilder(1L, ApiEntityType.VIRTUAL_MACHINE)
                        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                                .setVirtualMachine(VirtualMachineInfo.getDefaultInstance())
                                .build())
                        .build();

        assertTrue(associatedTargetFilter.apply(Stream.of(notSupportedEntity), graph)
                .collect(Collectors.toList()).isEmpty());
    }

    /**
     * Test the filter for a WorkloadController entity's controller type with given options.
     */
    @Test
    public void testSearchFilterControllerTypeWithOptions() {
        final SearchFilter searchFilter = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.WC_INFO_REPO_DTO_PROPERTY_NAME)
                .setObjectFilter(ObjectFilter.newBuilder()
                    .addFilters(Search.PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.CONTROLLER_TYPE)
                        .setStringFilter(StringFilter.newBuilder().addOptions(ControllerTypeCase.DEPLOYMENT_INFO.name()).build())
                    )
                )
            ).build();
        final TestGraphEntity vmEntity =
            TestGraphEntity.newBuilder(1L, ApiEntityType.VIRTUAL_MACHINE).build();
        final TestGraphEntity pmEntity =
            TestGraphEntity.newBuilder(2L, ApiEntityType.PHYSICAL_MACHINE).build();

        final TestGraphEntity wcEntityMatching =
            TestGraphEntity.newBuilder(3L, ApiEntityType.WORKLOAD_CONTROLLER)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setWorkloadController(WorkloadControllerInfo.newBuilder()
                        .setDeploymentInfo(DeploymentInfo.newBuilder().build()))
                    .build())
                .build();
        final TestGraphEntity wcEntityNotMatching =
            TestGraphEntity.newBuilder(4L, ApiEntityType.WORKLOAD_CONTROLLER)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setWorkloadController(WorkloadControllerInfo.newBuilder()
                        .setStatefulSetInfo(StatefulSetInfo.newBuilder().build()))
                    .build())
                .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchFilter);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        assertFalse(propertyFilter.test(vmEntity, graph));
        assertFalse(propertyFilter.test(pmEntity, graph));
        assertTrue(propertyFilter.test(wcEntityMatching, graph));
        assertFalse(propertyFilter.test(wcEntityNotMatching, graph));
    }

    /**
     * Test the filter for a WorkloadController entity's controller type with "Other" options.
     * We'll search for WorkloadControllers with any customer controller type.
     */
    @Test
    public void testSearchFilterControllerTypeWithOtherOptions() {
        final SearchFilter searchFilter = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.WC_INFO_REPO_DTO_PROPERTY_NAME)
                .setObjectFilter(ObjectFilter.newBuilder()
                    .addFilters(Search.PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.CONTROLLER_TYPE)
                        .setStringFilter(StringFilter.newBuilder().addOptions(SearchableProperties.OTHER_CONTROLLER_TYPE).build())
                    )
                )
            ).build();
        final TestGraphEntity vmEntity =
            TestGraphEntity.newBuilder(1L, ApiEntityType.VIRTUAL_MACHINE).build();
        final TestGraphEntity pmEntity =
            TestGraphEntity.newBuilder(2L, ApiEntityType.PHYSICAL_MACHINE).build();

        final TestGraphEntity wcEntityMatching =
            TestGraphEntity.newBuilder(3L, ApiEntityType.WORKLOAD_CONTROLLER)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setWorkloadController(WorkloadControllerInfo.newBuilder()
                        .setCustomControllerInfo(CustomControllerInfo.newBuilder().build()))
                    .build())
                .build();
        final TestGraphEntity wcEntityNotMatching =
            TestGraphEntity.newBuilder(4L, ApiEntityType.WORKLOAD_CONTROLLER)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setWorkloadController(WorkloadControllerInfo.newBuilder()
                        .setStatefulSetInfo(StatefulSetInfo.newBuilder().build()))
                    .build())
                .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchFilter);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        assertFalse(propertyFilter.test(vmEntity, graph));
        assertFalse(propertyFilter.test(pmEntity, graph));
        assertTrue(propertyFilter.test(wcEntityMatching, graph));
        assertFalse(propertyFilter.test(wcEntityNotMatching, graph));
    }

    /**
     * Test the filter for a WorkloadController entity's controller type with regex.
     */
    @Test
    public void testSearchFilterControllerTypeWithRegex() {
        final SearchFilter searchFilter = SearchFilter.newBuilder()
            .setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.WC_INFO_REPO_DTO_PROPERTY_NAME)
                .setObjectFilter(ObjectFilter.newBuilder()
                    .addFilters(Search.PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.CONTROLLER_TYPE)
                        .setStringFilter(StringFilter.newBuilder().setStringPropertyRegex("DeploymentCo.*").build())
                    )
                )
            ).build();
        final TestGraphEntity vmEntity =
            TestGraphEntity.newBuilder(1L, ApiEntityType.VIRTUAL_MACHINE).build();
        final TestGraphEntity pmEntity =
            TestGraphEntity.newBuilder(2L, ApiEntityType.PHYSICAL_MACHINE).build();

        final TestGraphEntity wcEntityMatching =
            TestGraphEntity.newBuilder(3L, ApiEntityType.WORKLOAD_CONTROLLER)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setWorkloadController(WorkloadControllerInfo.newBuilder()
                        .setCustomControllerInfo(CustomControllerInfo.newBuilder()
                            .setCustomControllerType("DeploymentConfig").build()))
                    .build())
                .build();
        final TestGraphEntity wcEntityNotMatching =
            TestGraphEntity.newBuilder(4L, ApiEntityType.WORKLOAD_CONTROLLER)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setWorkloadController(WorkloadControllerInfo.newBuilder()
                        .setStatefulSetInfo(StatefulSetInfo.newBuilder().build()))
                    .build())
                .build();

        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchFilter);
        assertTrue(filter instanceof PropertyFilter);
        PropertyFilter<TestGraphEntity> propertyFilter = (PropertyFilter<TestGraphEntity>)filter;

        assertFalse(propertyFilter.test(vmEntity, graph));
        assertFalse(propertyFilter.test(pmEntity, graph));
        assertTrue(propertyFilter.test(wcEntityMatching, graph));
        assertFalse(propertyFilter.test(wcEntityNotMatching, graph));
    }

    private TestGraphEntity makeVmWithConnectedNetworks(long id, String... connectedNetworks) {
        final VirtualMachineInfo vmInfo = VirtualMachineInfo.newBuilder()
                                              .addAllConnectedNetworks(Arrays.asList(connectedNetworks))
                                              .build();
        return TestGraphEntity.newBuilder(id, ApiEntityType.VIRTUAL_MACHINE)
                   .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                                            .setVirtualMachine(vmInfo)
                                            .build())
                   .build();
    }

    private PropertyFilter<TestGraphEntity> makeDiscoveringTargetFilter(StringFilter stringFilter) {
        return (PropertyFilter)filterFactory.filterFor(
                                 SearchFilter.newBuilder()
                                   .setPropertyFilter(
                                      Search.PropertyFilter.newBuilder()
                                          .setPropertyName(SearchableProperties.DISCOVERED_BY_TARGET)
                                          .setListFilter(ListFilter.newBuilder()
                                                            .setStringFilter(stringFilter)))
                                   .build());
    }

    private PropertyFilter<TestGraphEntity> makeDiscoveringTargetFilter(NumericFilter numericFilter) {
        return (PropertyFilter)filterFactory.filterFor(
                SearchFilter.newBuilder()
                        .setPropertyFilter(
                                Search.PropertyFilter.newBuilder()
                                        .setPropertyName(SearchableProperties.DISCOVERED_BY_TARGET)
                                        .setListFilter(ListFilter.newBuilder()
                                                .setNumericFilter(numericFilter)))
                        .build());
    }
}
