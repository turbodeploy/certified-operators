package com.vmturbo.topology.graph.search.filter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.LogicalOperator;
import com.vmturbo.common.protobuf.search.Search.MultiTraversalFilter;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.HotResizeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.HotResizeInfo.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ApplicationServiceInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessAccountInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CloudApplicationInfo;
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
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.topology.graph.TestGraphEntity;
import com.vmturbo.topology.graph.ThinSearchableProps.ThinComputeTierProps;
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
    private static final long TEN_SECONDS_MILLIS = 10_000L;

    private final TopologyFilterFactory<TestGraphEntity> filterFactory = new TopologyFilterFactory<>();

    /**
     * Map containing operators and their inverse so the inverse operation can be easily fetched ie EQ -> NE, GTE -> LT.
     */
    private static final Map<ComparisonOperator, ComparisonOperator> inverseOperator =
            ImmutableMap.of(
                    ComparisonOperator.EQ, ComparisonOperator.NE,
                    ComparisonOperator.NE, ComparisonOperator.EQ,
                    ComparisonOperator.LT, ComparisonOperator.GTE,
                    ComparisonOperator.LTE, ComparisonOperator.GT,
                    ComparisonOperator.GT, ComparisonOperator.LTE,
                    ComparisonOperator.GTE, ComparisonOperator.LT,
                    ComparisonOperator.MO, ComparisonOperator.NMO,
                    ComparisonOperator.NMO, ComparisonOperator.MO
                    );

    @Mock
    private TopologyGraph<TestGraphEntity> graph;

    @Before
    public void initMocks() {
        MockitoAnnotations.initMocks(this);
    }

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

        final TestGraphEntity entity1 = createGraphEntityBuilder(1234L,
                        ApiEntityType.VIRTUAL_MACHINE, "foo")
            .build();
        final TestGraphEntity entity2 = createGraphEntityBuilder(2345L,
                        ApiEntityType.VIRTUAL_MACHINE, "bar")
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

        final TestGraphEntity entity1 = createGraphEntityBuilder(1234L,
                        ApiEntityType.VIRTUAL_MACHINE, "MyEntity")
            .build();
        final TestGraphEntity entity2 = createGraphEntityBuilder(2345L,
                        ApiEntityType.VIRTUAL_MACHINE, "myentity")
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

        final TestGraphEntity entity1 = createGraphEntityBuilder(1234L,
                        ApiEntityType.VIRTUAL_MACHINE, "MyEntity")
            .build();
        final TestGraphEntity entity2 = createGraphEntityBuilder(2345L,
                        ApiEntityType.VIRTUAL_MACHINE, "myentity")
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

        final TestGraphEntity entity1 = createGraphEntityBuilder(1234L,
                        ApiEntityType.VIRTUAL_MACHINE, "foo")
            .build();
        final TestGraphEntity entity2 = createGraphEntityBuilder(2345L,
                        ApiEntityType.VIRTUAL_MACHINE, "bar")
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

        final TestGraphEntity entity1 = createGraphEntityBuilder(1234L,
                        ApiEntityType.VIRTUAL_MACHINE, "foo")
            .build();
        final TestGraphEntity entity2 = createGraphEntityBuilder(2345L,
                        ApiEntityType.VIRTUAL_MACHINE, "bar")
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
     * Checks in case comparison value in numeric filter is 0 and comparison operator is multiple of then exception
     * will be thrown.
     */
    @Test
    public void testIncorrectValueForMultipleOfOperator() {
        checkExceptionForOperatorRequiringDivision(ComparisonOperator.MO);
    }

    /**
     * Checks in case comparison value in numeric filter is 0 and comparison operator is not multiple of then exception
     * will be thrown.
     */
    @Test
    public void testIncorrectValueForNotMultipleOfOperator() {
        checkExceptionForOperatorRequiringDivision(ComparisonOperator.NMO);
    }

    private void checkExceptionForOperatorRequiringDivision(ComparisonOperator operator) {
        final TestGraphEntity entity = createVmWithProperty(1L, VirtualMachineInfo.Builder::setCoresPerSocketRatio, 1);
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
                String.format("Comparison operator %s require division operation, but comparison value is '0'",
                        operator));
        final SearchFilter searchFilter =
                createSearchFilterForVmIntegerProperty(SearchableProperties.VM_INFO_CORES_PER_SOCKET,
                        operator, 0);
        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter =
                (PropertyFilter<TestGraphEntity>) filter;
        propertyFilter.test(entity, graph);
    }

    /**
     * Checks that cores per socket ratio value for VM correctly filtered.
     */
    @Test
    public void testSearchFilterCoresPerSocket() {
        checkObjectIntegerFilter(VirtualMachineInfo.Builder::setCoresPerSocketRatio,
                        SearchableProperties.VM_INFO_CORES_PER_SOCKET);
    }

    /**
     * Checks that number of days unattached for volume are correctly filtered.
     */
    @Test
    public void testSearchFilterNumDaysUnattached() {
        checkObjectIntegerFilterForVolumes(VirtualVolumeInfo.Builder::setDaysUnattached,
                SearchableProperties.VOLUME_UNATTACHED_DAYS);
    }

    /**
     * Checks that number of CPUs value for VM correctly filtered.
     */
    @Test
    public void testSearchFilterNumCpus() {
        checkObjectIntegerFilter(VirtualMachineInfo.Builder::setNumCpus,
                        SearchableProperties.VM_INFO_NUM_CPUS);
    }

    /**
     * Checks that sockets value for VM correctly filtered.
     */
    @Test
    public void testSearchFilterSockets() {
        final int cpsr = 2;
        checkObjectIntegerFilter((builder, value) -> builder.setCoresPerSocketRatio(cpsr)
                        .setNumCpus(value * cpsr), SearchableProperties.VM_INFO_SOCKETS);
    }

    /**
     * Checks that only filter by bought commodity types is working as expected.
     */
    @Test
    public void checkHasBoughtCommoditiesFilter() {
        final CommoditiesBoughtFromProvider.Builder cbfp =
                        CommoditiesBoughtFromProvider.newBuilder().setProviderId(3L)
                                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                                        .setCommodityType(CommodityType.newBuilder()
                                                                        .setType(CommodityDTO.CommodityType.CLUSTER_VALUE)));
        final TestGraphEntity vm1 = TestGraphEntity.newBuilder(1L, ApiEntityType.VIRTUAL_MACHINE)
                        .addBoughtFromProvider(cbfp.build()).build();
        final TestGraphEntity vm2 =
                        TestGraphEntity.newBuilder(2L, ApiEntityType.VIRTUAL_MACHINE).build();
        final Search.PropertyFilter internalPropertyFilter = Search.PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.COMMODITY_TYPE_PROPERTY_NAME)
                        .setStringFilter(StringFilter.newBuilder()
                                        .addOptions(CommodityDTO.CommodityType.CLUSTER.name())
                                        .build()).build();
        final ListFilter listFilter = ListFilter.newBuilder().setObjectFilter(
                                        ObjectFilter.newBuilder().addFilters(internalPropertyFilter).build())
                        .build();
        final SearchFilter searchFilter = SearchFilter.newBuilder().setPropertyFilter(
                        Search.PropertyFilter.newBuilder().setPropertyName(
                                                        SearchableProperties.COMMODITY_BOUGHT_LIST_PROPERTY_NAME)
                                        .setListFilter(listFilter).build()).build();
        final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchFilter);
        assertTrue(filter instanceof PropertyFilter);
        final PropertyFilter<TestGraphEntity> propertyFilter =
                        (PropertyFilter<TestGraphEntity>)filter;
        Assert.assertThat(propertyFilter.test(vm1, graph), CoreMatchers.is(true));
        Assert.assertThat(propertyFilter.test(vm2, graph), CoreMatchers.is(false));
    }

    @Test
    public void testSearchFilterAppCount() {
        // Test for VirtualMachineSpecs filter by app count. This entity may represent Azure App Service Plans (ASPs) and related/similar entities like Scale Sets/Autoscaling groups
        Map<TestGraphEntity, Integer> entities = createVirtualMachineSpecsToTest(ApplicationServiceInfo.Builder::setAppCount);
        // Test that the filter works properly for all of these ops and their inverse op.
        checkObjectIntegerFilter( SearchableProperties.VIRTUAL_MACHINE_SPEC_APP_COUNT, entities, ComparisonOperator.EQ, 2);
        checkObjectIntegerFilter( SearchableProperties.VIRTUAL_MACHINE_SPEC_APP_COUNT, entities, ComparisonOperator.GT, 2);
        checkObjectIntegerFilter( SearchableProperties.VIRTUAL_MACHINE_SPEC_APP_COUNT, entities, ComparisonOperator.GTE, 2);
        // sanity check NE
        checkObjectIntegerFilter( SearchableProperties.VIRTUAL_MACHINE_SPEC_APP_COUNT, entities, ComparisonOperator.NE, 2);
    }

    @Test
    public void testSearchFilterWebappHybridConnectionCount(){
        // Test for Application/App Component Specs filter by hybrid conn count. This entity may represent Azure App Service webapps
        Map<TestGraphEntity, Integer> entities = createAppComponentSpecsToTest(CloudApplicationInfo.Builder::setHybridConnectionCount);
        // Test that the filter works properly for all of these ops and their inverse op.
        checkObjectIntegerFilter( SearchableProperties.APP_COMPONENT_SPEC_HYBRID_CONNECTIONS, entities, ComparisonOperator.EQ, 2);
        checkObjectIntegerFilter( SearchableProperties.APP_COMPONENT_SPEC_HYBRID_CONNECTIONS, entities, ComparisonOperator.GT, 2);
        checkObjectIntegerFilter( SearchableProperties.APP_COMPONENT_SPEC_HYBRID_CONNECTIONS, entities, ComparisonOperator.GTE, 2);
        // sanity check NE
        checkObjectIntegerFilter( SearchableProperties.APP_COMPONENT_SPEC_HYBRID_CONNECTIONS, entities, ComparisonOperator.NE, 2);
    }

    @Test
    public void testSearchFilterWebappDeploymentSlotCount(){
        // Test for Application/App Component Specs filter by staging/deployment slot count. This entity may represent Azure App Service webapps
        Map<TestGraphEntity, Integer> entities = createAppComponentSpecsToTest(CloudApplicationInfo.Builder::setDeploymentSlotCount);
        // Test that the filter works properly for all of these ops and their inverse op.
        checkObjectIntegerFilter( SearchableProperties.APP_COMPONENT_SPEC_DEPLOYMENT_SLOTS, entities, ComparisonOperator.EQ, 2);
        checkObjectIntegerFilter( SearchableProperties.APP_COMPONENT_SPEC_DEPLOYMENT_SLOTS, entities, ComparisonOperator.GT, 2);
        checkObjectIntegerFilter( SearchableProperties.APP_COMPONENT_SPEC_DEPLOYMENT_SLOTS, entities, ComparisonOperator.GTE, 2);
        // sanity check NE
        checkObjectIntegerFilter( SearchableProperties.APP_COMPONENT_SPEC_DEPLOYMENT_SLOTS, entities, ComparisonOperator.NE, 2);
    }

    private void checkObjectIntegerFilter(
                    BiFunction<VirtualMachineInfo.Builder, Integer, VirtualMachineInfo.Builder> builderConfigurator,
                    String property) {
        final TestGraphEntity vm1 = createVmWithProperty(1L, builderConfigurator, 1);
        final TestGraphEntity vm2 = createVmWithProperty(2L, builderConfigurator, 2);
        final TestGraphEntity vm3 = createVmWithProperty(3L, builderConfigurator, 3);
        final ImmutableTable.Builder<ComparisonOperator, TestGraphEntity, Boolean> dataBuilder =
                        ImmutableTable.builder();
        dataBuilder.put(ComparisonOperator.EQ, vm1, false);
        dataBuilder.put(ComparisonOperator.EQ, vm2, true);
        dataBuilder.put(ComparisonOperator.EQ, vm3, false);
        dataBuilder.put(ComparisonOperator.LT, vm1, true);
        dataBuilder.put(ComparisonOperator.LT, vm2, false);
        dataBuilder.put(ComparisonOperator.LT, vm3, false);
        dataBuilder.put(ComparisonOperator.GT, vm1, false);
        dataBuilder.put(ComparisonOperator.GT, vm2, false);
        dataBuilder.put(ComparisonOperator.GT, vm3, true);
        dataBuilder.put(ComparisonOperator.MO, vm1, false);
        dataBuilder.put(ComparisonOperator.MO, vm2, true);
        dataBuilder.put(ComparisonOperator.MO, vm3, false);
        dataBuilder.put(ComparisonOperator.NMO, vm1, true);
        dataBuilder.put(ComparisonOperator.NMO, vm2, false);
        dataBuilder.put(ComparisonOperator.NMO, vm3, true);
        final Table<ComparisonOperator, TestGraphEntity, Boolean> data = dataBuilder.build();
        data.rowMap().forEach((operator, entityToFilterResult) -> {
            final SearchFilter searchFilter = createSearchFilterForVmIntegerProperty(property, operator, 2);
            final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchFilter);
            assertTrue(filter instanceof PropertyFilter);
            final PropertyFilter<TestGraphEntity> propertyFilter =
                            (PropertyFilter<TestGraphEntity>)filter;
            entityToFilterResult.forEach((entity, result) -> Assert.assertThat(
                            propertyFilter.test(entity, graph), CoreMatchers.is(result)));
        });
    }

    private void checkObjectIntegerFilterForVolumes(
            BiFunction<VirtualVolumeInfo.Builder, Integer, VirtualVolumeInfo.Builder> builderConfigurator,
            String property) {
        final TestGraphEntity vol1 = createVolWithProperty(1L, builderConfigurator, 1);
        final TestGraphEntity vol2 = createVolWithProperty(2L, builderConfigurator, 2);
        final TestGraphEntity vol3 = createVolWithProperty(3L, builderConfigurator, 3);
        final ImmutableTable.Builder<ComparisonOperator, TestGraphEntity, Boolean> dataBuilder =
                ImmutableTable.builder();
        dataBuilder.put(ComparisonOperator.EQ, vol1, false);
        dataBuilder.put(ComparisonOperator.EQ, vol2, true);
        dataBuilder.put(ComparisonOperator.EQ, vol3, false);
        dataBuilder.put(ComparisonOperator.LT, vol1, true);
        dataBuilder.put(ComparisonOperator.LT, vol2, false);
        dataBuilder.put(ComparisonOperator.LT, vol3, false);
        dataBuilder.put(ComparisonOperator.GT, vol1, false);
        dataBuilder.put(ComparisonOperator.GT, vol2, false);
        dataBuilder.put(ComparisonOperator.GT, vol3, true);
        dataBuilder.put(ComparisonOperator.LTE, vol1, true);
        dataBuilder.put(ComparisonOperator.LTE, vol2, true);
        dataBuilder.put(ComparisonOperator.LTE, vol3, false);
        dataBuilder.put(ComparisonOperator.GTE, vol1, false);
        dataBuilder.put(ComparisonOperator.GTE, vol2, true);
        dataBuilder.put(ComparisonOperator.GTE, vol3, true);
        final Table<ComparisonOperator, TestGraphEntity, Boolean> data = dataBuilder.build();
        data.rowMap().forEach((operator, entityToFilterResult) -> {
            final SearchFilter searchFilter = createSearchFilterForVolIntegerProperty(property, operator, 2);
            final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchFilter);
            assertTrue(filter instanceof PropertyFilter);
            final PropertyFilter<TestGraphEntity> propertyFilter =
                    (PropertyFilter<TestGraphEntity>)filter;
            entityToFilterResult.forEach((entity, result) -> Assert.assertThat(
                    propertyFilter.test(entity, graph), CoreMatchers.is(result)));
        });
    }

    private Map<TestGraphEntity, Integer> createAppComponentSpecsToTest(BiFunction<CloudApplicationInfo.Builder, Integer, CloudApplicationInfo.Builder> builderConfigurator){
        // Key is entity, value is expected property numeric value (needed to do as we can't really pass around getters and setters like that)
        Map<TestGraphEntity, Integer> entities = new HashMap<>();
        final TestGraphEntity acs1 = createAppComponentSpecWithProperty(1L, builderConfigurator, 1);
        entities.put(acs1, 1);
        final TestGraphEntity acs2 = createAppComponentSpecWithProperty(2L, builderConfigurator, 2);
        entities.put(acs2, 2);
        final TestGraphEntity acs3 = createAppComponentSpecWithProperty(3L, builderConfigurator, 3);
        entities.put(acs3, 3);
        return entities;
    }

    private Map<TestGraphEntity, Integer> createVirtualMachineSpecsToTest(BiFunction<ApplicationServiceInfo.Builder, Integer, ApplicationServiceInfo.Builder> builderConfigurator){
        // Key is entity, value is expected property numeric value (needed to do as we can't really pass around getters and setters like that)
        Map<TestGraphEntity, Integer> entities = new HashMap<>();
        final TestGraphEntity vms1 = createVirtualMachineSpecWithProperty(1L, builderConfigurator, 1);
        entities.put(vms1, 1);
        final TestGraphEntity vms2 = createVirtualMachineSpecWithProperty(2L, builderConfigurator, 2);
        entities.put(vms2, 2);
        final TestGraphEntity vms3 = createVirtualMachineSpecWithProperty(3L, builderConfigurator, 3);
        entities.put(vms3, 3);
        return entities;
    }


    private void checkObjectIntegerFilter(
            String property, Map<TestGraphEntity, Integer> entities, ComparisonOperator operation, int comparisonValue){
        final ImmutableTable.Builder<ComparisonOperator, TestGraphEntity, Boolean> dataBuilder =
                ImmutableTable.builder();
        for(Entry<TestGraphEntity, Integer> entity : entities.entrySet()){
            dataBuilder.put(operation, entity.getKey(), getExpectedResult(operation, comparisonValue, entity.getValue()));
        }
        final Table<ComparisonOperator, TestGraphEntity, Boolean> data = dataBuilder.build();
        data.rowMap().forEach((operator, entityToFilterResult) -> {
            final SearchFilter searchFilter = createGenericRegularSearchFilterForNumericValue(property, operator, 2);
            final SearchFilter reverseFilter = createGenericRegularSearchFilterForNumericValue(property, inverseOperator.get(operator), 2);

            final TopologyFilter<TestGraphEntity> filter = filterFactory.filterFor(searchFilter);
            final TopologyFilter<TestGraphEntity> reverse = filterFactory.filterFor(reverseFilter);

            assertTrue(filter instanceof PropertyFilter);
            assertTrue(reverse instanceof PropertyFilter);

            final PropertyFilter<TestGraphEntity> propertyFilter =
                    (PropertyFilter<TestGraphEntity>)filter;
            final PropertyFilter<TestGraphEntity> reversePropertyFilter =
                    (PropertyFilter<TestGraphEntity>)reverse;
            entityToFilterResult.forEach((entity, result) -> {
                // Test the filter and make sure the inverse operation works too.
                assertEquals(propertyFilter.test(entity, graph), result);
                assertNotEquals(reversePropertyFilter.test(entity, graph), result);
            });
        });
    }

    private boolean getExpectedResult(ComparisonOperator operation, int comparisonValue, int currentValue){
        switch(operation){
            case EQ:
                return comparisonValue == currentValue;
            case NE:
                return comparisonValue != currentValue;
            case GT:
                return comparisonValue < currentValue;
            case GTE:
                return comparisonValue <= currentValue;
            case LT:
                return comparisonValue > currentValue;
            case LTE:
                return comparisonValue >= currentValue;
            default:
                return false;
        }
    }

    private static SearchFilter createSearchFilterForVolIntegerProperty(String property,
                                                                       ComparisonOperator operator, int comparisonValue) {
        final Search.PropertyFilter numericPropertyFilter =
                Search.PropertyFilter.newBuilder().setPropertyName(property)
                        .setNumericFilter(NumericFilter.newBuilder()
                                .setComparisonOperator(operator).setValue(comparisonValue)
                                .build()).build();
        return SearchFilter.newBuilder().setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.VOLUME_REPO_DTO)
                .setObjectFilter(ObjectFilter.newBuilder().addFilters(numericPropertyFilter)
                        .build()).build()).build();
    }

    private static SearchFilter createSearchFilterForVmIntegerProperty(String property,
            ComparisonOperator operator, int comparisonValue) {
        final Search.PropertyFilter numericPropertyFilter =
                        Search.PropertyFilter.newBuilder().setPropertyName(property)
                                        .setNumericFilter(NumericFilter.newBuilder()
                                                        .setComparisonOperator(operator).setValue(comparisonValue)
                                                        .build()).build();
        return SearchFilter.newBuilder().setPropertyFilter(Search.PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.VM_INFO_REPO_DTO_PROPERTY_NAME)
                        .setObjectFilter(ObjectFilter.newBuilder().addFilters(numericPropertyFilter)
                                        .build()).build()).build();
    }

    private static SearchFilter createGenericRegularSearchFilterForNumericValue(String property,
            ComparisonOperator operator, int comparisonValue) {
        return SearchFilter.newBuilder().setPropertyFilter(Search.PropertyFilter.newBuilder()
                .setPropertyName(property)
                .setNumericFilter(NumericFilter.newBuilder()
                        .setComparisonOperator(operator).setValue(comparisonValue)
                        .build()).build()).build();
    }


    private static TestGraphEntity createVmWithProperty(long oid,
                    @Nonnull BiFunction<VirtualMachineInfo.Builder, Integer, VirtualMachineInfo.Builder> propertySetter,
                    int value) {
        return TestGraphEntity.newBuilder(oid, ApiEntityType.VIRTUAL_MACHINE).setTypeSpecificInfo(
                        TypeSpecificInfo.newBuilder().setVirtualMachine(
                                        propertySetter.apply(VirtualMachineInfo.newBuilder(), value)
                                                        .build()).build()).build();
    }

    private static TestGraphEntity createVirtualMachineSpecWithProperty(long oid,
            @Nonnull BiFunction<ApplicationServiceInfo.Builder, Integer, ApplicationServiceInfo.Builder> propertySetter,
            int value) {
        return TestGraphEntity.newBuilder(oid, ApiEntityType.VIRTUAL_MACHINE_SPEC).setTypeSpecificInfo(
                TypeSpecificInfo.newBuilder().setApplicationService(
                        propertySetter.apply(ApplicationServiceInfo.newBuilder(), value)
                                .build()).build()).build();
    }

    private static TestGraphEntity createAppComponentSpecWithProperty(long oid,
            @Nonnull BiFunction<CloudApplicationInfo.Builder, Integer, CloudApplicationInfo.Builder> propertySetter,
            int value) {
        return TestGraphEntity.newBuilder(oid, ApiEntityType.APPLICATION_COMPONENT_SPEC).setTypeSpecificInfo(
                TypeSpecificInfo.newBuilder().setCloudApplication(
                        propertySetter.apply(CloudApplicationInfo.newBuilder(), value)
                                .build()).build()).build();
    }

    private static TestGraphEntity createVolWithProperty(long oid,
                                                        @Nonnull BiFunction<VirtualVolumeInfo.Builder, Integer, VirtualVolumeInfo.Builder> propertySetter,
                                                        int value) {
        return TestGraphEntity.newBuilder(oid, ApiEntityType.VIRTUAL_VOLUME).setTypeSpecificInfo(
                TypeSpecificInfo.newBuilder().setVirtualVolume(
                        propertySetter.apply(VirtualVolumeInfo.newBuilder(), value)
                                .build()).build()).build();
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

    private static TestGraphEntity createVm(int commodityTypeNumber, Builder vmHotResizeInfo,
                    long oid) {
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

    /**
     * Checks that {@link MultiTraversalFilter} working as expected and combining results through OR
     * condition correctly.
     */
    @Test
    public void checkMultiRelationsFilterOrCondition() {
        final TestGraphEntity.Builder vv1 =
                        createGraphEntityBuilder(124L, ApiEntityType.VIRTUAL_VOLUME, "vv1");
        final TestGraphEntity.Builder vv2 =
                        createGraphEntityBuilder(125L, ApiEntityType.VIRTUAL_VOLUME, "vv2");
        final TestGraphEntity.Builder vv3 =
                        createGraphEntityBuilder(126L, ApiEntityType.VIRTUAL_VOLUME, "vv3");
        checkMultiRelationsFilter(LogicalOperator.OR, ImmutableSet.of(vv1, vv3),
                        ImmutableSet.of(vv2, vv3),
                        Stream.of(vv1, vv2, vv3).map(TestGraphEntity.Builder::build)
                                        .collect(Collectors.toSet()));
    }

    /**
     * Checks that {@link MultiTraversalFilter} working as expected and combining results through
     * AND condition correctly.
     */
    @Test
    public void checkMultiRelationsFilterAndCondition() {
        final TestGraphEntity.Builder vv1 =
                        createGraphEntityBuilder(124L, ApiEntityType.VIRTUAL_VOLUME, "vv1");
        final TestGraphEntity.Builder vv2 =
                        createGraphEntityBuilder(125L, ApiEntityType.VIRTUAL_VOLUME, "vv2");
        final TestGraphEntity.Builder vv3 =
                        createGraphEntityBuilder(126L, ApiEntityType.VIRTUAL_VOLUME, "vv3");
        checkMultiRelationsFilter(LogicalOperator.AND, ImmutableSet.of(vv1, vv3),
                        ImmutableSet.of(vv2, vv3), Collections.singleton(vv3.build()));
    }

    /**
     * Checks that multi relations filter will not cause significant performance degradation.
     */
    @Test
    public void checkMultiTraversalPerformanceTest() {
        final Map<Long, TestGraphEntity.Builder> storages = new HashMap<>();
        final AtomicLong idGenerator = new AtomicLong();
        final int storageAmount = 500;
        final int relatedEntitiesAmount = 3000;
        final Map<Long, Collection<TestGraphEntity.Builder>> storageIdToVvs = new HashMap<>();
        final Map<Long, Collection<TestGraphEntity.Builder>> storageIdToVms = new HashMap<>();
        IntStream.range(0, storageAmount).forEach(i -> {
            final long storageId = idGenerator.getAndIncrement();
            final TestGraphEntity.Builder storageBuilder =
                            createGraphEntityBuilder(storageId, ApiEntityType.STORAGE,
                                            String.format("st#%s", storageId));
            storages.put(storageId, storageBuilder);
            final Collection<TestGraphEntity.Builder> vvs = new HashSet<>();
            final Collection<TestGraphEntity.Builder> vms = new HashSet<>();
            IntStream.range(0, relatedEntitiesAmount).forEach(j -> {
                final long vvId = idGenerator.getAndIncrement();
                vvs.add(createGraphEntityBuilder(vvId, ApiEntityType.VIRTUAL_VOLUME,
                                String.format("vv#%s", vvId)));
                final long vmId = idGenerator.getAndIncrement();
                vms.add(createGraphEntityBuilder(vmId, ApiEntityType.VIRTUAL_MACHINE,
                                String.format("vm#%s", vmId)));
            });
            storageIdToVvs.put(storageId, vvs);
            storageIdToVms.put(storageId, vms);
            final TestGraphEntity storage = storageBuilder.build();
            final Collection<TestGraphEntity.Builder> consumers =
                            Stream.concat(vvs.stream(), vms.stream()).collect(Collectors.toSet());
            consumers.forEach(storageBuilder::addConsumer);
            Mockito.doAnswer((invocation) -> consumers.stream().map(TestGraphEntity.Builder::build))
                            .when(graph).getConsumers(storage);
            Mockito.doAnswer((invocation) -> Stream.concat(vvs.stream(), vms.stream())
                            .map(TestGraphEntity.Builder::build)).when(graph)
                            .getOwnersOrAggregators(storage);
        });

        final SearchFilter searchFilter = createSearchFilter(LogicalOperator.OR);
        final Iterator<Entry<Long, TestGraphEntity.Builder>> it = storages.entrySet().iterator();
        final int attempts = 50;
        int i = 0;
        float timeInFiltering = 0;
        while (it.hasNext() && i < attempts) {
            final long start = System.currentTimeMillis();
            final Entry<Long, TestGraphEntity.Builder> current = it.next();
            final Set<TestGraphEntity> result = filterFactory.filterFor(searchFilter)
                            .apply(Stream.of(current.getValue().build()), graph).collect(Collectors.toSet());
            final long end = System.currentTimeMillis();
            timeInFiltering += end - start;
            Assert.assertThat(result.size(), CoreMatchers.is(relatedEntitiesAmount * 2));
            final Long storageId = current.getKey();
            final Collection<TestGraphEntity.Builder> relatedVms =
                            storageIdToVms.getOrDefault(storageId, Collections.emptySet());
            final Collection<TestGraphEntity.Builder> relatedVolumes =
                            storageIdToVvs.getOrDefault(storageId, Collections.emptySet());
            Assert.assertThat(result, CoreMatchers.is(Stream
                            .concat(relatedVms.stream(), relatedVolumes.stream())
                            .map(TestGraphEntity.Builder::build).collect(Collectors.toSet())));
            i++;
        }
        Assert.assertTrue(
                        String.format("Multi-traversal filter execution takes more than '%s' milliseconds to complete",
                                        TEN_SECONDS_MILLIS),
                        timeInFiltering / attempts < TEN_SECONDS_MILLIS);
    }

    private void checkMultiRelationsFilter(LogicalOperator operation,
                    Collection<TestGraphEntity.Builder> consumers,
                    Collection<TestGraphEntity.Builder> connectedFrom,
                    Collection<TestGraphEntity> expectedObjects) {
        final SearchFilter searchFilter = createSearchFilter(operation);
        final TestGraphEntity.Builder targetEntityBuilder =
                        createGraphEntityBuilder(123, ApiEntityType.STORAGE, "st-1");
        final TestGraphEntity targetEntity = targetEntityBuilder.build();
        Mockito.doAnswer((invocation) -> consumers.stream().map(TestGraphEntity.Builder::build))
                        .when(graph).getConsumers(targetEntity);
        Mockito.doAnswer((invocation) -> connectedFrom.stream().map(TestGraphEntity.Builder::build))
                        .when(graph).getOwnersOrAggregators(targetEntity);
        final Set<TestGraphEntity> result = filterFactory.filterFor(searchFilter)
                        .apply(Stream.of(targetEntity), graph)
                        .collect(Collectors.toSet());
        Assert.assertThat(result, Matchers.is(expectedObjects));
    }

    private static TestGraphEntity.Builder createGraphEntityBuilder(long oid,
                    ApiEntityType entityType, String name) {
        return TestGraphEntity.newBuilder(oid, entityType).setName(name);
    }

    @Nonnull
    private static SearchFilter createSearchFilter(LogicalOperator operation) {
        final StoppingCondition stoppingCondition =
                        StoppingCondition.newBuilder().setNumberHops(1).build();
        final Search.TraversalFilter.Builder connectedFromFilter =
                        Search.TraversalFilter.newBuilder()
                                        .setTraversalDirection(TraversalDirection.CONNECTED_FROM)
                                        .setStoppingCondition(stoppingCondition);
        final Search.TraversalFilter.Builder consumesFilter = Search.TraversalFilter.newBuilder()
                        .setTraversalDirection(TraversalDirection.PRODUCES)
                        .setStoppingCondition(stoppingCondition);
        final MultiTraversalFilter.Builder multiTraversalFilter =
                        MultiTraversalFilter.newBuilder().setOperator(operation)
                                        .addTraversalFilter(connectedFromFilter)
                                        .addTraversalFilter(consumesFilter);
        return SearchFilter.newBuilder().setMultiTraversalFilter(multiTraversalFilter).build();
    }

    @Test
    public void testRegexContainsMatch() {
        final PropertyFilter<TestGraphEntity> displayNameFilter =
            filterFactory.filterFor(Search.PropertyFilter.newBuilder()
                .setPropertyName("displayName")
                .setStringFilter(StringFilter.newBuilder()
                        .setStringPropertyRegex("in-group")
                ).build());


        final TestGraphEntity entity = createGraphEntityBuilder(123L, ApiEntityType.VIRTUAL_MACHINE,
                        "entity-in-group-1")
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

        final TestGraphEntity entity = createGraphEntityBuilder(123L, ApiEntityType.VIRTUAL_MACHINE,
                        "entity-in-group-1")
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

        final TestGraphEntity entity = createGraphEntityBuilder(123L, ApiEntityType.VIRTUAL_MACHINE,
                        "entity-in-group-1")
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

        final Search.PropertyFilter searchFilterWithOnlyKey = Search.PropertyFilter
                .newBuilder()
                .setPropertyName(SearchableProperties.TAGS_TYPE_PROPERTY_NAME)
                .setMapFilter(
                        MapFilter
                                .newBuilder()
                                .setKey("KEY")
                ).build();

        final PropertyFilter<TestGraphEntity> positiveFilter = filterFactory.filterFor(positiveSearchFilter);
        final PropertyFilter<TestGraphEntity> negativeFilter = filterFactory.filterFor(negativeSearchFilter);
        final PropertyFilter<TestGraphEntity> filterWithRegex = filterFactory.filterFor(searchFilterWithRegex);
        final PropertyFilter<TestGraphEntity> filterWithOnlyKey = filterFactory.filterFor(searchFilterWithOnlyKey);

        // entity has no tags
        final TestGraphEntity noTagsEntity = TestGraphEntity.newBuilder(120L, ApiEntityType.VIRTUAL_MACHINE)
            .build();
        assertFalse(positiveFilter.test(noTagsEntity, graph));
        assertTrue(negativeFilter.test(noTagsEntity, graph));
        assertFalse(filterWithRegex.test(noTagsEntity, graph));
        assertFalse(filterWithOnlyKey.test(noTagsEntity, graph));

        // entity does not have the key
        final TestGraphEntity noKeyEntity = TestGraphEntity.newBuilder(121L, ApiEntityType.VIRTUAL_MACHINE)
            .addTag("OTHERKEY", Collections.singletonList("VALUE1"))
            .build();
        assertFalse(positiveFilter.test(noKeyEntity, graph));
        assertTrue(negativeFilter.test(noKeyEntity, graph));
        assertTrue(filterWithRegex.test(noKeyEntity, graph));
        assertFalse(filterWithOnlyKey.test(noKeyEntity, graph));

        // entity has the key, but not one of the values
        final TestGraphEntity wrongValueEntity = TestGraphEntity.newBuilder(122L, ApiEntityType.VIRTUAL_MACHINE)
            .addTag("OTHERKEY", Arrays.asList("VALUE1"))
            .addTag("KEY", Arrays.asList("VALUE3", "VALUE4"))
            .build();
        assertFalse(positiveFilter.test(wrongValueEntity, graph));
        assertTrue(negativeFilter.test(wrongValueEntity, graph));
        assertTrue(filterWithRegex.test(wrongValueEntity, graph));
        assertTrue(filterWithOnlyKey.test(wrongValueEntity, graph));

        // entity has the key, and one of the values
        final TestGraphEntity rightValueEntity = TestGraphEntity.newBuilder(123L, ApiEntityType.VIRTUAL_MACHINE)
            .addTag("KEY", Arrays.asList("VALUE2", "VALUE4"))
            .build();
        assertTrue(positiveFilter.test(rightValueEntity, graph));
        assertFalse(negativeFilter.test(rightValueEntity, graph));
        assertFalse(filterWithRegex.test(rightValueEntity, graph));
        assertTrue(filterWithOnlyKey.test(rightValueEntity, graph));
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
     * Indirectly tests ThinComputeTierProps.
     */
    @Test
    public void testSearchFilterForComputeTierConsumerEntityType() {
        // This TestGraphEntity entity has no vendor info
        // Should not match VirtualMachineSpec or VirtualMachine
        final TestGraphEntity compTierNoVendorId = TestGraphEntity.newBuilder(TopologyEntityDTO.newBuilder().setOid(1234L)
                        .setEntityType(EntityType.COMPUTE_TIER.getNumber())
                        .build())
                .build();
        // TestGraphEntity built from a TopologyEntityDTO with a VMSPECPROFILE vendorId
        // Should match VirtualMachineSpec but not VirtualMachine
        final TestGraphEntity compTierVmSpecProfile = TestGraphEntity.newBuilder(TopologyEntityDTO.newBuilder().setOid(1234L)
                        .setEntityType(EntityType.COMPUTE_TIER.getNumber())
                        .setOrigin(Origin.newBuilder()
                                .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                                        .putDiscoveredTargetData(123L,
                                                PerTargetEntityInformation.newBuilder()
                                                        .setVendorId("azure::VMSPECPROFILE::F1")
                                                        .build())
                                        .build()))
                        .build())
                .build();
        // TestGraphEntity built from a TopologyEntityDTO with a VMPROFILE vendorId
        // Should match VirtualMachine but not VirtualMachineSpec
        final TestGraphEntity compTierVmProfile = TestGraphEntity.newBuilder(TopologyEntityDTO.newBuilder().setOid(1234L)
                        .setEntityType(EntityType.COMPUTE_TIER.getNumber())
                        .setOrigin(Origin.newBuilder()
                                .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                                        .putDiscoveredTargetData(123L,
                                                PerTargetEntityInformation.newBuilder()
                                                        .setVendorId("azure::VMPROFILE::Standard_D4s_v3")
                                                        .build())
                                        .build()))
                        .build())
                .build();
        // TestGraphEntity built from a TopologyEntityDTO with a vendorId containing neither VMPROFILE nor VMSPECPROFILE
        // Should default to VirtualMachine
        final TestGraphEntity compTierNoProfile = TestGraphEntity.newBuilder(TopologyEntityDTO.newBuilder().setOid(1234L)
                        .setEntityType(EntityType.COMPUTE_TIER.getNumber())
                        .setOrigin(Origin.newBuilder()
                                .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                                        .putDiscoveredTargetData(123L,
                                                PerTargetEntityInformation.newBuilder()
                                                        .setVendorId("t3.small")
                                                        .build())
                                        .build()))
                        .build())
                .build();

        final PropertyFilter<TestGraphEntity> vmSpecFilter =
                getComputeTierConsumerEntityTypeSearchFilter("VirtualMachineSpec", true);
        final PropertyFilter<TestGraphEntity> notVmSpecFilter =
                getComputeTierConsumerEntityTypeSearchFilter("VirtualMachineSpec", false);
        final PropertyFilter<TestGraphEntity> vmFilter =
                getComputeTierConsumerEntityTypeSearchFilter("VirtualMachine", true);
        final PropertyFilter<TestGraphEntity> notVmFilter =
                getComputeTierConsumerEntityTypeSearchFilter("VirtualMachine", false);

        // VMSPECPROFILE filter
        assertFalse(vmSpecFilter.test(compTierNoVendorId, graph));
        assertTrue(vmSpecFilter.test(compTierVmSpecProfile, graph));
        assertFalse(vmSpecFilter.test(compTierVmProfile, graph));
        assertFalse(vmSpecFilter.test(compTierNoProfile, graph));

        // VMPROFILE filter
        assertFalse(vmFilter.test(compTierNoVendorId, graph));
        assertFalse(vmFilter.test(compTierVmSpecProfile, graph));
        assertTrue(vmFilter.test(compTierVmProfile, graph));
        assertTrue(vmFilter.test(compTierNoProfile, graph));

        // not VMSPECPROFILE filter
        assertTrue(notVmSpecFilter.test(compTierNoVendorId, graph));
        assertFalse(notVmSpecFilter.test(compTierVmSpecProfile, graph));
        assertTrue(notVmSpecFilter.test(compTierVmProfile, graph));
        assertTrue(notVmSpecFilter.test(compTierNoProfile, graph));

        // not VMPROFILE filter
        assertTrue(notVmFilter.test(compTierNoVendorId, graph));
        assertTrue(notVmFilter.test(compTierVmSpecProfile, graph));
        assertFalse(notVmFilter.test(compTierVmProfile, graph));
        assertFalse(notVmFilter.test(compTierNoProfile, graph));
    }

    private PropertyFilter<TestGraphEntity> getComputeTierConsumerEntityTypeSearchFilter(
            String consumerEntityType, boolean positiveMatch) {
        final SearchFilter searchFilter = SearchFilter.newBuilder()
                .setPropertyFilter(Search.PropertyFilter.newBuilder()
                        .setStringFilter(StringFilter.newBuilder()
                                .setPositiveMatch(positiveMatch)
                                .addOptions(consumerEntityType)
                                .build())
                        .setPropertyName(SearchableProperties.COMPUTE_TIER_CONSUMER_ENTITY_TYPE))
                .build();
        final TopologyFilter<TestGraphEntity> topologyFilter = filterFactory.filterFor(searchFilter);
        assertTrue(topologyFilter instanceof PropertyFilter);
        return (PropertyFilter<TestGraphEntity>)topologyFilter;
    }

    /**
     * Directly tests ThinComputeTierProps and that it handles vendorIds correctly.
     */
    @Test
    public void testGetComputeTierConsumerEntityType() {
        testGetComputeTierConsumerEntityType(getComputeTierTopologyEntity("azure::VMSPECPROFILE::F1"),
                EntityType.VIRTUAL_MACHINE_SPEC);
        testGetComputeTierConsumerEntityType(getComputeTierTopologyEntity("azure::VMPROFILE::Standard_D4s_v3"),
                EntityType.VIRTUAL_MACHINE);
        testGetComputeTierConsumerEntityType(getComputeTierTopologyEntity("t3.small"),
                EntityType.VIRTUAL_MACHINE);
    }

    private void testGetComputeTierConsumerEntityType(TopologyEntityDTO computeTierEntity,
            EntityType expectedEntityType) {
        final ThinComputeTierProps ctProps = new ThinComputeTierProps(null, null,
                computeTierEntity);
        final Set<EntityType> types = ctProps.getConsumerEntityTypes();
        assertThat(types.size(), equalTo(1));
        assertThat(types.iterator().next(), equalTo(expectedEntityType));
    }

    private TopologyEntityDTO getComputeTierTopologyEntity(String vendorId) {
        return TopologyEntityDTO.newBuilder()
                .setOid(1234L)
                .setEntityType(EntityType.COMPUTE_TIER.getNumber())
                .setOrigin(Origin.newBuilder()
                        .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                                .putDiscoveredTargetData(123L,
                                        PerTargetEntityInformation.newBuilder()
                                                .setVendorId(vendorId)
                                                .build())
                                .build()))
                .build();
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

    /**
     *  Tests the filter for the isVdi indicator for virtual machines.
     */
    @Test
    public void testSearchFilterForVMIsVdiMatch() {
        final SearchFilter searchCriteria = SearchFilter.newBuilder()
                .setPropertyFilter(Search.PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.IS_VDI)
                        .setStringFilter(StringFilter.newBuilder().setStringPropertyRegex("false")))
                .build();
        final PropertyFilter<TestGraphEntity> filter = (PropertyFilter)filterFactory.filterFor(
                searchCriteria);
        final TestGraphEntity vm1 = makeVmWithVdiStatus(1, true);
        final TestGraphEntity vm2 = makeVmWithVdiStatus(2, false);
        assertTrue(filter.test(vm2, graph));
        assertFalse(filter.test(vm1, graph));
    }

    /**
     * Used to obtain a TestGraphEntity with a VM having VDI status set.
     * @param id  vm id.
     * @param status VM VDI status.
     * @return TestGraphEntity.
     */
    private TestGraphEntity makeVmWithVdiStatus(long id, boolean status) {
        final VirtualMachineInfo vmInfo = VirtualMachineInfo.newBuilder().setIsVdi(status).build();
        return TestGraphEntity.newBuilder(id, ApiEntityType.VIRTUAL_MACHINE).setTypeSpecificInfo(
                TypeSpecificInfo.newBuilder().setVirtualMachine(vmInfo).build()).build();
    }
}
