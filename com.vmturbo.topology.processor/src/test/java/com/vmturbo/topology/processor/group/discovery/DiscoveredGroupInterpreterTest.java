package com.vmturbo.topology.processor.group.discovery;

import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.CLUSTER_DTO;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.DISPLAY_NAME;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.PLACEHOLDER_FILTER;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.SELECTION_DTO;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.STATIC_MEMBER_DTO;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.TARGET_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintInfo;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.MembersList;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.SelectionSpec;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.SelectionSpec.ExpressionType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.SelectionSpec.PropertyDoubleList;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.SelectionSpec.PropertyStringList;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.SelectionSpecList;
import com.vmturbo.topology.processor.entity.Entity;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupInterpreter.DefaultPropertyFilterConverter;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupInterpreter.PropertyFilterConverter;

public class DiscoveredGroupInterpreterTest {
    @Test
    public void testPropertyConverterDouble() {
        final PropertyFilterConverter converter = new DefaultPropertyFilterConverter();
        final SelectionSpec spec = SelectionSpec.newBuilder()
                .setProperty("prop")
                .setPropertyValueDouble(10)
                .setExpressionType(ExpressionType.LARGER_THAN)
                .build();
        final Optional<PropertyFilter> filterOpt = converter.selectionSpecToPropertyFilter(spec);
        assertTrue(filterOpt.isPresent());

        final PropertyFilter filter = filterOpt.get();
        assertEquals("prop", filter.getPropertyName());
        assertTrue(filter.hasNumericFilter());
        assertEquals(ComparisonOperator.GT, filter.getNumericFilter().getComparisonOperator());
        assertEquals(10.0, filter.getNumericFilter().getValue(), 0);
    }

    @Test
    public void testPropertyConverterString() {
        final PropertyFilterConverter converter = new DefaultPropertyFilterConverter();
        final SelectionSpec spec = SelectionSpec.newBuilder()
                .setProperty("prop")
                .setPropertyValueString("val")
                .setExpressionType(ExpressionType.EQUAL_TO)
                .build();

        final Optional<PropertyFilter> filterOpt = converter.selectionSpecToPropertyFilter(spec);
        assertTrue(filterOpt.isPresent());

        final PropertyFilter filter = filterOpt.get();
        assertEquals("prop", filter.getPropertyName());
        assertTrue(filter.hasStringFilter());
        assertEquals("val", filter.getStringFilter().getStringPropertyRegex());
        assertTrue(filter.getStringFilter().getMatch());
    }

    @Test
    public void testPropertyConverterUnhandled() {
        final PropertyFilterConverter converter = new DefaultPropertyFilterConverter();
        final SelectionSpec spec1 = SelectionSpec.newBuilder()
                .setProperty("prop")
                .setPropertyValueDoubleList(PropertyDoubleList.newBuilder()
                        .addPropertyValue(10.0))
                .setExpressionType(ExpressionType.LIST_OVERLAP)
                .build();
        final SelectionSpec spec2 = SelectionSpec.newBuilder()
                .setProperty("prop")
                .setPropertyValueStringList(PropertyStringList.newBuilder()
                        .addPropertyValue("val"))
                .setExpressionType(ExpressionType.LIST_OVERLAP)
                .build();

        assertFalse(converter.selectionSpecToPropertyFilter(spec1).isPresent());
        assertFalse(converter.selectionSpecToPropertyFilter(spec2).isPresent());
    }

    @Test
    public void testGroupConverterMemberList() {
        final EntityStore store = mock(EntityStore.class);
        when(store.getTargetEntityIdMap(TARGET_ID))
                .thenReturn(Optional.of(ImmutableMap.of("1", 1L)));

        Entity entity = mock(Entity.class);
        when(store.getEntity(1))
                .thenReturn(Optional.of(entity));
        when(entity.getEntityType()).thenReturn(EntityType.VIRTUAL_MACHINE);

        final PropertyFilterConverter propConverter = mock(PropertyFilterConverter.class);
        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(store, propConverter);
        final Optional<GroupInfo.Builder> groupInfoOpt = converter.sdkToGroup(STATIC_MEMBER_DTO, TARGET_ID);
        assertTrue(groupInfoOpt.isPresent());

        final GroupInfo groupInfo = groupInfoOpt.get().build();
        // ID should be assigned.
        assertEquals(DiscoveredGroupConstants.GROUP_NAME, groupInfo.getName());
        assertEquals(EntityType.VIRTUAL_MACHINE_VALUE, groupInfo.getEntityType());
        assertTrue(groupInfo.hasStaticGroupMembers());
        final StaticGroupMembers members = groupInfo.getStaticGroupMembers();
        assertEquals(1L, members.getStaticMemberOids(0));
    }

    @Test
    public void testGroupConverterMemberListMissingEntity() {
        final EntityStore store = mock(EntityStore.class);
        when(store.getTargetEntityIdMap(TARGET_ID))
                .thenReturn(Optional.of(ImmutableMap.of("2", 2L)));
        final PropertyFilterConverter propConverter = mock(PropertyFilterConverter.class);
        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(store, propConverter);
        final Optional<GroupInfo.Builder> groupInfoOpt = converter.sdkToGroup(STATIC_MEMBER_DTO, TARGET_ID);
        assertFalse(groupInfoOpt.isPresent());
    }

    @Test
    public void testGroupConverterSearchParameters() {
        final EntityStore store = mock(EntityStore.class);
        final PropertyFilterConverter propConverter = mock(PropertyFilterConverter.class);
        // Just return the same property filter all the time.
        when(propConverter.selectionSpecToPropertyFilter(any()))
                .thenReturn(Optional.of(PLACEHOLDER_FILTER));

        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(store, propConverter);
        final Optional<GroupInfo.Builder> infoOpt = converter.sdkToGroup(SELECTION_DTO, TARGET_ID);
        assertTrue(infoOpt.isPresent());

        final GroupInfo info = infoOpt.get().build();
        assertEquals(DiscoveredGroupConstants.GROUP_NAME, info.getName());
        assertEquals(EntityType.VIRTUAL_MACHINE_VALUE, info.getEntityType());
        assertTrue(info.hasSearchParametersCollection());
        assertEquals(1, info.getSearchParametersCollection().getSearchParametersCount());

        SearchParameters searchParameters = info.getSearchParametersCollection().getSearchParameters(0);
        assertEquals(PLACEHOLDER_FILTER, searchParameters.getStartingFilter());
        assertEquals(PLACEHOLDER_FILTER, searchParameters.getSearchFilter(0).getPropertyFilter());
    }

    @Test
    public void testClusterInfo() {
        final EntityStore store = mock(EntityStore.class);
        when(store.getTargetEntityIdMap(TARGET_ID))
                .thenReturn(Optional.of(ImmutableMap.of("1", 1L)));
        Entity entity = mock(Entity.class);
        when(store.getEntity(1))
                .thenReturn(Optional.of(entity));
        when(entity.getEntityType()).thenReturn(EntityType.PHYSICAL_MACHINE);

        final PropertyFilterConverter propConverter = mock(PropertyFilterConverter.class);
        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(store, propConverter);
        Optional<ClusterInfo.Builder> clusterInfo = converter.sdkToCluster(CommonDTO.GroupDTO.newBuilder()
                .setEntityType(EntityType.PHYSICAL_MACHINE)
                .setDisplayName(DISPLAY_NAME)
                .setConstraintInfo(ConstraintInfo.newBuilder()
                        .setConstraintType(ConstraintType.CLUSTER)
                        .setConstraintId("constraint")
                        .setConstraintName("name"))
                .setMemberList(MembersList.newBuilder().addMember("1").build())
                .build(), TARGET_ID);
        assertTrue(clusterInfo.isPresent());
        assertEquals(Type.COMPUTE, clusterInfo.get().getClusterType());
        assertEquals(DISPLAY_NAME, clusterInfo.get().getName());
        assertEquals(1, clusterInfo.get().getMembers().getStaticMemberOidsCount());
        assertEquals(1, clusterInfo.get().getMembers().getStaticMemberOids(0));
    }

    @Test
    public void testStorageClusterInfo() {
        final EntityStore store = mock(EntityStore.class);
        when(store.getTargetEntityIdMap(TARGET_ID))
                .thenReturn(Optional.of(ImmutableMap.of("1", 1L)));

        Entity entity = mock(Entity.class);
        when(store.getEntity(1))
                .thenReturn(Optional.of(entity));
        when(entity.getEntityType()).thenReturn(EntityType.STORAGE);

        final PropertyFilterConverter propConverter = mock(PropertyFilterConverter.class);
        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(store, propConverter);
        Optional<ClusterInfo.Builder> clusterInfo = converter.sdkToCluster(CommonDTO.GroupDTO.newBuilder()
                .setEntityType(EntityType.STORAGE)
                .setDisplayName(DISPLAY_NAME)
                .setConstraintInfo(ConstraintInfo.newBuilder()
                        .setConstraintType(ConstraintType.CLUSTER)
                        .setConstraintId("constraint")
                        .setConstraintName("name"))
                .setMemberList(MembersList.newBuilder().addMember("1").build())
                .build(), TARGET_ID);
        assertTrue(clusterInfo.isPresent());
        assertEquals(Type.STORAGE, clusterInfo.get().getClusterType());
        assertEquals(DISPLAY_NAME, clusterInfo.get().getName());
        assertEquals(1, clusterInfo.get().getMembers().getStaticMemberOidsCount());
        assertEquals(1, clusterInfo.get().getMembers().getStaticMemberOids(0));
    }

    @Test
    public void testNoClusterInvalidConstraintInfo() {
        final EntityStore store = mock(EntityStore.class);
        when(store.getTargetEntityIdMap(TARGET_ID))
                .thenReturn(Optional.of(ImmutableMap.of("1", 1L)));
        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(store,
                mock(PropertyFilterConverter.class));
        assertFalse(converter.sdkToCluster(CLUSTER_DTO.toBuilder()
                .setGroupName("blah")
                .build(), TARGET_ID).isPresent());

        assertFalse(converter.sdkToCluster(CLUSTER_DTO.toBuilder()
                .setConstraintInfo(ConstraintInfo.newBuilder()
                        // Only ConstraintType.CLUSTER should count as a cluster.
                        .setConstraintType(ConstraintType.MERGE)
                        .setConstraintId("constraint")
                        .setConstraintName("name"))
                .build(), TARGET_ID).isPresent());
    }

    @Test
    public void testNoClusterBecauseNoMemberList() {
        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(mock(EntityStore.class),
                mock(PropertyFilterConverter.class));
        assertFalse(converter.sdkToCluster(CLUSTER_DTO.toBuilder()
                // Using a selection spec instead of a static list - invalid!
                .setSelectionSpecList(SelectionSpecList.getDefaultInstance())
                .build(), TARGET_ID).isPresent());

        assertFalse(converter.sdkToCluster(CLUSTER_DTO.toBuilder()
                // Using a source group ID instead of a static list - invalid!
                .setSourceGroupId("sourceGroupId")
                .build(), TARGET_ID).isPresent());
    }

    @Test
    public void testNoClusterInvalidEntityType() {
        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(mock(EntityStore.class),
                mock(PropertyFilterConverter.class));
        assertFalse(converter.sdkToCluster(CLUSTER_DTO.toBuilder()
                // Clusters must be PM or Storage
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                .build(), TARGET_ID).isPresent());
    }
}
