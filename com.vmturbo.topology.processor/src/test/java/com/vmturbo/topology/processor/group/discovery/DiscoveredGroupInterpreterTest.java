package com.vmturbo.topology.processor.group.discovery;

import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.CLUSTER_DTO;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.DISPLAY_NAME;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.PLACEHOLDER_FILTER;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.SELECTION_DTO;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.STATIC_MEMBER_DTO;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.TARGET_ID;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintInfo;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.MembersList;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.SelectionSpec;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.SelectionSpec.ExpressionType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.SelectionSpec.PropertyDoubleList;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.SelectionSpec.PropertyStringList;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.SelectionSpecList;
import com.vmturbo.topology.processor.conversions.SdkToTopologyEntityConverter;
import com.vmturbo.topology.processor.entity.Entity;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupInterpreter.DefaultPropertyFilterConverter;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupInterpreter.GroupInterpretationContext;
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
        final GroupInterpretationContext context =
            new GroupInterpretationContext(TARGET_ID, Collections.singletonList(STATIC_MEMBER_DTO));
        final Optional<GroupInfo.Builder> groupInfoOpt = converter.sdkToGroup(STATIC_MEMBER_DTO, context);
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
        final GroupInterpretationContext context =
            new GroupInterpretationContext(TARGET_ID, Collections.singletonList(STATIC_MEMBER_DTO));
        final Optional<GroupInfo.Builder> groupInfoOpt = converter.sdkToGroup(STATIC_MEMBER_DTO, context);
        // In the case of a missing entity we still return the group, with the empty
        assertTrue(groupInfoOpt.get().getStaticGroupMembers().getStaticMemberOidsList().isEmpty());
    }

    @Test
    public void testGroupConverterSearchParameters() {
        final EntityStore store = mock(EntityStore.class);
        final PropertyFilterConverter propConverter = mock(PropertyFilterConverter.class);
        // Just return the same property filter all the time.
        when(propConverter.selectionSpecToPropertyFilter(any()))
                .thenReturn(Optional.of(PLACEHOLDER_FILTER));

        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(store, propConverter);
        final GroupInterpretationContext context =
            new GroupInterpretationContext(TARGET_ID, Collections.singletonList(STATIC_MEMBER_DTO));
        final Optional<GroupInfo.Builder> infoOpt = converter.sdkToGroup(SELECTION_DTO, context);
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

    /**
     * Tests group without display name. group_name should is expected to be used instead in
     * {@code name} of the result group.
     */
    @Test
    public void testGroupWithoutDisplayName() {
        final EntityStore store = mock(EntityStore.class);
        final PropertyFilterConverter propConverter = mock(PropertyFilterConverter.class);
        // Just return the same property filter all the time.
        when(propConverter.selectionSpecToPropertyFilter(any()))
                .thenReturn(Optional.of(PLACEHOLDER_FILTER));

        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(store, propConverter);
        final GroupDTO group = GroupDTO.newBuilder(SELECTION_DTO)
            .clearDisplayName()
            .build();
        final GroupInterpretationContext context =
            new GroupInterpretationContext(TARGET_ID, Collections.singletonList(group));
        final Optional<GroupInfo.Builder> infoOpt = converter.sdkToGroup(group, context);
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
        final GroupDTO group = GroupDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE)
            .setDisplayName(DISPLAY_NAME)
            .setConstraintInfo(ConstraintInfo.newBuilder()
                .setConstraintType(ConstraintType.CLUSTER)
                .setConstraintId("constraint")
                .setConstraintName("name"))
            .setMemberList(MembersList.newBuilder().addMember("1").build())
            .addEntityProperties(
                EntityProperty.newBuilder()
                    .setNamespace(SdkToTopologyEntityConverter.TAG_NAMESPACE)
                    .setName("key")
                    .setValue("value1")
                    .build())
            .addEntityProperties(
                EntityProperty.newBuilder()
                    .setNamespace("ignore")
                    .setName("key")
                    .setValue("value3")
                    .build())
            .addEntityProperties(
                EntityProperty.newBuilder()
                    .setNamespace(SdkToTopologyEntityConverter.TAG_NAMESPACE)
                    .setName("key")
                    .setValue("value2")
                    .build())
            .build();
        final GroupInterpretationContext context =
            new GroupInterpretationContext(TARGET_ID, Collections.singletonList(group));
        Optional<ClusterInfo.Builder> clusterInfo = converter.sdkToCluster(group, context);
        assertTrue(clusterInfo.isPresent());
        assertEquals(Type.COMPUTE, clusterInfo.get().getClusterType());
        assertEquals(DISPLAY_NAME, clusterInfo.get().getDisplayName());
        assertEquals(1, clusterInfo.get().getMembers().getStaticMemberOidsCount());
        assertEquals(1, clusterInfo.get().getMembers().getStaticMemberOids(0));
        assertEquals(1, clusterInfo.get().getTags().getTagsCount());
        final TagValuesDTO tagValuesDTO = clusterInfo.get().getTags().getTagsOrThrow("key");
        assertEquals(2, tagValuesDTO.getValuesCount());
        final Set<String> tagValues = tagValuesDTO.getValuesList().stream().collect(Collectors.toSet());
        assertEquals(ImmutableSet.of("value1", "value2"), tagValues);
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
        final GroupDTO group = CommonDTO.GroupDTO.newBuilder()
            .setEntityType(EntityType.STORAGE)
            .setDisplayName(DISPLAY_NAME)
            .setConstraintInfo(ConstraintInfo.newBuilder()
                .setConstraintType(ConstraintType.CLUSTER)
                .setConstraintId("constraint")
                .setConstraintName("name"))
            .setMemberList(MembersList.newBuilder().addMember("1").build())
            .build();
        final GroupInterpretationContext context =
            new GroupInterpretationContext(TARGET_ID, Collections.singletonList(group));
        Optional<ClusterInfo.Builder> clusterInfo = converter.sdkToCluster(group, context);
        assertTrue(clusterInfo.isPresent());
        assertEquals(Type.STORAGE, clusterInfo.get().getClusterType());
        assertEquals(DISPLAY_NAME, clusterInfo.get().getDisplayName());
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
        final GroupDTO groupNameCluster = CLUSTER_DTO.toBuilder()
            .setGroupName("blah")
            .build();
        final GroupInterpretationContext context =
            new GroupInterpretationContext(TARGET_ID, Collections.singletonList(groupNameCluster));
        assertFalse(converter.sdkToCluster(groupNameCluster, context).isPresent());

        final GroupDTO invalidConstraintCluster = CLUSTER_DTO.toBuilder()
            .setConstraintInfo(ConstraintInfo.newBuilder()
                // Only ConstraintType.CLUSTER should count as a cluster.
                .setConstraintType(ConstraintType.MERGE)
                .setConstraintId("constraint")
                .setConstraintName("name"))
            .build();
        final GroupInterpretationContext context2 =
            new GroupInterpretationContext(TARGET_ID, Collections.singletonList(invalidConstraintCluster));
        assertFalse(converter.sdkToCluster(invalidConstraintCluster, context2).isPresent());
    }

    @Test
    public void testNoClusterBecauseNoMemberList() {
        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(mock(EntityStore.class),
                mock(PropertyFilterConverter.class));
        final GroupDTO selectionSpecCluster = CLUSTER_DTO.toBuilder()
            // Using a selection spec instead of a static list - invalid!
            .setSelectionSpecList(SelectionSpecList.getDefaultInstance())
            .build();
        final GroupInterpretationContext context =
            new GroupInterpretationContext(TARGET_ID, Collections.singletonList(selectionSpecCluster));
        assertFalse(converter.sdkToCluster(selectionSpecCluster, context).isPresent());

        final GroupDTO sourceGroupCluster = CLUSTER_DTO.toBuilder()
            // Using a source group ID instead of a static list - invalid!
            .setSourceGroupId("sourceGroupId")
            .build();
        final GroupInterpretationContext context2 =
            new GroupInterpretationContext(TARGET_ID, Collections.singletonList(sourceGroupCluster));
        assertFalse(converter.sdkToCluster(sourceGroupCluster, context2).isPresent());
    }

    @Test
    public void testNoClusterInvalidEntityType() {
        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(mock(EntityStore.class),
                mock(PropertyFilterConverter.class));
        final GroupDTO invalidEntityCluster = CLUSTER_DTO.toBuilder()
            // Clusters must be PM or Storage
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .build();
        final GroupInterpretationContext context =
            new GroupInterpretationContext(TARGET_ID, Collections.singletonList(invalidEntityCluster));
        assertFalse(converter.sdkToCluster(invalidEntityCluster, context).isPresent());
    }

    /**
     * Test SDK input where groups contain other groups.
     * The output should be a "flattened" list of groups with all their leaf members.
     */
    @Test
    public void testGroupOfGroups() {
        final EntityStore store = mock(EntityStore.class);
        when(store.getTargetEntityIdMap(TARGET_ID))
            .thenReturn(Optional.of(ImmutableMap.of("1", 1L, "2", 2L, "3", 3L)));
        Entity entity1 = mock(Entity.class);
        when(entity1.getEntityType()).thenReturn(EntityType.VIRTUAL_MACHINE);
        Entity entity2 = mock(Entity.class);
        when(entity2.getEntityType()).thenReturn(EntityType.VIRTUAL_MACHINE);
        Entity entity3 = mock(Entity.class);
        when(entity3.getEntityType()).thenReturn(EntityType.VIRTUAL_MACHINE);
        when(store.getEntity(1L))
            .thenReturn(Optional.of(entity1));
        when(store.getEntity(2L))
            .thenReturn(Optional.of(entity2));
        when(store.getEntity(3L))
            .thenReturn(Optional.of(entity3));

        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(store,
            mock(PropertyFilterConverter.class));
        final GroupDTO parentGroup = GroupDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .setDisplayName(DISPLAY_NAME)
            .setGroupName("parent")
            .setMemberList(MembersList.newBuilder()
                .addMember("child1")
                .addMember("1"))
            .build();
        final GroupDTO child1Group = GroupDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .setDisplayName(DISPLAY_NAME)
            .setGroupName("child1")
            .setMemberList(MembersList.newBuilder()
                .addMember("child2")
                .addMember("2"))
            .build();
        final GroupDTO child2Group = GroupDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .setDisplayName(DISPLAY_NAME)
            .setGroupName("child2")
            .setMemberList(MembersList.newBuilder()
                .addMember("3"))
            .build();

        final Collection<InterpretedGroup> interpretedGroups =
            converter.interpretSdkGroupList(Arrays.asList(child1Group, child2Group, parentGroup), TARGET_ID);
        assertThat(interpretedGroups.size(), is(3));

        final Map<String, InterpretedGroup> groupsByGroupName =
            converter.interpretSdkGroupList(Arrays.asList(child1Group, child2Group, parentGroup), TARGET_ID)
                .stream()
                .collect(Collectors.toMap(
                    group -> group.getOriginalSdkGroup().getGroupName(),
                    Function.identity()));
        assertThat(groupsByGroupName.size(), is(3));
        assertThat(groupsByGroupName.get(parentGroup.getGroupName()).getStaticMembers(),
            containsInAnyOrder(1L, 2L, 3L));
        assertThat(groupsByGroupName.get(child1Group.getGroupName()).getStaticMembers(),
            containsInAnyOrder(2L, 3L));
        assertThat(groupsByGroupName.get(child2Group.getGroupName()).getStaticMembers(),
            containsInAnyOrder(3L));
    }

    @Test
    public void testGroupOfGroupsDepth() {
        final EntityStore store = mock(EntityStore.class);
        when(store.getTargetEntityIdMap(TARGET_ID))
            .thenReturn(Optional.of(ImmutableMap.of("1111", 1111L)));
        Entity entity1 = mock(Entity.class);
        when(entity1.getEntityType()).thenReturn(EntityType.VIRTUAL_MACHINE);
        Entity entity2 = mock(Entity.class);
        when(entity2.getEntityType()).thenReturn(EntityType.VIRTUAL_MACHINE);
        Entity entity3 = mock(Entity.class);
        when(entity3.getEntityType()).thenReturn(EntityType.VIRTUAL_MACHINE);
        when(store.getEntity(1111L))
            .thenReturn(Optional.of(entity1));

        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(store,
            mock(PropertyFilterConverter.class));

        final int depth = DiscoveredGroupInterpreter.MAX_NESTING_DEPTH;
        final List<GroupDTO> x = makeNestedGroups(depth);
        final Map<String, InterpretedGroup> interpretedGroupsByName =
            converter.interpretSdkGroupList(x, TARGET_ID).stream()
                .collect(Collectors.toMap(
                    group -> group.getOriginalSdkGroup().getGroupName(),
                    Function.identity()));
        assertThat(interpretedGroupsByName.size(), is(x.size()));
        assertThat(interpretedGroupsByName.get(Integer.toString(depth - 1)).getStaticMembers(),
            containsInAnyOrder(1111L));
    }

    @Test
    public void testGroupOfGroupsExceedsMaxDepth() {
        final EntityStore store = mock(EntityStore.class);
        when(store.getTargetEntityIdMap(TARGET_ID))
            .thenReturn(Optional.of(ImmutableMap.of("1111", 1111L)));
        Entity entity1 = mock(Entity.class);
        when(entity1.getEntityType()).thenReturn(EntityType.VIRTUAL_MACHINE);
        Entity entity2 = mock(Entity.class);
        when(entity2.getEntityType()).thenReturn(EntityType.VIRTUAL_MACHINE);
        Entity entity3 = mock(Entity.class);
        when(entity3.getEntityType()).thenReturn(EntityType.VIRTUAL_MACHINE);
        when(store.getEntity(1111L))
            .thenReturn(Optional.of(entity1));

        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(store,
            mock(PropertyFilterConverter.class));

        final int depth = DiscoveredGroupInterpreter.MAX_NESTING_DEPTH + 1;
        final List<GroupDTO> x = makeNestedGroups(depth);
        final Map<String, InterpretedGroup> interpretedGroupsByName =
            converter.interpretSdkGroupList(x, TARGET_ID).stream()
                .collect(Collectors.toMap(
                    group -> group.getOriginalSdkGroup().getGroupName(),
                    Function.identity()));
        assertThat(interpretedGroupsByName.size(), is(x.size()));
        assertTrue(interpretedGroupsByName.get(Integer.toString(depth - 1)).getStaticMembers().isEmpty());
    }

    @Nonnull
    private List<GroupDTO> makeNestedGroups(int depth) {
        final List<GroupDTO> list = new ArrayList<>(depth);
        if (depth == 0) {
            return list;
        }

        final GroupDTO leafGroup = GroupDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .setDisplayName(DISPLAY_NAME)
            .setGroupName("leaf")
            .setMemberList(MembersList.newBuilder()
                .addMember("1111"))
            .build();
        list.add(leafGroup);
        if (depth <= 1) {
            return list;
        }

        for (int i = 1; i < depth; ++i) {
            list.add(0, leafGroup.toBuilder()
                .setDisplayName(Integer.toString(i))
                .setGroupName(Integer.toString(i))
                .setMemberList(MembersList.newBuilder()
                    .addMember(list.get(0).getGroupName()))
                .build());
        }
        return list;
    }

    @Test
    public void testGroupOfGroupsEntityTypeMismatchError() {
        final EntityStore store = mock(EntityStore.class);
        when(store.getTargetEntityIdMap(TARGET_ID))
            .thenReturn(Optional.of(ImmutableMap.of("1", 1L, "2", 2L)));
        Entity entity1 = mock(Entity.class);
        when(entity1.getEntityType()).thenReturn(EntityType.VIRTUAL_MACHINE);
        Entity entity2 = mock(Entity.class);
        when(entity2.getEntityType()).thenReturn(EntityType.STORAGE);
        when(store.getEntity(1L))
            .thenReturn(Optional.of(entity1));
        when(store.getEntity(2L))
            .thenReturn(Optional.of(entity2));

        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(store,
            mock(PropertyFilterConverter.class));
        final GroupDTO parentGroup = GroupDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .setDisplayName(DISPLAY_NAME)
            .setGroupName("parent")
            .setMemberList(MembersList.newBuilder()
                .addMember("child1")
                .addMember("1"))
            .build();
        final GroupDTO child1Group = GroupDTO.newBuilder()
            .setEntityType(EntityType.STORAGE)
            .setDisplayName(DISPLAY_NAME)
            .setGroupName("child1")
            .setMemberList(MembersList.newBuilder()
                .addMember("2"))
            .build();

        final Map<String, InterpretedGroup> groupsByGroupName =
            converter.interpretSdkGroupList(Arrays.asList(parentGroup, child1Group), TARGET_ID)
                .stream()
                .collect(Collectors.toMap(
                    group -> group.getOriginalSdkGroup().getGroupName(),
                    Function.identity()));
        assertThat(groupsByGroupName.size(), is(2));
        // The parent group should only contain the VM member
        assertThat(groupsByGroupName.get(parentGroup.getGroupName()).getStaticMembers(),
            containsInAnyOrder(1L));
        assertThat(groupsByGroupName.get(child1Group.getGroupName()).getStaticMembers(),
            containsInAnyOrder(2L));
    }

    @Test
    public void testGroupOfGroupsNonStaticError() {
        final EntityStore store = mock(EntityStore.class);
        when(store.getTargetEntityIdMap(TARGET_ID))
            .thenReturn(Optional.of(ImmutableMap.of("1", 1L)));
        Entity entity1 = mock(Entity.class);
        when(entity1.getEntityType()).thenReturn(EntityType.VIRTUAL_MACHINE);
        when(store.getEntity(1L))
            .thenReturn(Optional.of(entity1));

        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(store,
            mock(PropertyFilterConverter.class));
        final GroupDTO parentGroup = GroupDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .setDisplayName(DISPLAY_NAME)
            .setGroupName("parent")
            .setMemberList(MembersList.newBuilder()
                .addMember("child1")
                .addMember("1"))
            .build();
        // The child group is a "dynamic" group.
        final GroupDTO child1Group = GroupDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .setDisplayName(DISPLAY_NAME)
            .setGroupName("child1")
            .setSelectionSpecList(SelectionSpecList.getDefaultInstance())
            .build();

        final Map<String, InterpretedGroup> groupsByGroupName =
            converter.interpretSdkGroupList(Arrays.asList(parentGroup, child1Group), TARGET_ID)
                .stream()
                .collect(Collectors.toMap(
                    group -> group.getOriginalSdkGroup().getGroupName(),
                    Function.identity()));
        assertThat(groupsByGroupName.size(), is(2));

        // The parent group should only contain the VM member
        assertThat(groupsByGroupName.get(parentGroup.getGroupName()).getStaticMembers(),
            containsInAnyOrder(1L));
    }

    @Test
    public void testEmptyGroup() {
        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(mock(EntityStore.class),
            mock(PropertyFilterConverter.class));
        final GroupDTO group = GroupDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .setDisplayName(DISPLAY_NAME)
            .setGroupName("parent")
            .build();
        final Collection<InterpretedGroup> result =
            converter.interpretSdkGroupList(Collections.singletonList(group), TARGET_ID);
        assertThat(result.size(), is(1));
        assertTrue(result.iterator().next().getStaticMembers().isEmpty());
    }

    @Test
    public void testGroupOfGroupsCycle() {
        final EntityStore store = mock(EntityStore.class);
        when(store.getTargetEntityIdMap(TARGET_ID))
            .thenReturn(Optional.of(ImmutableMap.of("1", 1L, "2", 2L)));
        final Entity entity1 = mock(Entity.class);
        when(entity1.getEntityType()).thenReturn(EntityType.VIRTUAL_MACHINE);
        final Entity entity2 = mock(Entity.class);
        when(entity2.getEntityType()).thenReturn(EntityType.VIRTUAL_MACHINE);
        when(store.getEntity(1L))
            .thenReturn(Optional.of(entity1));
        when(store.getEntity(2L))
            .thenReturn(Optional.of(entity2));
        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(store,
            mock(PropertyFilterConverter.class));
        final GroupDTO parentGroup = GroupDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .setDisplayName(DISPLAY_NAME)
            .setGroupName("parent")
            .setMemberList(MembersList.newBuilder()
                .addMember("child1")
                .addMember("1"))
            .build();
        final GroupDTO child1Group = GroupDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .setDisplayName(DISPLAY_NAME)
            .setGroupName("child1")
            .setMemberList(MembersList.newBuilder()
                .addMember("parent")
                .addMember("2"))
            .build();

        final Map<String, InterpretedGroup> groupsByGroupName =
            converter.interpretSdkGroupList(Arrays.asList(parentGroup, child1Group), TARGET_ID)
                .stream()
                .collect(Collectors.toMap(
                    group -> group.getOriginalSdkGroup().getGroupName(),
                    Function.identity()));
        assertThat(groupsByGroupName.size(), is(2));
        // In a cyclical dependency, we should just act as if the cycle didn't exist.
        // The groups only contain the "other" members.
        assertThat(groupsByGroupName.get(parentGroup.getGroupName()).getStaticMembers(),
            containsInAnyOrder(1L));
        assertThat(groupsByGroupName.get(child1Group.getGroupName()).getStaticMembers(),
            containsInAnyOrder(2L));
    }

    @Test
    public void testContextCycle() {
        final GroupInterpretationContext context =
            new GroupInterpretationContext(TARGET_ID, Collections.emptyList());
        assertFalse(context.inCycle());
        context.pushVisitedGroup("foo");
        // foo
        assertFalse(context.inCycle());
        context.pushVisitedGroup("bar");
        // foo -> bar
        assertFalse(context.inCycle());
        context.pushVisitedGroup("foo");
        // foo -> bar -> foo
        assertTrue(context.inCycle());
        context.popVisitedGroup();
        // foo -> bar
        assertTrue(context.inCycle());
        context.popVisitedGroup();
        // foo
        assertTrue(context.inCycle());

        context.popVisitedGroup();
        assertFalse(context.inCycle());
    }
}
