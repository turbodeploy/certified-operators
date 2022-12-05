package com.vmturbo.topology.processor.group.discovery;

import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.CLUSTER_DTO;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.DISPLAY_NAME;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.GROUP_NAME;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.PLACEHOLDER_FILTER;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.RESOURCE_GROUP_DTO;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.RESOURCE_GROUP_DTO_WITHOUT_OWNER;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.SELECTION_DTO;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.STATIC_MEMBER_DTO;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.SUBSCRIPTION_ID;
import static com.vmturbo.topology.processor.group.discovery.DiscoveredGroupConstants.TARGET_ID;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters.EntityFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintInfo;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.MembersList;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.SelectionSpec;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.SelectionSpec.ExpressionType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.SelectionSpec.PropertyDoubleList;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.SelectionSpec.PropertyStringList;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.SelectionSpecList;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.TagValues;
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
        assertTrue(filter.getStringFilter().getPositiveMatch());
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

        Entity entity = mock(Entity.class);
        when(store.getEntity(1))
                .thenReturn(Optional.of(entity));
        when(entity.getEntityType()).thenReturn(EntityType.VIRTUAL_MACHINE);

        final PropertyFilterConverter propConverter = mock(PropertyFilterConverter.class);
        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(store, propConverter);
        final GroupInterpretationContext context =
            new GroupInterpretationContext(TARGET_ID, Collections.singletonList(STATIC_MEMBER_DTO), ImmutableMap.of("1", 1L));
        InterpretedGroup interpretedGroup = converter.interpretGroup(STATIC_MEMBER_DTO, context);
        assertTrue(interpretedGroup.getGroupDefinition().isPresent());

        final GroupDefinition groupDef = interpretedGroup.getGroupDefinition().get().build();
        // ID should be assigned.
        assertEquals(DiscoveredGroupConstants.GROUP_NAME, interpretedGroup.getSourceId());
        assertTrue(groupDef.hasStaticGroupMembers());
        final List<StaticMembersByType> membersByTypeList =
                groupDef.getStaticGroupMembers().getMembersByTypeList();
        assertEquals(1, membersByTypeList.size());
        assertEquals(EntityType.VIRTUAL_MACHINE_VALUE, membersByTypeList.get(0).getType().getEntity());
        assertThat(membersByTypeList.get(0).getMembersList(), containsInAnyOrder(1L));
    }

    @Test
    public void testGroupConverterMemberListMissingEntity() {
        final EntityStore store = mock(EntityStore.class);
        final PropertyFilterConverter propConverter = mock(PropertyFilterConverter.class);
        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(store, propConverter);
        final GroupInterpretationContext context =
            new GroupInterpretationContext(TARGET_ID, Collections.singletonList(STATIC_MEMBER_DTO), ImmutableMap.of("2", 2L));
        final Optional<GroupDefinition.Builder> groupDefOpt = converter.sdkToGroupDefinition(
                STATIC_MEMBER_DTO, context);
        // In the case of a missing entity we still return the group, with the empty
        assertTrue(groupDefOpt.get().getStaticGroupMembers().getMembersByType(0)
            .getMembersList().isEmpty());
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
            new GroupInterpretationContext(TARGET_ID, Collections.singletonList(STATIC_MEMBER_DTO), Collections.emptyMap());
        final InterpretedGroup interpretedGroup = converter.interpretGroup(SELECTION_DTO, context);
        assertTrue(interpretedGroup.getGroupDefinition().isPresent());

        final GroupDefinition groupDef = interpretedGroup.getGroupDefinition().get().build();
        assertEquals(DiscoveredGroupConstants.GROUP_NAME, interpretedGroup.getSourceId());

        assertTrue(groupDef.hasEntityFilters());
        assertEquals(1, groupDef.getEntityFilters().getEntityFilterCount());
        final EntityFilter entityFilter = groupDef.getEntityFilters().getEntityFilter(0);
        assertEquals(EntityType.VIRTUAL_MACHINE_VALUE, entityFilter.getEntityType());

        assertEquals(1, entityFilter.getSearchParametersCollection().getSearchParametersCount());
        SearchParameters searchParameters = entityFilter.getSearchParametersCollection()
                .getSearchParameters(0);
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
        final GroupInterpretationContext context = new GroupInterpretationContext(
                TARGET_ID, Collections.singletonList(group), Collections.emptyMap());
        final InterpretedGroup interpretedGroup = converter.interpretGroup(group, context);

        assertTrue(interpretedGroup.getGroupDefinition().isPresent());

        final GroupDefinition groupDef = interpretedGroup.getGroupDefinition().get().build();
        assertEquals(DiscoveredGroupConstants.GROUP_NAME, interpretedGroup.getSourceId());
        assertEquals(DiscoveredGroupConstants.GROUP_NAME, groupDef.getDisplayName());
    }

    @Test
    public void testClusterInfo() {
        final EntityStore store = mock(EntityStore.class);
        Entity entity = mock(Entity.class);
        when(store.getEntity(1))
                .thenReturn(Optional.of(entity));
        when(entity.getEntityType()).thenReturn(EntityType.PHYSICAL_MACHINE);

        final PropertyFilterConverter propConverter = mock(PropertyFilterConverter.class);
        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(store, propConverter);
        final GroupDTO group = GroupDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE)
            .setGroupType(GroupType.COMPUTE_HOST_CLUSTER)
            .setDisplayName(DISPLAY_NAME)
            .setConstraintInfo(ConstraintInfo.newBuilder()
                .setConstraintType(ConstraintType.CLUSTER)
                .setConstraintId("constraint")
                .setConstraintName("name"))
            .setMemberList(MembersList.newBuilder().addMember("1").build())
            .addEntityProperties(CommonDTO.EntityDTO.EntityProperty.newBuilder()
                .setNamespace("VCTAGS")
                .setName("key")
                .setValue("value1")
                .build())
            .addEntityProperties(CommonDTO.EntityDTO.EntityProperty.newBuilder()
                .setNamespace("VCTAGS")
                .setName("key")
                .setValue("value2")
                .build())
            .build();
        final GroupInterpretationContext context =
            new GroupInterpretationContext(TARGET_ID, Collections.singletonList(group), ImmutableMap.of("1", 1L));
        Optional<GroupDefinition.Builder> clusterOpt = converter.sdkToGroupDefinition(group, context);
        assertTrue(clusterOpt.isPresent());

        final GroupDefinition cluster = clusterOpt.get().build();

        assertEquals(GroupType.COMPUTE_HOST_CLUSTER, cluster.getType());
        assertEquals(DISPLAY_NAME, cluster.getDisplayName());
        assertEquals(1, cluster.getStaticGroupMembers().getMembersByTypeCount());
        assertThat(GroupProtoUtil.getAllStaticMembers(cluster), containsInAnyOrder(1L));
        assertEquals(1, cluster.getTags().getTagsCount());
        final TagValuesDTO tagValuesDTO = cluster.getTags().getTagsOrThrow("key");
        assertEquals(2, tagValuesDTO.getValuesCount());
        final Set<String> tagValues = new HashSet<>(tagValuesDTO.getValuesList());
        assertEquals(ImmutableSet.of("value1", "value2"), tagValues);
    }

    @Test
    public void testStorageClusterInfo() {
        final EntityStore store = mock(EntityStore.class);

        Entity entity = mock(Entity.class);
        when(store.getEntity(1))
                .thenReturn(Optional.of(entity));
        when(entity.getEntityType()).thenReturn(EntityType.STORAGE);

        final PropertyFilterConverter propConverter = mock(PropertyFilterConverter.class);
        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(store, propConverter);
        final GroupDTO group = CommonDTO.GroupDTO.newBuilder()
            .setEntityType(EntityType.STORAGE)
            .setGroupType(GroupType.STORAGE_CLUSTER)
            .setDisplayName(DISPLAY_NAME)
            .setConstraintInfo(ConstraintInfo.newBuilder()
                .setConstraintType(ConstraintType.CLUSTER)
                .setConstraintId("constraint")
                .setConstraintName("name"))
            .setMemberList(MembersList.newBuilder().addMember("1").build())
            .build();
        final GroupInterpretationContext context =
            new GroupInterpretationContext(TARGET_ID, Collections.singletonList(group), ImmutableMap.of("1", 1L));
        Optional<GroupDefinition.Builder> clusterDefOpt = converter.sdkToGroupDefinition(group, context);
        assertTrue(clusterDefOpt.isPresent());

        final GroupDefinition cluster = clusterDefOpt.get().build();
        assertEquals(GroupType.STORAGE_CLUSTER, cluster.getType());
        assertEquals(DISPLAY_NAME, cluster.getDisplayName());
        assertEquals(1, cluster.getStaticGroupMembers().getMembersByTypeCount());
        assertThat(GroupProtoUtil.getAllStaticMembers(cluster), containsInAnyOrder(1L));
    }

    @Test
    public void testGroupFromMergeConstraintInfo() {
        final EntityStore store = mock(EntityStore.class);
        Entity entity = mock(Entity.class);
        when(store.getEntity(1)).thenReturn(Optional.of(entity));
        when(entity.getEntityType()).thenReturn(EntityType.PHYSICAL_MACHINE);

        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(store,
                mock(PropertyFilterConverter.class));
        final GroupDTO mergeConstraintGroup = CLUSTER_DTO.toBuilder()
            .setConstraintInfo(ConstraintInfo.newBuilder()
                .setConstraintType(ConstraintType.MERGE)
                .setConstraintId("constraint")
                .setConstraintName("name"))
            .build();
        final GroupInterpretationContext context2 = new GroupInterpretationContext(
                TARGET_ID, Collections.singletonList(mergeConstraintGroup), ImmutableMap.of("1", 1L));
        Optional<GroupDefinition.Builder> group = converter.sdkToGroupDefinition(
                mergeConstraintGroup, context2);
        assertTrue(group.isPresent());
        assertThat(GroupProtoUtil.getAllStaticMembers(group.get()), containsInAnyOrder(1L));
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
            new GroupInterpretationContext(TARGET_ID, Collections.singletonList(selectionSpecCluster), Collections.emptyMap());
        assertFalse(converter.sdkToGroupDefinition(selectionSpecCluster, context).isPresent());

        final GroupDTO sourceGroupCluster = CLUSTER_DTO.toBuilder()
            // Using a source group ID instead of a static list - invalid!
            .setSourceGroupId("sourceGroupId")
            .build();
        final GroupInterpretationContext context2 =
            new GroupInterpretationContext(TARGET_ID, Collections.singletonList(sourceGroupCluster), Collections.emptyMap());
        assertFalse(converter.sdkToGroupDefinition(sourceGroupCluster, context2).isPresent());
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
        when(store.getEntity(1L)).thenReturn(Optional.of(entity1));
        when(store.getEntity(2L)).thenReturn(Optional.of(entity2));
        when(store.getEntity(3L)).thenReturn(Optional.of(entity3));

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
        assertThat(groupsByGroupName.get(parentGroup.getGroupName()).getAllStaticMembers(),
            containsInAnyOrder(1L, 2L, 3L));
        assertThat(groupsByGroupName.get(child1Group.getGroupName()).getAllStaticMembers(),
            containsInAnyOrder(2L, 3L));
        assertThat(groupsByGroupName.get(child2Group.getGroupName()).getAllStaticMembers(),
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
        assertThat(interpretedGroupsByName.get(Integer.toString(depth - 1)).getAllStaticMembers(),
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
        assertTrue(interpretedGroupsByName.get(Integer.toString(depth - 1)).getAllStaticMembers().isEmpty());
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

    /**
     * Test SDK input where groups contain multiple entity types. The output should be a
     * "flattened" list of groups with all their leaf members grouped by entity type.
     *
     * <p>Below is the sdk dto to interpret.
     *
     * <pre>
     * parentGroup {
     *     childGroup {
     *         entity3 (st)
     *     },
     *     entity1 (vm),
     *     entity2 (pm)
     * }
     * </pre>
     *
     * <p>After interpretation, it should contain 3 types of members:
     * <pre>
     * parentGroup {
     *     vm: [entity1],
     *     pm: [entity2],
     *     st: [entity3]
     * }
     * </pre>
     */
    @Test
    public void testHeterogeneousGroup() {
        final EntityStore store = mock(EntityStore.class);
        when(store.getTargetEntityIdMap(TARGET_ID)).thenReturn(Optional.of(
                ImmutableMap.of("entity1", 1L, "entity2", 2L, "entity3", 3L)));
        Entity entity1 = mock(Entity.class);
        when(entity1.getEntityType()).thenReturn(EntityType.VIRTUAL_MACHINE);
        Entity entity2 = mock(Entity.class);
        when(entity2.getEntityType()).thenReturn(EntityType.PHYSICAL_MACHINE);
        Entity entity3 = mock(Entity.class);
        when(entity3.getEntityType()).thenReturn(EntityType.STORAGE);
        when(store.getEntity(1L)).thenReturn(Optional.of(entity1));
        when(store.getEntity(2L)).thenReturn(Optional.of(entity2));
        when(store.getEntity(3L)).thenReturn(Optional.of(entity3));

        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(store,
                mock(PropertyFilterConverter.class));
        final GroupDTO parentGroup = GroupDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE)
                .setDisplayName(DISPLAY_NAME)
                .setGroupName("parentGroup")
                .setMemberList(MembersList.newBuilder()
                        .addMember("childGroup")
                        .addMember("entity1")
                        .addMember("entity2"))
                .build();
        final GroupDTO childGroup = GroupDTO.newBuilder()
                .setEntityType(EntityType.STORAGE)
                .setDisplayName(DISPLAY_NAME)
                .setGroupName("childGroup")
                .setMemberList(MembersList.newBuilder()
                        .addMember("entity3"))
                .build();

        final Collection<InterpretedGroup> interpretedGroups =
                converter.interpretSdkGroupList(Arrays.asList(childGroup, parentGroup), TARGET_ID);
        assertThat(interpretedGroups.size(), is(2));

        final Map<String, InterpretedGroup> groupsByGroupName = interpretedGroups.stream()
                .collect(Collectors.toMap(group -> group.getOriginalSdkGroup().getGroupName(),
                        Function.identity()));
        assertThat(groupsByGroupName.size(), is(2));
        assertThat(groupsByGroupName.get(parentGroup.getGroupName()).getAllStaticMembers(),
                containsInAnyOrder(1L, 2L, 3L));
        assertThat(groupsByGroupName.get(childGroup.getGroupName()).getAllStaticMembers(),
                containsInAnyOrder(3L));

        // verify that there are 3 types of entities in the members
        final Map<MemberType, List<Long>> membersByType =
                groupsByGroupName.get(parentGroup.getGroupName()).getStaticMembersByType();
        assertThat(membersByType.size(), is(3));
        assertThat(membersByType.get(MemberType.newBuilder()
                .setEntity(EntityType.VIRTUAL_MACHINE_VALUE).build()), containsInAnyOrder(1L));
        assertThat(membersByType.get(MemberType.newBuilder()
                .setEntity(EntityType.PHYSICAL_MACHINE_VALUE).build()), containsInAnyOrder(2L));
        assertThat(membersByType.get(MemberType.newBuilder()
                .setEntity(EntityType.STORAGE_VALUE).build()), containsInAnyOrder(3L));
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
        assertThat(groupsByGroupName.get(parentGroup.getGroupName()).getAllStaticMembers(),
            containsInAnyOrder(1L));
    }

    @Test
    public void testEmptyGroup() {
        final EntityStore store = mock(EntityStore.class);
        when(store.getTargetEntityIdMap(TARGET_ID)).thenReturn(Optional.of(Collections.emptyMap()));
        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(store,
            mock(PropertyFilterConverter.class));
        final GroupDTO group = GroupDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .setDisplayName(DISPLAY_NAME)
            .setGroupName("parent")
            .setMemberList(MembersList.getDefaultInstance())
            .build();
        final Collection<InterpretedGroup> result =
            converter.interpretSdkGroupList(Collections.singletonList(group), TARGET_ID);
        assertThat(result.size(), is(1));
        List<StaticMembersByType> staticMembers = result.iterator().next().getStaticMembers();
        assertThat(staticMembers.size(), is(1));
        assertThat(staticMembers.get(0).getType(), is(MemberType.newBuilder()
                .setEntity(EntityType.VIRTUAL_MACHINE_VALUE).build()));
        assertThat(staticMembers.get(0).getMembersCount(), is(0));
    }

    /**
     * Test that an empty resource group is created even if the sdk RG is empty.
     */
    @Test
    public void testEmptyGroupResourceGroup() {
        final EntityStore store = mock(EntityStore.class);
        when(store.getTargetEntityIdMap(TARGET_ID)).thenReturn(Optional.of(Collections.emptyMap()));
        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(store,
                mock(PropertyFilterConverter.class));
        final GroupDTO group = GroupDTO.newBuilder()
                .setGroupType(GroupType.RESOURCE)
                .setEntityType(EntityType.UNKNOWN)
                .setDisplayName(DISPLAY_NAME)
                .setGroupName("rg")
                .setMemberList(MembersList.getDefaultInstance())
                .putTags("key", TagValues.newBuilder()
                    .addAllValue(Arrays.asList("value1", "value2"))
                    .build())
                .build();
        final Collection<InterpretedGroup> result =
                converter.interpretSdkGroupList(Collections.singletonList(group), TARGET_ID);
        // verify an empty rg is created
        assertThat(result.size(), is(1));
        InterpretedGroup rg = result.iterator().next();
        assertThat(rg.getSourceId(), is(group.getGroupName()));
        assertThat(rg.getGroupDefinition().get().getType(), is(GroupType.RESOURCE));
        assertThat(rg.getGroupDefinition().get().getDisplayName(), is(DISPLAY_NAME));
        assertThat(rg.getStaticMembers().size(), is(0));
        assertEquals(1, rg.getGroupDefinition().get().getTags().getTagsCount());
        final TagValuesDTO tagValuesDTO = rg.getGroupDefinition().get().getTags().getTagsOrThrow("key");
        assertEquals(2, tagValuesDTO.getValuesCount());
        final Set<String> tagValues = new HashSet<>(tagValuesDTO.getValuesList());
        assertEquals(ImmutableSet.of("value1", "value2"), tagValues);
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
        assertThat(groupsByGroupName.get(parentGroup.getGroupName()).getAllStaticMembers(),
            containsInAnyOrder(1L));
        assertThat(groupsByGroupName.get(child1Group.getGroupName()).getAllStaticMembers(),
            containsInAnyOrder(2L));
    }

    @Test
    public void testContextCycle() {
        final GroupInterpretationContext context =
            new GroupInterpretationContext(TARGET_ID, Collections.emptyList(), Collections.emptyMap());
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

    @Test
    public void testGroupOfStorageCluster() {
        final EntityStore store = mock(EntityStore.class);
        when(store.getTargetEntityIdMap(TARGET_ID))
            .thenReturn(Optional.of(ImmutableMap.of("storage1", 1L, "storage2", 2L)));
        Entity entity1 = mock(Entity.class);
        when(entity1.getEntityType()).thenReturn(EntityType.STORAGE);
        Entity entity2 = mock(Entity.class);
        when(entity2.getEntityType()).thenReturn(EntityType.STORAGE);
        when(store.getEntity(1L))
            .thenReturn(Optional.of(entity1));
        when(store.getEntity(2L))
            .thenReturn(Optional.of(entity2));

        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(store,
            mock(PropertyFilterConverter.class));
        final GroupDTO parentGroup = GroupDTO.newBuilder()
            .setEntityType(EntityType.STORAGE)
            .setDisplayName(DISPLAY_NAME)
            .setGroupName("parent")
            .setMemberList(MembersList.newBuilder()
                .addMember("storageClusterId1")
                .addMember("storage1"))
            .build();
        final GroupDTO childGroup = GroupDTO.newBuilder()
            .setEntityType(EntityType.STORAGE)
            .setDisplayName(DISPLAY_NAME)
            .setConstraintInfo(ConstraintInfo.newBuilder()
                .setConstraintType(ConstraintType.CLUSTER)
                .setConstraintId("storageClusterId1")
                .setConstraintName("storageClusterName1"))
            .setMemberList(MembersList.newBuilder()
                .addMember("storage2"))
            .build();

        final Collection<InterpretedGroup> interpretedGroups =
            converter.interpretSdkGroupList(Arrays.asList(childGroup, parentGroup), TARGET_ID);
        assertThat(interpretedGroups.size(), is(2));

        final Map<String, InterpretedGroup> groupsByGroupName = interpretedGroups.stream()
                .collect(Collectors.toMap(
                    group -> GroupProtoUtil.extractId(group.getOriginalSdkGroup()),
                    Function.identity()));
        // verify that storage cluster's member is added to parent group's member list
        assertThat(groupsByGroupName.get(parentGroup.getGroupName()).getAllStaticMembers(),
            containsInAnyOrder(1L, 2L));
        assertThat(groupsByGroupName.get(GroupProtoUtil.extractId(childGroup))
                .getAllStaticMembers(), containsInAnyOrder(2L));
    }

    @Test
    public void testBuyersSellersGroupWithSameConstraintId() {
        final EntityStore store = mock(EntityStore.class);
        when(store.getTargetEntityIdMap(TARGET_ID))
            .thenReturn(Optional.of(ImmutableMap.of("vm1", 1L, "pm1", 2L)));
        Entity entity1 = mock(Entity.class);
        when(entity1.getEntityType()).thenReturn(EntityType.VIRTUAL_MACHINE);
        Entity entity2 = mock(Entity.class);
        when(entity2.getEntityType()).thenReturn(EntityType.PHYSICAL_MACHINE);
        when(store.getEntity(1L)).thenReturn(Optional.of(entity1));
        when(store.getEntity(2L)).thenReturn(Optional.of(entity2));
        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(store,
            mock(PropertyFilterConverter.class));

        final String CONSTRAINT_ID = "1234";
        final GroupDTO sellersGroup = GroupDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE)
            .setDisplayName(DISPLAY_NAME)
            .setConstraintInfo(ConstraintInfo.newBuilder()
                .setConstraintType(ConstraintType.BUYER_SELLER_AFFINITY)
                .setConstraintId(CONSTRAINT_ID)
                .setConstraintName("GROUP-DRS-1"))
            .setMemberList(MembersList.newBuilder()
                .addMember("pm1"))
            .build();
        final GroupDTO buyersGroup = GroupDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .setDisplayName(DISPLAY_NAME)
            .setConstraintInfo(ConstraintInfo.newBuilder()
                .setConstraintType(ConstraintType.BUYER_SELLER_AFFINITY)
                .setConstraintId(CONSTRAINT_ID)
                .setConstraintName("GROUP-DRS-2")
                .setIsBuyer(true))
            .setMemberList(MembersList.newBuilder()
                .addMember("vm1"))
            .build();

        final Collection<InterpretedGroup> interpretedGroups =
            converter.interpretSdkGroupList(Arrays.asList(buyersGroup, sellersGroup), TARGET_ID);
        // verify that both buyer and seller groups are interpreted correctly
        assertThat(interpretedGroups.size(), is(2));

        final Map<Integer, String> groupIdByEntityType = interpretedGroups.stream()
                .collect(Collectors.toMap(
                    group -> group.getGroupDefinition().get().getStaticGroupMembers()
                            .getMembersByType(0).getType().getEntity(),
                        InterpretedGroup::getSourceId));

        // verify that id is prefixed for both buyer and seller group
        assertThat(groupIdByEntityType.get(EntityType.VIRTUAL_MACHINE_VALUE), is(
            GroupProtoUtil.BUYERS_GROUP_ID_PREFIX + CONSTRAINT_ID));
        assertThat(groupIdByEntityType.get(EntityType.PHYSICAL_MACHINE_VALUE), is(
            GroupProtoUtil.SELLERS_GROUP_ID_PREFIX + CONSTRAINT_ID));
    }

    /**
     * Test that groupDefinition successfully populated owner field (Entity OID)
     * e.g BusinessAccount is ResourceGroup owner in Azure.
     */
    @Test
    public void testSdkToGroupDefinitionPopulateOwner() {
        final EntityStore store = mock(EntityStore.class);
        final long groupOwnerOID = 1L;
        when(store.getTargetEntityIdMap(TARGET_ID)).thenReturn(
                Optional.of(ImmutableMap.of(SUBSCRIPTION_ID, groupOwnerOID)));
        final PropertyFilterConverter propConverter = mock(PropertyFilterConverter.class);
        final DiscoveredGroupInterpreter converter =
                new DiscoveredGroupInterpreter(store, propConverter);
        final GroupInterpretationContext context = new GroupInterpretationContext(TARGET_ID,
                Collections.singletonList(RESOURCE_GROUP_DTO), ImmutableMap.of(SUBSCRIPTION_ID, groupOwnerOID));
        final Optional<GroupDefinition.Builder> groupDefOpt =
                converter.sdkToGroupDefinition(RESOURCE_GROUP_DTO, context);
        assertEquals(groupDefOpt.get().getOwner(), groupOwnerOID);
    }

    /**
     * Test case when owner is not set in sdkDto, so groupDefinition don't have information about
     * groupOwner.
     */
    @Test
    public void testSdkToGroupDefinitionWithoutOwner() {
        final EntityStore store = mock(EntityStore.class);
        final long groupOwnerOID = 1L;
        when(store.getTargetEntityIdMap(TARGET_ID)).thenReturn(
                Optional.of(ImmutableMap.of(SUBSCRIPTION_ID, groupOwnerOID)));
        final PropertyFilterConverter propConverter = mock(PropertyFilterConverter.class);
        final DiscoveredGroupInterpreter converter =
                new DiscoveredGroupInterpreter(store, propConverter);
        final GroupInterpretationContext context = new GroupInterpretationContext(TARGET_ID,
                Collections.singletonList(RESOURCE_GROUP_DTO_WITHOUT_OWNER), Collections.emptyMap());
        final Optional<GroupDefinition.Builder> groupDefOpt =
                converter.sdkToGroupDefinition(RESOURCE_GROUP_DTO_WITHOUT_OWNER, context);
        assertFalse(groupDefOpt.get().hasOwner());
    }

    @Test
    public void testGroupConverterNestedGroups() {
        final EntityStore store = mock(EntityStore.class);
        final String accountId = "account_id";
        final Long accountOid = 1L;
        final String nestedGroupId = "nested_group_id";
        when(store.getTargetEntityIdMap(TARGET_ID))
                .thenReturn(Optional.of(ImmutableMap.of(accountId, accountOid)));

        final Entity entity = mock(Entity.class);
        when(store.getEntity(accountOid))
                .thenReturn(Optional.of(entity));
        when(entity.getEntityType()).thenReturn(EntityType.BUSINESS_ACCOUNT);

        final PropertyFilterConverter propConverter = mock(PropertyFilterConverter.class);
        final DiscoveredGroupInterpreter converter = new DiscoveredGroupInterpreter(store,
                propConverter);
        final GroupDTO group = CommonDTO.GroupDTO.newBuilder()
                .setGroupType(GroupType.BUSINESS_ACCOUNT_FOLDER)
                .setEntityType(EntityType.BUSINESS_ACCOUNT)
                .setDisplayName(DISPLAY_NAME)
                .setGroupName(GROUP_NAME)
                .setMemberList(MembersList.newBuilder()
                        .addMember(accountId)
                        .addMember(nestedGroupId))
                .build();
        final GroupInterpretationContext context = new GroupInterpretationContext(
                TARGET_ID, Collections.singletonList(group), ImmutableMap.of(accountId, accountOid));
        InterpretedGroup interpretedGroup = converter.interpretGroup(group, context);
        assertTrue(interpretedGroup.getGroupDefinition().isPresent());

        final GroupDefinition groupDef = interpretedGroup.getGroupDefinition().get().build();
        assertTrue(groupDef.hasStaticGroupMembers());
        final List<StaticMembersByType> membersByTypeList =
                groupDef.getStaticGroupMembers().getMembersByTypeList();
        assertEquals(2, membersByTypeList.size());
        for (final StaticMembersByType memberByType : membersByTypeList) {
            if (memberByType.getType().hasEntity()) {
                assertEquals(EntityType.BUSINESS_ACCOUNT_VALUE, memberByType.getType().getEntity());
                assertThat(memberByType.getMembersList(), containsInAnyOrder(accountOid));
            } else if (memberByType.getType().hasGroup()) {
                assertEquals(GroupType.BUSINESS_ACCOUNT_FOLDER, memberByType.getType().getGroup());
                assertThat(memberByType.getMemberLocalIdList(), containsInAnyOrder(nestedGroupId));
            } else {
                fail("Unexpected StaticMembersByType.memberType: "
                        + memberByType.getType().getTypeCase());
            }
        }
    }
}
