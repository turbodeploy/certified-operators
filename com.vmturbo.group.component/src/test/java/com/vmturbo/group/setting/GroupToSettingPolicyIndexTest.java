package com.vmturbo.group.setting;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.setting.SettingProto.Scope;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;

public class GroupToSettingPolicyIndexTest {

    private final SettingPolicyInfo info = SettingPolicyInfo.newBuilder()
            .setName("test")
            .setScope(Scope.newBuilder()
                    .addGroups(111)
                    .addGroups(222)
                    .build())
            .build();

    private final SettingPolicy settingPolicy =
            SettingPolicy.newBuilder()
                    .setId(99L)
                    .setInfo(info)
                    .build();

    GroupToSettingPolicyIndex groupToSettingPolicyIndex;

    @Before
    public void setUp() throws Exception {
        groupToSettingPolicyIndex =
                new GroupToSettingPolicyIndex(Stream.of(settingPolicy));
    }

    @Test
    public void testGetSettingPolicyIdsForGroup() throws Exception {
        Set<Long> policyIds =
                groupToSettingPolicyIndex.getSettingPolicyIdsForGroup(111L);

        assertThat(policyIds, containsInAnyOrder(99L));
    }

    @Test
    public void testGetSettingPolicyIdsForGroupMulti() throws Exception {
        Set<Long> groupIds = new HashSet<>();
        groupIds.add(111L);
        groupIds.add(222L);

        Map<Long, Set<Long>> policyIdsByGroupId =
                groupToSettingPolicyIndex.getSettingPolicyIdsForGroup(groupIds);

        assertThat(policyIdsByGroupId.get(111L), containsInAnyOrder(99L));
        assertThat(policyIdsByGroupId.get(222L), containsInAnyOrder(99L));
    }

    @Test
    public void testIndexAdd() throws Exception {
        SettingPolicyInfo newInfo = SettingPolicyInfo.newBuilder()
                .setName("testadd")
                .setScope(Scope.newBuilder()
                        .addGroups(333L)
                        .build())
                .build();

        SettingPolicy newSettingPolicy =
                SettingPolicy.newBuilder()
                        .setId(88L)
                        .setInfo(newInfo)
                        .build();

        groupToSettingPolicyIndex.add(newSettingPolicy);

        Set<Long> policyIds =
                groupToSettingPolicyIndex.getSettingPolicyIdsForGroup(333L);
        assertThat(policyIds, containsInAnyOrder(88L));
    }

    @Test
    public void testIndexUpdate() throws Exception {
        SettingPolicyInfo newInfo = SettingPolicyInfo.newBuilder()
                .setName("testadd")
                .setScope(Scope.newBuilder()
                        .addGroups(333L)
                        .build())
                .build();

        SettingPolicy updatedSettingPolicy =
                settingPolicy.toBuilder()
                        .setInfo(newInfo)
                        .build();

        groupToSettingPolicyIndex.update(updatedSettingPolicy);

        Set<Long> policyIds =
                groupToSettingPolicyIndex.getSettingPolicyIdsForGroup(333L);
        assertThat(policyIds, containsInAnyOrder(99L));

        policyIds = groupToSettingPolicyIndex.getSettingPolicyIdsForGroup(111L);
        assertThat(policyIds.isEmpty(), is(true));
    }

    @Test
    public void testIndexRemove() throws Exception {
        groupToSettingPolicyIndex.remove(Collections.singletonList(settingPolicy));
        Set<Long> policyIds =
                groupToSettingPolicyIndex.getSettingPolicyIdsForGroup(111L);

        assertThat(policyIds.isEmpty(), is(true));
    }

    @Test
    public void testIndexClear() throws Exception {

        groupToSettingPolicyIndex.clear();

        Set<Long> groupIds = new HashSet<>();
        groupIds.add(111L);
        groupIds.add(222L);

        Map<Long, Set<Long>> policyIdsByGroupId =
                groupToSettingPolicyIndex.getSettingPolicyIdsForGroup(groupIds);

        assertThat(policyIdsByGroupId.values().stream()
                        .map(i -> i.size())
                        .mapToInt(Integer::intValue)
                        .sum(),
                is(0));
    }
}
