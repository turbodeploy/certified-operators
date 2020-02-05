package com.vmturbo.topology.processor.probes.internal;

import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.CONTAINER;
import static com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import static com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType.ENTITY_DEFINITION;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * Test class for {@link UserDefinedEntitiesProbeExplorer}.
 */
public class UserDefinedEntitiesProbeExplorerTest {

    /**
     * Should collect one group with one member.
     */
    @Test
    public void testGetGroups() {
        Grouping group = Grouping.newBuilder().setDefinition(GroupDefinition.newBuilder()
                .setType(ENTITY_DEFINITION).build()).build();
        TopologyEntityDTO member = TopologyEntityDTO.newBuilder().setEntityType(CONTAINER.getNumber()).setOid(100L).build();
        UserDefinedEntitiesProbeRetrieval retrieval = Mockito.mock(UserDefinedEntitiesProbeRetrieval.class);
        Mockito.when(retrieval.getGroups(Mockito.any(GroupType.class))).thenReturn(Collections.singleton(group));
        Mockito.when(retrieval.getMembers(Mockito.any(Long.class))).thenReturn(Collections.singleton(member));
        UserDefinedEntitiesProbeExplorer explorer = new UserDefinedEntitiesProbeExplorer(retrieval);
        Map<Grouping, Collection<TopologyEntityDTO>> groups = explorer.getUserDefinedGroups();
        Assert.assertEquals(1, groups.size());
        Assert.assertNotNull(groups.get(group));
        Assert.assertEquals(groups.get(group).iterator().next(), member);
    }

    /**
     * There should not be groups without members.
     */
    @Test
    public void testGetGroupsNoMembers() {
        Grouping group = Grouping.newBuilder().build();
        Collection<TopologyEntityDTO> emptyMembers = Collections.emptySet();
        UserDefinedEntitiesProbeRetrieval retrieval = Mockito.mock(UserDefinedEntitiesProbeRetrieval.class);
        Mockito.when(retrieval.getGroups(Mockito.any(GroupType.class))).thenReturn(Collections.singleton(group));
        Mockito.when(retrieval.getMembers(Mockito.any(Long.class))).thenReturn(emptyMembers);
        UserDefinedEntitiesProbeExplorer explorer = new UserDefinedEntitiesProbeExplorer(retrieval);
        Map<Grouping, Collection<TopologyEntityDTO>> groups = explorer.getUserDefinedGroups();
        Assert.assertEquals(0, groups.size());
    }

}
