package com.vmturbo.mediation.udt.explore;

import static com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;

import java.util.Collections;
import java.util.Set;

import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetOwnersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.GetTopologyDataDefinitionsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchParameters.FilterSpecs;
import com.vmturbo.common.protobuf.search.Search.SearchTagValuesRequest;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Test class for {@link DataRequests}.
 */
public class DataRequestsTest {

    private DataRequests dataRequests = new DataRequests();

    /**
     * Verify 'GetTopologyDataDefinitionsRequest' request creating.
     */
    @Test
    public void testTddRequest() {
        GetTopologyDataDefinitionsRequest tddRequest = dataRequests.tddRequest();
        Assert.assertNotNull(tddRequest);
    }

    /**
     * Verify 'SearchEntitiesRequest' request creating.
     */
    @Test
    public void testCreateSearchEntityRequest() {
        FilterSpecs filterSpecs = FilterSpecs.newBuilder()
                .setExpressionType("exp_type")
                .setFilterType("f_type")
                .setExpressionValue("exp_value")
                .build();
        SearchParameters searchParameters = SearchParameters.newBuilder()
                .setSourceFilterSpecs(filterSpecs)
                .build();
        SearchEntitiesRequest request = dataRequests.createFilterEntityRequest(Collections.singletonList(searchParameters));
        Assert.assertEquals(filterSpecs, request.getSearch().getSearchParametersList().get(0).getSourceFilterSpecs());
    }

    /**
     * Verify 'GetMembersRequest' request creating.
     */
    @Test
    public void testGetGroupMembersRequest() {
        long id = 1111L;
        GetMembersRequest request = dataRequests.getGroupMembersRequest(id);
        Assert.assertEquals(1, request.getIdList().size());
        Assert.assertTrue(request.getIdList().contains(id));
    }

    /**
     * Verify 'GetGroupsRequest' request creating.
     */
    @Test
    public void testGetGroupRequest() {
        GroupFilter groupFilter = GroupFilter.newBuilder().setGroupType(GroupType.RESOURCE).build();
        GetGroupsRequest groupRequest = dataRequests.getGroupRequest(groupFilter);

        Assert.assertNotNull(groupRequest);
        Assert.assertSame(groupFilter, groupRequest.getGroupFilter());
    }

    /**
     * Verify 'GetOwnersRequest' request creating.
     */
    @Test
    public void testGetGroupOwnerRequest() {
        GetOwnersRequest groupOwnerRequest = dataRequests.getGroupOwnerRequest(
                Collections.singletonList(1L), GroupType.STORAGE_CLUSTER);

        Assert.assertNotNull(groupOwnerRequest);
        Assert.assertEquals(GroupType.STORAGE_CLUSTER, groupOwnerRequest.getGroupType());
    }

    /**
     * Verify 'GetOwnersRequest' request creating.
     */
    @Test
    public void testGetGroupOwnerRequestIfGroupTypeIsUndefined() {
        GetOwnersRequest groupOwnerRequest = dataRequests.getGroupOwnerRequest(
                Collections.singletonList(1L), null);

        Assert.assertNotNull(groupOwnerRequest);
        Assert.assertEquals(GroupType.REGULAR, groupOwnerRequest.getGroupType());
    }

    /**
     * Verify 'SearchTagValuesRequest' request creating.
     */
    @Test
    public void testGetSearchTagValuesRequest() {
        SearchTagValuesRequest request = dataRequests.getSearchTagValuesRequest("tag1", EntityType.SERVICE);
        Assert.assertNotNull(request);
        Assert.assertEquals("tag1", request.getTagKey());
        Assert.assertEquals(EntityType.SERVICE_VALUE, request.getEntityType());
    }
}
