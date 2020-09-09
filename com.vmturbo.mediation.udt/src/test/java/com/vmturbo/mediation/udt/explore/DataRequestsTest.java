package com.vmturbo.mediation.udt.explore;

import static com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;

import java.util.Collections;
import java.util.Set;

import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.GetTopologyDataDefinitionsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchParameters.FilterSpecs;

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
}
