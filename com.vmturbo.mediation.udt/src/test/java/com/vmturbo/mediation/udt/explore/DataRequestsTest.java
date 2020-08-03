package com.vmturbo.mediation.udt.explore;

import static com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import static com.vmturbo.common.protobuf.search.SearchableProperties.TAGS_TYPE_PROPERTY_NAME;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.GetTopologyDataDefinitionsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchParameters.FilterSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test class for {@link DataRequests}.
 */
public class DataRequestsTest {

    private DataRequests dataRequests = new DataRequests();

    /**
     * Verify 'SearchEntitiesRequest' request creating.
     */
    @Test
    public void testEntitiesByTagRequest() {
        String tag = "region";
        EntityType type = EntityType.VIRTUAL_MACHINE;
        SearchEntitiesRequest request = dataRequests.entitiesByTagRequest(tag, type);
        Search.SearchQuery searchQuery = request.getSearch();
        Assert.assertNotNull(searchQuery);
        List<SearchParameters> parameters = searchQuery.getSearchParametersList();
        Assert.assertFalse(parameters.isEmpty());
        SearchParameters parameter = parameters.get(0);

        Assert.assertEquals("entityType", parameter.getStartingFilter().getPropertyName());
        Assert.assertEquals(type.getNumber(), parameter.getStartingFilter().getNumericFilter().getValue());
        Assert.assertEquals(TAGS_TYPE_PROPERTY_NAME, parameter.getSearchFilterList().get(0).getPropertyFilter().getPropertyName());
        Assert.assertEquals(tag, parameter.getSearchFilterList().get(0).getPropertyFilter().getMapFilter().getKey());
    }

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
     * Verify 'RetrieveTopologyEntitiesRequest' request creating.
     */
    @Test
    public void testGetEntitiesByOidsRequest() {
        Set<Long> oids = Sets.newHashSet(10L, 20L);
        RetrieveTopologyEntitiesRequest request = dataRequests.getEntitiesByOidsRequest(oids);
        Assert.assertTrue(request.getEntityOidsList().containsAll(oids));
        Assert.assertEquals(oids.size(), request.getEntityOidsList().size());
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
