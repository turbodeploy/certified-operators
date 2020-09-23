package com.vmturbo.api.component.external.api.service;

import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.searchquery.EntityCountRequestApiDTO;
import com.vmturbo.api.dto.searchquery.EntityMetadataRequestApiDTO;
import com.vmturbo.api.dto.searchquery.EntityQueryApiDTO;
import com.vmturbo.api.dto.searchquery.FieldValueTypeApiDTO;
import com.vmturbo.api.dto.searchquery.GroupCountRequestApiDTO;
import com.vmturbo.api.dto.searchquery.GroupMetadataRequestApiDTO;
import com.vmturbo.api.dto.searchquery.GroupQueryApiDTO;
import com.vmturbo.api.dto.searchquery.SearchAllQueryApiDTO;
import com.vmturbo.api.dto.searchquery.SearchCountRecordApiDTO;
import com.vmturbo.api.dto.searchquery.SearchQueryRecordApiDTO;
import com.vmturbo.api.pagination.searchquery.SearchQueryPaginationResponse;
import com.vmturbo.api.serviceinterfaces.ISearchQueryService;
import com.vmturbo.search.IApiQueryEngine;

/**
 * This object serves the requests to the new search API.
import com.vmturbo.api.pagination.searchquery.SearchQueryPaginationResponse;
import com.vmturbo.api.serviceinterfaces.ISearchQueryService;
import com.vmturbo.search.IApiQueryEngine;

/**
 * This object serves the requests to the new search API.
 * <ul>
 *     <li>POST /entities/query</li>
 *     <li>POST /entities/count</li>
 *     <li>POST /entities/query/fields</li>
 *     <li>POST /groups/query</li>
 *     <li>POST /groups/count</li>
 *     <li>POST /groups/query/fields</li>
 *     <li>POST /search/query</li>
 * </ul>
 */
public class SearchQueryService implements ISearchQueryService {

    private IApiQueryEngine apiQueryEngine;

    public SearchQueryService(final IApiQueryEngine apiQueryEngine) {
        this.apiQueryEngine = apiQueryEngine;
    }

    /**
     * Search entities.  Endpoint: POST /entities/query
     *
     * @param input {@link EntityQueryApiDTO} object with specifications for the search
     * @return list of entities returned
     */
    @Override
    @Nonnull
    public SearchQueryPaginationResponse<SearchQueryRecordApiDTO> searchEntities(
            @Nonnull EntityQueryApiDTO input) throws Exception {
        return apiQueryEngine.processEntityQuery(input);
    }

    /**
     * Search groups.  Endpoint: POST /groups/query
     *
     * @param input {@link GroupQueryApiDTO} object with specifications for the search
     * @return list of groups returned
     * @throws Exception when some error happens
     */
    @Override
    @Nonnull
    public SearchQueryPaginationResponse<SearchQueryRecordApiDTO> searchGroups(
            @Nonnull GroupQueryApiDTO input) throws Exception {
        return apiQueryEngine.processGroupQuery(input);
    }

    /**
     * Search groups.  Endpoint: POST /groups/query
     *
     * @param input {@link GroupQueryApiDTO} object with specifications for the search
     * @return list of groups returned
     * @throws Exception when some error happens
     */
    @Override
    @Nonnull
    public SearchQueryPaginationResponse<SearchQueryRecordApiDTO> searchAll(
            @Nonnull SearchAllQueryApiDTO input) throws Exception {
        return apiQueryEngine.processSearchAllQuery(input);
    }




    /**
     * Count entities.  Endpoint: POST /entities/count
     *
     * @param input {@link EntityCountRequestApiDTO} object with specifications for the count
     * @return list of counts returned
     * @throws Exception when some error happens
     */
    @Override
    @Nonnull
    public List<SearchCountRecordApiDTO> countEntities(@Nonnull EntityCountRequestApiDTO input)
            throws Exception {
        return apiQueryEngine.countEntities(input);
    }


    /**
     * Count groups.  Endpoint: POST /groups/count
     *
     * @param input {@link GroupCountRequestApiDTO} object with specifications for the count
     * @return list of counts returned
     * @throws Exception when some error happens
     */
    @Override
    @Nonnull
    public List<SearchCountRecordApiDTO> countGroups(@Nonnull GroupCountRequestApiDTO input)
            throws Exception {
        return apiQueryEngine.countGroups(input);
    }

    /**
     * Field metadata for entities.  Endpoint: POST /entities/query/fields
     *
     * @param input {@link EntityMetadataRequestApiDTO} object that specifies
     *              the entity type for which the request is made
     * @return list of fields associated with their values
     * @throws Exception when some error happens
     */
    @Override
    @Nonnull
    public List<FieldValueTypeApiDTO> entityFields(@Nonnull EntityMetadataRequestApiDTO input)
            throws Exception {
        return apiQueryEngine.entityFields(input);
    }

    /**
     * Field metadata for groups.  Endpoint: POST /groups/query/fields
     *
     * @param input {@link GroupMetadataRequestApiDTO} object that specifies
     *        the entity type for which the request is made
     * @return list of fields associated with their values
     * @throws Exception when some error happens
     */
    @Override
    @Nonnull
    public List<FieldValueTypeApiDTO> groupFields(@Nonnull GroupMetadataRequestApiDTO input) throws Exception {
        return apiQueryEngine.groupFields(input);
    }
}
