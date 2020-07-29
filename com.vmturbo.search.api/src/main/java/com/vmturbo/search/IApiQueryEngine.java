package com.vmturbo.search;

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

/**
 * An interface for establishing entity and group queries against search DB.
 */
public interface IApiQueryEngine {

    /**
     * Processes {@link EntityQueryApiDTO} and returns paginated results.
     *
     * @param entityQueryApiDTO the entity search query
     * @return a paginated list of records
     * @throws Exception when the query cannot be processed.
     */
    @Nonnull
    SearchQueryPaginationResponse<SearchQueryRecordApiDTO> processEntityQuery(
        @Nonnull EntityQueryApiDTO entityQueryApiDTO) throws Exception;

    /**
     * Processes {@link GroupQueryApiDTO} and returns paginated results.
     *
     * @param request the group search query
     * @return a paginated list of records
     * @throws Exception when the query cannot be processed.
     */
    SearchQueryPaginationResponse<SearchQueryRecordApiDTO> processGroupQuery(
        @Nonnull GroupQueryApiDTO request) throws Exception;

    /**
     * Processes {@link SearchAllQueryApiDTO} and returns paginated results.
     *
     * @param request the search all query
     * @return a paginated list of records
     * @throws Exception when the query cannot be processed.
     */
    SearchQueryPaginationResponse<SearchQueryRecordApiDTO> processSearchAllQuery(@Nonnull SearchAllQueryApiDTO request)
            throws Exception;

    /**
     * Processes a {@link EntityCountRequestApiDTO} and returns an (unpaginated) list of results.
     *
     * @param entityCountRequestApiDTO describes the group by parameters of the request
     * @return a list of results
     * @throws Exception when the query cannot be processed
     */
    List<SearchCountRecordApiDTO> countEntites(
        @Nonnull EntityCountRequestApiDTO entityCountRequestApiDTO) throws  Exception;

    /**
     * Processes a {@link GroupCountRequestApiDTO} and returns an (unpaginated) list of results.
     *
     * @param groupCountRequestApiDTO describes the group by parameters of the request
     * @return a list of results
     * @throws Exception when the query cannot be processed
     */
    List<SearchCountRecordApiDTO> countGroups(
        @Nonnull GroupCountRequestApiDTO groupCountRequestApiDTO) throws Exception;

    /**
     * Processes a {@link EntityMetadataRequestApiDTO} and returns an (unpaginated) list of results.
     *
     * @param entityMetadataRequestApiDTO describes the entity who's fields we request
     * @return a list of results
     * @throws Exception when the query cannot be processed
     */
    List<FieldValueTypeApiDTO> entityFields(
            @Nonnull EntityMetadataRequestApiDTO entityMetadataRequestApiDTO) throws Exception;

    /**
     * Processes a {@link GroupMetadataRequestApiDTO} and returns an (unpaginated) list of results.
     *
     * @param groupMetadataRequestApiDTO describes the group who's fields we request
     * @return a list of results
     * @throws Exception when the query cannot be processed
     */
    List<FieldValueTypeApiDTO> groupFields(
            @Nonnull GroupMetadataRequestApiDTO groupMetadataRequestApiDTO) throws Exception;
}
