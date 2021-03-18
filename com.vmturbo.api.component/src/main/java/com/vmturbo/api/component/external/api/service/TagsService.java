package com.vmturbo.api.component.external.api.service;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.TagsMapper;
import com.vmturbo.api.component.external.api.mapper.TagsPaginationMapper;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entity.TagApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.pagination.SearchPaginationRequest;
import com.vmturbo.api.pagination.TagPaginationRequest.TagPaginationResponse;
import com.vmturbo.api.pagination.TagPaginationRequest;
import com.vmturbo.api.serviceinterfaces.ITagsService;
import com.vmturbo.common.api.mappers.EnvironmentTypeMapper;
import com.vmturbo.common.protobuf.PaginationProtoUtil;
import com.vmturbo.common.protobuf.common.Pagination;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.MapFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchTagsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchTagsResponse;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.utils.StringConstants;

/**
 * Tags service implementation.
 * Provides the implementation for the tags management:
 * <ul>
 * <li>Get all the available tags</li>
 * <li>Get all the entities that are related to a tag key</li>
 * </ul>
 */
public class TagsService implements ITagsService {
    private final SearchServiceBlockingStub searchServiceBlockingStub;
    private final GroupExpander groupExpander;
    private final RepositoryApi repositoryApi;
    private final PaginationMapper paginationMapper;
    private final TagsPaginationMapper tagsPaginationMapper;

    public TagsService(@Nonnull SearchServiceBlockingStub searchServiceBlockingStub,
            @Nonnull final RepositoryApi repositoryApi, @Nonnull GroupExpander groupExpander,
            @Nonnull PaginationMapper paginationMapper,
            @Nonnull TagsPaginationMapper tagsPaginationMapper) {
        this.searchServiceBlockingStub = searchServiceBlockingStub;
        this.repositoryApi = repositoryApi;
        this.groupExpander = groupExpander;
        this.paginationMapper = paginationMapper;
        this.tagsPaginationMapper = tagsPaginationMapper;
    }

    /**
     * Get all available tags.
     *
     * @param scopes if not null, limit the search to the given scopes.
     * @param entityType if not null, limit the search to the given entity types.
     * @param envType if not null, limit the search to the given environment types.
     * @param paginationRequest the {@link TagPaginationRequest}. If paginationRequest
     *                          is null then full response is returned.
     * @return a list of all available tags in the live topology.
     * @throws Exception happens when the use of remote services fails.
     */
    @Override
    public TagPaginationResponse getTags(
            @Nullable final List<String> scopes,
            @Nullable final String entityType,
            @Nullable final EnvironmentType envType,
            @Nullable TagPaginationRequest paginationRequest) throws OperationFailedException {

        // We don't currently support tags on nested group types (e.g. clusters), so short-circuit
        // here.
        if (GroupsService.NESTED_GROUP_TYPES.contains(entityType)) {
            return paginationRequest.buildEmptyResponse();
        }

        // get relevant service ids using the group service membership endpoint
        final SearchTagsRequest.Builder requestBuilder = SearchTagsRequest.newBuilder();
        if (scopes != null && !scopes.isEmpty()) {
            requestBuilder.addAllEntities(groupExpander.expandUuids(new HashSet<>(scopes)));
        }
        if (envType != null) {
            requestBuilder.setEnvironmentType(EnvironmentTypeMapper.fromApiToXL(envType));
        }
        if (entityType != null) {
            requestBuilder.setEntityType(ApiEntityType.fromString(entityType).typeNumber());
        }

        //Check to see if pagination is requested,
        //else use legacy code path with no pagination for backwards compatibility.
        if (paginationRequest != null) {
             requestBuilder.setPaginationParams(tagsPaginationMapper.toProtoParams(paginationRequest));
        }

        // perform the search
        final SearchTagsResponse response;
        try {
            response = searchServiceBlockingStub.searchTags(requestBuilder.build());
        } catch (Exception e) {
            final StringBuilder msgBuilder = new StringBuilder("Retrieval of tags failed. ");
            if (scopes != null && !scopes.isEmpty()) {
                msgBuilder
                        .append("Search was restricted to scopes: ")
                        .append(scopes.stream().collect(Collectors.joining(", ")))
                        .append(". ");
            }
            if (envType != null) {
                msgBuilder
                        .append("Search was restricted to environment type: ")
                        .append(envType.toString())
                        .append(". ");
            }
            if (entityType != null) {
                msgBuilder
                        .append("Search was restricted to entity type: ")
                        .append(ApiEntityType.fromString(entityType))
                        .append(". ");
            }
            throw new OperationFailedException(msgBuilder.toString(), e);
        }

        final Integer totalRecordCount = response.getPaginationResponse().getTotalRecordCount();
        final List<TagApiDTO> tagApiDTOS = TagsMapper.convertTagsToApi(response.getTags().getTagsMap());

        if (paginationRequest == null) {
            try {
                TagPaginationRequest tagPaginationRequest = new TagPaginationRequest();
                return tagPaginationRequest.allResultsResponse(tagApiDTOS, false);
            } catch (InvalidOperationException e) {
                throw new OperationFailedException("Retrieval of tags failed.");
            }
        }
        return PaginationProtoUtil.getNextCursor(response.getPaginationResponse())
            .map(nextCursor -> paginationRequest.nextPageResponse(tagApiDTOS, nextCursor, totalRecordCount))
            .orElseGet(() -> paginationRequest.finalPageResponse(tagApiDTOS, totalRecordCount));
    }

    /**
     * Get all service entities with tags that contain a specific key.
     *
     * @param tagKey tag key to look for.
     * @return list of entities that contain tags with the specified key.
     * @throws Exception happens when the use of remote services fails.
     */
    @Override
    public ResponseEntity<List<ServiceEntityApiDTO>> getEntitiesByTagKey(@Nonnull final String tagKey, @Nullable
            SearchPaginationRequest paginationRequest) throws Exception {
        final SearchParameters.Builder searchParams = SearchParameters.newBuilder()
            .setStartingFilter(PropertyFilter.newBuilder()
                .setPropertyName(StringConstants.TAGS_ATTR)
                .setMapFilter(MapFilter.newBuilder().setKey(tagKey).build()));


        //Check to see if pagination is requested,
        //else use legacy code path with no pagination for backwards compatibility.
        if (paginationRequest != null) {
            final SearchRequest searchRequest = repositoryApi.newSearchRequest(searchParams.build());
            searchRequest.usePriceIndexPopulator(true);
            final Pagination.PaginationParameters paginationParameters = paginationMapper.toProtoParams(paginationRequest);
            return searchRequest.getPaginatedSEList(paginationParameters);
        }
        List<ServiceEntityApiDTO> results = repositoryApi.newSearchRequest(searchParams.build())
                .getSEList();
        return new ResponseEntity<List<ServiceEntityApiDTO>>(results, new HttpHeaders(), HttpStatus.OK);
    }
}
