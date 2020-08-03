package com.vmturbo.api.component.external.api.service;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.TagsMapper;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entity.TagApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.serviceinterfaces.ITagsService;
import com.vmturbo.common.api.mappers.EnvironmentTypeMapper;
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

    public TagsService(@Nonnull SearchServiceBlockingStub searchServiceBlockingStub,
            @Nonnull final RepositoryApi repositoryApi,
            @Nonnull GroupExpander groupExpander) {
        this.searchServiceBlockingStub = searchServiceBlockingStub;
        this.repositoryApi = repositoryApi;
        this.groupExpander = groupExpander;
    }

    /**
     * Get all available tags.
     *
     * @param scopes if not null, limit the search to the given scopes.
     * @param entityType if not null, limit the search to the given entity types.
     * @param envType if not null, limit the search to the given environment types.
     * @return a list of all available tags in the live topology.
     * @throws Exception happens when the use of remote services fails.
     */
    @Override
    public List<TagApiDTO> getTags(
            @Nullable final List<String> scopes,
            @Nullable final String entityType,
            @Nullable final EnvironmentType envType) throws OperationFailedException {

        // We don't currently support tags on nested group types (e.g. clusters), so short-circuit
        // here.
        if (GroupsService.NESTED_GROUP_TYPES.contains(entityType)) {
            return Collections.emptyList();
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

        // convert to desired format
        return TagsMapper.convertTagsToApi(response.getTags().getTagsMap());
    }

    /**
     * Get all service entities with tags that contain a specific key.
     *
     * @param tagKey tag key to look for.
     * @return list of entities that contain tags with the specified key.
     * @throws Exception happens when the use of remote services fails.
     */
    @Override
    public List<ServiceEntityApiDTO> getEntitiesByTagKey(@Nonnull final String tagKey) throws Exception {
        final SearchParameters params = SearchParameters.newBuilder()
            .setStartingFilter(PropertyFilter.newBuilder()
                .setPropertyName(StringConstants.TAGS_ATTR)
                .setMapFilter(MapFilter.newBuilder().setKey(tagKey).build()))
            .build();
        return repositoryApi.newSearchRequest(params)
            .getSEList();
    }
}
