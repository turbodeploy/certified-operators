package com.vmturbo.api.component.external.api.service;

import java.util.List;

import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entity.TagApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.serviceinterfaces.ITagsService;

/**
 * Tags service implementation.
 * Provides the implementation for the tags management:
 * <ul>
 * <li>Get all the available tags</li>
 * <li>Get all the entities that are related to a tag key</li>
 * </ul>
 */
public class TagsService implements ITagsService {

    @Override
    public List<TagApiDTO> getTags(final List<String> scopes, final String entityType,
                                   final EnvironmentType envType) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ServiceEntityApiDTO> getEntitiesByTagKey(final String tagKey) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }
}
