package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.validation.Errors;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import com.vmturbo.api.component.communication.RestAuthenticationProvider;
import com.vmturbo.api.component.external.api.mapper.LoginProviderMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entity.TagApiDTO;
import com.vmturbo.api.dto.user.ActiveDirectoryApiDTO;
import com.vmturbo.api.dto.user.ActiveDirectoryGroupApiDTO;
import com.vmturbo.api.dto.user.ChangePasswordApiDTO;
import com.vmturbo.api.dto.user.UserApiDTO;
import com.vmturbo.api.exceptions.UnauthorizedObjectException;
import com.vmturbo.api.serviceinterfaces.ITagsService;
import com.vmturbo.api.serviceinterfaces.IUsersService;
import com.vmturbo.auth.api.usermgmt.ActiveDirectoryDTO;
import com.vmturbo.auth.api.usermgmt.ActiveDirectoryGroupDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO.PROVIDER;
import com.vmturbo.auth.api.usermgmt.AuthUserModifyDTO;

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
    public List<TagApiDTO> getTags(final String entityType) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ServiceEntityApiDTO> getEntitiesByTagKey(final String tagKey) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }
}
