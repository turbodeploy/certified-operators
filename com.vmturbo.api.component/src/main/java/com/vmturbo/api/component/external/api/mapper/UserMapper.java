package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import org.apache.commons.collections4.CollectionUtils;

import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.user.UserApiDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO.PROVIDER;

/**
 *
 */
public class UserMapper {

    /**
     * Generates the UserApiDTO.
     *
     * @param dto The internal User object.
     * @param apiGroupsByOid An (optional) mapping from group oid -> {@link GroupApiDTO}. If this is
     *                       provided, then the GroupApiDTO objects from the map will be referenced
     *                       in the converted objects. Otherwise, simple GroupAPIDTO objects will
     *                       be created that just contain the group oid.
     * @return The generated UserApiDTO.
     */
    public static UserApiDTO toUserApiDTO(final @Nonnull AuthUserDTO dto,
                                          Map<Long, GroupApiDTO> apiGroupsByOid) {
        UserApiDTO user = new UserApiDTO();
        user.setLoginProvider(LoginProviderMapper.toApi(dto.getProvider()));
        user.setUsername(dto.getUser());
        user.setRoleName(dto.getRoles().get(0));
        user.setScope(groupOidsToGroupApiDTOs(dto.getScopeGroups(), apiGroupsByOid));
        user.setUuid(dto.getUuid());
        // Mandatory fields.
        user.setFeatures(Collections.emptyList());
        user.setType("DedicatedCustomer");
        user.setDisplayName(dto.getUser());
        return user;
    }

    /**
     * Convert an {@link AuthUserDTO} to {@link UserApiDTO}, without using a group translation map.
     *
     * @param dto The {@link AuthUserDTO} to convert
     * @return the matching {@link UserApiDTO}
     */
    public static UserApiDTO toUserApiDTO(final @Nonnull AuthUserDTO dto) {
        return toUserApiDTO(dto, null);
    }

    /**
     * Convert an {@link UserApiDTO} object to an {@link AuthUserDTO}.
     *
     * @param userApiDTO the {@link UserApiDTO} to convert
     * @return the equivalent {@link AuthUserDTO} object
     */
    public static AuthUserDTO toAuthUserDTO(final @Nonnull UserApiDTO userApiDTO) {
        return toAuthUserDTO(userApiDTO, true);
    }

    public static AuthUserDTO toAuthUserDTONoPassword(final @Nonnull UserApiDTO userApiDTO) {
        return toAuthUserDTO(userApiDTO, false);
    }

    private static AuthUserDTO toAuthUserDTO(final @Nonnull UserApiDTO userApiDTO, boolean keepPassword) {

        // According to UserApiDTO, the login provider values may be one of: "LOCAL", "LDAP".
        final AuthUserDTO.PROVIDER provider =
                LoginProviderMapper.fromApi(userApiDTO.getLoginProvider());

        // only propagate the password if withPassword = true and the provider is "local"
        final String password = keepPassword && provider.equals(PROVIDER.LOCAL) ? userApiDTO.getPassword() : null;

        // convert to the list of roles. (note -- only one role seems to be supported in the UserApiDTO object)
        List<String> roles = ImmutableList.of(userApiDTO.getRoleName().toUpperCase());

        // capture any scope groups -- just keep the group id
        List<Long> groupsInScope = groupApiDTOsToOids(userApiDTO.getScope());

        // The explicitly added LDAP users will be kept in the same local database.
        AuthUserDTO authUserDTO = new AuthUserDTO(provider, userApiDTO.getUsername(), password, null,
                null, null, roles, groupsInScope);

        return authUserDTO;
    }

    /**
     * Convert a list of group oids to {@link GroupApiDTO} objects, potentially using the
     * GroupsService to retrieve group information from the group service, if a reference to it is
     * supplied. If the group service reference is null, then this method will create simple
     * GroupApiDTO objects that carry just the group oid.
     *
     * The null reference is recommended if you don't need those group details - this will avoid
     * an extra network call to the group service. Otherwise, you can use the group service if you
     * need all the group details available, such as when showing the user scope in the UI.
     *
     * @param groupOids the list of group oids to convert
     * @param apiGroupsByOid an optional map of group oid -> {@link GroupApiDTO} objects, which may
     *                       contain more group details.
     * @return a list of {@link GroupApiDTO} objects, one per oid.
     */
    public static List<GroupApiDTO> groupOidsToGroupApiDTOs(List<Long> groupOids,
                                                             Map<Long, GroupApiDTO> apiGroupsByOid) {
        if (CollectionUtils.isEmpty(groupOids)) {
            return Collections.EMPTY_LIST;
        }

        // if no map was passed in, use the empty map
        if (apiGroupsByOid == null) {
            apiGroupsByOid = Collections.emptyMap();
        }
        // convert the scope, using the map
        List<GroupApiDTO> convertedGroups = new ArrayList<>();
        for (Long groupOid : groupOids) {
            GroupApiDTO group = apiGroupsByOid.get(groupOid);
            if (group == null) {
                // add a simple group api dto w/just the oid set
                GroupApiDTO groupApiDTO = new GroupApiDTO();
                groupApiDTO.setUuid(String.valueOf(groupOid));
                convertedGroups.add(groupApiDTO);
            } else {
                convertedGroups.add(group);
            }
        }
        return convertedGroups;
    }

    /**
     * Convert a list of {@link GroupApiDTO} objects to a list of group oids.
     *
     * @param groupDTOs the source list of {@link GroupApiDTO} objects.
     * @return the list of oids for all groups in the list.
     */
    public static List<Long> groupApiDTOsToOids(List<GroupApiDTO> groupDTOs) {
        if (CollectionUtils.isEmpty(groupDTOs)) {
            return Collections.EMPTY_LIST;
        }
        // capture any scope groups -- just keep the group id
        return groupDTOs.stream()
                .map(GroupApiDTO::getUuid)
                .map(Long::valueOf)
                .collect(Collectors.toList());
    }

}
