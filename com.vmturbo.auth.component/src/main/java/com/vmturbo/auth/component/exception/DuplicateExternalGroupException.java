package com.vmturbo.auth.component.exception;

import com.vmturbo.auth.api.usermgmt.SecurityGroupDTO;

/**
 * Exception used in {@link com.vmturbo.auth.component.services.AuthUsersController#createSSOGroup(SecurityGroupDTO)}
 * if the group already exists.
 */
public class DuplicateExternalGroupException extends RuntimeException {
    /**
     * Basic constructor.
     *
     * @param groupName the name of the duplicate group
     */
    public DuplicateExternalGroupException(String groupName) {

        super(String.format("A group with the name %s already exists", groupName));
    }
}
