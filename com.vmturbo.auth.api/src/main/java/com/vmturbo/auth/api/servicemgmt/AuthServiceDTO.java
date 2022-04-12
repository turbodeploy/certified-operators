package com.vmturbo.auth.api.servicemgmt;

import java.io.Serializable;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.auth.api.servicemgmt.AuthServiceHelper.LOCATION;
import com.vmturbo.auth.api.servicemgmt.AuthServiceHelper.ROLE;
import com.vmturbo.auth.api.servicemgmt.AuthServiceHelper.TYPE;

/**
 * An AuthServiceDTO represents the Service object to be exchanged with the AUTH component.
 */
public class AuthServiceDTO implements Serializable {

    /**
     * The service type.
     */
    private TYPE type;

    /**
     * The location of the service.
     */
    private LOCATION location;

    /**
     * The roles.
     */
    private Set<ROLE> roles;

    /**
     * The service name.
     */
    private String name;

    /**
     * The internal jwt auth token.
     */
    private String internalToken;

    /**
     * The external token.
     */
    private String externalToken;

    /**
     * The uuid.
     */
    private String uuid;

    /**
     * The user's IP address.
     */
    private String ipAddress;

    /**
     * Constructor.
     *
     * @param type service type
     * @param location location type
     * @param roles the roles
     * @param name the name
     * @param internalToken the internally generated jwt auth token of Auth component
     * @param externalToken the externally generated token
     * @param uuid the uuid
     * @param ipAddress the ip address
     */
    public AuthServiceDTO(final @Nonnull TYPE type,
                       final @Nonnull LOCATION location,
                       final @Nonnull Set<ROLE> roles,
                       final @Nullable String name,
                       final @Nullable String internalToken,
                       final @Nullable String externalToken,
                       final @Nullable String uuid,
                       final @Nullable String ipAddress) {
        this.type = type;
        this.location = location;
        this.roles = roles;
        this.name = name;
        this.internalToken = internalToken;
        this.externalToken = externalToken;
        this.uuid = uuid;
        this.ipAddress = ipAddress;
    }

    /**
     * Get type.
     *
     * @return the type
     */
    public TYPE getType() {
        return type;
    }

    /**
     * Set type.
     *
     * @param type the type to set
     */
    public void setType(TYPE type) {
        this.type = type;
    }

    /**
     * Get location.
     *
     * @return the location
     */
    public LOCATION getLocation() {
        return location;
    }

    /**
     * Set location.
     *
     * @param location the location to set
     */
    public void setLocation(LOCATION location) {
        this.location = location;
    }

    /**
     * Get roles.
     *
     * @return the roles
     */
    public Set<ROLE> getRoles() {
        return roles;
    }

    /**
     * Set roles.
     *
     * @param roles the roles to set
     */
    public void setRoles(Set<ROLE> roles) {
        this.roles = roles;
    }

    /**
     * Get name.
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Set name.
     *
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Get internal token.
     *
     * @return the token
     */
    public String getInternalToken() {
        return internalToken;
    }

    /**
     * Set internal token.
     *
     * @param internalToken the token to set
     */
    public void setInternalToken(String internalToken) {
        this.internalToken = internalToken;
    }

    /**
     * Get external token.
     *
     * @return the token
     */
    public String getExternalToken() {
        return externalToken;
    }

    /**
     * Set external token.
     *
     * @param externalToken the token to set
     */
    public void setExternalToken(String externalToken) {
        this.externalToken = externalToken;
    }

    /**
     * Get uuid.
     *
     * @return the uuid
     */
    public String getUuid() {
        return uuid;
    }

    /**
     * Set uuid.
     *
     * @param uuid the uuid to set
     */
    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    /**
     * Get ipaddress.
     *
     * @return the ipAddress
     */
    public String getIpAddress() {
        return ipAddress;
    }

    /**
     * Set ipaddress.
     *
     * @param ipAddress the ipAddress to set
     */
    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }
}
