package com.vmturbo.auth.api.usermgmt;

import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An ActiveDirectoryDTO represents a collection of Active Directory servers which can be
 * discovered by a combination of a domain, a provider URL, as well as a secure connection flag.
 * An ActiveDirectoryDTO also contains a list of AD groups for this collection of AD servers that
 * has been defined in the UI.
 * The members may be {@code null} as JSON deserialization ,
 */
public class ActiveDirectoryDTO {
    /**
     * The domain name.
     */
    private String domainName;

    /**
     * The optional login provider URI.
     * If set, we use it for AD servers search, otherwise we search AD servers for the domain.
     */
    private String loginProviderURI;

    /**
     * The secure connection flag.
     */
    private boolean secure;

    /**
     * The groups.
     * The API layer semantics requires that groups may be {@code null}.
     */
    private List<ActiveDirectoryGroupDTO> groups;

    /**
     * Constructs the ActiveDirectoryDTO.
     * We need this constructor for the the JSON deserialization.
     * We do not wish this constructor to be publicly accessible.
     */
    private ActiveDirectoryDTO() {
    }

    /**
     * Constructs the ActiveDirectoryDTO.
     *
     * @param domainName       The domain name.
     * @param loginProviderURI The login provider URI.
     * @param secure           The secure connection flag.
     */
    public ActiveDirectoryDTO(final @Nonnull String domainName,
                              final @Nullable String loginProviderURI,
                              final boolean secure) {
        this(domainName, loginProviderURI, secure, null);
    }

    /**
     * Constructs the ActiveDirectoryDTO.
     *
     * @param domainName       The domain name.
     * @param loginProviderURI The login provider URI.
     * @param secure           The secure connection flag.
     * @param groups           The groups.
     */
    public ActiveDirectoryDTO(final String domainName, final String loginProviderURI,
                              final boolean secure, final List<ActiveDirectoryGroupDTO> groups) {
        this.domainName = domainName;
        this.loginProviderURI = loginProviderURI;
        this.secure = secure;
        this.groups = groups;
    }

    /**
     * Returns the domain name.
     *
     * @return The domain name.
     */
    public @Nonnull String getDomainName() {
        return domainName;
    }

    /**
     * Returns the login provider URI.
     *
     * @return The login provider URI.
     */
    public @Nullable String getLoginProviderURI() {
        return loginProviderURI;
    }

    /**
     * Returns the secure connection flag.
     *
     * @return The secure connection flag.
     */
    public boolean isSecure() {
        return secure;
    }

    /**
     * Returns groups.
     *
     * @return The groups.
     */
    public @Nullable List<ActiveDirectoryGroupDTO> getGroups() {
        return groups;
    }

    /**
     * Sets the groups.
     *
     * @param groups The groups.
     */
    public void setGroups(final @Nullable List<ActiveDirectoryGroupDTO> groups) {
        this.groups = groups;
    }
}
