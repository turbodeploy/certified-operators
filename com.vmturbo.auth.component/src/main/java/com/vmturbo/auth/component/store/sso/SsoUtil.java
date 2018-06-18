package com.vmturbo.auth.component.store.sso;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The {@link SsoUtil} is a SSO helper utility class.
 */
public class SsoUtil {
    /**
     * The logger.
     */
    private final Logger logger = LogManager.getLogger(SsoUtil.class);

    /**
     * The domain name.
     */
    private String domainName_;

    /**
     * The login provider URL.
     */
    private String loginProviderURI_;
    /**
     * The secure login provider flag.
     */
    private boolean secureLoginProvider_;

    /**
     * The search base for the AD lookup.
     */
    private String adSearchBase_;

    /**
     * The AD or SAML groups. The {@code <Group name, Roles>} mapping.
     */
    private final Map<String, String> ssoGroups_ = Collections.synchronizedMap(new HashMap<>());

    /**
     * The AD or SAML users that were derived from groups.
     */
    private final Set<String> ssoGroupUsers_ = Collections.synchronizedSet(new HashSet<>());

    /**
     * Sets the domain name as well as the AD search base.
     * The AD search base replaces all ".<subdomain>." subdomain portions with "DC=<subdomain>,".
     *
     * @param domainName The domain name.
     */
    public synchronized void setDomainName(String domainName) {
        domainName_ = domainName;
        adSearchBase_ = "";
        if (!Strings.isNullOrEmpty(domainName_)) {
            String[] temp;
            temp = domainName_.split("\\.");

            for (int i = 0; i < temp.length; i++) {
                adSearchBase_ = adSearchBase_ + "DC=" + temp[i] + ",";
            }
            adSearchBase_ = adSearchBase_.substring(0, adSearchBase_.length() - 1);
        }
    }

    /**
     * Sets the secure login provider flag.
     *
     * @param secureLoginProvider The secure login provider flag.
     */
    public synchronized void setSecureLoginProvider(boolean secureLoginProvider) {
        secureLoginProvider_ = secureLoginProvider;
    }

    /**
     * Sets the login provider URL.
     *
     * @param loginProviderURI The login provider URL.
     */
    public synchronized void setLoginProviderURI(String loginProviderURI) {
        loginProviderURI_ = loginProviderURI;
    }

    /**
     * Resets the AUTH provider.
     */
    public synchronized void reset() {
        domainName_ = null;
        loginProviderURI_ = null;
        ssoGroups_.clear();
        ssoGroupUsers_.clear();
    }

    /**
     * Removes the trailing '.' from the host name and adds the provider URI to the providers
     * collection.
     *
     * @param providerURI The provider URL.
     * @param ldapServers The LDAP servers collection.
     * @throws SecurityException In case of an invalid provider URI.
     */
    private void addProviderURL(final @Nonnull String providerURI,
                                final @Nonnull Collection<String> ldapServers)
            throws SecurityException {

        try {
            URI uri = new URI(providerURI);
            String host = uri.getHost().toUpperCase();
            if (host.endsWith(".")) {
                host = host.substring(0, host.length() - 1);
            }
            ldapServers.add(uri.getScheme() + "://" + host + ":" + uri.getPort());
        } catch (URISyntaxException e) {
            throw new SecurityException(e);
        }
    }

    /**
     * Locates the LDAP servers in the Windows domain.
     *
     * @return The non-empty list of the LDAP servers in the Windows domain.
     * @throws SecurityException In the case of of an empty LDAP server list.
     */
    public @Nonnull Collection<String> findLDAPServersInWindowsDomain() throws SecurityException {
        // The loginProviderURI_ is optional. If we have it, force using it.
        if (!Strings.isNullOrEmpty(loginProviderURI_)) {
            // In case the domain is specified, and the login provider URL does not contain it,
            // add the proper URL path.
            return ImmutableList.of(loginProviderURI_);
        }
        // In case we have neither login provider URI nor domain name, don't even try to search.
        if (Strings.isNullOrEmpty(domainName_)) {
            throw new SecurityException("The AD has not been setup yet");
        }

        Collection<String> ldapServers = new ArrayList<>();
        Hashtable<String, String> props = new Hashtable<>();
        props.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory");
        props.put("java.naming.provider.url", "dns:");
        DirContext ctx = null;
        try {
            ctx = new InitialDirContext(props);
            // This is how Windows domain controllers are registered in DNS
            // As per: https://technet.microsoft.com/en-us/library/cc961719.aspx
            // ans:    https://technet.microsoft.com/en-us/library/cc759550(v=ws.10).aspx
            Attributes attributes = ctx.getAttributes("_ldap._tcp.dc._msdcs." + domainName_,
                                                      new String[]{"SRV"});
            Attribute attr = attributes.get("SRV");
            String prefix = secureLoginProvider_ ? "ldaps://" : "ldap://";
            if (attr != null) {
                int attrSize = attr.size();
                for (int i = 0; i < attrSize; i++) {
                    String srvRecord = attr.get(i).toString();
                    // Each SRV record is in the format "priority weight port server" (space
                    // separated) for example: "0 100 389 dc1.company.com."
                    String[] srvRecordParts = srvRecord.split(" ");
                    addProviderURL(prefix + srvRecordParts[3] + ":" + srvRecordParts[2],
                                   ldapServers);
                }
            }
        } catch (NamingException e) {
            logger.error("LDAP servers search failed", e);
        } finally {
            closeContext(ctx);
        }
        // All callers expect the non-empty LDAP servers list.
        if (ldapServers.isEmpty()) {
            throw new SecurityException("Unable to locate any LDAP servers");
        }
        return ldapServers;
    }

    /**
     * Returns UPN.
     *
     * @param userName     The user name in the form of domain\\user.
     * @param userPassword The password.
     * @return The UPN or {@code null} in case of an error.
     */
    private @Nullable String getUpn(@Nonnull String userName, String userPassword) {
        @Nonnull Collection<String> ldapServers = findLDAPServersInWindowsDomain();
        if (adSearchBase_.length() == 0) {
            logger.error("AD SearchBase is empty");
            return null;
        }
        DirContext ctx = null;
        for (String ldapServer : ldapServers) {
            Hashtable<String, String> props = composeLDAPConnProps(ldapServer,
                                                                   userName.split("\\\\")[1] + "@" +
                                                                   domainName_,
                                                                   userPassword);
            String searchFilter =
                    "(&(objectClass=person)(SamAccountName=" + userName.split("\\\\")[1] + "))";
            String returnAttrs[] = {"userPrincipalName"};
            SearchControls sCtrl = new SearchControls();
            sCtrl.setSearchScope(SearchControls.SUBTREE_SCOPE);
            sCtrl.setReturningAttributes(returnAttrs);
            try {
                ctx = new InitialDirContext(props);
                NamingEnumeration<SearchResult> answer = ctx.search(adSearchBase_,
                                                                    searchFilter, sCtrl);
                while (answer.hasMoreElements()) {
                    SearchResult sr = answer.next();
                    if (sr.getAttributes().size() > 0) {
                        return sr.getAttributes().get(returnAttrs[0]).toString().split(" ")[1];
                    }
                }
            } catch (NamingException namEx) {
                logger.trace("LDAP connection failed: ", namEx);
            } finally {
                closeContext(ctx);
            }
        }
        return null;
    }

    /**
     * Adds or replaces the AD or SAML group.
     *
     * @param groupName The group name.
     * @param roleName  The role name.
     * @return {@code true} iff the new group was added.
     */
    public boolean putGroup(final @Nonnull String groupName, final @Nonnull String roleName) {
        return null != ssoGroups_.put(groupName, roleName);
    }

    /**
     * Deletes the group.
     *
     * @param groupName The group name.
     * @return {@code true} iff the group existed before this call.
     */
    public boolean deleteGroup(final @Nonnull String groupName) {
        return null != ssoGroups_.remove(groupName);
    }

    /**
     * Closes the directory context.
     *
     * @param ctx The directory context.
     */
    public void closeContext(final @Nullable DirContext ctx) {
        if (ctx != null) {
            try {
                ctx.close();
            } catch (NamingException e) {
                logger.trace("Error closing context", e);
            }
        }
    }

    /**
     * Authorize user in SAML group. Internally we are reusing {@link SsoUtil#ssoGroups_} to hold group.
     *
     * @param userName  user name
     * @param groupName claimed group name
     * @return user role if found
     */
    public @Nonnull Optional<String> authorizeSAMLUserInGroup(final @Nonnull String userName,
                                                              final @Nonnull String groupName) {
        boolean foundGroup = ssoGroups_
                .keySet()
                .stream()
                .anyMatch(name -> name.equals(groupName.toLowerCase()));
        if (foundGroup) {
            if (!ssoGroupUsers_.contains(userName)) {
                ssoGroupUsers_.add(userName);
            }
            return Optional.ofNullable(ssoGroups_.get(groupName));
         }
         return Optional.empty();
    }


    /**
     * Try to authenticate the user as a member of the AD group.
     *
     * @param userName     Name of the user.
     * @param userPassword Password user presented.
     * @param ldapServers  A list of LDAP servers to query.  Assumed to be non-empty.
     * @return The role name or {@code null} if failed.
     */
    public @Nullable String authenticateUserInGroup(final @Nonnull String userName,
                                                    final @Nonnull String userPassword,
                                                    final @Nonnull Collection<String> ldapServers) {
        String upn;

        if (userName.contains("\\")) {
            upn = getUpn(userName, userPassword);
            if (upn == null) {
                logger.error("LoginManager::authenticateUserInGroup - upn is null");
                return null;
            }
        } else if (!userName.contains("@")) {
            upn = userName + "@" + domainName_;
        } else {
            upn = userName;
        }

        if (adSearchBase_.length() == 0) {
            logger.error("AD SearchBase is empty");
            return null;
        }

        DirContext ctx = null;
        for (String ldapServer : ldapServers) {
            Hashtable<String, String> props = composeLDAPConnProps(ldapServer, upn, userPassword);
            String searchFilter = "(&(objectClass=person)(userPrincipalName=" + upn + "))";
            String returnAttrs[] = {"memberOf"};
            SearchControls sCtrl = new SearchControls();
            sCtrl.setSearchScope(SearchControls.SUBTREE_SCOPE);
            sCtrl.setReturningAttributes(returnAttrs);
            try {
                ctx = new InitialDirContext(props);
                NamingEnumeration<SearchResult> answer = ctx.search(adSearchBase_,
                                                                    searchFilter, sCtrl);
                // Loop through the results and check every single value in attribute "memberOf"
                while (answer.hasMoreElements()) {
                    SearchResult sr = answer.next();
                    Attribute memberOf = sr.getAttributes().get(returnAttrs[0]);
                    if (memberOf == null) {
                        continue;
                    }
                    String memberOfAttrValue = memberOf.toString().toLowerCase();
                    for (String groupName : ssoGroups_.keySet().toArray(new String[0])) {
                        String adGroupNotChanged = groupName;
                        // Prepend CN= to the groupName if the user only specified the name of
                        // the group
                        if (!groupName.startsWith("CN=")) {
                            groupName = "CN=" + groupName;
                        }
                        if (memberOfAttrValue.contains(groupName.toLowerCase())) {
                            if (!ssoGroupUsers_.contains(userName)) {
                                ssoGroupUsers_.add(userName);
                                return ssoGroups_.get(adGroupNotChanged);
                            }
                        }
                    }
                }
            } catch (NamingException authEx) {
                return null;
            } finally {
                closeContext(ctx);
            }
        }
        return null;
    }

    /**
     * Authenticates the AD user.
     *
     * @param userName The user name.
     * @param password The password.
     * @throws SecurityException In case of an error parsing or decrypting the data.
     */
    public void authenticateADUser(final @Nonnull String userName,
                                   final @Nonnull String password)
            throws SecurityException {
        // The username is in one of three formats:  Domain\Username or Username@Domain,
        // or just plain Username.  We transform these in different ways (for unknown
        // historical reasons:
        // 1. Domain\Username -> upperCase(Domain)\Username
        // 2. Username@Domain is retained as passed
        // 3. Username -> Username@Domain

        // Note: For \'s because the parameter is a regular expression, so both Java and
        // the regex parser need a layer of escaping.
        String domainUserName;
        String[] userNameSplit = userName.split("\\\\", 2);
        if (userNameSplit.length == 2) {                              // Domain\Username form
            domainUserName = userNameSplit[0].toUpperCase() + "\\" + userNameSplit[1];
        } else if (userName.contains("@")) {                          // Username@Domain form
            domainUserName = userName;
        } else {                                                      // Username form
            domainUserName = userName + "@" + domainName_;
        }

        // Get the AD servers we can query.  If there are none, there's no point in going on.
        DirContext ctx = null;
        for (String server : findLDAPServersInWindowsDomain()) {
            Hashtable<String, String> props =
                    composeLDAPConnProps(server, domainUserName, password);
            try {
                ctx = new InitialDirContext(props);
                // Make sure we have authenticated by querying the attributes.
                ctx.getAttributes("");
                return;
            } catch (NamingException e) {
                logger.error("LDAP connection to " + server + " failed", e);
            } finally {
                closeContext(ctx);
            }
        }
        // If we're here, we got NamingException's for all our LDAP servers.
        throw new SecurityException("Unable to authenticate " + userName);
    }

    /**
     * Composes the LDAP connection properties.
     *
     * @param server   The LDAP server.
     * @param user     The domain user.
     * @param password The password.
     * @return The LDAP connection properties.
     */
    private Hashtable<String, String> composeLDAPConnProps(final @Nonnull String server,
                                                           final @Nonnull String user,
                                                           final @Nonnull String password) {
        Hashtable<String, String> props = new Hashtable<>();
        props.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        props.put(Context.PROVIDER_URL, server);
        if (secureLoginProvider_) {
            props.put(Context.SECURITY_PROTOCOL, "ssl");
        }
        props.put(Context.SECURITY_AUTHENTICATION, "simple");
        props.put(Context.SECURITY_PRINCIPAL, user);
        props.put(Context.SECURITY_CREDENTIALS, password);
        return props;
    }
}
