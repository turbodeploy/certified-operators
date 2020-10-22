package com.vmturbo.auth.component.store.sso;

import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.ADMINISTRATOR;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.ADVISOR;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.AUTOMATOR;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.DEPLOYER;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.OBSERVER;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.OPERATIONAL_OBSERVER;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.SHARED_ADVISOR;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.SHARED_OBSERVER;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.SITE_ADMIN;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.naming.CommunicationException;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.usermgmt.SecurityGroupDTO;

/**
 * The {@link SsoUtil} is a SSO helper utility class.
 */
public class SsoUtil {
    /**
     * Error message when AD is not setup.
     */
    @VisibleForTesting
    public static final String AD_NOT_SETUP = "The AD has not been setup yet";
    private static final String CTX_FACTORY = "com.sun.jndi.ldap.LdapCtxFactory";

    /**
     * Role to privilege map which is used for calculating least privileges (permissions). The bigger number on value,
     * the more privilege the role has. For example, ADMINISTRATOR role have max privileges with 100 as value;
     * SHARED_OBSERVER has min privileges with 40 as value.
     * {@link CaseInsensitiveMap} is used there to support both case insensitive group name matching.
     */
    private static final Map<String, Integer> LEAST_PRIVILEGE_MAP =
            new CaseInsensitiveMap<>(ImmutableMap.<String, Integer>builder()
                    .put(ADMINISTRATOR, 100)
                    .put(SITE_ADMIN, 90)
                    .put(AUTOMATOR, 80)
                    .put(ADVISOR, 70)
                    .put(SHARED_ADVISOR, 60)
                    .put(DEPLOYER, 50)
                    .put(OBSERVER, 40)
                    .put(SHARED_OBSERVER, 30)
                    .put(OPERATIONAL_OBSERVER, 20)
                    .build());
    private static final String FULL_USERNAME_FORMAT = "%s@%s";
    private static final Pattern BACKSLASH_SPLITTER = Pattern.compile("\\\\");
    private static final Pattern AT_SPLITTER = Pattern.compile("@");
    private static final String CANONICAL_NAME = "cn";

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
     * The AD or SAML groups. The {@code <Group name, SecurityGroupDTO>} mapping. Group name is case insensitive.
     */
    @GuardedBy("usersGroupsLock")
    private final Map<String, SecurityGroupDTO> ssoGroups_ = new CaseInsensitiveMap<>();

    /**
     * The AD or SAML users that were derived from groups.
     */
    @GuardedBy("usersGroupsLock")
    private final Set<String> ssoGroupUsers_ = new HashSet<>();

    private final Object usersGroupsLock = new Object();

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
        synchronized (usersGroupsLock) {
            ssoGroups_.clear();
            ssoGroupUsers_.clear();
        }
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
            throw new SecurityException(AD_NOT_SETUP);
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
     * Returns user distinguished name or {@code null} in case user with this name is not found.
     *
     * @param pureUsername The user name without domain name, i.e. username might be specified
     * in three ways:
     * <ul>
     *     <li> username;
     *     <li> username@domain;
     *     <li> domain\\username;
     * </ul>
     * pureUsername will contain value from item 1 always.
     * @param userPassword The password.
     * @param ldapServers LDAP server URIs to which we will connect
     * @return The distinguished name of the found user or {@code null} in case of an error.
     */
    @Nullable
    private String getUserDn(@Nonnull String pureUsername, @Nonnull String userPassword,
                    @Nonnull Iterable<String> ldapServers) {
        if (adSearchBase_.isEmpty()) {
            logger.error("AD SearchBase is empty");
            return null;
        }
        final String searchFilter =
                        String.format("(&(objectClass=person)(SamAccountName=%s))", pureUsername);
        final SearchControls sCtrl = new SearchControls();
        sCtrl.setSearchScope(SearchControls.SUBTREE_SCOPE);
        for (String ldapServer : ldapServers) {
            final Hashtable<String, String> props =
                            composeLDAPConnProps(ldapServer, createFullUsername(pureUsername),
                                            userPassword);
            DirContext ctx = null;
            try {
                ctx = new InitialDirContext(props);
                final NamingEnumeration<SearchResult> answer = ctx.search(adSearchBase_,
                                                                    searchFilter, sCtrl);
                while (answer.hasMoreElements()) {
                    final SearchResult sr = answer.next();
                    final String dn = sr.getNameInNamespace();
                    if (StringUtils.isNotBlank(dn)) {
                        return dn;
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
     * @param securityGroup  The security group to store.
     * @return {@code true} iff the new group was added.
     */
    public boolean putSecurityGroup(final @Nonnull String groupName, final @Nonnull SecurityGroupDTO securityGroup) {
        synchronized (usersGroupsLock) {
            return null != ssoGroups_.put(groupName, securityGroup);
        }
    }

    /**
     * Deletes the group.
     *
     * @param groupName The group name.
     * @return {@code true} iff the group existed before this call.
     */
    public boolean deleteSecurityGroup(final @Nonnull String groupName) {
        synchronized (usersGroupsLock) {
            return null != ssoGroups_.remove(groupName);
        }
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
     * @return security group, if found
     */
    @Nonnull
    public Optional<SecurityGroupDTO> authorizeSAMLUserInGroup(final @Nonnull String userName,
                    final @Nullable String groupName) {
        synchronized (usersGroupsLock) {
            final Set<SecurityGroupDTO> matchedGroups =
                            findMatchedGroups(Collections.singleton(groupName));
            return matchedGroups.stream().findAny().map(g -> {
                ensureUser(userName);
                return g;
            });
        }
    }

    /**
     * Try to authenticate the user as a member of the AD group.
     *
     * @param userName Name of the user.
     * @param userPassword Password user presented.
     * @param ldapServers A list of LDAP servers to query.  Assumed to be
     *                 non-empty.
     * @param multipleGroupSupport support multiple group
     * @return The security group, or {@code null} if failed.
     */
    @Nullable
    public List<SecurityGroupDTO> authenticateUserInGroup(final @Nonnull String userName,
                    final @Nonnull String userPassword,
                    final @Nonnull Collection<String> ldapServers,
                    final boolean multipleGroupSupport) {
        final String pureUsername = getPureUsername(userName);
        final String fullUsername = createFullUsername(pureUsername);
        final String dn = getUserDn(pureUsername, userPassword, ldapServers);
        /*
          In cae multiple groups support is enabled, then recursive filter will be used, otherwise
          only groups which have user DN as direct member will be returned.
          The filter below is a recursive filter, which is searching for LDAP objects which have
          member attribute pointing to the specified DN.
          https://ldapwiki.com/wiki/Active%20Directory%20User%20Related%20Searches#section-Active+Directory+User+Related+Searches-AllGroupsAUserIsAMemberOfIncludingNestedGroups
         */
        final String recursiveFilter = multipleGroupSupport ? ":1.2.840.113556.1.4.1941" : "";
        final String searchFilter = String.format("(member%s:=%s)", recursiveFilter, dn);
        final SearchControls sCtrl = new SearchControls();
        sCtrl.setSearchScope(SearchControls.SUBTREE_SCOPE);
        final Set<SecurityGroupDTO> matchedGroups = new HashSet<>();
        for (String ldapServer : ldapServers) {
            final Hashtable<String, String> props =
                            composeLDAPConnProps(ldapServer, fullUsername, userPassword);
            DirContext ctx = null;
            try {
                ctx = new InitialDirContext(props);
                final NamingEnumeration<SearchResult> answer =
                                ctx.search(adSearchBase_, searchFilter, sCtrl);

                while (answer.hasMoreElements()) {
                    final SearchResult sr = answer.next();
                    final String groupDn = sr.getNameInNamespace();
                    if (StringUtils.isBlank(groupDn)) {
                        continue;
                    }
                    final String ldapObjectName = new LdapName(groupDn).getRdns().stream()
                                    .filter(rdn -> CANONICAL_NAME.equalsIgnoreCase(rdn.getType()))
                                    // Stream#reduce call is important, because we want to get
                                    // last CN item from all existing. Last item should contain
                                    // canonical name of the LDAP object. Reduce is helping to find
                                    // the last record
                                    .map(Rdn::getValue).reduce((f, l) -> l).map(String::valueOf)
                                    .orElse(null);

                    synchronized (usersGroupsLock) {
                        final Collection<SecurityGroupDTO> foundGroups =
                                        findMatchedGroups(Collections.singleton(ldapObjectName));
                        if (!foundGroups.isEmpty()) {
                            if (multipleGroupSupport) {
                                matchedGroups.addAll(foundGroups);
                            } else {
                                ensureUser(userName);
                                return Collections.singletonList(foundGroups.iterator().next());
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

       return sortGroupWithLeastPrivilege(matchedGroups);
    }

    @Nonnull
    private String createFullUsername(@Nonnull String username) {
        if (username.contains("@")) {
            return username;
        }
        return String.format(FULL_USERNAME_FORMAT, username, domainName_);
    }

    @Nonnull
    private static String getPureUsername(@Nonnull String username) {
        return split(split(username, BACKSLASH_SPLITTER, 1), AT_SPLITTER, 0);
    }

    @Nonnull
    private static String split(@Nonnull String username, @Nonnull Pattern splitter, int item) {
        if (splitter.matcher(username).find()) {
            return splitter.split(username)[item];
        }
        return username;
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
        props.put(Context.INITIAL_CONTEXT_FACTORY, CTX_FACTORY);
        props.put(Context.PROVIDER_URL, server);
        if (secureLoginProvider_) {
            props.put(Context.SECURITY_PROTOCOL, "ssl");
        }
        props.put(Context.SECURITY_AUTHENTICATION, "simple");
        props.put(Context.SECURITY_PRINCIPAL, user);
        props.put(Context.SECURITY_CREDENTIALS, password);
        return props;
    }

    /**
     * Checks whether AD is available.
     *
     * @return {@code true} iff LDAP is available.
     */
    public boolean isADAvailable() {
        Collection<String> ldapServers = findLDAPServersInWindowsDomain();
        for (String srv : ldapServers) {
            Hashtable<String, String> props = new Hashtable<>();
            props.put(Context.INITIAL_CONTEXT_FACTORY, CTX_FACTORY);
            props.put(Context.PROVIDER_URL, srv);
            if (secureLoginProvider_) {
                props.put(Context.SECURITY_PROTOCOL, "ssl");
            }
            props.put(Context.SECURITY_AUTHENTICATION, "simple");
            Integer timeout = Integer.getInteger("ldapTimeout", 2000);
            props.put("com.sun.jndi.ldap.connect.timeout", timeout.toString());
            props.put("com.sun.jndi.ldap.read.timeout", timeout.toString());
            try {
                InitialDirContext ctx = new InitialDirContext(props);
                ctx.getAttributes("");
                return true;
            } catch (CommunicationException e) {
                logger.warn("The AD server " + srv + " is unreachable.");
            } catch (NamingException e) {
                return true;
            }
        }
        return false;
    }

    /**
     * Find the least privilege groups.
     *
     * @param userGroups external user groups.
     * @return sorted list of external groups in ascending orders.
     */
   private static List<SecurityGroupDTO> sortGroupWithLeastPrivilege(@Nonnull final Set<SecurityGroupDTO> userGroups) {
        return userGroups.stream()
                .sorted(Comparator.comparing(group -> LEAST_PRIVILEGE_MAP.get(group.getRoleName())))
                .collect(Collectors.toList());
    }

    /**
     * Assert user in multiple external groups.
     *
     * @param userName the user name to be asserted.
     * @param assignedGroups the assigned external group from SAML assertions
     * @return {@link SecurityGroupDTO} if matches found.
     */
    @Nonnull
    public Optional<SecurityGroupDTO> authorizeSAMLUserInGroups(@Nonnull final String userName,
            @Nonnull final Iterable<String> assignedGroups) {
        // find the matches groups
        synchronized (usersGroupsLock) {
            final Set<SecurityGroupDTO> matchedGroup = findMatchedGroups(assignedGroups);
            // this is the least privilege group
            final Optional<SecurityGroupDTO> leastPrivilegedGroup =
                            sortGroupWithLeastPrivilege(matchedGroup).stream()
                                            .findFirst();
            leastPrivilegedGroup.ifPresent(g -> ensureUser(userName));
            return leastPrivilegedGroup;
        }
    }

    // ensure user is in the ssoGroupUsers_ collection.
    @GuardedBy("usersGroupsLock")
    private boolean ensureUser(@Nonnull String userName) {
        final boolean newUser = !ssoGroupUsers_.contains(userName);
        if (newUser) {
            ssoGroupUsers_.add(userName);
        }
        return newUser;
    }

    @Nonnull
    @GuardedBy("usersGroupsLock")
    private Set<SecurityGroupDTO> findMatchedGroups(@Nonnull Iterable<String> assignedGroups) {
        // find the matches groups
        final Set<SecurityGroupDTO> matchedGroups = new HashSet<>();
        for (String assignedGroup : assignedGroups) {
            final SecurityGroupDTO group = ssoGroups_.get(assignedGroup);
            if (group != null) {
                matchedGroups.add(group);
            }
        }
        return matchedGroups;
    }
}
