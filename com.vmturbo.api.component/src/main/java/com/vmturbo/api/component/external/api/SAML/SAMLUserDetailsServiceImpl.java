package com.vmturbo.api.component.external.api.SAML;


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensaml.saml2.core.Attribute;
import org.opensaml.xml.XMLObject;
import org.opensaml.xml.schema.XSAny;
import org.opensaml.xml.schema.XSString;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.saml.SAMLCredential;
import org.springframework.security.saml.userdetails.SAMLUserDetailsService;

import com.vmturbo.api.component.external.api.service.ServiceConfig;
import com.vmturbo.auth.api.auditing.AuditAction;
import com.vmturbo.auth.api.auditing.AuditLogEntry;
import com.vmturbo.auth.api.auditing.AuditLogUtils;
import com.vmturbo.auth.api.authentication.credentials.SAMLUser;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;


/**
 * Implementation of SAMLUserDetailsService that creates users with mapping roles.
 */
public class SAMLUserDetailsServiceImpl implements SAMLUserDetailsService {
    private static Logger logger = LogManager.getLogger();
    public static final String SYSTEM = "SYSTEM";
    private static final String DUMMY_PASSWORD = "DUMMY_PASSWORD";
    private static final String GROUP = "group";
    private static final String LOGIN_SUCCESSFUL_FOR_SAML_GROUP = "Login successful for SAML group: ";
    private static final String SAML_USER = ", SAML user: ";
    private static final String LOGIN_SUCCESSFUL_FOR_SAML_USER = "Login successful for SAML user: ";
    private static final String SAML_ERROR_MSG = "Failed to find local group or account for the SAML user: ";
    @Autowired
    private ServiceConfig serviceConfig;

    /**
     * The method is supposed to identify local account of user referenced by data in the SAML assertion
     * and return UserDetails object describing the user. In case the user has no local account,
     * we will reject the user login and throw UsernameNotFoundException.
     * <p>
     * Returned object should correctly implement the getAuthorities method as it will be used to populate
     * entitlements inside the Authentication object.
     *
     * @param credential data populated from SAML message used to validate the user
     * @return a fully populated user record (never <code>null</code>)
     * @throws UsernameNotFoundException if the user details object can't be populated
     */
    @Override
    public Object loadUserBySAML(SAMLCredential credential) throws UsernameNotFoundException {
        final String username = credential.getNameID().getValue();
        final Collection<GrantedAuthority> grantedAuths = new ArrayList<>();
        final StringBuilder auditDetails = new StringBuilder();
        String remoteClientIP = credential.getAdditionalData() == null ? SYSTEM : credential.getAdditionalData().toString();

        Optional<AuthUserDTO> user = Optional.ofNullable(
                credential.getAttributes()
                        .stream()
                        .filter(attribute -> GROUP.equalsIgnoreCase(attribute.getName()))
                        .findFirst()
                        .flatMap(attribute -> {
                            String groupName = getXmlObject(attribute);
                            Optional<AuthUserDTO> authUserDTO = serviceConfig
                                    .authenticationService()
                                    .authorize(username, Optional.of(groupName), remoteClientIP);

                            // add audit details
                            authUserDTO.ifPresent(groupUser -> auditDetails.append(LOGIN_SUCCESSFUL_FOR_SAML_GROUP + groupName
                                    + SAML_USER + username));
                            return authUserDTO;
                        })
                        .orElseGet(
                                () -> {
                                    Optional<AuthUserDTO> authUserDTO = serviceConfig
                                            .authenticationService()
                                            .authorize(username, Optional.empty(), remoteClientIP);
                                    // add audit details
                                    authUserDTO.ifPresent(individualUser ->
                                            auditDetails.append(LOGIN_SUCCESSFUL_FOR_SAML_USER + username));
                                    return authUserDTO.orElseThrow(() ->
                                    {
                                        AuditLogEntry entry = new AuditLogEntry.Builder(AuditAction.LOGIN,
                                                SAML_ERROR_MSG, false)
                                                .remoteClientIP(remoteClientIP)
                                                .targetName(username)
                                                .build();
                                        AuditLogUtils.audit(entry);
                                        return new UsernameNotFoundException(SAML_ERROR_MSG + username);
                                    });
                                }
                        ));

        user.ifPresent(u ->
                grantedAuths.add(new SimpleGrantedAuthority("ROLE_" + u.getRoles().get(0).toUpperCase())));
        AuditLogEntry entry = new AuditLogEntry.Builder(AuditAction.LOGIN, auditDetails.toString(), true)
                .remoteClientIP(remoteClientIP)
                .targetName(username)
                .build();
        AuditLogUtils.audit(entry);
        return new SAMLUser(username, DUMMY_PASSWORD, grantedAuths, user.get(), remoteClientIP);
    }

    /**
     * Return attribute value from SAML XML response.
     *
     * @param attr SAML attribute
     * @return attribute value
     */
    @VisibleForTesting
    static String getXmlObject(@Nonnull final Attribute attr) {
        Objects.requireNonNull(attr);
        final List<XMLObject> value = attr.getAttributeValues();
        return value.stream()
                .findFirst()
                .flatMap(obj -> getAttributeValue(obj))
                .orElse("");
     }

    private static Optional<String>  getAttributeValue(final @Nonnull XMLObject object) {
        if (object instanceof XSString) {
            return Optional.ofNullable(((XSString) object).getValue());
        } else if (object instanceof XSAny) {
            return Optional.ofNullable(((XSAny) object).getTextContent());
        } else {
            logger.error("Failed to get the SAML group value: {}", object.toString());
            return Optional.empty();
        }
    }
}
