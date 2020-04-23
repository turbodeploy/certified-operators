package com.vmturbo.api.component.communication;

import static java.util.Collections.singletonList;

import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensaml.core.xml.XMLObject;
import org.opensaml.core.xml.schema.XSAny;
import org.opensaml.core.xml.schema.XSString;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.saml2.provider.service.authentication.OpenSamlAuthenticationProvider;
import org.springframework.security.saml2.provider.service.authentication.Saml2Authentication;
import org.springframework.security.saml2.provider.service.authentication.Saml2AuthenticationToken;
import org.springframework.web.client.RestTemplate;

import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationVerifier;
import com.vmturbo.auth.api.authorization.kvstore.IComponentJwtStore;

/**
 * Custom Spring authorization provider to authorize SAML request.
 */
public class SamlAuthenticationProvider extends RestAuthenticationProvider {

    /**
     * The logger.
     */
    private static final Logger logger =
            LogManager.getLogger(SamlAuthenticationProvider.class);
    private static final Pattern PATTERN = Pattern.compile("^http", Pattern.CASE_INSENSITIVE);
    private final IComponentJwtStore componentJwtStore;
    private final OpenSamlAuthenticationProvider authProvider;
    @Autowired
    private HttpServletRequest request;

    /**
     * Construct the authentication provider.
     *
     * @param authHost          The authentication host.
     * @param authPort          The authentication port.
     * @param authRoute         The authorize path.
     * @param restTemplate      The REST endpoint.
     * @param verifier          The verifier.
     * @param componentJwtStore The component JWT store.
     * @param externalGroupTag  The asserted group name.
     */
    public SamlAuthenticationProvider(final @Nonnull String authHost, final int authPort,
            final @Nonnull String authRoute, final @Nonnull RestTemplate restTemplate,
            final @Nonnull JWTAuthorizationVerifier verifier,
            final @Nonnull IComponentJwtStore componentJwtStore,
            final @Nonnull String externalGroupTag) {
        super(authHost, authPort, authRoute, restTemplate, verifier);
        this.componentJwtStore = Objects.requireNonNull(componentJwtStore);
        authProvider = new OpenSamlAuthenticationProvider();
        authProvider.setResponseTimeValidationSkew(Duration.ofMinutes(10L));
        // extractor group attribute from assertion's AttributeStatement, e.g.:
        //      <saml:AttributeStatement>
        //         <saml:Attribute Name="group" NameFormat="urn:oasis:names:tc:SAML:2.0:attrname-format:basic">
        //            <saml:AttributeValue xsi:type="xs:string">turbo_admin_group</saml:AttributeValue>
        //         </saml:Attribute>
        //      </saml:AttributeStatement>
        authProvider.setAuthoritiesExtractor(assertion -> assertion.getAttributeStatements()
                .stream()
                .flatMap(statement -> statement.getAttributes().stream())
                .filter(attribute -> externalGroupTag.equalsIgnoreCase(attribute.getName()))
                .flatMap(attribute -> attribute.getAttributeValues().stream())
                .findFirst()
                .flatMap(xmlObject -> getAttributeValue(xmlObject))
                .map(groupName -> singletonList(new SimpleGrantedAuthority(groupName)))
                .orElse(Collections.EMPTY_LIST));
    }

    private static Optional<String> getAttributeValue(final @Nonnull XMLObject object) {
        if (object instanceof XSString) {
            return Optional.ofNullable(((XSString)object).getValue());
        } else if (object instanceof XSAny) {
            return Optional.ofNullable(((XSAny)object).getTextContent());
        } else {
            logger.error("Failed to get the SAML group value: {}", object.toString());
            return Optional.empty();
        }
    }

    /**
     * Performs authentication with the request object passed in. It will grant an authority to
     * authentication object. The authority is currently the role of user and will be used to verify
     * if a resource can be accessed by a user.
     *
     * @param authentication the authentication request object.
     * @return a fully authenticated object including credentials and authorities. May return
     * <code>null</code> if the <code>AuthenticationProvider</code> is unable to support
     * authentication of the passed <code>Authentication</code> object. In such a case, the next
     * <code>AuthenticationProvider</code> that supports the presented
     * <code>Authentication</code> class will be tried.
     * @throws AuthenticationException if authentication fails.
     */
    @Override
    @Nonnull
    public Authentication authenticate(Authentication authentication)
            throws AuthenticationException {
        if (authentication instanceof Saml2AuthenticationToken) {

            Saml2AuthenticationToken authenticationToken =
                    convert((Saml2AuthenticationToken)authentication);
            Saml2Authentication saml2Authentication =
                    (Saml2Authentication)authProvider.authenticate(authenticationToken);

            final String username = saml2Authentication.getName();
            // only support one external group.
            final Optional<String> groupOpt = saml2Authentication.getAuthorities()
                    .stream()
                    .map(authority -> authority.getAuthority())
                    .findFirst();

            final Authentication authorizationToken1 =
                    SecurityContextHolder.getContext().getAuthentication();
            String remoteIpAddress = "unknown";
            if (authorizationToken1 instanceof CustomSamlAuthenticationToken) {
                remoteIpAddress =
                        ((CustomSamlAuthenticationToken)authorizationToken1).getRemoteIpAddress();
                logger.trace("remote IP address is:  " + remoteIpAddress);
                SecurityContextHolder.getContext().setAuthentication(null);
            }

            return super.authorize(username, groupOpt, remoteIpAddress, componentJwtStore);
        } else if (authentication instanceof CustomSamlAuthenticationToken) {
            logger.trace("Request is CustomSamlAuthenticationToken, cleaning it up.");
            SecurityContextHolder.getContext().setAuthentication(null);
        }

        return authentication;
    }

    // Request to API component is from Nginx using HTTP, but the assertion is HTTPS from IDP.
    // to pass the assertion validation, we need to manually convert it to HTTPS.
    private Saml2AuthenticationToken convert(Saml2AuthenticationToken token) {

        return new Saml2AuthenticationToken(token.getSaml2Response(),
                token.getRecipientUri().replaceFirst(PATTERN.pattern(), "https"),
                token.getIdpEntityId(), token.getLocalSpEntityId(), token.getX509Credentials());
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return Saml2AuthenticationToken.class.isAssignableFrom(authentication) ||
                CustomSamlAuthenticationToken.class.isAssignableFrom(authentication);
    }
}
