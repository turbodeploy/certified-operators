/*
 * Copyright 2002-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.vmturbo.api.component.external.api;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.minidev.json.JSONArray;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.core.oidc.user.DefaultOidcUser;
import org.springframework.security.web.access.ExceptionTranslationFilter;
import org.springframework.security.web.authentication.SimpleUrlAuthenticationSuccessHandler;
import org.springframework.security.web.authentication.WebAuthenticationDetails;
import org.springframework.security.web.savedrequest.HttpSessionRequestCache;
import org.springframework.security.web.savedrequest.RequestCache;
import org.springframework.security.web.savedrequest.SavedRequest;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;

import com.vmturbo.api.component.communication.RestAuthenticationProvider;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationVerifier;
import com.vmturbo.auth.api.authorization.kvstore.ComponentJwtStore;
import com.vmturbo.auth.api.authorization.kvstore.IComponentJwtStore;

/**
 * An authentication success strategy which can make use of the {@link
 * org.springframework.security.web.savedrequest.DefaultSavedRequest} which may have been stored in
 * the session by the {@link ExceptionTranslationFilter}. When such a request is intercepted and
 * requires authentication, the request data is stored to record the original destination before the
 * authentication process commenced, and to allow the request to be reconstructed when a redirect to
 * the same URL occurs. This class is responsible for performing the redirect to the original URL if
 * appropriate.
 *
 * <p>Following a successful authentication, it decides on the redirect destination, based on the
 * following scenarios:
 * <ul>
 * <li>
 * If the {@code alwaysUseDefaultTargetUrl} property is set to true, the
 * {@code defaultTargetUrl} will be used for the destination. Any
 * {@code DefaultSavedRequest} stored in the session will be removed.</li>
 * <li>
 * If the {@code targetUrlParameter} has been set on the request, the value will be used
 * as the destination. Any {@code DefaultSavedRequest} will again be removed.</li>
 * <li>
 * If a {@link SavedRequest} is found in the {@code RequestCache} (as set by the
 * {@link ExceptionTranslationFilter} to record the original destination before the
 * authentication process commenced), a redirect will be performed to the Url of that
 * original destination. The {@code SavedRequest} object will remain cached and be picked
 * up when the redirected request is received (See
 * <a href="{@docRoot}/org/springframework/security/web/savedrequest/SavedRequestAwareWrapper.html">SavedRequestAwareWrapper</a>).
 * </li>
 * <li>
 * If no {@link SavedRequest} is found, it will delegate to the base class.</li>
 * </ul>
 *
 * @author Luke Taylor
 * @since 3.0
 */

public class CustomRequestAwareAuthenticationSuccessHandler extends
        SimpleUrlAuthenticationSuccessHandler {
    protected final Log logger = LogFactory.getLog(this.getClass());

    private RequestCache requestCache = new HttpSessionRequestCache();
    private final IComponentJwtStore componentJwtStore;
    private RestAuthenticationProvider authProvider;

    public CustomRequestAwareAuthenticationSuccessHandler(String authHost, Integer authPort, String authRoute, RestTemplate restTemplate,
            JWTAuthorizationVerifier verifier, ComponentJwtStore componentJwtStore) {
         authProvider = new RestAuthenticationProvider(
                authHost,
                authPort,
                authRoute,
                restTemplate,
                verifier);
        this.componentJwtStore = Objects.requireNonNull(componentJwtStore);
    }

    @Override
    public void onAuthenticationSuccess(HttpServletRequest request, HttpServletResponse response,
            Authentication authentication) throws ServletException, IOException {
        setupSecurity(authentication);
        SavedRequest savedRequest = requestCache.getRequest(request, response);

        if (savedRequest == null) {
            super.onAuthenticationSuccess(request, response, authentication);

            return;
        }
        String targetUrlParameter = getTargetUrlParameter();
        if (isAlwaysUseDefaultTargetUrl() || (targetUrlParameter != null
            && StringUtils.hasText(request.getParameter(targetUrlParameter)))) {
            requestCache.removeRequest(request, response);
            super.onAuthenticationSuccess(request, response, authentication);

            return;
        }

        clearAuthenticationAttributes(request);

        // Use the DefaultSavedRequest URL
        String targetUrl = savedRequest.getRedirectUrl();
        logger.debug("Redirecting to DefaultSavedRequest Url: " + targetUrl);

        getRedirectStrategy().sendRedirect(request, response, targetUrl);
    }

    // TODO: assign user scopes
    private void setupSecurity(Authentication authentication) {
        // By default user the name of the user with an empty group membership
        String email = authentication.getName();
        String remoteIpAddress = "UNKNOWN_IP";
        if (authentication.getDetails() instanceof WebAuthenticationDetails) {
            remoteIpAddress = ((WebAuthenticationDetails)authentication.getDetails()).getRemoteAddress();
        }
        logger.debug("OpenID user name: " + email);
        Optional<String> group = Optional.empty();
        if (authentication.getPrincipal() instanceof DefaultOidcUser) {
            DefaultOidcUser user = (DefaultOidcUser)authentication.getPrincipal();
            // Use the user's email address if returned
            if (user.getAttributes().get("email") instanceof String) {
                email = (String)user.getAttributes().get("email");
                logger.debug("OpenID user email: " + email);
            }
            // Use the first group of the user if returned
            if (user.getAttributes().get("groups") instanceof JSONArray) {
                JSONArray groups = (JSONArray)user.getAttributes().get("groups");
                logger.debug("OpenID user groups: " + groups.toString());
                if (groups.size() > 0) {
                    group = Optional.of(groups.get(0).toString());
                }
            }
        }
        Authentication resultAuthentication = authProvider
            .authorize(email, group, remoteIpAddress, componentJwtStore);

    SecurityContextHolder.getContext()
                .setAuthentication(resultAuthentication);
    }

    public void setRequestCache(RequestCache requestCache) {
        this.requestCache = requestCache;
    }
}
