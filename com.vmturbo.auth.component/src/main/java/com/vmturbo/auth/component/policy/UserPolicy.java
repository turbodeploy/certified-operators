package com.vmturbo.auth.component.policy;

import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.REPORT_EDITOR;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.auditing.AuditAction;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.auth.component.store.AuthProviderBase.UserInfo;
import com.vmturbo.components.api.FormattedString;

/**
 * System wide user policy.
 */
public class UserPolicy {
    private final LoginPolicy loginPolicy;

    private final Logger logger = LogManager.getLogger();
    private final ReportPolicy reportPolicy;

    /**
     * Constructor.
     *
     * @param loginPolicy login policy.
     * @param reportPolicy report policy.
     */
    public UserPolicy(@Nonnull final LoginPolicy loginPolicy, @Nonnull final ReportPolicy reportPolicy) {
        this.loginPolicy = Objects.requireNonNull(loginPolicy);
        this.reportPolicy = Objects.requireNonNull(reportPolicy);
        logger.info("System is using {} login policy.", loginPolicy);
        logger.info("System has maximum allowed report editor {}.", reportPolicy.getAllowedMaximumEditor());
    }

    /**
     * Get allowed maximum number of editors.
     *
     * @return true if it's allowed.
     */
    public int getAllowedMaximumEditor() {
        return reportPolicy.getAllowedMaximumEditor();
    }

    /**
     * Is adding new user with report editor role allowed?
     *
     * @param userToCheck user to be add report editor role.
     * @param allUsers    all existing users.
     * @throws IllegalArgumentException If not allowed.
     */
    public void isAddingReportEditorRoleAllowed(@Nonnull final AuthUserDTO userToCheck,
            @Nonnull final List<AuthUserDTO> allUsers) {

        if (roleMatched(userToCheck, REPORT_EDITOR)) {
            if (!CollectionUtils.isEmpty(userToCheck.getScopeGroups())) {
                throw new IllegalArgumentException(FormattedString.format(
                    "Scoped user {} cannot be designated as Report Editor.", userToCheck.getUser()));
            }
            long userCount = allUsers.stream()
                    .filter(user -> user.getProvider().equals(userToCheck.getProvider())
                            && roleMatched(user, REPORT_EDITOR))
                    .count();
            if (userCount >= getAllowedMaximumEditor()) {
                throw new IllegalArgumentException(String.format(
                        "Reporting policy forbid assigning more Report Editor role to user: %s. The maximum allowed user is: %s.",
                        userToCheck.getUser(), getAllowedMaximumEditor()));
            }
        }
    }

    private boolean roleMatched(@Nonnull AuthUserDTO userInfo, @Nonnull String roleName) {
        return userInfo.getRoles().stream().anyMatch(role -> role.equalsIgnoreCase(roleName));
    }

    /**
     * Is local user login allowed?
     *
     * @return true if it's allowed.
     */
    public boolean isLocalUserLoginAllowed() {
        return loginPolicy == LoginPolicy.ALL || loginPolicy == LoginPolicy.LOCAL_ONLY;
    }

    /**
     * Is AD user login allowed?
     *
     * @return true if it's allowed.
     */
    public boolean isADUserLoginAllowed() {
        return loginPolicy == LoginPolicy.ALL || loginPolicy == LoginPolicy.AD_ONLY;
    }

    /**
     * Is SAML user login allowed?
     *
     * @return true if it's allowed.
     */
    public boolean isSAMLAllowed() {
        return loginPolicy == LoginPolicy.ALL || loginPolicy == LoginPolicy.SAML_ONLY;
    }

    /**
     * Special policy for AD_ONLY mode. Get allowed user to login in recovery mode. If in AD_ONLY
     * mode, and AD is not available, return the local admin user which is allowed to login.
     *
     * @param isAdAvailable is AD available
     * @param userSupplier  function to retrieve user, it's function because we want to only
     *                      retrieve user when preconditions are met.
     * @return {@link UserInfo} user that is allowed to login
     */
    public Optional<UserInfo> getAllowedUserToLoginInRecoveryMode(boolean isAdAvailable,
            @Nonnull Supplier<Optional<UserInfo>> userSupplier) {
        if (loginPolicy == LoginPolicy.AD_ONLY && !isAdAvailable) {
            final Optional<UserInfo> userInfo = userSupplier.get();
            if (userInfo.map(u -> u.isAdminUser()).orElse(false)) {
                return userInfo;
            }
        }
        return Optional.empty();
    }

    /**
     * Get audit actions.
     *
     * @return {@link AuditAction}
     */
    public AuditAction getAuditAction() {
        if (loginPolicy == LoginPolicy.ALL) {
            return AuditAction.SET_DEFAULT_AUTH;
        } else if (loginPolicy == LoginPolicy.LOCAL_ONLY) {
            return AuditAction.SET_LOCAL_ONLY_AUTH;
        } else if (loginPolicy == LoginPolicy.AD_ONLY) {
            return AuditAction.SET_AD_ONLY_AUTH;
        } else if (loginPolicy == LoginPolicy.SAML_ONLY) {
            return AuditAction.SET_SAML_AUTH;
        }
        throw new SecurityException("Found not supported login policy: " + loginPolicy.name());
    }

    /**
     * Supported login policy.
     */
    public enum LoginPolicy {
        /**
         * Allow all types of login.
         */
        ALL,
        /**
         * Only allow local user login, currently not enforced.
         */
        LOCAL_ONLY,
        /**
         * Only allow AD user login, currently enforced.
         */
        AD_ONLY,
        /**
         * Only allow SAML user login, currently not enforced.
         */
        SAML_ONLY
    }
}
