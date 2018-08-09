package com.vmturbo.auth.api.auditing;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;


/**
 * Audit log utility class.
 */
@ThreadSafe
public final class AuditLogUtils {

    private static final String LOOPBACK = "127.0.0.1"; // assume it's IPv4 for now
    public static final String SYSTEM = "SYSTEM";
    private static final String RESULT_FAILURE = "result=Failure";
    private static final String RESULT_SUCCESS = "result=Success";
    private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final String LEFT_BRACKET = "(";
    public static final String RIGHT_BRACKET = ")";

    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger
            .getLogger(AuditLogUtils.class);
    private static final Logger auditLogger = LogManager.getLogger("com.vmturbo.platform.audit");

    private AuditLogUtils() {
    }

    /**
     * Creates the audit entry.
     *
     * @param entry audit log entry
     */
    public static void audit(@Nonnull final AuditLogEntry entry) {
        writeAuditToFile(
                entry.getAction().displayName(),
                entry.getActionInitiator(),
                entry.getTargetName(),
                entry.getDetailMessage(),
                entry.getRemoteClientIP(),
                entry.isSuccessful());
    }

    /**
     * Write audit data to audit.log file. Sample output:
     * 2017-07-18T16:26:38,155 [10.10.10.123] [administrator] TURBONOMICAUDIT: "2017-07-18 16:26:38”, “10.10.10.200”,
     * “Create User”, “newUser”, “result=Success”, “administrator created new user newUser.”
     *
     * @param action          the action name
     * @param actionInitiator the user or system that initialized the action.
     * @param targetName      the target object
     * @param message         detail message
     * @param remoteClientIP  remote client IP address
     * @param isSuccessful    is the action successful
     */
    private static void writeAuditToFile(String action, String actionInitiator, String targetName,
                                         String message, String remoteClientIP, boolean isSuccessful) {
        final Date date = new Date();

        String result = isSuccessful ? RESULT_SUCCESS : RESULT_FAILURE;
        if (actionInitiator == null) {
            actionInitiator = getLoggedInUserName().orElse(SYSTEM);
        }
        if (remoteClientIP == null) {
            remoteClientIP = getRemoteIpAddress();
            if (remoteClientIP == null && actionInitiator.equalsIgnoreCase(SYSTEM)) {
                remoteClientIP = getLocalIpAddress();
            }
        }
        // local IP address, action initiator, DateAndTime, remote IP, action, target object,
        // result, detail message
        auditLogger.info("[{}] [{}] TURBONOMICAUDIT: \"{}\", \"{}\", \"{}\", \"{}\"," +
                        " \"{}\", \"{}\"", getLocalIpAddress(), actionInitiator, dateFormat.format(date),
                remoteClientIP, action, targetName, result, message);
    }

    /**
     * Get logged in user name from gRPC context.
     *
     * @return username if user was logged in from UI
     */
    private static Optional<String> getLoggedInUserName() {
        return Optional.ofNullable(SecurityConstant.USER_ID_CTX_KEY.get());
    }


    /**
     * Get remote IP address.
     *
     * @return remote IP address if available otherwise fall back to "lookback".
     */
    private static String getRemoteIpAddress() {
        return SecurityConstant.USER_IP_ADDRESS_KEY.get();
    }

    /**
     * Get local IP address.
     *
     * @return local IP address if available otherwise fall back to "lookback".
     */
    private static String getLocalIpAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return LOOPBACK;
        }
    }


    /**
     * Get user name and UUID from gRPC security context, and combine them.
     * E.g.: administrator(2173090650448).
     * Note: only UUID is unique to user, so it will support changing user name scenario.
     * @return Combined user name and UUID
     */
    public static String getUserNameAndUuidFromGrpcSecurityContext() {
        final String userName = SecurityConstant.USER_ID_CTX_KEY.get();
        final String userUuid = SecurityConstant.USER_UUID_KEY.get();
        // Automated actions will have SYSTEM as user.
        // TODO: when all component gRPC requests have component name and uuid, throw security exception
        // instead of returning "SYSTEM".
        return (userName != null && userUuid != null) ?
                userName + LEFT_BRACKET + userUuid + RIGHT_BRACKET : SYSTEM;
    }
}
