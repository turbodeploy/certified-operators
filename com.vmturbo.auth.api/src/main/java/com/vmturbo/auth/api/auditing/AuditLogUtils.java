package com.vmturbo.auth.api.auditing;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.cloudbees.syslog.Facility;
import com.cloudbees.syslog.MessageFormat;
import com.cloudbees.syslog.Severity;
import com.cloudbees.syslog.sender.TcpSyslogMessageSender;
import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.authentication.credentials.SAMLUserUtils;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.components.common.utils.EnvironmentUtils;

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
    private static final String REMOTE_IP_ADDRESS = "RemoteIpAddress";
    private static final int stringLength = "RemoteIpAddress: ".length();
    private static final long maxConnectRetryCount = 10;
    private static final int low = 1;
    private static final int high = 1000;
    private static final Random random = new Random();
    private static TcpSyslogMessageSender messageSender = new TcpSyslogMessageSender();

    // thread pool for auditing, so auditing will not block main operation
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    static {
        messageSender.setDefaultAppName("");
        messageSender.setDefaultFacility(Facility.AUTHPRIV);
        messageSender.setDefaultSeverity(Severity.INFORMATIONAL);
        // TODO OM-47212 Externalize remote audit rsyslog server hostname
        messageSender.setSyslogServerHostname(EnvironmentUtils.getOptionalEnvProperty("rsyslog_host").orElse("rsyslog"));
        messageSender.setSyslogServerPort(2514);
        messageSender.setMessageFormat(MessageFormat.RFC_5424);
        messageSender.setSsl(false);
    }

    private static final Logger logger = LogManager.getLogger();

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
        final String result = isSuccessful ? RESULT_SUCCESS : RESULT_FAILURE;
        final String initiator = (actionInitiator != null) ? actionInitiator : getLoggedInUserName();

        final String remoteIP = (remoteClientIP != null) ? remoteClientIP : (getRemoteIpAddress() != null) ?
            getRemoteIpAddress() : (actionInitiator != null && actionInitiator.equalsIgnoreCase(SYSTEM)) ?
            getLocalIpAddress() : "";

        scheduler.schedule(() -> {
            // local IP address, action initiator, DateAndTime, remote IP, action, target object,
            // result, detail message
            sendToRsyslog(String.format("[%s] [%s] TURBONOMICAUDIT: \"%s\", \"%s\", \"%s\", \"%s\"," +
                    " \"%s\", \"%s\"", getLocalIpAddress(), initiator, dateFormat.format(date),
                remoteIP, action, targetName, result, message), messageSender, maxConnectRetryCount);
        }, 0, TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    static void sendToRsyslog(@Nonnull final String auditMessage,
                              @Nonnull final TcpSyslogMessageSender messageSender,
                              final long maxRetryCount) {
        int count = 0;
        while (true) {
            try {
                messageSender.sendMessage(auditMessage);
                return;
            } catch (IOException | RuntimeException e) {
                if (count++ >= maxRetryCount) {
                    logger.error("Cannot send audit message with " + maxRetryCount + " retries");
                    // Since already log the error and audit message, there is not much we can do.
                    return;
                }

                logger.error("Failed to send out audit message:{}", e.getMessage());
                logger.info("Waiting for rsyslog to response. Retry #" + count);
                try {
                    // Randomized exponential backoff, maximun wait time is
                    // 2^maxConnectRetryCount(10) = 1024 seconds
                    Thread.sleep(((int) Math.round(Math.pow(2, count)) * 1000)
                        + (random.nextInt(high - low) + low));
                } catch (InterruptedException e1) {
                    throw new RuntimeException(
                        "Exception while retrying connection to repository", e1);
                }
            }
        }
    }

    /**
     * Get logged in user name from gRPC context.
     *
     * @return username if user was logged in from UI
     */
    private static String getLoggedInUserName() {
        return Optional.ofNullable(SecurityConstant.USER_ID_CTX_KEY.get())
            .orElseGet(() -> SAMLUserUtils.getAuthUserDTO().map(dto -> dto.getUser())
                .orElse(SYSTEM));
    }


    /**
     * Get remote IP address.
     *
     * @return remote IP address if available otherwise fall back to "lookback".
     */
    private static String getRemoteIpAddress() {
        return Optional.ofNullable((SecurityConstant.USER_IP_ADDRESS_KEY.get()))
            .orElseGet(() -> SAMLUserUtils.getAuthUserDTO().map(dto -> dto.getIpAddress())
            .orElse(null));
    }

    /**
     * Get local IP address.
     *
     * @return local IP address if available otherwise fall back to "lookback".
     */
    public static String getLocalIpAddress() {
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
