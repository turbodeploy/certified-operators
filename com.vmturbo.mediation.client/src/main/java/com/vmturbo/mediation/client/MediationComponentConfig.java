package com.vmturbo.mediation.client;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

import javax.annotation.PostConstruct;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.base.MoreObjects;

import com.vmturbo.communication.ConnectionConfig;

/**
 * Mediation worker configuration (except probe-specific parts).
 */
@Component
public class MediationComponentConfig implements ConnectionConfig {

    private Logger log = LogManager.getLogger();


    // configuration value to reflect the Validator IP address - TODO: use service discovery instead of configuration
    @Value("${serverAddress:ws://topology-processor:8080/remoteMediation}")
    private String serverAddress;

    // ssl connection values - by default, the empty string -> no SSL connection
    @Value("${sslKeystorePath:''}")
    private String sslKeystorePath;
    @Value("${sslKeystorePassword:''}")
    private String sslKeystorePassword;

    // configuration value to use for authentication when creating the connection to the Validator
    @Value("${userName:vmtRemoteMediation}")
    private String userName;
    @Value("${userPassword:vmtRemoteMediation}")
    private String userPassword;

    // configuration value to control the number of retries that are handled silently, i.e. not logged.
    @Value("${silentRetryTime:0}")
    private long silentRetryTime;

    // configuration value to control the time interval to wait in between websocket connection attempts.
    @Value("${connRetryIntervalSeconds:10}")
    private long connRetryInterval;

    @Value("${websocket.pong.timeout:30000}")
    private long pongMessageTimeout;

    @Value("${websocket.send.atomic.timeout:30}")
    private long websocketAtomicSendTimeout;

    // just for debugging - for now
    @Value("${instance_id}")
    private String instance_id;

    @Override
    public URI getServerAddress() {
        try {
            return new URI(serverAddress);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * The time interval to wait in between websocket connection attempts.
     *
     * @return time interval in seconds.
     */
    @Override
    public long getConnRetryIntervalSeconds() {
        return connRetryInterval;
    }

    @Override
    public File getSSLKeystoreFile() {
        return new File(sslKeystorePath);
    }

    @Override
    public String getSSLKeystorePassword() {
        return sslKeystorePassword;
    }

    @Override
    public long getSilentRetriesTime() {
        return silentRetryTime;
    }

    @Override
    public String getUserName() {
        return userName;
    }

    @Override
    public String getUserPassword() {
        return userPassword;
    }

    @Override
    public long getPongMessageTimeout() {
        return pongMessageTimeout;
    }

    @Override
    public long getAtomicSendTimeoutSec() {
        return websocketAtomicSendTimeout;
    }

    @PostConstruct
    private void startup() {
        log.debug("======================================== Mediation Component Configuration");
        log.debug(toString());
        log.debug("========================================");
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("instance_id", instance_id)
                .add("serverAddress", serverAddress)
                .add("sslKeystorePath", sslKeystorePath)
                .add("sslKeystorePassword", sslKeystorePassword)
                .add("userName", userName)
                .add("userPassword", userPassword == null ? "null" : "xxxx")
                .add("silentRetryTime", silentRetryTime)
                .toString();
    }

}
