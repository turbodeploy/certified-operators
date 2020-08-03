package com.vmturbo.clustermgr.api;

import java.io.Serializable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import org.apache.commons.lang3.StringUtils;

/**
 * HTTP proxy configuration.
 */
@ApiModel(
        description = "Model to describe http proxy settings, like username, password, port number and host")
@JsonInclude(Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class HttpProxyConfig implements Serializable {

    private static final long serialVersionUID = -3507162278537462018L;
    private static final String HIDDEN_PASSWORD = "*****";

    @ApiModelProperty(value = "whether proxy is enabled or not, required", required = true)
    private final boolean isProxyEnabled;

    @ApiModelProperty(value = "proxy host", required = false)
    private final String proxyHost;

    @ApiModelProperty(value = "proxy host port number", required = false)
    private final Integer proxyPortNumber;

    @ApiModelProperty(value = "proxy username")
    private final String userName;

    @ApiModelProperty(value = "proxy password")
    private final String password;

    /**
     * Constructs proxy configuration.
     * @param isProxyEnabled whether proxy server is configured
     * @param proxyHost proxy server host
     * @param proxyPort proxy server port
     * @param username proxy server user name
     * @param password proxy server user password
     */
    @JsonCreator
    public HttpProxyConfig(@JsonProperty("isProxyEnabled") boolean isProxyEnabled,
                           @JsonProperty("proxyHost") @Nullable String proxyHost,
                           @JsonProperty("proxyPort") @Nullable Integer proxyPort,
                           @JsonProperty("username") @Nullable String username,
                           @JsonProperty("password") @Nullable String password) {
        this.isProxyEnabled = isProxyEnabled;
        this.proxyHost = proxyHost;
        this.proxyPortNumber = proxyPort;
        this.userName = username;
        this.password = password;
    }

    public String getPassword() {
        return password;
    }

    public String getUserName() {
        return userName;
    }

    public Integer getProxyPortNumber() {
        return proxyPortNumber;
    }

    public String getProxyHost() {
        return proxyHost;
    }

    public boolean getIsProxyEnabled() {
        return isProxyEnabled;
    }

    /**
     * Returns tuple containing proxy server host and port number.
     *
     * @return tuple
     */
    @JsonIgnore
    @Nonnull
    public String getProxyHostAndPortTuple() {
        if (StringUtils.isBlank(proxyHost)) {
            return "";
        }
        return Joiner.on(":").skipNulls().join(StringUtils.trimToNull(proxyHost), proxyPortNumber);
    }

    /**
     * Returns tuple containing proxy server user name and password number.
     *
     * @return tuple
     */
    @JsonIgnore
    @Nonnull
    public String getUserNameAndPasswordTuple() {
        if (StringUtils.isBlank(userName) || StringUtils.isBlank(password)) {
            return "";
        }
        return Joiner.on(":")
                .skipNulls()
                .join(StringUtils.trimToNull(userName), StringUtils.trimToNull(password));
    }

    /**
     * Returns tuple containing proxy server user name and password masked with asterisks.
     *
     * @return tuple
     */
    @JsonIgnore
    @Nonnull
    public String getUserNameAndHiddenPasswordTuple() {
        if (StringUtils.isBlank(userName) || StringUtils.isBlank(password)) {
            return "";
        }
        return Joiner.on(":").skipNulls().join(StringUtils.trimToNull(userName), HIDDEN_PASSWORD);
    }
}
