package com.vmturbo.testbases.flyway;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import com.google.common.net.MediaType;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class provides access to a GitLab instance via its rest api.
 *
 * <p>We currently use this solely for getting a list of tags during automated builds, since
 * those builds do not fetch tags into the local repo.</p>
 */
public class GitlabApi {
    private static Logger logger = LogManager.getLogger();

    /** Default gitlab host for use in turbonomic builds. */
    public static final String TURBO_GITLAB_HOST = "git.turbonomic.com";
    /** Default gitlab project name = XL project. */
    public static final String XL_PROJECT_PATH = "turbonomic/xl";
    /** Environment variable that can be used to pass Gitlab private token. */
    public static final String API_TOKEN_ENV_NAME = "GIT_TOKEN";

    static final String API_TOKEN_HEADER_NAME = "Private-Token";
    static final String JSON_MEDIA_TYPE = MediaType.JSON_UTF_8.toString();
    static final ImmutableList<String> API_PATH_SEGMENTS = ImmutableList.of("api", "v4");
    static final ImmutableList<String> PROJECTS_RESOURCE_PATH_SEGMENTS =
            ImmutableList.of("projects");
    static final ImmutableList<String> TAGS_RESOURCE_PATH_SEGMENTS =
            ImmutableList.of("repository","tags");

    private static ObjectMapper objectMapper = new ObjectMapper();
    private final Supplier<CloseableHttpClient> httpClientSupplier;

    static final ResponseHandler<JsonNode> jsonResponseHandler = response -> {
        int status = response.getStatusLine().getStatusCode();
        if (status >= 200 && status < 300) {
            HttpEntity entity = response.getEntity();
            return entity != null ? objectMapper.readTree(EntityUtils.toString(entity))
                    : JsonNodeFactory.instance.arrayNode();
        } else {
            throw new ClientProtocolException(
                    "Failed to perform Gitlab API request: " + response.getStatusLine());
        }
    };

    private final String gitlabHost;
    private final String fullProjectPath;

    /**
     * Create a new instance.
     *
     * @param gitlabHost      host name or IP address of gitlab service
     * @param fullProjectPath full path name of project (not url-encoded)
     * @param httpClientSupplier a supplier to obtain a HTTP client
     */
    public GitlabApi(String gitlabHost, String fullProjectPath,
            @Nonnull Supplier<CloseableHttpClient> httpClientSupplier) {
        this.gitlabHost = gitlabHost;
        this.fullProjectPath = fullProjectPath;
        this.httpClientSupplier = Objects.requireNonNull(httpClientSupplier);
    }

    /**
     * Retrieve commit tags from Gitlab.
     *
     * <p>Result is a map keyed by tag name that supplies the git hash of the tagged commit.</p>
     *
     * <p>Note that the <code>search</code> query param of the GitLab API method used here is
     * documented as supporting "^" at the front of a search string to require that the search
     * term appear at the front of the tag name, but empirically, it does not appear to work.
     * Without the initial caret, this returns tags that <i>contain</i> the search term, which
     * is good enough for our use.</p>
     *
     * @param nameMustContain required substring of retrieved tags
     * @return map of tag names -> hash ids
     * @throws IOException if there's an IO error
     * @throws URISyntaxException if the request URI is malformed
     */
    public Map<String, String> getTags(String nameMustContain) throws IOException, URISyntaxException {
        final Map<String, String> queryParms = ImmutableMap.<String, String>builder()
                .put("order-by", "name")
                .put("search", nameMustContain)
                .build();
        final List<String> pathSegments = ImmutableList.<String>builder()
                .addAll(PROJECTS_RESOURCE_PATH_SEGMENTS)
                .add(fullProjectPath)
                .addAll(TAGS_RESOURCE_PATH_SEGMENTS)
                .build();
        final JsonNode tags = callApi(pathSegments, queryParms);
        return tags instanceof ArrayNode ?
                Streams.stream(tags.elements())
                        .map(tag -> Pair.of(tag.path("name"), tag.path("commit").path("id")))
                        .filter(pair -> pair.getKey().isTextual() && pair.getValue().isTextual())
                        .collect(Collectors.toMap(
                                pair -> pair.getKey().asText(),
                                pair -> pair.getValue().asText()))
                : Collections.emptyMap();
    }

    /**
     * Make an API request and return JSON results.
     *
     * @param pathSegments path segments for URI, not url-encoded
     * @param queryParams query parameters
     *
     * @return JsonNode parsed from response
     * @throws IOException        if there's an IO issue
     * @throws URISyntaxException if the URI is malformed
     */
    private JsonNode callApi(List<String> pathSegments, Map<String, String> queryParams)
            throws IOException, URISyntaxException {
        final List<String> fullPathSegments = ImmutableList.<String>builder()
                .addAll(API_PATH_SEGMENTS)
                .addAll(pathSegments)
                .build();
        URIBuilder uriBuilder = new URIBuilder()
                .setScheme("https")
                .setHost(gitlabHost)
                .setPathSegments(fullPathSegments);
        queryParams.forEach((name, value) -> uriBuilder.setParameter(name, value));
        HttpGet request = new HttpGet(uriBuilder.build());
        request.addHeader(API_TOKEN_HEADER_NAME, getApiToken());
        request.addHeader(HttpHeaders.ACCEPT, JSON_MEDIA_TYPE);
        try (CloseableHttpClient client = httpClientSupplier.get()) {
            return client.execute(request, jsonResponseHandler, HttpCoreContext.create());
        }
    }

    /**
     * Get a Private API token to use for Gitlab authentication.
     *
     * @return the API token
     */
    private static String getApiToken() {
        return System.getenv(API_TOKEN_ENV_NAME);
    }
}

