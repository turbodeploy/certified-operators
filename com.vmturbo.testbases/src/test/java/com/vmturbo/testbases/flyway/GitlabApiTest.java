package com.vmturbo.testbases.flyway;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.collect.Streams;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpCoreContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.runner.RunWith;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Tests of the {@link GitlabApi} class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(HttpClients.class)
public class GitlabApiTest {

    private JsonNode tagsResponse;
    private CloseableHttpClient httpClient;
    private HttpUriRequest httpRequest;

    /**
     * Create a new instance, and load the sample JSON response for use by the mocked
     * response handler.
     *
     * @throws IOException if there's a problem loading the response data
     */
    public GitlabApiTest() throws IOException {
        this.tagsResponse = getTagsResponse();
    }

    /**
     * Set up mocks used by the tests.
     *
     * @throws IOException won't happen when building mocks, but required to be declared
     */
    @Before
    public void before() throws IOException {
        mockStatic(HttpClients.class);
        this.httpClient = mock(CloseableHttpClient.class);
        when(HttpClients.createDefault()).thenReturn(httpClient);
        when(httpClient
                .execute(any(HttpUriRequest.class), any(ResponseHandler.class), any(HttpCoreContext.class)))
                .thenAnswer((Answer<JsonNode>)invocation -> {
                    this.httpRequest = invocation.getArgumentAt(0, HttpUriRequest.class);
                    return tagsResponse;
                });
    }

    @Rule
    EnvironmentVariables environmentVariables = new EnvironmentVariables();

    /**
     * Test that tags API response is parsed properly and returns the expected results.
     *
     * @throws IOException        if there's an IO problem
     * @throws URISyntaxException if request URI is malformed
     */
    @Test
    public void testGetTags() throws IOException, URISyntaxException {
        final GitlabApi gitlabApi = new GitlabApi("dummy", "turbonommic/xl");
        final Map<String, String> tags = gitlabApi.getTags("xl-develop-ci-");
        assertEquals(6, tags.size());
        assertEquals(
                Streams.stream(tagsResponse.elements())
                        .map(tag -> tag.path("name").asText())
                        .collect(Collectors.toSet()),
                tags.keySet());
        assertEquals(
                Streams.stream(tagsResponse.elements())
                        .map(tag -> tag.path("commit").path("id").asText())
                        .collect(Collectors.toSet()),
                new HashSet<>(tags.values()));
    }

    /**
     * Test that tags with null or missing name or commit id are omitted from results.
     *
     * @throws IOException        if there's an IO problem
     * @throws URISyntaxException fi the request URI is malformed
     */
    @Test
    public void testGetTagsIgnoresNulls() throws IOException, URISyntaxException {
        // get expected test results prior to mucking with the http response data
        final Set<String> expectedNames = Streams.stream(tagsResponse.elements())
                .map(tag -> tag.path("name").asText())
                .collect(Collectors.toSet());
        final Set<String> expectedHashes = Streams.stream(tagsResponse.elements())
                .map(tag -> tag.path("commit").path("id").asText())
                .collect(Collectors.toSet());
        addToTagsResponse(null, "xxx");
        addToTagsResponse("xxx", null);
        addToTagsResponse(null, null);
        final GitlabApi gitlabApi = new GitlabApi("dummy", "turbonommic/xl");
        final Map<String, String> tags = gitlabApi.getTags("xl-develop-ci-");
        assertEquals(6, tags.size());
        assertEquals(expectedNames, tags.keySet());
        assertEquals(expectedHashes, new HashSet<>(tags.values()));
    }

    /**
     * Augment the standard tags response with JSON entries that are missing name and/or id in
     * order to test that they are omitted from getTags result.
     *
     * @param name name to use in an added entry, or null
     * @param hash commit id to use in an added entry, or null
     */
    private void addToTagsResponse(@Nullable final String name, @Nullable final String hash) {
        final ObjectNode entry = JsonNodeFactory.instance.objectNode()
                .put("name", name);
        entry.putObject("commit").put("id", hash);
        ((ArrayNode)tagsResponse).add(entry);
    }

    /**
     * Test that no tags are returned if the gitlab response is not a JSON array.
     *
     * @throws IOException        if there's an IO issue
     * @throws URISyntaxException if the api request URI is malformed
     */
    @Test
    public void testGetTagsNotAnArray() throws IOException, URISyntaxException {
        final GitlabApi gitlabApi = new GitlabApi("dummy", "turbonommic/xl");
        this.tagsResponse = JsonNodeFactory.instance.objectNode();
        final Map<String, String> tags = gitlabApi.getTags("xl-develop-ci-");
        assertEquals(0, tags.size());
    }

    /**
     * Test that the response handler properly parses a JSON response body.
     *
     * @throws IOException if there's an IO problem
     */
    @Test
    public void testResponseHandlerWithResults() throws IOException {
        HttpResponse response = mock(HttpResponse.class);
        StatusLine statusLine = mock(StatusLine.class);
        HttpEntity httpEntity = mock(HttpEntity.class);
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        when(response.getEntity()).thenReturn(httpEntity);
        String jsonString = new ObjectMapper().writeValueAsString(tagsResponse);
        when(httpEntity.getContent()).thenReturn(
                new ByteArrayInputStream(jsonString.getBytes(Charsets.UTF_8)));
        final JsonNode responseJson = GitlabApi.jsonResponseHandler.handleResponse(response);
        assertEquals(tagsResponse, responseJson);
    }

    /**
     * Test that an empty API response produces an empty array from the response handler.
     *
     * @throws IOException if there's an IO problem
     */
    @Test
    public void testResponseHandlerWithNullEnity() throws IOException {
        HttpResponse response = mock(HttpResponse.class);
        StatusLine statusLine = mock(StatusLine.class);
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        when(response.getEntity()).thenReturn(null);
        final JsonNode responseJson = GitlabApi.jsonResponseHandler.handleResponse(response);
        assertEquals(JsonNodeFactory.instance.arrayNode(), responseJson);
    }

    /**
     * Test that any API response with code above the 2XX range produces an empty tags list.
     *
     * @throws IOException if there's an IO problem
     */
    @Test(expected = ClientProtocolException.class)
    public void testResponseHandlerWithErrorResponse() throws IOException {
        HttpResponse response = mock(HttpResponse.class);
        StatusLine statusLine = mock(StatusLine.class);
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(500);
        final JsonNode responseJson = GitlabApi.jsonResponseHandler.handleResponse(response);
        assertEquals(JsonNodeFactory.instance.arrayNode(), responseJson);
    }

    /**
     * Test that any API response with a code below the 2XX range produces an empty tags list.
     *
     * @throws IOException if there's an IO problem
     */
    @Test(expected = ClientProtocolException.class)
    public void testResponseHandlerWithInfoResponse() throws IOException {
        HttpResponse response = mock(HttpResponse.class);
        StatusLine statusLine = mock(StatusLine.class);
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(100);
        final JsonNode responseJson = GitlabApi.jsonResponseHandler.handleResponse(response);
        assertEquals(JsonNodeFactory.instance.arrayNode(), responseJson);
    }

    /**
     * Check that URIs generated for requests are correct.
     *
     * @throws IOException        if there are IO problems
     * @throws URISyntaxException if the request URI is malformed
     */
    @Test
    public void testRequestUriIsCorrect() throws IOException, URISyntaxException {
        final GitlabApi gitlabApi = new GitlabApi(GitlabApi.TURBO_GITLAB_HOST, GitlabApi.XL_PROJECT_PATH);
        gitlabApi.getTags("xl-develop-ci-");
        assertEquals("https://git.turbonomic.com/api/v4/projects/turbonomic%2Fxl/repository/tags" +
                "?order-by=name" +
                "&search=xl-develop-ci-", httpRequest.getURI().toString());
    }

    /**
     * Check that the request headers are correct.
     *
     * <p>We should always be including a header defining our private API token.</p>
     * @throws IOException if there are IO problems
     * @throws URISyntaxException if the request URI is malformed
     */
    @Test
    public void testRequestHeadersAreCorrect() throws IOException, URISyntaxException {
        environmentVariables.set(GitlabApi.API_TOKEN_ENV_NAME, "shhh!");
        final GitlabApi gitlabApi = new GitlabApi(GitlabApi.TURBO_GITLAB_HOST, GitlabApi.XL_PROJECT_PATH);
        gitlabApi.getTags("xl-develop-ci-");
        assertEquals(1, httpRequest.getHeaders(GitlabApi.API_TOKEN_HEADER_NAME).length);
        assertEquals("shhh!", httpRequest.getFirstHeader(GitlabApi.API_TOKEN_HEADER_NAME).getValue());
    }

    /**
     * Read the tags response resource and parse it to a {@link JsonNode} value.
     *
     * @return the parsed JSON value
     * @throws IOException if there's an IO problem
     */
    private static JsonNode getTagsResponse() throws IOException {
        URL tagsResponseResource = GitlabApiTest.class.getResource("tagsResponse.json");
        return new ObjectMapper().readTree(tagsResponseResource);
    }

}
