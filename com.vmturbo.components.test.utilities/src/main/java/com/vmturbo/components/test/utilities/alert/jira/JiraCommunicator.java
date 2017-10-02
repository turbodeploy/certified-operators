package com.vmturbo.components.test.utilities.alert.jira;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.web.client.RestTemplate;

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.test.utilities.alert.jira.JiraIssue.JiraTransition;

/**
 * A class that facilitates communication with Jira.
 * Communicates over a REST interface.
 *
 * For documentation about the Jira REST interface, see:
 * https://developer.atlassian.com/static/rest/jira/6.1.html
 */
public class JiraCommunicator {
    public static final String DEFAULT_USERNAME = "professor";
    private static final String DEFAULT_PASSWORD = "Turbonomic6";
    private static final String DEFAULT_ROOT_JIRA_URL = "https://vmturbo.atlassian.net/rest/api/latest/";

    /**
     * Message converter to decode incoming responses.
     */
    private final GsonHttpMessageConverter msgConverter;
    private final RestTemplate restTemplate;

    /**
     * Create a communicator that uses the default user and talks to the Turbonomic Jira instance.
     */
    public JiraCommunicator() {
        this(DEFAULT_USERNAME, DEFAULT_PASSWORD, DEFAULT_ROOT_JIRA_URL);
    }

    /**
     * Create a communicator that uses a specific user and talks to a specific Jira instance.
     *
     * @param username The username of the user to communicate as.
     * @param password The password for the user.
     * @param root_uri The root URI of the Jira instance to communicate with.
     */
    public JiraCommunicator(@Nonnull final String username,
                            @Nonnull final String password,
                            @Nonnull final String root_uri) {
        msgConverter = new GsonHttpMessageConverter();
        msgConverter.setGson(ComponentGsonFactory.createGson());

        restTemplate = new RestTemplateBuilder()
            .rootUri(root_uri)
            .basicAuthorization(username, password)
            .additionalMessageConverters(msgConverter)
            .build();
    }

    /**
     * Create a new Jira issue.
     *
     * Throws an exception if the requested issue cannot be created.
     *
     * @param issue The issue to create.
     * @return The created issue.
     */
    public JiraIssue createIssue(final JiraIssue issue) {
        final ResponseEntity<JiraIssue> createdIssue = restTemplate.exchange("/issue", HttpMethod.POST,
            new HttpEntity<>(issue), JiraIssue.class);

        return createdIssue.getBody();
    }

    /**
     * Get a specific issue by its issue ID (ie OM-12345).
     * In true Jira parlance, this method finds the issue by its "key".
     *
     * Throws an exception if the expected issue cannot be found.
     *
     * @param issueId The ID/key of the issue to retrieve.
     * @return The issue with the given id/key.
     */
    public JiraIssue getIssue(@Nonnull final String issueId) {
        final ResponseEntity<JiraIssue> createdIssue = restTemplate.exchange("/issue/" + issueId, HttpMethod.GET,
            null, JiraIssue.class);

        return createdIssue.getBody();
    }

    /**
     * Attempt to find issues that provides that match the search fields specified in the query.
     * Finds a list of matching issues.
     *
     * @param query The query on which to find issues.
     * @return An exact match for the query, or empty if there are none.
     */
    public List<JiraIssue> search(@Nonnull final JiraIssue.Query query) {
        final String request = "/search?jql=" + query.toJql();
        final ResponseEntity<JiraQueryResponse> foundIssues = restTemplate.exchange(request, HttpMethod.GET,
            null, JiraQueryResponse.class);

        return foundIssues.getBody().getIssues();
    }

    /**
     * Attempt to find an issue that provides an exact match for all fields specified in the query.
     * Finds at most one matching issue. If there is more than one exact match, there is no guarantee
     * about which will be returned. If a field in the query is empty, it will not be used in the search.
     *
     * This query operates by first finding exact matches on label and creator fields and full
     * text matches on summary and description fields in the query using the Jira REST API.
     * The matching issues are then exact matched against the summary and description fields.
     *
     * @param query The query on which to find issues.
     * @return An exact match for the query, or empty if there are none.
     */
    public Optional<JiraIssue> exactSearch(@Nonnull final JiraIssue.Query query) {
        return search(query).stream()
            .filter(issue -> exactMatchField(issue.getFields().getSummary(), query.getSummarySearch()))
            .filter(issue -> exactMatchField(issue.getFields().getDescription(), query.getDescriptionSearch()))
            .findFirst();
    }

    /**
     * Post a comment to an issue.
     * Throws an exception if the expected issue cannot be found.
     *
     * @param issueId The ID of the issue
     * @param commentBody The comment to post to the issue.
     * @return The created comment body.
     */
    public String commentOnIssue(@Nonnull final String issueId, @Nonnull final String commentBody) {
        final JiraComment comment = new JiraComment();
        comment.setBody(commentBody);

        final ResponseEntity<JiraComment> createdIssue = restTemplate.exchange("/issue/" + issueId + "/comment",
            HttpMethod.POST, new HttpEntity<>(comment), JiraComment.class);

        return createdIssue.getBody().getBody();
    }

    /**
     * Transition the state of an issue in Jira.
     * Throws an exception if the expected issue cannot be transitioned.
     *
     * @param issueId The ID of the issue.
     * @param transitionType The type of transition to be performed. Not all transitions can always be performed.
     *                       If the requested transition fails an exception will be thrown. Note that closing an
     *                       issue will fail because this method provides no means to specify a resolution which
     *                       is required when closing an issue.
     */
    public void transitionIssue(@Nonnull final String issueId, @Nonnull final JiraTransition.Type transitionType) {
        final JiraTransition transition = new JiraTransition();
        transition.setId(Integer.toString(transitionType.id));
        final JiraTransitionRequest request = new JiraTransitionRequest();
        request.setTransition(transition);

        restTemplate.exchange("/issue/" + issueId + "/transitions",
            HttpMethod.POST, new HttpEntity<>(request), JiraComment.class);
    }

    /**
     * Test if the field is an exact match for the optional search on that field.
     * If no search is provided (ie the search is empty), the test is considered vacuously true.
     *
     * @param field The field to match against the search.
     * @param fieldSearch The search to match against the field.
     * @return Whether the field was an exact match for the seach. True by default if no
     *         search is provided.
     */
    private boolean exactMatchField(@Nullable final String field, @Nonnull final Optional<String> fieldSearch) {
        return fieldSearch.
            map(match -> match.equals(field))
            .orElse(true);
    }

    /**
     * A helper that stores the issues in a Jira query response.
     */
    public static class JiraQueryResponse {
        private List<JiraIssue> issues;

        public List<JiraIssue> getIssues() {
            return issues;
        }
    }

    /**
     * A helper class to represent a comment.
     */
    public static class JiraComment {
        private String body;

        public String getBody() {
            return body;
        }

        public void setBody(String comment) {
            this.body = comment;
        }
    }

    public static class JiraTransitionRequest {
        private JiraTransition transition;

        public JiraTransition getTransition() {
            return transition;
        }

        public void setTransition(JiraTransition transition) {
            this.transition = transition;
        }
    }
}
