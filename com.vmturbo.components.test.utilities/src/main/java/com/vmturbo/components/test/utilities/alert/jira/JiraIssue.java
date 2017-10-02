package com.vmturbo.components.test.utilities.alert.jira;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import com.vmturbo.components.test.utilities.alert.jira.JiraIssue.JiraComponent.Type;

/**
 * This class contains utilities for interacting with objects
 * sent to and fetched from Jira.
 *
 * For more information on the Jira REST API, see:
 * https://developer.atlassian.com/static/rest/jira/6.1.html
 */
public class JiraIssue {
    private String key;
    private Fields fields;

    private JiraIssue() {
        // Default constructor for GSON usage
    }

    private JiraIssue(@Nonnull final Fields fields) {
        this.fields = fields;
    }

    public Fields getFields() {
        return fields;
    }

    public void setFields(Fields fields) {
        this.fields = fields;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    /**
     * Find out if an issue is in a closed state. If the issue has not been created yet, it is not closed.
     *
     * @return If the issue has been closed.
     */
    public boolean isClosed() {
        final JiraStatus status = getFields().getStatus();
        return status != null && status.isClosed();
    }

    /**
     * The type of an issue (ie story/bug)
     */
    public static class IssueType {
        private String name;
        private Boolean subtask;

        public enum Type {
            Story("Story"),
            Bug("Bug");

            public final String name;

            Type(String name) {
                this.name = name;
            }
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Boolean getSubtask() {
            return subtask;
        }

        public void setSubtask(Boolean subtask) {
            this.subtask = subtask;
        }
    }

    /**
     * The Jira Project (ie "OM")
     */
    public static class JiraProject {
        private String key;
        private String name;

        public enum Type {
            OpsManager("OM", "Operations Manager");

            public final String name;
            public final String key;

            Type(String key, String name) {
                this.key = key;
                this.name = name;
            }
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    /**
     * The version. Usually specifies which version of a product
     * the issue affects or should be fixed for.
     */
    public static class JiraVersion {
        private String name;
        private String description;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }
    }

    /**
     * The priority of an issue (ie P1, P2, P3, P4)
     */
    public static class JiraPriority {
        public enum Type {
            P1(1, "P1"),
            P2(2, "P2"),
            P3(3, "P3"),
            P4(4, "P4");

            public final int id;
            public final String name;

            Type(int id, String name) {
                this.id = id;
                this.name = name;
            }
        }

        private String name;
        private String id;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }
    }

    /**
     * The person assigned to the issue.
     */
    public static class Assignee {
        private String name;
        private String displayName;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDisplayName() {
            return displayName;
        }

        public void setDisplayName(String displayName) {
            this.displayName = displayName;
        }
    }

    /**
     * The status of an issue. (ie "In Progress", "Closed", "In Testing", etc.)
     */
    public static class JiraStatus {
        private String description;
        private String name;
        private Integer id;

        private static final Set<String> CLOSED_STATUSES = ImmutableSet.<String>builder()
            .add("Closed")
            .add("In Testing")
            .add("Resolved")
            .build();

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public boolean isClosed() {
            return CLOSED_STATUSES.contains(getName());
        }
    }

    /**
     * The component an issue belongs to (ie "XL")
     */
    public static class JiraComponent {
        private String id;
        private String name;

        public enum Type {
            XL(13501, "XL");

            public final int id;
            public final String name;

            Type(int id, String name) {
                this.id = id;
                this.name = name;
            }
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    /**
     * A state transition for an issue in Jira.
     * ie "Send for Review", "Close", "Reopen". Transitioning an issue
     * may cause a change in that issue's status.
     */
    public static class JiraTransition {
        public enum Type {
            ASSIGN_DETAILS(51, "Assign Details"),
            SEND_FOR_REVIEW(111, "Send for Review"),
            SHELVE(131, "Shelve"),

            // Close is currently not supported. To support, need to provide a way to supply resolution as well.
            CLOSE(141, "Close"),
            REOPEN(101, "Reopen"),
            START_TESTING(171, "Start Testing");

            public final int id;
            public final String name;

            Type(int id, String name) {
                this.id = id;
                this.name = name;
            }
        }

        private String name;
        private String id;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }
    }

    /**
     * The fields retrieved on an issue that describe it.
     */
    public static class Fields {
        private IssueType issuetype;
        private JiraProject project;
        private List<JiraVersion> fixVersions;
        private String created;
        private JiraPriority priority;
        private List<String> labels;
        private List<JiraVersion> versions;
        private Assignee assignee;
        private String updated;
        private JiraStatus status;
        private List<JiraComponent> components;
        private String description;
        private String summary;

        public IssueType getIssuetype() {
            return issuetype;
        }

        public void setIssuetype(IssueType issuetype) {
            this.issuetype = issuetype;
        }

        public JiraProject getProject() {
            return project;
        }

        public void setProject(JiraProject project) {
            this.project = project;
        }

        public List<JiraVersion> getFixVersions() {
            return fixVersions;
        }

        public void setFixVersions(List<JiraVersion> fixVersions) {
            this.fixVersions = fixVersions;
        }

        public String getCreated() {
            return created;
        }

        public void setCreated(String created) {
            this.created = created;
        }

        public JiraPriority getPriority() {
            return priority;
        }

        public void setPriority(JiraPriority priority) {
            this.priority = priority;
        }

        public List<String> getLabels() {
            return labels;
        }

        public void setLabels(List<String> labels) {
            this.labels = labels;
        }

        public List<JiraVersion> getVersions() {
            return versions;
        }

        public void setVersions(List<JiraVersion> versions) {
            this.versions = versions;
        }

        public Assignee getAssignee() {
            return assignee;
        }

        public void setAssignee(Assignee assignee) {
            this.assignee = assignee;
        }

        public String getUpdated() {
            return updated;
        }

        public void setUpdated(String updated) {
            this.updated = updated;
        }

        public JiraStatus getStatus() {
            return status;
        }

        public void setStatus(JiraStatus status) {
            this.status = status;
        }

        public List<JiraComponent> getComponents() {
            return components;
        }

        public void setComponents(List<JiraComponent> components) {
            this.components = components;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public String getSummary() {
            return summary;
        }

        public void setSummary(String summary) {
            this.summary = summary;
        }
    }

    @Override
    public String toString() {
        final Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(this);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder xlBugBuilder() {
        return newBuilder()
            .components(Type.XL)
            .priority(JiraPriority.Type.P2)
            .project(JiraProject.Type.OpsManager)
            .type(IssueType.Type.Bug);
    }

    /**
     * A builder for creating an issue by hand.
     */
    public static class Builder {
        private final Fields fields;

        private Builder() {
            fields = new Fields();
        }

        public Builder type(IssueType.Type type) {
            final IssueType issueType = new IssueType();
            issueType.setName(type.name);
            fields.setIssuetype(issueType);

            return this;
        }

        public Builder project(@Nonnull final JiraProject.Type type) {
            final JiraProject project = new JiraProject();
            project.setKey(type.key);
            project.setName(type.name);
            fields.setProject(project);

            return this;
        }

        public Builder fixVersion(@Nonnull final String fixVersionName) {
            final JiraVersion version = new JiraVersion();
            version.setName(fixVersionName);

            List<JiraVersion> versions = fields.getFixVersions();
            versions = (versions == null) ? new ArrayList<>() : versions;
            versions.add(version);
            fields.setFixVersions(versions);

            return this;
        }

        public Builder version(@Nonnull final String versionName) {
            final JiraVersion version = new JiraVersion();
            version.setName(versionName);

            List<JiraVersion> versions = fields.getVersions();
            versions = (versions == null) ? new ArrayList<>() : versions;
            versions.add(version);
            fields.setVersions(versions);

            return this;
        }

        public Builder labels(@Nonnull final String... labels) {
            fields.setLabels(Arrays.asList(labels));

            return this;
        }

        public Builder labels(@Nonnull final List<String> labels) {
            fields.setLabels(labels);

            return this;
        }

        public Builder priority(@Nonnull final JiraPriority.Type type) {
            final JiraPriority priority = new JiraPriority();
            priority.setName(type.name);
            priority.setId(Integer.toString(type.id));
            fields.setPriority(priority);

            return this;
        }

        public Builder components(@Nonnull final JiraComponent.Type... components) {
            fields.setComponents(
                Arrays.stream(components)
                    .map(component -> {
                        final JiraComponent jc = new JiraComponent();
                        jc.setName(component.name);
                        jc.setId(Integer.toString(component.id));
                        return jc;
                    }).collect(Collectors.toList()));

            return this;
        }

        public Builder description(@Nonnull final String description) {
            fields.setDescription(description);

            return this;
        }

        public Builder summary(@Nonnull final String summary) {
            fields.setSummary(summary);

            return this;
        }

        public Builder assignee(@Nonnull final String assigneeName) {
            return assignee(assigneeName, null);
        }

        public Builder assignee(@Nonnull final String assigneeName, @Nullable final String assigneeDisplayName) {
            final Assignee assignee = new Assignee();
            assignee.setName(assigneeName);
            assignee.setDisplayName(assigneeDisplayName);
            fields.setAssignee(assignee);

            return this;
        }

        @Nonnull
        public JiraIssue build() {
            return new JiraIssue(fields);
        }
    }

    /**
     * A query for jira issues, which can optionally perform lookups on:
     * - Exact match for one or more labels
     * - Exact match for a creator
     * - Full text search on a description
     * - Full text search on a summary
     */
    public static class Query {
        private Optional<List<String>> labels = Optional.empty();
        private Optional<String> creator = Optional.empty();
        private Optional<String> summarySearch = Optional.empty();
        private Optional<String> descriptionSearch = Optional.empty();

        private Query() {
        }

        public Optional<List<String>> getLabels() {
            return labels;
        }

        public Optional<String> getCreator() {
            return creator;
        }

        public Optional<String> getSummarySearch() {
            return summarySearch;
        }

        public Optional<String> getDescriptionSearch() {
            return descriptionSearch;
        }

        /**
         * Transform to a query string suitable for a JQL search.
         * For details see: https://confluence.atlassian.com/jirasoftwarecloud/advanced-searching-764478330.html
         *
         * @return A query string for suitable for searching for specific Jira issues.
         */
        public String toJql() {
            String labelsQuery = labels.map(l -> l.stream()
                .map(label -> String.format("labels=\"%s\"", label))
                .collect(Collectors.joining(" AND "))
            ).orElse("");

            return labelsQuery +                            // Exact match for labels
                and("creator=", creator) +                  // Exact match on creator
                and("summary~", summarySearch) +            // Full text search on summary
                and("description~", descriptionSearch);     // Full text search on description
        }

        private String and(@Nonnull final String queryPrefix, @Nonnull final Optional<String> optionalField) {
            return optionalField
                .map(field -> String.format(" AND %s\"%s\"", queryPrefix, field))
                .orElse("");
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public static class Builder {
            private final Query query;

            private Builder() {
                this.query = new Query();
            }

            public Builder labels(@Nonnull final List<String> labels) {
                query.labels = Optional.of(labels);
                return this;
            }

            public Builder labels(@Nonnull final String... labels) {
                query.labels = Optional.of(Arrays.asList(labels));
                return this;
            }

            public Builder creator(@Nonnull final String creator) {
                query.creator = Optional.of(creator);
                return this;
            }

            public Builder summarySearch(@Nonnull final String summarySearch) {
                query.summarySearch = Optional.of(summarySearch);
                return this;
            }

            public Builder descriptionSearch(@Nonnull final String descriptionSearch) {
                query.descriptionSearch = Optional.of(descriptionSearch);
                return this;
            }

            public Query build() {
                return query;
            }
        }
    }
}
