package com.vmturbo.licensing.utils;

import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Sets;

import org.apache.commons.lang3.StringUtils;

public enum CWOMLicenseEdition {

    CWOM_ESSENTIALS("C1-ECS-WOM-E", Sets.newHashSet("historical_data", "custom_reports", "planner", "optimizer", "multiple_vc", "scoped_user_view",
            "customized_views", "group_editor", "automated_actions", "active_directory", "full_policy",
            "action_script", "applications", "loadbalancer", "deploy", "aggregation", "cloud_targets",
            "cluster_flattening", "public_cloud", "vdi_control", "API2", "cloud_cost", "fabric")),
    CWOM_ADVANCED("C1-ECS-WOM-S", Sets.newHashSet("historical_data", "custom_reports", "planner", "optimizer", "multiple_vc", "scoped_user_view", "customized_views",
            "group_editor", "vmturbo_api", "automated_actions", "active_directory", "full_policy", "action_script",
            "applications", "app_control", "loadbalancer", "deploy", "aggregation", "fabric", "storage", "cloud_targets",
            "cluster_flattening", "network_control", "public_cloud", "vdi_control", "API2", "cloud_cost")),
    CWOM_PREMIER("C1-ECS-WOM-A", Sets.newHashSet("historical_data", "custom_reports", "planner", "optimizer", "multiple_vc", "scoped_user_view", "customized_views",
            "group_editor", "vmturbo_api", "automated_actions", "active_directory", "full_policy", "action_script", "applications",
            "app_control", "loadbalancer", "deploy", "aggregation", "fabric", "storage", "cloud_targets", "cluster_flattening",
            "network_control", "container_control", "public_cloud", "vdi_control", "API2", "scaling", "custom_policies", "SLA", "cloud_cost"));

    private final String prefix;
    private final SortedSet<String> features;

    CWOMLicenseEdition(String prefix, Set<String> features) {
        this.prefix = prefix;
        this.features = ImmutableSortedSet.<String>naturalOrder()
                .addAll(features)
                .build();
    }

    public String getPrefix() {
        return prefix;
    }

    public SortedSet<String> getFeatures() {
        return features;
    }

    public boolean matchesFeatureName(String featureName) {
        return StringUtils.trimToEmpty(featureName).startsWith(this.prefix);
    }

    public static Optional<CWOMLicenseEdition> valueOfFeatureName(String featureName) {
        return Stream.of(values())
                .filter(edition -> edition.matchesFeatureName(featureName))
                .findFirst();
    }
}

