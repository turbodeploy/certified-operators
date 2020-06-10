package com.vmturbo.licensing.utils;

import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class LicenseFeatureFactory {

    /**
     * License Manager feature strings.
     */
    private static final String HIST_DATA = "historical_data";
    private static final String CUST_REPORTS = "custom_reports";
    private static final String OPTIMIZER = "optimizer";
    private static final String MULT_VC = "multiple_vc";
    private static final String SCOPED_USER = "scoped_user_view";
    private static final String CUSTOMIZED_VIEWS = "customized_views";
    private static final String GROUP_EDITOR = "group_editor";
    private static final String VMT_API = "vmturbo_api";
    private static final String AUTO_ACTIONS = "automated_actions";
    private static final String ACTIVE_DIR = "active_directory";
    private static final String FULL_POLICY = "full_policy";
    private static final String ACTION_SCRIPT = "action_script";
    private static final String APP = "applications";
    private static final String LB = "loadbalancer";
    private static final String DEPLOY = "deploy";
    private static final String AGGREGATION = "aggregation";
    private static final String COMMUNITY = "community";
    private static final String CLOUD_TARGETS = "cloud_targets";
    private static final String FREEMIUM = "freemium";
    private static final String TRIAL = "trial";
    private static final SortedSet<String> trialFeatureSet =
            Sets.newTreeSet(Lists.newArrayList(
                    ACTION_SCRIPT,
                    ACTIVE_DIR,
                    AGGREGATION,
                    APP,
                    AUTO_ACTIONS,
                    CLOUD_TARGETS,
                    CUST_REPORTS,
                    CUSTOMIZED_VIEWS,
                    DEPLOY,
                    FULL_POLICY,
                    GROUP_EDITOR,
                    HIST_DATA,
                    LB,
                    MULT_VC,
                    OPTIMIZER,
                    SCOPED_USER,
                    VMT_API,
                    TRIAL
            ));
    private static final SortedSet<String> freemiumFeatureSet =
            Sets.newTreeSet(Lists.newArrayList(
                    HIST_DATA,
                    CUSTOMIZED_VIEWS,
                    AUTO_ACTIONS,
                    CUST_REPORTS,
                    OPTIMIZER,
                    FREEMIUM
            ));
    private static final SortedSet<String> vhmFeatureSet =
            Sets.newTreeSet(Lists.newArrayList(
                    COMMUNITY
            ));
    private static final Map<LicenseType, SortedSet<String>> licenseTypeToFeatureSet =
            new ImmutableMap.Builder<LicenseType, SortedSet<String>>()
                    .put(LicenseType.TRIAL, trialFeatureSet)
                    .put(LicenseType.FREEMIUM, freemiumFeatureSet)
                    .put(LicenseType.VHM, vhmFeatureSet)
                    .build();

    public static final SortedSet<String> getFeatureSet(LicenseType licenseType) {
        SortedSet<String> featureSet = new TreeSet<>();
        featureSet.addAll(licenseTypeToFeatureSet.get(licenseType));
        return featureSet;
    }

    public enum LicenseType {
        TRIAL, FREEMIUM, VHM;

        public static boolean contains(String s) {
            for (LicenseType choice : values()) {
                if (choice.name().equals(s))
                    return true;
            }
            return false;
        }
    }
}
