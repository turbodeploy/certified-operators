package com.vmturbo.components.test.utilities.alert;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

/**
 * Suggest metric names when the user has given an incorrect one.
 */
public class MetricNameSuggestor {
    /**
     * A suggestion for a metric name along with the levenshtein distance
     * from the some name. Suggestions can be sorted by their distance.
     */
    public static class Suggestion implements Comparable<Suggestion> {
        /**
         * The levenshtein distance from a target property name.
         */
        private final int distance;

        /**
         * The name of the protobuf field associated with the method.
         */
        private final String suggestedMetricName;

        public Suggestion(final int distance,
                          @Nonnull final String suggestedMetricName) {
            this.distance = distance;
            this.suggestedMetricName = suggestedMetricName;
        }

        public int getDistance() {
            return distance;
        }

        public String getSuggestedMetricName() {
            return suggestedMetricName;
        }

        public boolean isPerfectMatch() {
            return distance == 0;
        }

        /**
         * Compare to another {@code Suggestion} by comparing their distances.
         * @param other The other {@code Suggestion} to compare against
         * @return The value of comparing {@code this} Suggestion's distance against the other's
         */
        @Override
        public int compareTo(@Nonnull Suggestion other){
            return Integer.compare(this.distance, other.distance);
        }
    }

    public static class CostThreshold {
        public final int wordLength;
        public final int threshold;

        public CostThreshold(int wordLength, int threshold) {
            this.wordLength = wordLength;
            this.threshold = threshold;
        }
    }

    public static final CostThreshold SHORT_PROPERTY_THRESHOLD = new CostThreshold(6, 2);
    public static final CostThreshold MEDIUM_PROPERTY_THRESHOLD = new CostThreshold(12, 4);
    public static final CostThreshold LONG_PROPERTY_THRESHOLD = new CostThreshold(Integer.MAX_VALUE, 6);

    /**
     * Compute a list of suggested metrics that are "close to" a given alert metric name.
     * Compute the levenshtein distance against the alert metric's name, and return those
     * considered "close enough".
     *
     * Suggestions are returned by their distance from the target alertMetricName.
     *
     * @param alertMetricName The name for a property
     * @param allMetricNames The methods to consider as suggestions
     * @return A list of suggestions that fuzzy match against the metric's name.
     */
    public List<Suggestion> computeSuggestions(@Nonnull final String alertMetricName,
                                               @Nonnull final Collection<String> allMetricNames) {
        int costThreshold = computeCostThreshold(alertMetricName);

        return allMetricNames.stream()
            .filter(otherMetricName -> levenshteinDistance(alertMetricName, otherMetricName) <= costThreshold)
            .map(otherMetricName -> {
                int cost = levenshteinDistance(alertMetricName, otherMetricName);
                return new Suggestion(cost, otherMetricName); })
            .sorted()
            .collect(Collectors.toList());
    }

    /**
     * Compute the levenshtein cost threshold for a property. Longer property names
     * produce a higher cost threshold.
     *
     * @param name The name of the property to check
     * @return the levenshtein cost threshold for a property
     */
    private static int computeCostThreshold(@Nonnull String name) {
        if (name.isEmpty()) {
            return 0;
        } else if (name.length() < SHORT_PROPERTY_THRESHOLD.wordLength) {
            return SHORT_PROPERTY_THRESHOLD.threshold;
        } else if (name.length() < MEDIUM_PROPERTY_THRESHOLD.wordLength) {
            return MEDIUM_PROPERTY_THRESHOLD.threshold;
        } else {
            return LONG_PROPERTY_THRESHOLD.threshold;
        }
    }

    /**
     * Calculate the levenshtein distance between two strings
     * https://en.wikipedia.org/wiki/Levenshtein_distance
     * Code attribution: https://en.wikibooks.org/wiki/Algorithm_Implementation/Strings/Levenshtein_distance#Java
     * Header comments and {@code @Nonnull} annotations not in the original.
     *
     * @param lhs The String to compute distance with the {@code rhs}
     * @param rhs The String to compute distance with the {@code lhs}
     * @return The levenshtein distance between the lhs and rhs.
     */
    private static int levenshteinDistance(@Nonnull CharSequence lhs, @Nonnull CharSequence rhs) {
        int len0 = lhs.length() + 1;
        int len1 = rhs.length() + 1;

        // the array of distances
        int[] cost = new int[len0];
        int[] newcost = new int[len0];

        // initial cost of skipping prefix in String s0
        for (int i = 0; i < len0; i++) cost[i] = i;

        // dynamically computing the array of distances

        // transformation cost for each letter in s1
        for (int j = 1; j < len1; j++) {
            // initial cost of skipping prefix in String s1
            newcost[0] = j;

            // transformation cost for each letter in s0
            for(int i = 1; i < len0; i++) {
                // matching current letters in both strings
                int match = (lhs.charAt(i - 1) == rhs.charAt(j - 1)) ? 0 : 1;

                // computing cost for each transformation
                int cost_replace = cost[i - 1] + match;
                int cost_insert  = cost[i] + 1;
                int cost_delete  = newcost[i - 1] + 1;

                // keep minimum cost
                newcost[i] = Math.min(Math.min(cost_insert, cost_delete), cost_replace);
            }

            // swap cost/newcost arrays
            int[] swap = cost; cost = newcost; newcost = swap;
        }

        // the distance is the cost for transforming all letters in both strings
        return cost[len0 - 1];
    }
}
