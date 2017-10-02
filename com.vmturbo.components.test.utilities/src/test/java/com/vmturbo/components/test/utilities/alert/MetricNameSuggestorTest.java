package com.vmturbo.components.test.utilities.alert;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import com.vmturbo.components.test.utilities.alert.MetricNameSuggestor.Suggestion;

public class MetricNameSuggestorTest {

    private final MetricNameSuggestor suggestor = new MetricNameSuggestor();

    @Test
    public void testComputeSuggestionsEmpty() {
        final List<Suggestion> suggestions = suggestor.computeSuggestions("foo", Collections.emptyList());

        assertTrue(suggestions.isEmpty());
    }

    @Test
    public void testComputeSuggestionsExactMatch() {
        final List<Suggestion> suggestions = suggestor.computeSuggestions("foo", Collections.singletonList("foo"));

        assertEquals("foo", suggestions.get(0).getSuggestedMetricName());
        assertTrue(suggestions.get(0).isPerfectMatch());
    }

    @Test
    public void testComputeSuggestionsSortedInOrderOfDistance() {
        final List<String> suggestionNames = suggestor.computeSuggestions(
            "target_metric_name", Arrays.asList("target_metric_name_count", "target_metric_name_sum"))
            .stream()
            .map(Suggestion::getSuggestedMetricName)
            .collect(Collectors.toList());

        assertThat(suggestionNames, contains("target_metric_name_sum", "target_metric_name_count"));
    }

    @Test
    public void testComputeSuggestionsSkipsNonMatches() {
        final List<String> suggestionNames = suggestor.computeSuggestions(
            "target_metric_name", Arrays.asList("target_metric_name_sum", "unrelated_metric"))
            .stream()
            .map(Suggestion::getSuggestedMetricName)
            .collect(Collectors.toList());

        assertThat(suggestionNames, contains("target_metric_name_sum"));
        assertThat(suggestionNames, not(contains("unrelated_metric")));
    }
}