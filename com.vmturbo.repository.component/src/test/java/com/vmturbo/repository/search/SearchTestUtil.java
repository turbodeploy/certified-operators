package com.vmturbo.repository.search;

import java.util.Collection;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;

public class SearchTestUtil {

    @Nonnull
    public static StringFilter makeRegexStringFilter(String regex, boolean caseSensitive) {
        return StringFilter.newBuilder()
                .setStringPropertyRegex(regex)
                .setCaseSensitive(caseSensitive)
                .setPositiveMatch(true)
                .build();
    }

    @Nonnull
    public static StringFilter makeListStringFilter(Collection<String> options, boolean caseSensitive) {
        return StringFilter.newBuilder()
                .addAllOptions(options)
                .setCaseSensitive(caseSensitive)
                .setPositiveMatch(true)
                .build();
    }
}
