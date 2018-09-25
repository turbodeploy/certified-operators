package com.vmturbo.repository.search;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;

public class SearchTestUtil {

    @Nonnull
    public static StringFilter makeStringFilter(String regex, boolean caseSensitive) {
        return StringFilter.newBuilder()
                .setStringPropertyRegex(regex)
                .setCaseSensitive(caseSensitive)
                .setMatch(true)
                .build();
    }
}
