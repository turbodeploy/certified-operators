package com.vmturbo.common.protobuf;

import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.group.GroupDTO.NameFilter;

/**
 * Utilities for dealing with messages defined in group/GroupDTO.proto.
 */
public class GroupDTOUtil {

    /**
     * @param name The name to compare with the filter.
     * @param filter The name filter.
     * @return True if the name matches the filter.
     */
    public static boolean nameFilterMatches(@Nonnull final String name,
                                            @Nonnull final NameFilter filter) {
        final boolean patternMatches = Pattern.matches(filter.getNameRegex(), name);
        return patternMatches ^ filter.getNegateMatch();
    }
}
