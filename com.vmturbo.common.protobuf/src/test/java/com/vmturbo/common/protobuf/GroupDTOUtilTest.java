package com.vmturbo.common.protobuf;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.NameFilter;

public class GroupDTOUtilTest {

    private static final String REGEX = ".*1";

    private static final String MATCHING_NAME = "test 1";

    private static final String NOT_MATCHING_NAME = "test 2";

    @Test
    public void testNameMatches() {
        assertTrue(GroupDTOUtil.nameFilterMatches(MATCHING_NAME,
                NameFilter.newBuilder().setNameRegex(REGEX).build()));
    }

    @Test
    public void testNameNotMatches() {
        assertFalse(GroupDTOUtil.nameFilterMatches(NOT_MATCHING_NAME,
                NameFilter.newBuilder().setNameRegex(REGEX).build()));
    }

    @Test
    public void testNegateMatch() {
        assertFalse(GroupDTOUtil.nameFilterMatches(MATCHING_NAME,
                NameFilter.newBuilder()
                    .setNameRegex(REGEX)
                    .setNegateMatch(true)
                    .build()));
    }

    @Test
    public void testNegateNoMatch() {
        assertTrue(GroupDTOUtil.nameFilterMatches(NOT_MATCHING_NAME,
                NameFilter.newBuilder()
                        .setNameRegex(REGEX)
                        .setNegateMatch(true)
                        .build()));
    }
}
