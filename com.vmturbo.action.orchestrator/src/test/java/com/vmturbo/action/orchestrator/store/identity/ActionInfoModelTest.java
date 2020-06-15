package com.vmturbo.action.orchestrator.store.identity;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;

/**
 * Unit test for {@link ActionInfoModel}.
 */
public class ActionInfoModelTest {
    /**
     * Tests how the long details information is being truncated. It is required not to fail when
     * saving to the DB.
     */
    @Test
    public void testTooLargeDetails() {
        final String details = createLargeString(500);
        final String additionalDetail1 = createLargeString(400);
        final String additionalDetail2 = "asdfasdfasfadsf";
        final ActionInfoModel model = new ActionInfoModel(ActionTypeCase.ACTIVATE, 1L, details,
                Sets.newHashSet(additionalDetail1, additionalDetail2));
        Assert.assertEquals(255, model.getDetails().get().length());
        Assert.assertEquals(2, model.getAdditionalDetails().get().size());
        final Optional<String> detail2Actual = model.getAdditionalDetails().get().stream().filter(
                str -> str.equals(additionalDetail2)).findAny();
        Assert.assertEquals(Optional.of(additionalDetail2), detail2Actual);

        final String detail1Actual = model.getAdditionalDetails().get().stream().filter(
                str -> !str.equals(additionalDetail2)).findAny().get();
        Assert.assertEquals(200, detail1Actual.length());
        Assert.assertThat(additionalDetail1, CoreMatchers.startsWith(detail1Actual));
    }

    /**
     * Tests how the long details information is being processed. It is large, but not too large
     * to fit into the DB tables.
     */
    @Test
    public void testLargeDetails() {
        final String details = createLargeString(255);
        final String additionalDetail1 = createLargeString(200);
        final String additionalDetail2 = "asdfasdfasfadsf";
        final ActionInfoModel model = new ActionInfoModel(ActionTypeCase.ACTIVATE, 1L, details,
                Sets.newHashSet(additionalDetail1, additionalDetail2));
        Assert.assertEquals(Optional.of(details), model.getDetails());
        Assert.assertEquals(Optional.of(Sets.newHashSet(additionalDetail1, additionalDetail2)),
                model.getAdditionalDetails());
    }

    @Nonnull
    private String createLargeString(int length) {
        final StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(i % 10);
        }
        return sb.toString();
    }
}
