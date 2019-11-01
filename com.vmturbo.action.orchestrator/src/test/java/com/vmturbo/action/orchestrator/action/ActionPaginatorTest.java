package com.vmturbo.action.orchestrator.action;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.ActionPaginator.ActionPaginatorFactory;
import com.vmturbo.action.orchestrator.action.ActionPaginator.DefaultActionPaginatorFactory;
import com.vmturbo.action.orchestrator.action.ActionPaginator.PaginatedActionViews;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.ActionOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;

public class ActionPaginatorTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private ActionPaginatorFactory paginatorFactory = new DefaultActionPaginatorFactory(10, 10);

    @Test
    public void testInvalidCursor() {
        final ActionPaginator paginator = paginatorFactory.newPaginator();
        expectedException.expect(IllegalArgumentException.class);
        paginator.applyPagination(Stream.empty(), PaginationParameters.newBuilder()
                .setCursor("INVALID")
                .build());
    }

    @Test
    public void testNoCursor() {
        final ActionView mockView = newActionView(1, ActionCategory.PREVENTION);
        final ActionPaginator paginator = paginatorFactory.newPaginator();
        final PaginatedActionViews results =
                paginator.applyPagination(Stream.of(mockView), PaginationParameters.getDefaultInstance());
        assertThat(results.getResults(), containsInAnyOrder(mockView));
    }

    private ActionView newActionView(final long id, Consumer<ActionDTO.Action.Builder> recommendationCustomizer) {
        final ActionDTO.Action.Builder recommendationBuilder = ActionDTO.Action.newBuilder()
                .setId(id)
                .setInfo(ActionInfo.getDefaultInstance())
                .setExplanation(Explanation.getDefaultInstance())
                .setDeprecatedImportance(0.0);
        recommendationCustomizer.accept(recommendationBuilder);
        final ActionView actionView = ActionOrchestratorTestUtils.mockActionView(
                recommendationBuilder.build());
        when(actionView.getId()).thenReturn(id);
        return actionView;
    }

    @Nonnull
    private ActionView newActionView(final long id, @Nullable final ActionCategory actionCategory) {
        final ActionView actionView = newActionView(id, x -> {});
        when(actionView.getActionCategory()).thenReturn(actionCategory);
        return actionView;
    }

    @Test
    public void testDefaultLimit() {
        final ActionPaginatorFactory smallLimitFactory = new DefaultActionPaginatorFactory(1, 10);
        // Relying on default order - by severity
        final ActionView mockView1 = newActionView(1, ActionCategory.PREVENTION);
        final ActionView mockView2 = newActionView(2, ActionCategory.PERFORMANCE_ASSURANCE);
        final ActionPaginator paginator = smallLimitFactory.newPaginator();
        final PaginatedActionViews results = paginator.applyPagination(
                Stream.of(mockView1, mockView2), PaginationParameters.newBuilder()
                        // Not setting limit - should use default.
                        .build());
        // Should just be one result.
        assertThat(results.getResults(), containsInAnyOrder(mockView1));
    }

    @Test
    public void testMaxLimitExceeded() {
        final ActionPaginatorFactory smallLimitFactory = new DefaultActionPaginatorFactory(1, 1);
        // Relying on default order - by severity
        final ActionView mockView1 = newActionView(1, ActionCategory.PREVENTION);
        final ActionView mockView2 = newActionView(2, ActionCategory.PERFORMANCE_ASSURANCE);
        final ActionPaginator paginator = smallLimitFactory.newPaginator();
        final PaginatedActionViews results = paginator.applyPagination(
                Stream.of(mockView1, mockView2), PaginationParameters.newBuilder()
                        // Exceeding maximum
                        .setLimit(2)
                        .build());
        // Should just be one result.
        assertThat(results.getResults(), containsInAnyOrder(mockView1));
    }

    @Test
    public void testLimit() {
        // Relying on default order - by severity
        final ActionView mockView1 = newActionView(1, ActionCategory.PREVENTION);
        final ActionView mockView2 = newActionView(2, ActionCategory.PERFORMANCE_ASSURANCE);
        final ActionPaginator paginator = paginatorFactory.newPaginator();
        final PaginatedActionViews results = paginator.applyPagination(
                Stream.of(mockView1, mockView2), PaginationParameters.newBuilder()
                        .setLimit(1)
                        .build());
        assertThat(results.getResults(), containsInAnyOrder(mockView1));
    }

    @Test
    public void testNextCursor() {
        // Relying on default order - by severity
        final ActionView mockView1 = newActionView(1, ActionCategory.PREVENTION);
        final ActionView mockView2 = newActionView(2, ActionCategory.PERFORMANCE_ASSURANCE);
        final ActionPaginator paginator = paginatorFactory.newPaginator();
        final PaginatedActionViews results = paginator.applyPagination(
                Stream.of(mockView1, mockView2), PaginationParameters.newBuilder()
                    .setLimit(1)
                    .build());
        assertThat(results.getResults(), containsInAnyOrder(mockView1));
        assertThat(results.getNextCursor(), is(Optional.of("1")));
    }

    @Test
    public void testNextNextCursor() {
        // Relying on default order - by severity
        final ActionView mockView1 = newActionView(1, ActionCategory.EFFICIENCY_IMPROVEMENT);
        final ActionView mockView2 = newActionView(2, ActionCategory.PREVENTION);
        final ActionView mockView3 = newActionView(3, ActionCategory.PERFORMANCE_ASSURANCE);
        final ActionPaginator paginator = paginatorFactory.newPaginator();
        // Relying on initial next cursor working properly.
        final String nextCursor = paginator.applyPagination(
            Stream.of(mockView1, mockView2, mockView3),
            PaginationParameters.newBuilder()
                .setLimit(1)
                .build()).getNextCursor().get();

        final PaginatedActionViews results = paginator.applyPagination(
            Stream.of(mockView1, mockView2, mockView3),
            PaginationParameters.newBuilder()
                .setLimit(1)
                .setCursor(nextCursor)
                .build());
        assertThat(results.getResults(), containsInAnyOrder(mockView2));
        assertThat(results.getNextCursor(), is(Optional.of("2")));
    }

    @Test
    public void testNoMoreResultsCursor() {
        // Relying on default order - by severity
        final ActionView mockView1 = newActionView(1, ActionCategory.EFFICIENCY_IMPROVEMENT);
        final ActionView mockView2 = newActionView(2, ActionCategory.PERFORMANCE_ASSURANCE);
        final ActionPaginator paginator = paginatorFactory.newPaginator();
        final PaginatedActionViews results = paginator.applyPagination(
                Stream.of(mockView1, mockView2), PaginationParameters.newBuilder()
                        .setLimit(3)
                        .build());
        assertThat(results.getResults(), containsInAnyOrder(mockView1, mockView2));
        assertThat(results.getNextCursor(), is(Optional.empty()));
    }

    @Test
    public void testDefaultOrderBy() {
        final ActionView mockView1 = newActionView(1, ActionCategory.PREVENTION);
        final ActionView mockView1LargerId = newActionView(4, ActionCategory.PREVENTION);
        final ActionView mockView2 = newActionView(3, ActionCategory.PERFORMANCE_ASSURANCE);
        final ActionPaginator paginator = paginatorFactory.newPaginator();
        final PaginatedActionViews results = paginator.applyPagination(
                Stream.of(mockView1, mockView2, mockView1LargerId), PaginationParameters.newBuilder()
                        .setLimit(3)
                        .build());
        // Should be in ordered by importance.
        assertThat(results.getResults(), contains(mockView1, mockView1LargerId, mockView2));
        final PaginatedActionViews results2 = paginator.applyPagination(
                Stream.of(mockView1, mockView2, mockView1LargerId), PaginationParameters.newBuilder()
                        .setLimit(3)
                        .setAscending(false)
                        .build());
        assertThat(results2.getResults(), contains(mockView2, mockView1LargerId, mockView1));
    }

    @Test
    public void testOrderBySeverity() {
        final ActionView mockView1 = newActionView(1, ActionCategory.EFFICIENCY_IMPROVEMENT);
        final ActionView mockView1LargerId = newActionView(2, ActionCategory.EFFICIENCY_IMPROVEMENT);
        final ActionView mockView2 = newActionView(3, ActionCategory.PERFORMANCE_ASSURANCE);
        final ActionPaginator paginator = paginatorFactory.newPaginator();
        final PaginatedActionViews ascendingResults = paginator.applyPagination(
                Stream.of(mockView1, mockView2, mockView1LargerId), PaginationParameters.newBuilder()
                        .setOrderBy(OrderBy.newBuilder()
                                .setAction(ActionOrderBy.ACTION_SEVERITY))
                        .setAscending(true)
                        .build());
        assertThat(ascendingResults.getResults(), contains(mockView1, mockView1LargerId, mockView2));
        final PaginatedActionViews descendingResults = paginator.applyPagination(
                Stream.of(mockView1, mockView2), PaginationParameters.newBuilder()
                        .setOrderBy(OrderBy.newBuilder()
                                .setAction(ActionOrderBy.ACTION_SEVERITY))
                        .setAscending(false)
                        .build());
        assertThat(descendingResults.getResults(), contains(mockView2, mockView1));
    }

    @Test
    public void testOrderByDetails() {
        final ActionInfo info = ActionInfo.newBuilder()
            .setMove(Move.newBuilder()
                .setTarget(ActionEntity.newBuilder()
                    .setId(1)
                    .setType(1)))
            .build();

        final ActionView smallerView = newActionView(1, action -> action.setInfo(info));
        final ActionView smallerViewLargerId = newActionView(2, action -> action.setInfo(info));
        final ActionView largerView = newActionView(3, action -> action.setInfo(info));

        final String smallerDesc = "Move VM 1 from A to B";
        final String largerDesc = "Start VM 2 on PM 1";
        when(smallerView.getDescription()).thenReturn(smallerDesc);
        // Same description.
        when(smallerViewLargerId.getDescription()).thenReturn(smallerDesc);

        when(largerView.getDescription()).thenReturn(largerDesc);
        final ActionPaginator paginator = paginatorFactory.newPaginator();
        final PaginatedActionViews ascendingResults = paginator.applyPagination(
                Stream.of(smallerView, largerView, smallerViewLargerId), PaginationParameters.newBuilder()
                        .setOrderBy(OrderBy.newBuilder()
                                .setAction(ActionOrderBy.ACTION_NAME))
                        .setAscending(true)
                        .build());
        assertThat(ascendingResults.getResults(), contains(smallerView, smallerViewLargerId, largerView));
        final PaginatedActionViews descendingResults = paginator.applyPagination(
                Stream.of(smallerView, largerView, smallerViewLargerId), PaginationParameters.newBuilder()
                        .setOrderBy(OrderBy.newBuilder()
                                .setAction(ActionOrderBy.ACTION_NAME))
                        .setAscending(false)
                        .build());
        assertThat(descendingResults.getResults(), contains(largerView, smallerViewLargerId, smallerView));
    }

    @Test
    public void testOrderByRiskCategory() {
        final ActionInfo resizeInfo = ActionInfo.newBuilder()
            .setResize(Resize.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                            .setId(1)
                            .setType(1)))
            .build();
        final ActionView smallerView = newActionView(1, action -> action.setInfo(resizeInfo));
        // Action category name is sorted alphabetically
        final ActionCategory smallerCat = ActionCategory.EFFICIENCY_IMPROVEMENT;
        final ActionCategory largerCat = ActionCategory.PERFORMANCE_ASSURANCE;
        when(smallerView.getActionCategory()).thenReturn(smallerCat);
        final ActionView smallerViewLargerId = newActionView(2, action -> action.setInfo(resizeInfo));
        when(smallerViewLargerId.getActionCategory()).thenReturn(smallerCat);
        final ActionView largerView = newActionView(3, action -> action.setInfo(resizeInfo));
        when(largerView.getActionCategory()).thenReturn(largerCat);

        final ActionPaginator paginator = paginatorFactory.newPaginator();
        final PaginatedActionViews ascendingResults = paginator.applyPagination(
            Stream.of(smallerView, largerView, smallerViewLargerId), PaginationParameters.newBuilder()
                .setOrderBy(OrderBy.newBuilder()
                    .setAction(ActionOrderBy.ACTION_RISK_CATEGORY))
                .setAscending(true)
                .build());
        assertThat(ascendingResults.getResults(), contains(smallerView, smallerViewLargerId, largerView));
        final PaginatedActionViews descendingResults = paginator.applyPagination(
            Stream.of(smallerViewLargerId, smallerView, largerView), PaginationParameters.newBuilder()
                .setOrderBy(OrderBy.newBuilder()
                    .setAction(ActionOrderBy.ACTION_RISK_CATEGORY))
                .setAscending(false)
                .build());
        assertThat(descendingResults.getResults(), contains(largerView, smallerViewLargerId, smallerView));
    }

    @Test
    public void testOrderBySavings() {
        final ActionInfo resizeInfo = ActionInfo.newBuilder()
            .setResize(Resize.newBuilder()
                .setTarget(ActionEntity.newBuilder()
                    .setId(1)
                    .setType(1)))
            .build();
        final ActionView smallerView = newActionView(1, action -> action
            .setSavingsPerHour(CurrencyAmount.newBuilder()
                .setAmount(1))
            .setInfo(resizeInfo));
        final ActionView smallerViewLargerId = newActionView(2, action -> action
            .setSavingsPerHour(CurrencyAmount.newBuilder()
                .setAmount(1))
            .setInfo(resizeInfo));
        final ActionView largerView = newActionView(3, action -> action
            .setSavingsPerHour(CurrencyAmount.newBuilder()
                .setAmount(2))
            .setInfo(resizeInfo));

        final ActionPaginator paginator = paginatorFactory.newPaginator();
        final PaginatedActionViews ascendingResults = paginator.applyPagination(
            Stream.of(smallerView, largerView, smallerViewLargerId), PaginationParameters.newBuilder()
                .setOrderBy(OrderBy.newBuilder()
                    .setAction(ActionOrderBy.ACTION_SAVINGS))
                .setAscending(true)
                .build());
        assertThat(ascendingResults.getResults(), contains(smallerView, smallerViewLargerId, largerView));
        final PaginatedActionViews descendingResults = paginator.applyPagination(
            Stream.of(smallerView, largerView, smallerViewLargerId), PaginationParameters.newBuilder()
                .setOrderBy(OrderBy.newBuilder()
                    .setAction(ActionOrderBy.ACTION_SAVINGS))
                .setAscending(false)
                .build());
        assertThat(descendingResults.getResults(), contains(largerView, smallerViewLargerId, smallerView));
    }

    @Test
    public void testOrderByRecommendationDate() {
        final ActionInfo resizeInfo = ActionInfo.newBuilder()
            .setResize(Resize.newBuilder()
                .setTarget(ActionEntity.newBuilder()
                    .setId(1)
                    .setType(1)))
            .build();
        final ActionView smallerView = newActionView(1, action -> action
            .setInfo(resizeInfo));
        LocalDateTime smallerTime = LocalDateTime.of(2007, 7, 7, 7, 7);
        LocalDateTime largerTime = LocalDateTime.of(2008, 8, 8, 8, 8);
        when(smallerView.getRecommendationTime()).thenReturn(smallerTime);
        final ActionView smallerViewLargerId = newActionView(2, action -> action
            .setInfo(resizeInfo));
        when(smallerViewLargerId.getRecommendationTime()).thenReturn(smallerTime);
        final ActionView largerView = newActionView(3, action -> action
            .setInfo(resizeInfo));
        when(largerView.getRecommendationTime()).thenReturn(largerTime);

        final ActionPaginator paginator = paginatorFactory.newPaginator();
        final PaginatedActionViews ascendingResults = paginator.applyPagination(
            Stream.of(smallerView, largerView, smallerViewLargerId), PaginationParameters.newBuilder()
                .setOrderBy(OrderBy.newBuilder()
                    .setAction(ActionOrderBy.ACTION_RECOMMENDATION_TIME))
                .setAscending(true)
                .build());
        assertThat(ascendingResults.getResults(), contains(smallerView, smallerViewLargerId, largerView));
        final PaginatedActionViews descendingResults = paginator.applyPagination(
            Stream.of(smallerViewLargerId, smallerView, largerView), PaginationParameters.newBuilder()
                .setOrderBy(OrderBy.newBuilder()
                    .setAction(ActionOrderBy.ACTION_RECOMMENDATION_TIME))
                .setAscending(false)
                .build());
        assertThat(descendingResults.getResults(), contains(largerView, smallerViewLargerId, smallerView));
    }
}
