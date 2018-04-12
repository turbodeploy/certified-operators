package com.vmturbo.action.orchestrator.action;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.action.orchestrator.action.ActionPaginator.ActionPaginatorFactory;
import com.vmturbo.action.orchestrator.action.ActionPaginator.DefaultActionPaginatorFactory;
import com.vmturbo.action.orchestrator.action.ActionPaginator.PaginatedActionViews;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.Builder;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.ActionOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;

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
        final ActionView mockView = newActionView(action -> action.setImportance(1.0));
        final ActionPaginator paginator = paginatorFactory.newPaginator();
        final PaginatedActionViews results =
                paginator.applyPagination(Stream.of(mockView), PaginationParameters.getDefaultInstance());
        assertThat(results.getResults(), containsInAnyOrder(mockView));
    }

    private ActionView newActionView(Consumer<ActionDTO.Action.Builder> recommendationCustomizer) {
        final ActionView actionView = mock(ActionView.class);
        final ActionDTO.Action.Builder recommendationBuilder = ActionDTO.Action.newBuilder()
                .setId(1)
                .setInfo(ActionInfo.getDefaultInstance())
                .setExplanation(Explanation.getDefaultInstance());
        recommendationCustomizer.accept(recommendationBuilder);
        when(actionView.getRecommendation()).thenReturn(recommendationBuilder.build());
        return actionView;
    }

    @Test
    public void testDefaultLimit() {
        final ActionPaginatorFactory smallLimitFactory = new DefaultActionPaginatorFactory(1, 10);
        // Relying on default order - by severity
        final ActionView mockView1 = newActionView(action -> action.setImportance(1.0));
        final ActionView mockView2 = newActionView(action -> action.setImportance(2.0));
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
        final ActionView mockView1 = newActionView(action -> action.setImportance(1.0));
        final ActionView mockView2 = newActionView(action -> action.setImportance(2.0));
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
        final ActionView mockView1 = newActionView(action -> action.setImportance(1.0));
        final ActionView mockView2 = newActionView(action -> action.setImportance(2.0));
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
        final ActionView mockView1 = newActionView(action -> action.setImportance(1.0));
        final ActionView mockView2 = newActionView(action -> action.setImportance(2.0));
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
        final ActionView mockView1 = newActionView(action -> action.setImportance(1.0));
        final ActionView mockView2 = newActionView(action -> action.setImportance(2.0));
        final ActionView mockView3 = newActionView(action -> action.setImportance(3.0));
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
        final ActionView mockView1 = newActionView(action -> action.setImportance(1.0));
        final ActionView mockView2 = newActionView(action -> action.setImportance(2.0));
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
        final ActionView mockView1 = newActionView(action -> action.setImportance(1.0));
        final ActionView mockView2 = newActionView(action -> action.setImportance(2.0));
        final ActionPaginator paginator = paginatorFactory.newPaginator();
        final PaginatedActionViews results = paginator.applyPagination(
                Stream.of(mockView1, mockView2), PaginationParameters.newBuilder()
                        .setLimit(3)
                        .build());
        // Should be in ordered by importance.
        assertThat(results.getResults(), contains(mockView1, mockView2));
        final PaginatedActionViews results2 = paginator.applyPagination(
                Stream.of(mockView1, mockView2), PaginationParameters.newBuilder()
                        .setLimit(3)
                        .setAscending(false)
                        .build());
        assertThat(results2.getResults(), contains(mockView2, mockView1));
    }

    @Test
    public void testOrderByDescending() {
        // Relying on default order - by severity
        final ActionView mockView1 = newActionView(action -> action.setImportance(1.0));
        final ActionView mockView2 = newActionView(action -> action.setImportance(2.0));
        final ActionPaginator paginator = paginatorFactory.newPaginator();
        final PaginatedActionViews results = paginator.applyPagination(
                Stream.of(mockView1, mockView2), PaginationParameters.newBuilder()
                        .setLimit(3)
                        .setAscending(false)
                        .build());
        assertThat(results.getResults(), contains(mockView2, mockView1));
    }

    @Test
    public void testOrderBySeverity() {
        final ActionView mockView1 = newActionView(action -> action.setImportance(1.0));
        final ActionView mockView2 = newActionView(action -> action.setImportance(2.0));
        final ActionPaginator paginator = paginatorFactory.newPaginator();
        final PaginatedActionViews ascendingResults = paginator.applyPagination(
                Stream.of(mockView1, mockView2), PaginationParameters.newBuilder()
                        .setOrderBy(OrderBy.newBuilder()
                                .setAction(ActionOrderBy.ACTION_SEVERITY))
                        .setAscending(true)
                        .build());
        assertThat(ascendingResults.getResults(), contains(mockView1, mockView2));
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
        final ActionView smallerView = newActionView(action -> action
            .setImportance(1.0)
            .setInfo(ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                        .setId(1)
                        .setType(1)))));
        // Activate considered "larger" because it appears later in the oneof.
        final ActionView largerView = newActionView(action -> action
            .setImportance(1.0)
            .setInfo(ActionInfo.newBuilder()
                .setActivate(Activate.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                        .setId(1)
                        .setType(1)))));

        final ActionPaginator paginator = paginatorFactory.newPaginator();
        final PaginatedActionViews ascendingResults = paginator.applyPagination(
                Stream.of(smallerView, largerView), PaginationParameters.newBuilder()
                        .setOrderBy(OrderBy.newBuilder()
                                .setAction(ActionOrderBy.ACTION_TYPE))
                        .setAscending(true)
                        .build());
        assertThat(ascendingResults.getResults(), contains(smallerView, largerView));
        final PaginatedActionViews descendingResults = paginator.applyPagination(
                Stream.of(smallerView, largerView), PaginationParameters.newBuilder()
                        .setOrderBy(OrderBy.newBuilder()
                                .setAction(ActionOrderBy.ACTION_TYPE))
                        .setAscending(false)
                        .build());
        assertThat(descendingResults.getResults(), contains(largerView, smallerView));
    }

    @Test
    public void testOrderByRiskCategory() {
        final ActionInfo resizeInfo = ActionInfo.newBuilder()
            .setResize(Resize.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                            .setId(1)
                            .setType(1)))
            .build();
        final ActionView smallerView = newActionView(action -> action
                .setImportance(1.0)
                .setInfo(resizeInfo));
        when(smallerView.getActionCategory()).thenReturn(ActionCategory.PERFORMANCE_ASSURANCE);
        final ActionView largerView = newActionView(action -> action
                .setImportance(1.0)
                .setInfo(resizeInfo));
        when(largerView.getActionCategory()).thenReturn(ActionCategory.EFFICIENCY_IMPROVEMENT);

        final ActionPaginator paginator = paginatorFactory.newPaginator();
        final PaginatedActionViews ascendingResults = paginator.applyPagination(
                Stream.of(smallerView, largerView), PaginationParameters.newBuilder()
                        .setOrderBy(OrderBy.newBuilder()
                                .setAction(ActionOrderBy.ACTION_RISK_CATEGORY))
                        .setAscending(true)
                        .build());
        assertThat(ascendingResults.getResults(), contains(smallerView, largerView));
        final PaginatedActionViews descendingResults = paginator.applyPagination(
                Stream.of(smallerView, largerView), PaginationParameters.newBuilder()
                        .setOrderBy(OrderBy.newBuilder()
                                .setAction(ActionOrderBy.ACTION_RISK_CATEGORY))
                        .setAscending(false)
                        .build());
        assertThat(descendingResults.getResults(), contains(largerView, smallerView));
    }

}
