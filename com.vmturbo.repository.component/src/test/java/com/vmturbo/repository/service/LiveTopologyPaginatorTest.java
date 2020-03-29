package com.vmturbo.repository.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.SearchOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;
import com.vmturbo.repository.service.LiveTopologyPaginator.PaginatedResults;

public class LiveTopologyPaginatorTest {

    private final RepoGraphEntity host1 = RepoGraphEntity.newBuilder(TopologyEntityDTO.newBuilder()
        .setOid(3)
        .setEntityType(ApiEntityType.PHYSICAL_MACHINE.typeNumber())
        .setDisplayName("c")
        .build()).build();

    private final RepoGraphEntity host2 = RepoGraphEntity.newBuilder(TopologyEntityDTO.newBuilder()
        .setOid(2)
        .setEntityType(ApiEntityType.PHYSICAL_MACHINE.typeNumber())
        .setDisplayName("b")
        .build()).build();

    private final RepoGraphEntity vm = RepoGraphEntity.newBuilder(TopologyEntityDTO.newBuilder()
        .setOid(1)
        .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
        .setDisplayName("a")
        .build()).build();

    @Test
    public void testPaginatorFullIterationAscending() {
        List<RepoGraphEntity> entities = Arrays.asList(host2, host1, vm);
        final PaginationParameters paginationParameters = PaginationParameters.newBuilder()
            .setLimit(2)
            .setAscending(true)
            .setOrderBy(OrderBy.newBuilder()
                .setSearch(SearchOrderBy.ENTITY_NAME))
            .build();

        final LiveTopologyPaginator liveTopologyPaginator = new LiveTopologyPaginator(5, 5);
        final List<RepoGraphEntity> paginatedEntities = new ArrayList<>();
        String nextCursor = "";
        do {
            PaginatedResults results = liveTopologyPaginator.paginate(entities.stream(), paginationParameters.toBuilder()
                .setCursor(nextCursor)
                .build());
            nextCursor = results.paginationResponse().getNextCursor();
            paginatedEntities.addAll(results.nextPageEntities());
        } while (!StringUtils.isEmpty(nextCursor));

        assertThat(paginatedEntities, contains(vm, host2, host1));
    }

    @Test
    public void testPaginatorFullIterationDescending() {
        List<RepoGraphEntity> entities = Arrays.asList(host2, host1, vm);
        final PaginationParameters paginationParameters = PaginationParameters.newBuilder()
            .setLimit(2)
            .setAscending(false)
            .setOrderBy(OrderBy.newBuilder()
                .setSearch(SearchOrderBy.ENTITY_NAME))
            .build();

        final LiveTopologyPaginator liveTopologyPaginator = new LiveTopologyPaginator(5, 5);
        final List<RepoGraphEntity> paginatedEntities = new ArrayList<>();
        String nextCursor = "";
        do {
            PaginatedResults results = liveTopologyPaginator.paginate(entities.stream(), paginationParameters.toBuilder()
                .setCursor(nextCursor)
                .build());
            nextCursor = results.paginationResponse().getNextCursor();
            paginatedEntities.addAll(results.nextPageEntities());
        } while (!StringUtils.isEmpty(nextCursor));

        assertThat(paginatedEntities, contains(host1, host2, vm));
    }

    @Test
    public void testPaginatorDefaultLimit() {
        List<RepoGraphEntity> entities = Arrays.asList(host2, host1, vm);
        final PaginationParameters paginationParameters = PaginationParameters.newBuilder()
            // No limit set
            .setAscending(true)
            .setOrderBy(OrderBy.newBuilder()
                .setSearch(SearchOrderBy.ENTITY_NAME))
            .build();

        // Default limit is 2
        final int defaultLimit = 2;
        final LiveTopologyPaginator liveTopologyPaginator = new LiveTopologyPaginator(defaultLimit, 5);
        assertThat(liveTopologyPaginator.paginate(entities.stream(), paginationParameters)
            .nextPageEntities().size(), is(defaultLimit));
    }

    @Test
    public void testPaginatorEnforceMaxLimit() {
        List<RepoGraphEntity> entities = Arrays.asList(host2, host1, vm);
        final PaginationParameters paginationParameters = PaginationParameters.newBuilder()
            .setLimit(100)
            .setAscending(true)
            .setOrderBy(OrderBy.newBuilder()
                .setSearch(SearchOrderBy.ENTITY_NAME))
            .build();

        // Max limit is 2
        final int maxLimit = 2;
        final LiveTopologyPaginator liveTopologyPaginator = new LiveTopologyPaginator(1, maxLimit);
        assertThat(liveTopologyPaginator.paginate(entities.stream(), paginationParameters)
            .nextPageEntities().size(), is(maxLimit));
    }

    @Test
    public void testPaginatorStableSort() {
        // Two entities with the same name should be sorted by ID.
        final RepoGraphEntity vm2 = RepoGraphEntity.newBuilder(TopologyEntityDTO.newBuilder()
            .setOid(vm.getOid() + 1)
            .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
            .setDisplayName(vm.getDisplayName())
            .build()).build();
        final RepoGraphEntity vm3 = RepoGraphEntity.newBuilder(TopologyEntityDTO.newBuilder()
            .setOid(vm.getOid() + 2)
            .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
            .setDisplayName(vm.getDisplayName())
            .build()).build();
        final List<RepoGraphEntity> entities = Arrays.asList(vm2, vm, vm3);
        final LiveTopologyPaginator liveTopologyPaginator = new LiveTopologyPaginator(5, 5);
        List<RepoGraphEntity> page = liveTopologyPaginator.paginate(entities.stream(), PaginationParameters.newBuilder()
            .setAscending(true)
            .setOrderBy(OrderBy.newBuilder()
                .setSearch(SearchOrderBy.ENTITY_NAME))
            .build()).nextPageEntities();

        assertThat(page, contains(vm, vm2, vm3));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPaginatorInvalidCursor() {
        final PaginationParameters paginationParameters = PaginationParameters.newBuilder()
            .setCursor("foo")
            .build();
        final LiveTopologyPaginator liveTopologyPaginator = new LiveTopologyPaginator(1, 1);
        liveTopologyPaginator.paginate(Stream.of(vm), paginationParameters);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPaginatorInvalidCursorNegativeNumber() {
        final PaginationParameters paginationParameters = PaginationParameters.newBuilder()
            .setCursor("-1")
            .build();
        final LiveTopologyPaginator liveTopologyPaginator = new LiveTopologyPaginator(1, 1);
        liveTopologyPaginator.paginate(Stream.of(vm), paginationParameters);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPaginatorInvalidLimit() {
        final PaginationParameters paginationParameters = PaginationParameters.newBuilder()
            .setLimit(-1)
            .build();
        final LiveTopologyPaginator liveTopologyPaginator = new LiveTopologyPaginator(1, 1);
        liveTopologyPaginator.paginate(Stream.of(vm), paginationParameters);
    }

}
