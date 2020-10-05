package com.vmturbo.repository.plan.db;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.Optional;

import org.junit.Test;

import com.vmturbo.common.protobuf.repository.RepositoryDTO.EntityFilter;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RequestDetails;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyEntityFilter;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.repository.plan.db.PlanEntityFilter.PlanEntityFilterConverter;

/**
 * Unit tests for the {@link PlanEntityFilterConverter}.
 */
public class PlanEntityFilterTest {

    private PlanEntityFilterConverter converter = new PlanEntityFilterConverter();

    /**
     * Conversion with a {@link TopologyEntityFilter}.
     */
    @Test
    public void testTopologyEntityFilterConversion() {
        TopologyEntityFilter filter = TopologyEntityFilter.getDefaultInstance();
        PlanEntityFilter ret = converter.newPlanFilter(filter);
        assertThat(ret.getTargetEntities(), is(Optional.empty()));
        assertThat(ret.getTargetTypes(), is(Optional.empty()));
        assertThat(ret.getUnplacedOnly(), is(false));
    }

    /**
     * Conversion with a {@link TopologyEntityFilter} with an entity type restriction.
     */
    @Test
    public void testTopologyEntityFilterConversionWithTypes() {
        TopologyEntityFilter filter = TopologyEntityFilter.newBuilder()
                .addEntityTypes(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                .build();
        PlanEntityFilter ret = converter.newPlanFilter(filter);
        assertThat(ret.getTargetEntities(), is(Optional.empty()));
        assertThat(ret.getTargetTypes(), is(Optional.of(Collections.singleton((short)ApiEntityType.VIRTUAL_MACHINE.typeNumber()))));
        assertThat(ret.getUnplacedOnly(), is(false));
    }

    /**
     * Conversion with a {@link TopologyEntityFilter} with an "unplaced only" restriction.
     */
    @Test
    public void testTopologyEntityFilterConversionUnplacedOnly() {
        TopologyEntityFilter filter = TopologyEntityFilter.newBuilder()
            .setUnplacedOnly(true)
            .build();
        PlanEntityFilter ret = converter.newPlanFilter(filter);
        assertThat(ret.getTargetEntities(), is(Optional.empty()));
        assertThat(ret.getTargetTypes(), is(Optional.empty()));
        assertThat(ret.getUnplacedOnly(), is(true));
    }

    /**
     * Conversion with a {@link RequestDetails}.
     */
    @Test
    public void testRequestDetailsConversion() {
        RequestDetails requestDetails = RequestDetails.newBuilder()
                .build();
        PlanEntityFilter ret = converter.newPlanFilter(requestDetails);
        assertThat(ret.getTargetEntities(), is(Optional.empty()));
        assertThat(ret.getTargetTypes(), is(Optional.empty()));
        assertThat(ret.getUnplacedOnly(), is(false));
    }

    /**
     * Conversion with a {@link RequestDetails} with a type restriction.
     */
    @Test
    public void testRequestDetailsConversionWithType() {
        RequestDetails requestDetails = RequestDetails.newBuilder()
                .setRelatedEntityType(ApiEntityType.VIRTUAL_MACHINE.apiStr())
                .build();
        PlanEntityFilter ret = converter.newPlanFilter(requestDetails);
        assertThat(ret.getTargetEntities(), is(Optional.empty()));
        assertThat(ret.getTargetTypes(), is(Optional.of(Collections.singleton((short)ApiEntityType.VIRTUAL_MACHINE.typeNumber()))));
        assertThat(ret.getUnplacedOnly(), is(false));
    }

    /**
     * Conversion with a {@link RequestDetails} with an oid restriction.
     */
    @Test
    public void testRequestDetailsConversionWithTargetIds() {
        RequestDetails requestDetails = RequestDetails.newBuilder()
                .setEntityFilter(EntityFilter.newBuilder()
                    .addEntityIds(1L))
                .build();
        PlanEntityFilter ret = converter.newPlanFilter(requestDetails);
        assertThat(ret.getTargetEntities(), is(Optional.of(Collections.singleton(1L))));
        assertThat(ret.getTargetTypes(), is(Optional.empty()));
        assertThat(ret.getUnplacedOnly(), is(false));
    }

    /**
     * Conversion with a {@link RetrieveTopologyEntitiesRequest}.
     */
    @Test
    public void testRetrieveTopologyEntitiesRequestConversion() {
        RetrieveTopologyEntitiesRequest requestDetails = RetrieveTopologyEntitiesRequest.newBuilder()
                .build();
        PlanEntityFilter ret = converter.newPlanFilter(requestDetails);
        assertThat(ret.getTargetEntities(), is(Optional.empty()));
        assertThat(ret.getTargetTypes(), is(Optional.empty()));
        assertThat(ret.getUnplacedOnly(), is(false));
    }

    /**
     * Conversion with a {@link RetrieveTopologyEntitiesRequest} with an entity type restriction.
     */
    @Test
    public void testRetrieveTopologyEntitiesRequestConversionWithTypes() {
        RetrieveTopologyEntitiesRequest requestDetails = RetrieveTopologyEntitiesRequest.newBuilder()
                .addEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                .build();
        PlanEntityFilter ret = converter.newPlanFilter(requestDetails);
        assertThat(ret.getTargetEntities(), is(Optional.empty()));
        assertThat(ret.getTargetTypes(), is(Optional.of(Collections.singleton((short)ApiEntityType.VIRTUAL_MACHINE.typeNumber()))));
        assertThat(ret.getUnplacedOnly(), is(false));
    }

    /**
     * Conversion with a {@link RetrieveTopologyEntitiesRequest} with an id restriction.
     */
    @Test
    public void testRetrieveTopologyEntitiesRequestConversionWithIds() {
        RetrieveTopologyEntitiesRequest requestDetails = RetrieveTopologyEntitiesRequest.newBuilder()
                .addEntityOids(123L)
                .build();
        PlanEntityFilter ret = converter.newPlanFilter(requestDetails);
        assertThat(ret.getTargetEntities(), is(Optional.of(Collections.singleton(123L))));
        assertThat(ret.getTargetTypes(), is(Optional.empty()));
        assertThat(ret.getUnplacedOnly(), is(false));
    }

}