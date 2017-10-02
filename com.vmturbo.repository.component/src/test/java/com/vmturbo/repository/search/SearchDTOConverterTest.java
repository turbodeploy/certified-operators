package com.vmturbo.repository.search;

import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;

import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("unchecked")
public class SearchDTOConverterTest {

    private static final String CAPACITY = "capacity";
    private static final String ENTITY_TYPE = "entityType";
    private static final String VIRTUAL_MACHINE = "VirtualMachine";
    private static final String DISPLAY_NAME = "displayName";
    private static final String FOO = "FOO";

    private static Search.PropertyFilter.Builder ENTITY_TYPE_VM = Search.PropertyFilter.newBuilder()
            .setPropertyName(ENTITY_TYPE)
            .setStringFilter(Search.PropertyFilter.StringFilter.newBuilder()
                    .setStringPropertyRegex(VIRTUAL_MACHINE)
                    .build());

    private static Search.PropertyFilter.Builder DISPLAY_NAME_FOO = Search.PropertyFilter.newBuilder()
            .setPropertyName(DISPLAY_NAME)
            .setStringFilter(Search.PropertyFilter.StringFilter.newBuilder()
                    .setStringPropertyRegex(FOO)
                    .build());

    private static Search.PropertyFilter.Builder CAPACITY_GTE_TWO = Search.PropertyFilter.newBuilder()
            .setPropertyName(CAPACITY)
            .setNumericFilter(Search.PropertyFilter.NumericFilter.newBuilder()
                    .setComparisonOperator(Search.ComparisonOperator.GTE)
                    .setValue(2L)
                    .build());

    private static Search.SearchFilter.TraversalFilter.Builder TWO_HOPS_TRAVERSAL = Search.SearchFilter.TraversalFilter.newBuilder()
            .setTraversalDirection(Search.SearchFilter.TraversalFilter.TraversalDirection.PRODUCES)
            .setStoppingCondition(Search.SearchFilter.TraversalFilter.StoppingCondition.newBuilder()
                    .setNumberHops(2).build());

    private static Search.SearchFilter.TraversalFilter.Builder CAPACITY_COND_TRAVERSAL = Search.SearchFilter.TraversalFilter.newBuilder()
            .setTraversalDirection(Search.SearchFilter.TraversalFilter.TraversalDirection.CONSUMES)
            .setStoppingCondition(Search.SearchFilter.TraversalFilter.StoppingCondition.newBuilder()
                    .setStoppingPropertyFilter(CAPACITY_GTE_TWO).build());

    @Test
    public void testStartingFilterConversion() throws Throwable {
        final SearchParameters searchParameters = SearchParameters.newBuilder()
                .setStartingFilter(ENTITY_TYPE_VM)
                .build();

        final List<AQLRepr> aqlReprs = SearchDTOConverter.toAqlRepr(searchParameters);

        assertThat(aqlReprs)
                .hasSize(1)
                .containsExactly(AQLRepr
                        .fromFilters(Filter.stringPropertyFilter(ENTITY_TYPE, Filter.StringOperator.REGEX, VIRTUAL_MACHINE)));
    }

    @Test
    public void testMultiple() throws Throwable {
        final SearchParameters searchParameters = SearchParameters.newBuilder()
                .setStartingFilter(ENTITY_TYPE_VM)
                .addSearchFilter(Search.SearchFilter.newBuilder().setPropertyFilter(DISPLAY_NAME_FOO).build())
                .build();

        final List<AQLRepr> aqlReprs = SearchDTOConverter.toAqlRepr(searchParameters);

        assertThat(aqlReprs)
                .hasSize(2)
                .containsExactly(
                        AQLRepr.fromFilters(Filter.stringPropertyFilter(ENTITY_TYPE, Filter.StringOperator.REGEX, VIRTUAL_MACHINE)),
                        AQLRepr.fromFilters(Filter.stringPropertyFilter(DISPLAY_NAME, Filter.StringOperator.REGEX, FOO))
                );
    }

    @Test
    public void testMultipleTypes() throws Throwable {
        final SearchParameters searchParameters = SearchParameters.newBuilder()
                .setStartingFilter(ENTITY_TYPE_VM)
                .addSearchFilter(Search.SearchFilter.newBuilder().setPropertyFilter(DISPLAY_NAME_FOO).build())
                .addSearchFilter(Search.SearchFilter.newBuilder().setTraversalFilter(TWO_HOPS_TRAVERSAL).build())
                .addSearchFilter(Search.SearchFilter.newBuilder().setPropertyFilter(CAPACITY_GTE_TWO).build())
                .addSearchFilter(Search.SearchFilter.newBuilder().setTraversalFilter(CAPACITY_COND_TRAVERSAL).build())
                .build();

        final List<AQLRepr> aqlReprs = SearchDTOConverter.toAqlRepr(searchParameters);

        assertThat(aqlReprs)
                .hasSize(5)
                .containsExactly(
                        AQLRepr.fromFilters(Filter.stringPropertyFilter(ENTITY_TYPE, Filter.StringOperator.REGEX, VIRTUAL_MACHINE)),
                        AQLRepr.fromFilters(Filter.stringPropertyFilter(DISPLAY_NAME, Filter.StringOperator.REGEX, FOO)),
                        AQLRepr.fromFilters(Filter.traversalHopFilter(Filter.TraversalDirection.CONSUMER, 2)),
                        AQLRepr.fromFilters(Filter.numericPropertyFilter(CAPACITY, Filter.NumericOperator.GTE, 2L)),
                        AQLRepr.fromFilters(Filter.traversalCondFilter(Filter.TraversalDirection.PROVIDER,
                                Filter.numericPropertyFilter(CAPACITY, Filter.NumericOperator.GTE, 2L)))
                );
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingValue() throws Throwable {
        final Search.PropertyFilter missingValueFilter = Search.PropertyFilter.newBuilder()
                .setPropertyName(CAPACITY)
                .setNumericFilter(Search.PropertyFilter.NumericFilter.newBuilder()
                        .setComparisonOperator(Search.ComparisonOperator.LT)
                        .build())
                .build();
        final SearchParameters searchParameters = SearchParameters.newBuilder()
                .setStartingFilter(ENTITY_TYPE_VM)
                .addSearchFilter(Search.SearchFilter.newBuilder().setPropertyFilter(DISPLAY_NAME_FOO).build())
                .addSearchFilter(Search.SearchFilter.newBuilder().setPropertyFilter(missingValueFilter).build())
                .build();

        SearchDTOConverter.toAqlRepr(searchParameters);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingNumOp() throws Throwable {
        final Search.PropertyFilter missingNumOpFilter = Search.PropertyFilter.newBuilder()
                .setPropertyName(CAPACITY)
                .setNumericFilter(Search.PropertyFilter.NumericFilter.newBuilder()
                        .setValue(20L)
                        .build())
                .build();
        final SearchParameters searchParameters = SearchParameters.newBuilder()
                .setStartingFilter(ENTITY_TYPE_VM)
                .addSearchFilter(Search.SearchFilter.newBuilder().setTraversalFilter(TWO_HOPS_TRAVERSAL).build())
                .addSearchFilter(Search.SearchFilter.newBuilder().setPropertyFilter(missingNumOpFilter).build())
                .build();

        SearchDTOConverter.toAqlRepr(searchParameters);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingFilter() throws Throwable {
        final Search.PropertyFilter missingFilter = Search.PropertyFilter.newBuilder()
                .setPropertyName(CAPACITY)
                .build();
        final SearchParameters searchParameters = SearchParameters.newBuilder()
                .setStartingFilter(ENTITY_TYPE_VM)
                .addSearchFilter(Search.SearchFilter.newBuilder().setTraversalFilter(TWO_HOPS_TRAVERSAL).build())
                .addSearchFilter(Search.SearchFilter.newBuilder().setPropertyFilter(missingFilter).build())
                .build();

        SearchDTOConverter.toAqlRepr(searchParameters);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingTraversalDirection() throws Throwable {
        final Search.SearchFilter.TraversalFilter missingDirection = Search.SearchFilter.TraversalFilter.newBuilder()
                .setStoppingCondition(Search.SearchFilter.TraversalFilter.StoppingCondition.newBuilder()
                        .setStoppingPropertyFilter(CAPACITY_GTE_TWO)
                        .build())
                .build();

        final SearchParameters searchParameters = SearchParameters.newBuilder()
                .setStartingFilter(ENTITY_TYPE_VM)
                .addSearchFilter(Search.SearchFilter.newBuilder().setTraversalFilter(TWO_HOPS_TRAVERSAL).build())
                .addSearchFilter(Search.SearchFilter.newBuilder().setTraversalFilter(missingDirection).build())
                .build();

        SearchDTOConverter.toAqlRepr(searchParameters);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingTraversalStoppingCond() throws Throwable {
        final Search.SearchFilter.TraversalFilter missingStoppingCond = Search.SearchFilter.TraversalFilter.newBuilder()
                .setTraversalDirection(Search.SearchFilter.TraversalFilter.TraversalDirection.CONSUMES)
                .build();

        final SearchParameters searchParameters = SearchParameters.newBuilder()
                .setStartingFilter(ENTITY_TYPE_VM)
                .addSearchFilter(Search.SearchFilter.newBuilder().setTraversalFilter(TWO_HOPS_TRAVERSAL).build())
                .addSearchFilter(Search.SearchFilter.newBuilder().setTraversalFilter(missingStoppingCond).build())
                .build();

        SearchDTOConverter.toAqlRepr(searchParameters);
    }

    @Test(expected = NullPointerException.class)
    public void testToSearchEntityNPE() {
        final ServiceEntityRepoDTO serviceEntityRepoDTO = null;
        SearchDTOConverter.toSearchEntity(serviceEntityRepoDTO);
    }

    @Test
    public void testToSearchEntityVM() {
        final String name = "vm-foo";
        final Long oid = 123L;
        ServiceEntityRepoDTO serviceEntityRepoDTO = new ServiceEntityRepoDTO();
        serviceEntityRepoDTO.setDisplayName(name);
        serviceEntityRepoDTO.setEntityType("VirtualMachine");
        serviceEntityRepoDTO.setState("ACTIVE");
        serviceEntityRepoDTO.setOid(oid.toString());

        Search.Entity entity = SearchDTOConverter.toSearchEntity(serviceEntityRepoDTO);

        assertEquals(name, entity.getDisplayName());
        assertEquals(EntityDTO.EntityType.VIRTUAL_MACHINE.getNumber(), entity.getType());
        assertEquals(TopologyDTO.EntityState.POWERED_ON.getNumber(), entity.getState());
        assertEquals((long)oid, entity.getOid());
    }

    @Test
    public void testToSearchEntityPM() {
        final String name = "pm-foo";
        final Long oid = 456L;
        ServiceEntityRepoDTO serviceEntityRepoDTO = new ServiceEntityRepoDTO();
        serviceEntityRepoDTO.setDisplayName(name);
        serviceEntityRepoDTO.setEntityType("PhysicalMachine");
        serviceEntityRepoDTO.setState("SUSPENDED");
        serviceEntityRepoDTO.setOid(oid.toString());

        Search.Entity entity = SearchDTOConverter.toSearchEntity(serviceEntityRepoDTO);

        assertEquals(name, entity.getDisplayName());
        assertEquals(EntityDTO.EntityType.PHYSICAL_MACHINE.getNumber(), entity.getType());
        assertEquals(TopologyDTO.EntityState.SUSPENDED.getNumber(), entity.getState());
        assertEquals((long)oid, entity.getOid());
    }
}