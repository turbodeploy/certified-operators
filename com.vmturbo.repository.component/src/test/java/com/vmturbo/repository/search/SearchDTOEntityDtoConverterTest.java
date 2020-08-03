package com.vmturbo.repository.search;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;

@SuppressWarnings("unchecked")
public class SearchDTOEntityDtoConverterTest {

    private static final String CAPACITY = "capacity";
    private static final String ENTITY_TYPE = "entityType";
    private static final String VIRTUAL_MACHINE = "VirtualMachine";
    private static final int VIRTUAL_MACHINE_NUMERIC = 10;
    private static final String DISPLAY_NAME = "displayName";
    private static final String FOO = "FOO";

    private static Search.PropertyFilter.Builder ENTITY_TYPE_VM = Search.PropertyFilter.newBuilder()
            .setPropertyName(ENTITY_TYPE)
            .setStringFilter(Search.PropertyFilter.StringFilter.newBuilder()
                    .setStringPropertyRegex(VIRTUAL_MACHINE)
                    .setCaseSensitive(true)
                    .build());

    private static Search.PropertyFilter.Builder ENTITY_TYPE_VM_NUMERIC = Search.PropertyFilter.newBuilder()
            .setPropertyName(ENTITY_TYPE)
            .setNumericFilter(Search.PropertyFilter.NumericFilter.newBuilder()
                    .setComparisonOperator(Search.ComparisonOperator.EQ)
                    .setValue(VIRTUAL_MACHINE_NUMERIC)
                    .build());

    private static Search.PropertyFilter.Builder DISPLAY_NAME_FOO = Search.PropertyFilter.newBuilder()
            .setPropertyName(DISPLAY_NAME)
            .setStringFilter(Search.PropertyFilter.StringFilter.newBuilder()
                    .setStringPropertyRegex(FOO)
                    .setCaseSensitive(false)
                    .build());

    private static Search.PropertyFilter.Builder CAPACITY_GTE_TWO = Search.PropertyFilter.newBuilder()
            .setPropertyName(CAPACITY)
            .setNumericFilter(Search.PropertyFilter.NumericFilter.newBuilder()
                    .setComparisonOperator(Search.ComparisonOperator.GTE)
                    .setValue(2L)
                    .build());

    private static Search.TraversalFilter.Builder TWO_HOPS_TRAVERSAL = Search.TraversalFilter.newBuilder()
            .setTraversalDirection(Search.TraversalFilter.TraversalDirection.PRODUCES)
            .setStoppingCondition(Search.TraversalFilter.StoppingCondition.newBuilder()
                    .setNumberHops(2).build());

    private static Search.TraversalFilter.Builder CAPACITY_COND_TRAVERSAL = Search.TraversalFilter.newBuilder()
            .setTraversalDirection(Search.TraversalFilter.TraversalDirection.CONSUMES)
            .setStoppingCondition(Search.TraversalFilter.StoppingCondition.newBuilder()
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
                .fromFilters(Filter.propertyFilter(
                        PropertyFilter.newBuilder()
                                .setPropertyName(ENTITY_TYPE)
                                .setStringFilter(ENTITY_TYPE_VM.getStringFilter())
                                .build())));

        ;
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
                AQLRepr.fromFilters(Filter.propertyFilter(
                        PropertyFilter.newBuilder()
                                .setPropertyName(ENTITY_TYPE)
                                .setStringFilter(ENTITY_TYPE_VM.getStringFilter())
                                .build())),
                AQLRepr.fromFilters(Filter.propertyFilter(
                        PropertyFilter.newBuilder()
                                .setPropertyName(DISPLAY_NAME)
                                .setStringFilter(DISPLAY_NAME_FOO.getStringFilter())
                                .build()))
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
                AQLRepr.fromFilters(Filter.propertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName(ENTITY_TYPE)
                        .setStringFilter(ENTITY_TYPE_VM.getStringFilter())
                        .build())),
                AQLRepr.fromFilters(Filter.propertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName(DISPLAY_NAME)
                        .setStringFilter(DISPLAY_NAME_FOO.getStringFilter())
                        .build())),
                AQLRepr.fromFilters(Filter.traversalHopFilter(Filter.TraversalDirection.CONSUMER, 2)),
                AQLRepr.fromFilters(Filter.propertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName(CAPACITY)
                        .setNumericFilter(NumericFilter.newBuilder()
                                .setComparisonOperator(ComparisonOperator.GTE)
                                .setValue(2L)
                                .build())
                        .build())),
                AQLRepr.fromFilters(Filter.traversalCondFilter(Filter.TraversalDirection.PROVIDER,
                        Filter.propertyFilter(PropertyFilter.newBuilder()
                                .setPropertyName(CAPACITY)
                                .setNumericFilter(NumericFilter.newBuilder()
                                        .setComparisonOperator(ComparisonOperator.GTE)
                                        .setValue(2L)
                                        .build())
                                .build())))
            );
    }

    @Test
    public void testEntityTypeNumeric() throws Throwable {
        final SearchParameters searchParameters = SearchParameters.newBuilder()
                .setStartingFilter(ENTITY_TYPE_VM_NUMERIC)
                .build();

        final List<AQLRepr> aqlReprs = SearchDTOConverter.toAqlRepr(searchParameters);

        // expect that the regex will be anchored to match the full string
        // numeric filter for entity type is converted to string filter
        assertThat(SearchDTOConverter.toAqlRepr(searchParameters).get(0).toAQL().toString())
                .contains("FILTER service_entity.entityType IN [\"VirtualMachine\"]");
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

        SearchDTOConverter.toAqlRepr(searchParameters).forEach(AQLRepr::toAQL);
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
        SearchDTOConverter.toAqlRepr(searchParameters).forEach(AQLRepr::toAQL);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingFilter() throws Throwable {
        final Search.PropertyFilter missingTypeFilterAndNextFilter = Search.PropertyFilter.newBuilder()
                .setPropertyName(CAPACITY)
                .build();
        final SearchParameters searchParameters = SearchParameters.newBuilder()
                .setStartingFilter(ENTITY_TYPE_VM)
                .addSearchFilter(Search.SearchFilter.newBuilder().setTraversalFilter(TWO_HOPS_TRAVERSAL).build())
                .addSearchFilter(Search.SearchFilter.newBuilder().setPropertyFilter(missingTypeFilterAndNextFilter).build())
                .build();

        SearchDTOConverter.toAqlRepr(searchParameters);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingTraversalDirection() throws Throwable {
        final Search.TraversalFilter missingDirection = Search.TraversalFilter.newBuilder()
                .setStoppingCondition(Search.TraversalFilter.StoppingCondition.newBuilder()
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
        final Search.TraversalFilter missingStoppingCond = Search.TraversalFilter.newBuilder()
                .setTraversalDirection(Search.TraversalFilter.TraversalDirection.CONSUMES)
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
        final long oid = 123L;
        ServiceEntityRepoDTO serviceEntityRepoDTO = new ServiceEntityRepoDTO();
        serviceEntityRepoDTO.setDisplayName(name);
        serviceEntityRepoDTO.setEntityType("VirtualMachine");
        serviceEntityRepoDTO.setState("ACTIVE");
        serviceEntityRepoDTO.setOid(Long.toString(oid));
        serviceEntityRepoDTO.setEnvironmentType("CLOUD");

        MinimalEntity entity = SearchDTOConverter.toSearchEntity(serviceEntityRepoDTO);

        assertEquals(name, entity.getDisplayName());
        assertEquals(EntityDTO.EntityType.VIRTUAL_MACHINE.getNumber(), entity.getEntityType());
        assertEquals(EnvironmentType.CLOUD, entity.getEnvironmentType());
        assertEquals(oid, entity.getOid());
    }

    @Test
    public void testToSearchEntityPM() {
        final String name = "pm-foo";
        final long oid = 456L;
        ServiceEntityRepoDTO serviceEntityRepoDTO = new ServiceEntityRepoDTO();
        serviceEntityRepoDTO.setDisplayName(name);
        serviceEntityRepoDTO.setEntityType("PhysicalMachine");
        serviceEntityRepoDTO.setState("SUSPEND");
        serviceEntityRepoDTO.setOid(Long.toString(oid));
        serviceEntityRepoDTO.setEnvironmentType("CLOUD");

        MinimalEntity entity = SearchDTOConverter.toSearchEntity(serviceEntityRepoDTO);

        assertEquals(name, entity.getDisplayName());
        assertEquals(EntityDTO.EntityType.PHYSICAL_MACHINE.getNumber(), entity.getEntityType());
        assertEquals(oid, entity.getOid());
        assertEquals(EnvironmentType.CLOUD, entity.getEnvironmentType());
    }
}
