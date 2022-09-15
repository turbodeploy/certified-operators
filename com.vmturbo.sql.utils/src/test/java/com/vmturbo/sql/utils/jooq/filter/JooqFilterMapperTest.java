package com.vmturbo.sql.utils.jooq.filter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;

import org.jooq.Comparator;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.junit.Test;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test class for {@link JooqFilterMapper}.
 */
public class JooqFilterMapperTest {

    /**
     * Tests condition generation for an IN condition in a list.
     */
    @Test
    public void testInCollection() {

        Field<Long> testField = DSL.field("test_field", Long.class);

        /**
         * Test filter.
         */
        abstract class TestFilter implements Supplier<List<Long>> {}

        final JooqFilterMapper<TestFilter> filterMapper = JooqFilterMapper.<TestFilter>builder()
                .addInCollection(TestFilter::get, testField)
                .build();


        /*
        Set up the filter and generate conditions
         */
        final TestFilter filter = new TestFilter() {

            public List<Long> get() {
                return ImmutableList.of(1L, 2L);
            }
        };
        final List<Condition> actualConditions = filterMapper.generateConditions(filter);

        /*
        Assertions
         */
        assertThat(actualConditions, hasSize(1));
        final Condition actualCondition = actualConditions.get(0);
        assertThat(actualCondition, equalTo(testField.in(1L, 2L)));
    }

    /**
     * Verifies a condition is not generated for a collection when the collection
     * is empty.
     */
    @Test
    public void testEmptyFilterInCollection() {

        Field<Long> testField = DSL.field("test_field", Long.class);

        /**
         * Test filter.
         */
        abstract class TestFilter implements Supplier<List<Long>> {}

        final JooqFilterMapper<TestFilter> filterMapper = JooqFilterMapper.<TestFilter>builder()
                .addInCollection(TestFilter::get, testField)
                .build();


        /*
        Set up the filter and generate conditions
         */
        final TestFilter filter = new TestFilter() {

            public List<Long> get() {
                return Collections.emptyList();
            }
        };
        final List<Condition> actualConditions = filterMapper.generateConditions(filter);

        /*
        Assertions
         */
        assertThat(actualConditions, hasSize(0));
    }

    /**
     * Tests generation of an IN condition for an enum value, in which the enum type should
     * be converted to a short.
     */
    @Test
    public void testInEnumCollection() {

        Field<Short> testField = DSL.field("test_field", Short.class);

        /**
         * Test filter.
         */
        abstract class TestFilter implements Supplier<List<EntityType>> {}

        final JooqFilterMapper<TestFilter> filterMapper = JooqFilterMapper.<TestFilter>builder()
                .addProtoEnumInShortCollection(TestFilter::get, testField)
                .build();


        /*
        Set up the filter and generate conditions
         */
        final TestFilter filter = new TestFilter() {

            public List<EntityType> get() {
                return ImmutableList.of(EntityType.VIRTUAL_MACHINE, EntityType.VIRTUAL_VOLUME);
            }
        };
        final List<Condition> actualConditions = filterMapper.generateConditions(filter);

        /*
        Assertions
         */
        assertThat(actualConditions, hasSize(1));
        final Condition actualCondition = actualConditions.get(0);
        // VM = 10, VV = 60
        assertThat(actualCondition, equalTo(testField.in((short)10, (short)60)));
    }

    /**
     * Tests generation of a condition for comparing against a single value in a filter.
     */
    @Test
    public void testValueComparison() {

        Field<Long> testField = DSL.field("test_field", Long.class);

        /**
         * Test filter.
         */
        abstract class TestFilter implements Supplier<Long> {}

        final JooqFilterMapper<TestFilter> filterMapper = JooqFilterMapper.<TestFilter>builder()
                .addValueComparison(Predicates.alwaysTrue(), TestFilter::get, testField, Comparator.GREATER)
                .build();


        /*
        Set up the filter and generate conditions
         */
        final TestFilter filter = new TestFilter() {

            public Long get() {
                return 3L;
            }
        };
        final List<Condition> actualConditions = filterMapper.generateConditions(filter);

        /*
        Assertions
         */
        assertThat(actualConditions, hasSize(1));
        final Condition actualCondition = actualConditions.get(0);
        assertThat(actualCondition, equalTo(testField.greaterThan(3L)));
    }
}
