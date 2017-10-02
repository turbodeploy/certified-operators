/**
 * IdentityServiceTest.java
 * Copyright (c) VMTurbo 2015
 */
package com.vmturbo.topology.processor.identity.services;

import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Test;

import com.vmturbo.platform.common.builders.metadata.EntityIdentityMetadataBuilder;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata;

/**
 * Test policy matcher.
 */
public class PolicyMatcherDefaultTest {

    /*
     * Returns the reference counter with the predefined number.
     */
    private PropertyReferenceCounter getRefCounter(int n) {
        PropertyReferenceCounter counter = new PropertyReferenceCounter();
        for (int i = 0; i < n; i++) {
            counter.increment();
        }
        return counter;
    }

    int defaultHeuristicThreshold = EntityIdentityMetadata.getDefaultInstance().getHeuristicThreshold();

    /**
     * Empty sets won't match. No reason for it.
     */
    @Test
    public void testEmpty() {
        SortedMap<Integer, Map<String, PropertyReferenceCounter>> heuristicsOld;
        SortedMap<Integer, Map<String, PropertyReferenceCounter>> heuristicsNew;

        heuristicsOld = new TreeMap<>();
        heuristicsNew = new TreeMap<>();

        PolicyMatcherDefault matcher = new PolicyMatcherDefault(defaultHeuristicThreshold);
        assertTrue(!matcher.matches(heuristicsOld, heuristicsNew));
    }

    /**
     * Totally identical.
     */
    @Test
    public void testEqualsSingle() {
        SortedMap<Integer, Map<String, PropertyReferenceCounter>> heuristicsOld;
        SortedMap<Integer, Map<String, PropertyReferenceCounter>> heuristicsNew;

        heuristicsOld = new TreeMap<>();
        heuristicsNew = new TreeMap<>();

        Map<String, PropertyReferenceCounter> mapOld = new TreeMap<>();
        mapOld.put("value1", getRefCounter(1));
        heuristicsOld.put(1, mapOld);

        Map<String, PropertyReferenceCounter> mapNew = new TreeMap<>();
        mapNew.put("value1", getRefCounter(1));
        heuristicsNew.put(1, mapNew);

        PolicyMatcherDefault matcher = new PolicyMatcherDefault(defaultHeuristicThreshold);
        assertTrue(matcher.matches(heuristicsOld, heuristicsNew));
    }

    /**
     * Four thirds.
     */
    @Test
    public void testEqualsFourThirds() {
        SortedMap<Integer, Map<String, PropertyReferenceCounter>> heuristicsOld;
        SortedMap<Integer, Map<String, PropertyReferenceCounter>> heuristicsNew;

        heuristicsOld = new TreeMap<>();
        heuristicsNew = new TreeMap<>();

        Map<String, PropertyReferenceCounter> mapOld = new TreeMap<>();
        mapOld.put("value1", getRefCounter(4));
        heuristicsOld.put(1, mapOld);

        Map<String, PropertyReferenceCounter> mapNew = new TreeMap<>();
        mapNew.put("value1", getRefCounter(3));
        heuristicsNew.put(1, mapNew);

        PolicyMatcherDefault matcher = new PolicyMatcherDefault(defaultHeuristicThreshold);
        assertTrue(matcher.matches(heuristicsOld, heuristicsNew));
    }

    /**
     * 1/2.
     * Expect failure.
     */
    @Test
    public void testEqualsHalf() {
        SortedMap<Integer, Map<String, PropertyReferenceCounter>> heuristicsOld;
        SortedMap<Integer, Map<String, PropertyReferenceCounter>> heuristicsNew;

        heuristicsOld = new TreeMap<>();
        heuristicsNew = new TreeMap<>();

        Map<String, PropertyReferenceCounter> mapOld = new TreeMap<>();
        mapOld.put("value1", getRefCounter(4));
        heuristicsOld.put(1, mapOld);

        Map<String, PropertyReferenceCounter> mapNew = new TreeMap<>();
        mapNew.put("value1", getRefCounter(2));
        heuristicsNew.put(1, mapNew);

        PolicyMatcherDefault matcher = new PolicyMatcherDefault(defaultHeuristicThreshold);
        assertTrue(!matcher.matches(heuristicsOld, heuristicsNew));
    }

    /**
     * Not the same rank.
     * Expect failure.
     */
    @Test
    public void testEqualsDifferentRank() {
        SortedMap<Integer, Map<String, PropertyReferenceCounter>> heuristicsOld;
        SortedMap<Integer, Map<String, PropertyReferenceCounter>> heuristicsNew;

        heuristicsOld = new TreeMap<>();
        heuristicsNew = new TreeMap<>();

        Map<String, PropertyReferenceCounter> mapOld = new TreeMap<>();
        mapOld.put("value1", getRefCounter(1));
        heuristicsOld.put(1, mapOld);

        Map<String, PropertyReferenceCounter> mapNew = new TreeMap<>();
        mapNew.put("value1", getRefCounter(1));
        heuristicsNew.put(2, mapNew);

        PolicyMatcherDefault matcher = new PolicyMatcherDefault(defaultHeuristicThreshold);
        assertTrue(!matcher.matches(heuristicsOld, heuristicsNew));
    }

    /**
     * Not the same values.
     * Expect failure.
     */
    @Test
    public void testEqualsDifferentValues() {
        SortedMap<Integer, Map<String, PropertyReferenceCounter>> heuristicsOld;
        SortedMap<Integer, Map<String, PropertyReferenceCounter>> heuristicsNew;

        heuristicsOld = new TreeMap<>();
        heuristicsNew = new TreeMap<>();

        Map<String, PropertyReferenceCounter> mapOld = new TreeMap<>();
        mapOld.put("value1", getRefCounter(1));
        heuristicsOld.put(1, mapOld);

        Map<String, PropertyReferenceCounter> mapNew = new TreeMap<>();
        mapNew.put("value2", getRefCounter(1));
        heuristicsNew.put(1, mapNew);

        PolicyMatcherDefault matcher = new PolicyMatcherDefault(defaultHeuristicThreshold);
        assertTrue(!matcher.matches(heuristicsOld, heuristicsNew));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testHeuristicThresholdTooLow() {
        new PolicyMatcherDefault(EntityIdentityMetadataBuilder.MINIMUM_HEURISTIC_THRESHOLD - 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testHeuristicThresholdTooHigh() {
        new PolicyMatcherDefault(EntityIdentityMetadataBuilder.MAXIMUM_HEURISTIC_THRESHOLD + 1);
    }
}
