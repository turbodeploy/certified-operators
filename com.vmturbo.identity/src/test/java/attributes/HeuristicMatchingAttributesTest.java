package attributes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.Test;

import com.google.common.collect.Sets;

import com.vmturbo.identity.attributes.HeuristicMatchingAttributes;
import com.vmturbo.identity.attributes.IdentityMatchingAttribute;

/**
 * Test the permutations of 'equals' conditions for HeuristicMatchingAttributes
 * - nonVolatileAttributes, VolatileAttributes, and HeuristicAttributes
 **/
public class HeuristicMatchingAttributesTest {
    @Test
    public void testMatchingAttributeEqual() {
        // arrange
        IdentityMatchingAttribute attr1 = new IdentityMatchingAttribute("id1", "value1");
        IdentityMatchingAttribute attr2 = new IdentityMatchingAttribute("id1", "value1");
        // act
        // assert
        assertEquals(attr1, attr2);
    }

    @Test
    public void testMatchingAttributeIdNotEqual() {
        // arrange
        IdentityMatchingAttribute attr1 = new IdentityMatchingAttribute("id1", "value1");
        IdentityMatchingAttribute attr2 = new IdentityMatchingAttribute("id2", "value1");
        // act
        // assert
        assertNotEquals(attr1, attr2);
    }

    @Test
    public void testMatchingAttributeValNotEqual() {
        // arrange
        IdentityMatchingAttribute attr1 = new IdentityMatchingAttribute("id1", "value1");
        IdentityMatchingAttribute attr2 = new IdentityMatchingAttribute("id1", "value2");
        // act
        // assert
        assertNotEquals(attr1, attr2);
    }

    @Test
    public void testItemAttributesEqual() {
        // arrange
        HeuristicMatchingAttributes item1 = HeuristicMatchingAttributes.newBuilder()
                .setNonVolatileAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id1", "value1")))
                .build();
        HeuristicMatchingAttributes item2 = HeuristicMatchingAttributes.newBuilder()
                .setNonVolatileAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id1", "value1")))
                .build();

        // act
        // assert
        assertEquals(item1, item2);
    }

    @Test
    public void testItemVolatileAttributesEqual() {
        // arrange
        HeuristicMatchingAttributes item1 = HeuristicMatchingAttributes.newBuilder()
                .setNonVolatileAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id1", "value1")))
                .setVolatileAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id2", "value2")))
                .setHeuristicAttributes(Sets.newHashSet())
                .build();
        HeuristicMatchingAttributes item2 = HeuristicMatchingAttributes.newBuilder()
                .setNonVolatileAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id1", "value1")))
                .setVolatileAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id2", "value2")))
                .setHeuristicAttributes(Sets.newHashSet())
                .build();
        // act
        // assert
        assertEquals(item1, item2);
    }

    @Test
    public void testItemVolatileAttributesNotEqual() {
        // arrange
        HeuristicMatchingAttributes item1 = HeuristicMatchingAttributes.newBuilder()
                .setNonVolatileAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id1", "value1")))
                .setVolatileAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id2", "value2")))
                .setHeuristicAttributes(Sets.newHashSet())
                .build();
        HeuristicMatchingAttributes item2 = HeuristicMatchingAttributes.newBuilder()
                .setNonVolatileAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id1", "value1")))
                .setVolatileAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id2", "not-value2")))
                .setHeuristicAttributes(Sets.newHashSet())
                .build();
        // act
        // assert
        assertNotEquals(item1, item2);
    }
    @Test
    public void testItemVolatileHeuristicEqual() {
        // arrange
        HeuristicMatchingAttributes item1 = HeuristicMatchingAttributes.newBuilder()
                .setNonVolatileAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id1", "value1")))
                .setVolatileAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id2", "value2")))
                .setHeuristicAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id3", "value3")))
                .build();
        HeuristicMatchingAttributes item2 = HeuristicMatchingAttributes.newBuilder()
                .setNonVolatileAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id1", "value1")))
                .setVolatileAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id2", "not-value2")))
                .setHeuristicAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id3", "value3")))
                .build();
        // act
        // assert
        assertEquals(item1, item2);
    }

    @Test
    public void testItemVolatileHeuristicNotEqual() {
        // arrange
        HeuristicMatchingAttributes item1 = HeuristicMatchingAttributes.newBuilder()
                .setNonVolatileAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id1", "value1")))
                .setVolatileAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id2", "value2")))
                .setHeuristicAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id3", "value3")))
                .build();
        HeuristicMatchingAttributes item2 = HeuristicMatchingAttributes.newBuilder()
                .setNonVolatileAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id1", "value1")))
                .setVolatileAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id2", "not-value2")))
                .setHeuristicAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id3", "not-value3"))).build();
        // act
        // assert
        assertNotEquals(item1, item2);
    }

    @Test
    public void testItemVolatileHeuristicPctEqual() {
        // arrange
        HeuristicMatchingAttributes item1 = HeuristicMatchingAttributes.newBuilder()
                .setNonVolatileAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id1", "value1")))
                .setVolatileAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id2", "value2")))
                .setHeuristicAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id3", "value3"),
                        new IdentityMatchingAttribute("id4", "value4"),
                        new IdentityMatchingAttribute("id5", "value5")))
                .setHeuristicThreshold(0.5F)
                .build();
        // two of the three heuristic attributes match - so with threshold 50% they are equal
        HeuristicMatchingAttributes item2 = HeuristicMatchingAttributes.newBuilder()
                .setNonVolatileAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id1", "value1")))
                .setVolatileAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id2", "not-value2")))
                .setHeuristicAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id3", "not-value3"),
                        new IdentityMatchingAttribute("id4", "value4"),
                        new IdentityMatchingAttribute("id5", "value5")))
                .setHeuristicThreshold(0.5F)
                .build();
        // act
        // assert
        assertEquals(item1, item2);
    }
    @Test
    public void testItemVolatileHeuristicPctNotEqual() {
        // arrange
        HeuristicMatchingAttributes item1 = HeuristicMatchingAttributes.newBuilder().
                setNonVolatileAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id1", "value1")))
                .setVolatileAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id2", "value2")))
                .setHeuristicAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id3", "value3"),
                        new IdentityMatchingAttribute("id4", "value4"),
                        new IdentityMatchingAttribute("id5", "value5")))
                .setHeuristicThreshold(0.75F)
                .build();
        // two of the three heuristic attributes match - so with threshold 75% they are not equal
        HeuristicMatchingAttributes item2 = HeuristicMatchingAttributes.newBuilder()
                .setNonVolatileAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id1", "value1")))
                .setVolatileAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id2", "not-value2")))
                .setHeuristicAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id3", "not-value3"),
                        new IdentityMatchingAttribute("id4", "value4"),
                        new IdentityMatchingAttribute("id5", "value5")))
                .setHeuristicThreshold(0.75F)
                .build();
        // act
        // assert
        assertNotEquals(item1, item2);
    }

}
