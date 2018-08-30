package attributes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.Test;

import com.google.common.collect.Sets;

import com.vmturbo.identity.attributes.IdentityMatchingAttribute;
import com.vmturbo.identity.attributes.SimpleMatchingAttributes;

/**
 * Test the permutations of .equals() for a SimpleMatchingAttribute - a list of attributes that
 * must match to be equal.
 **/
public class SimpleMatchingAttributesTest {

    /**
     * Test that attributes with the same (single) key/value are equal
     */
    @Test
    public void testItemAttributesEqual() {
        // arrange
        SimpleMatchingAttributes item1 = SimpleMatchingAttributes.newBuilder()
                .addAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id1", "value1")))
                .build();
        SimpleMatchingAttributes item2 = SimpleMatchingAttributes.newBuilder()
                .addAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id1", "value1")))
                .build();

        // act
        // assert
        assertEquals(item1, item2);
    }

    /**
     * Test that both types of builder calls, addAttribute() and addAttributes() produce equal
     * results.
     */
    @Test
    public void testItemAttributesBuilderEqual() {
        // arrange
        SimpleMatchingAttributes item1 = SimpleMatchingAttributes.newBuilder()
                .addAttribute("id1", "value1")
                .build();
        SimpleMatchingAttributes item2 = SimpleMatchingAttributes.newBuilder()
                .addAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id1", "value1")))
                .build();

        // act
        // assert
        assertEquals(item1, item2);
    }

    /**
     * Test that items with different attribute id's are not equal
     */
    @Test
    public void testItemAttributesValueNotEqual() {
        // arrange
        SimpleMatchingAttributes item1 = SimpleMatchingAttributes.newBuilder()
                .addAttribute("id1", "value1")
                .build();
        SimpleMatchingAttributes item2 = SimpleMatchingAttributes.newBuilder()
                .addAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id1", "value2")))
                .build();

        // act
        // assert
        assertNotEquals(item1, item2);
    }

    /**
     * Test that items with different values are not equal
     */
    @Test
    public void testItemAttributesIdNotEqual() {
        // arrange
        SimpleMatchingAttributes item1 = SimpleMatchingAttributes.newBuilder()
                .addAttribute("id1", "value1")
                .build();
        SimpleMatchingAttributes item2 = SimpleMatchingAttributes.newBuilder()
                .addAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id2", "value1")))
                .build();

        // act
        // assert
        assertNotEquals(item1, item2);
    }

    /**
     * Test that items with multiple matching attributes are equal, without regard to order added.
     */
    @Test
    public void testItemMultipleAttributesEqual() {
        // arrange
        SimpleMatchingAttributes item1 = SimpleMatchingAttributes.newBuilder()
                .addAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id1", "value1")))
                .addAttributes(Sets.newHashSet(new IdentityMatchingAttribute("id2", "value2")))
                .build();
        SimpleMatchingAttributes item2 = SimpleMatchingAttributes.newBuilder()
                .addAttribute("id2", "value2")
                .addAttribute("id1", "value1")
                .build();
        // act
        // assert
        assertEquals(item1, item2);
    }


}
