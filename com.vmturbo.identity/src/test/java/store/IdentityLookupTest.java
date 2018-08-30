package store;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

import com.vmturbo.identity.attributes.AttributeExtractor;
import com.vmturbo.identity.attributes.IdentityMatchingAttributes;
import com.vmturbo.identity.store.IdentityLookup;
import com.vmturbo.identity.store.IdentityStore;

/**
 * Test IdentityLookup classes specific to Workflows.
 **/
public class IdentityLookupTest {

    public static final String NAME_ATTRIBUTE = "name";
    public static final String TARGET_ID_ATTRIBUTE = "targetId";
    public static final String TEST_WORKFLOW_NAME = "test";
    public static final String TEST_WORKFLOW_NAME2 = "test2";
    public static final long TEST_WORKFLOW_TARGET_ID = 123L;
    public static final String TEST_WORKFLOW_TARGET_ID_STRING =
            Long.toString(TEST_WORKFLOW_TARGET_ID);
    private static final Long EXPECTED_OID = 456L;


    /**
     * Initialize the persistent store with a mapping and test that it is found.
     */
    @Test
    public void testIdentityStore() {

        // arrange
        IdentityStore mockIdentityStore = Mockito.mock(IdentityStore.class);

        // objects to pass into the 'getOids()' call
        Object mockItem1 = Mockito.mock(Object.class);
        Object mockItem2 = Mockito.mock(Object.class);
        // set up the AttributeExtractor to return a IdentityMatchingAttributes instance for each input item
        AttributeExtractor mockAttributeExtractor = Mockito.mock(AttributeExtractor.class);
        IdentityMatchingAttributes attr1 = Mockito.mock(IdentityMatchingAttributes.class);
        IdentityMatchingAttributes attr2 = Mockito.mock(IdentityMatchingAttributes.class);
        when(mockAttributeExtractor.extractAttributes(mockItem1))
                .thenReturn(attr1);
        when(mockAttributeExtractor.extractAttributes(mockItem2))
                .thenReturn(attr2);
        // set up the mock identity store to return an OID list given the IdentityMatchingAttribute objects
        // derived from the input objects
        when(mockIdentityStore.fetchOrAssignOids(Lists.newArrayList(attr1, attr2)))
                .thenReturn(Lists.newArrayList(1L, 2L));
        // create the IdentityLookup to test
        IdentityLookup identityLookup = new IdentityLookup(mockAttributeExtractor, mockIdentityStore);

        // act
        List<Long> oids = identityLookup.getOids(Lists.newArrayList(mockItem1, mockItem2));

        // assert
        assertThat(oids.size(), equalTo(2));
        assertThat(oids, containsInAnyOrder(1L, 2L));
        verify(mockAttributeExtractor).extractAttributes(mockItem1);
        verify(mockAttributeExtractor).extractAttributes(mockItem2);
        verifyNoMoreInteractions(mockAttributeExtractor);
        verify(mockIdentityStore).fetchOrAssignOids(anyList());
        verifyNoMoreInteractions(mockIdentityStore);
    }

}
