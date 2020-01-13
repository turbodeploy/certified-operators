package com.vmturbo.api.component.external.api.SAML;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.opensaml.saml2.core.Attribute;
import org.opensaml.xml.XMLObject;
import org.opensaml.xml.schema.XSAny;
import org.opensaml.xml.schema.XSString;
import org.opensaml.xml.schema.impl.XSAnyImpl;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link SAMLUserDetailsServiceImpl}.
 */
@RunWith(Parameterized.class)
public class SAMLUserDetailsServiceImplTest {

    private static final String TURBO_GROUP = "turbo_group";
    private final Attribute attribute;

    /**
     * Default constructor to support parameterized test.
     *
     * @param attribute {@link Attribute} from SAML responses
     */
    public SAMLUserDetailsServiceImplTest(Attribute attribute) {
        this.attribute = attribute;
    }

    /**
     * Parameters setup.
     *
     * @return list of parameters.
     */
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {buildSamlAttribute(() -> {
                    XSString xsString = mock(XSString.class);
                    when(xsString.getValue()).thenReturn(TURBO_GROUP);
                    return xsString;
                })}, {buildSamlAttribute(() -> {
                    XSAny xsAny = mock(XSAnyImpl.class);
                    when(xsAny.getTextContent()).thenReturn(TURBO_GROUP);
                    return xsAny;
                }

        )}
        });
    }

    private static Attribute buildSamlAttribute(Supplier<XMLObject> supplier) {
        Attribute attribute = mock(Attribute.class);
        XMLObject xmlObject = supplier.get();
        when(attribute.getAttributeValues()).thenReturn(Collections.singletonList(xmlObject));
        return attribute;
    }

    /**
     * Verify both {@link XSString} and {@link XSAny} types are supported.
     */
    @Test
    public void testGetXmlObject() {
        assertEquals(TURBO_GROUP, SAMLUserDetailsServiceImpl.getXmlObject(attribute));
    }
}