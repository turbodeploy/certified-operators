package com.vmturbo.action.orchestrator.velocity;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.velocity.exception.MethodInvocationException;
import org.apache.velocity.exception.VelocityException;
import org.apache.velocity.runtime.parser.ParseException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests templating conversion using Velocity.
 */
public class VelocityTest {

    /**
     * Wraps VelocityTestClass.  Must be public so that Velocity can read it.
     */
    public static class WrappingObject {

        private final VelocityTestClass inner;

        /**
         * Instantiates the the wrapping object with the test object inside.
         *
         * @param inner the test object.
         */
        public WrappingObject(VelocityTestClass inner) {
            this.inner = inner;
        }

        public VelocityTestClass getInner() {
            return inner;
        }
    }

    /**
     * Test class. Must be public so that Velocity can read it.
     */
    public static class VelocityTestClass {
        private final String id;
        private final String uuid;
        private final String description;
        private final String extraField;

        /**
         * Test class.
         * @param id test id
         * @param uuid test uuid
         * @param description test description
         * @param extraField test extra field
         */
        public VelocityTestClass(String id, String uuid, String description, String extraField) {
            this.id = id;
            this.uuid = uuid;
            this.description = description;
            this.extraField = extraField;
        }

        /**
         * Test class.
         * @param id test id
         * @param uuid test uuid
         * @param description test description
         */
        public VelocityTestClass(String id, String uuid, String description) {
            this(id, uuid, description, "");
        }

        public String getId() {
            return id;
        }

        public String getUuid() {
            return uuid;
        }

        public String getDescription() {
            return description;
        }

        public String getExtraField() {
            return extraField;
        }


    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final String escTemplate = "{\n" + "  \"id\": \"$esc.%s($action.id)\",\n"
            + "  \"uuid\": \"$esc.%s($action.uuid)\",\n"
            + "  \"description\": \"$esc.%s($action.description)\",\n"
            + "}";

    private final VelocityTestClass testObject =
            new VelocityTestClass("0",
                    "1",
                    "\\ / \f \t \b \n \r \u0001 \u0011 \u0111 \u1111 ' \\");

    private final WrappingObject wrappingObject = new WrappingObject(testObject);

    /** Rule to help test exceptions.*/
    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    /**
     * Test velocity java template conversion.
     * @throws IOException failed to write out template.
     * @throws ParseException failed to convert template.
     */
    @Test
    public void testVelocityJavaTemplate() throws IOException, ParseException {
        Assert.assertEquals(Velocity.apply(escTemplate.replaceAll("%s", "java"), testObject), "{\n"
                + "  \"id\": \"0\",\n" + "  \"uuid\": \"1\",\n"
                + "  \"description\": \"\\\\ / \\f \\t \\b \\n \\r \\u0001 \\u0011 \\u0111 \\u1111 ' \\\\\",\n"
                + "}");
    }

    /**
     * Test velocity json template conversion.
     * @throws IOException failed to write out template.
     * @throws ParseException failed to convert template.
     */
    @Test
    public void testVelocityJsonTemplate() throws IOException, ParseException {
        Assert.assertEquals(Velocity.apply(escTemplate.replaceAll("%s", "json"), testObject), "{\n"
                + "  \"id\": \"0\",\n" + "  \"uuid\": \"1\",\n"
                + "  \"description\": \"\\\\ \\/ \\f \\t \\b \\n \\r \\u0001 \\u0011 \\u0111 \\u1111 ' \\\\\",\n"
                + "}");
    }

    /**
     * Test velocity javascript template conversion.
     * @throws IOException failed to write out template.
     * @throws ParseException failed to convert template.
     */
    @Test
    public void testVelocityJavascriptTemplate() throws IOException, ParseException {
        Assert.assertEquals(Velocity.apply(escTemplate.replaceAll("%s", "javascript"), testObject), "{\n"
                + "  \"id\": \"0\",\n" + "  \"uuid\": \"1\",\n"
                + "  \"description\": \"\\\\ \\/ \\f \\t \\b \\n \\r \\u0001 \\u0011 \\u0111 \\u1111 \\' \\\\\",\n"
                + "}");
    }

    /**
     * Test velocity java template conversion on a null data object.
     * @throws IOException failed to write out template.
     * @throws ParseException failed to convert template.
     */
    @Test
    public void testVelocityTemplateNullObjectException() throws IOException, ParseException {
        thrown.expect(VelocityException.class);
        thrown.expectMessage("Attempted to access 'id'");
        Velocity.apply(escTemplate.replaceAll("%s", "java"), null);
    }

    /**
     * Test velocity template conversion on a data object that does not contain the field property
     * that the template requires.
     * @throws IOException failed to write out template.
     * @throws ParseException failed to convert template.
     */
    @Test
    public void testVelocityTemplateWithNonExistingFieldException() throws IOException, ParseException {
        thrown.expect(MethodInvocationException.class);
        thrown.expectMessage("does not contain property 'time'");
        String template = "{\n" + "  \"id\": \"$action.time\"\n" + "}";
        Velocity.apply(template, testObject);
    }

    /**
     * Velocity should convert the object to json.
     *
     * @throws Exception should not be thrown.
     */
    @Test(expected = Exception.class)
    public void testConverterTypoThrowsException() throws Exception {
        String template = "$convertor.toJson($action)";
        Velocity.apply(template, testObject);
    }

    /**
     * Velocity should convert the object to json.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testObjectConvertedToJson() throws Exception {
        String template = "$converter.toJson($action)";
        String expected = OBJECT_MAPPER.writeValueAsString(testObject);
        String actual = Velocity.apply(template, testObject);
        Assert.assertEquals(expected, actual);
    }

    /**
     * Velocity should convert the object and the nested object to json.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testNestedObjectConvertedToJson() throws Exception {
        String template = "$converter.toJson($action)";
        String expected = OBJECT_MAPPER.writeValueAsString(wrappingObject);
        String actual = Velocity.apply(template, wrappingObject);
        Assert.assertEquals(expected, actual);
    }

    /**
     * Velocity should convert the inner object to json.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testPartOfObjectConvertedToJson() throws Exception {
        String template = "$converter.toJson($action.inner)";
        String expected = OBJECT_MAPPER.writeValueAsString(wrappingObject.getInner());
        String actual = Velocity.apply(template, wrappingObject);
        Assert.assertEquals(expected, actual);
    }
}
