package com.vmturbo.action.orchestrator.velocity;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.runtime.RuntimeServices;
import org.apache.velocity.runtime.RuntimeSingleton;
import org.apache.velocity.runtime.parser.ParseException;

/**
 * Velocity template class.
 */
public class Velocity {
    private Velocity() {}

    private static final RuntimeServices runtimeServices;
    private static final EscapeToolWithJson escapeTool;

    static {
        // utility obj to be used with velocity template engine
        runtimeServices = RuntimeSingleton.getRuntimeServices();
        // Velocity will throw a MethodInvocationException for references that are not defined in the context,
        // or have not been defined with a #set directive. This setting will also throw an exception if an attempt
        // is made to call a non-existing property on an object or if the object is null.
        // More properties can be found at https://velocity.apache.org/engine/2.0/configuration.html
        runtimeServices.setProperty("runtime.strict_mode.enable", "true");
        escapeTool = new EscapeToolWithJson();
    }

    /**
     * Applies object data to a template using Velocity.
     *
     * @param templateStr The template body.
     * @param data The object containing the data.
     * @return String template body with variables filled in
     * @throws ParseException Velocity template failed to parse the data or template
     * @throws IOException Failed to write template
     */
    public static String apply(final String templateStr, Object data)
            throws ParseException, IOException {
        StringReader reader = new StringReader(templateStr);
        Template template = new Template();
        template.setRuntimeServices(runtimeServices);
        // runtimeServices parses the plain string and produces a SimpleNode object (AST Tree)
        template.setData(runtimeServices.parse(reader, new Template()));
        template.initDocument();

        VelocityContext context = new VelocityContext();
        context.put("action", data);
        context.put("esc", escapeTool);

        try (StringWriter writer = new StringWriter()) {
            template.merge(context, writer);
            return writer.toString();
        }
    }
}
