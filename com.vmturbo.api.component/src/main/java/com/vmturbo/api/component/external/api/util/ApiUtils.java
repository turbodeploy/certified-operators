package com.vmturbo.api.component.external.api.util;

import java.text.MessageFormat;

/**
 * Utility functions in support of the XL External API implementation
 **/
public class ApiUtils {

    public static final String NOT_IMPLEMENTED_MESSAGE = "REST API message is" +
            " not implemented in Turbonomic XL";

    public static UnsupportedOperationException notImplementedInXL() {
        return new UnsupportedOperationException(NOT_IMPLEMENTED_MESSAGE);
    }
}
