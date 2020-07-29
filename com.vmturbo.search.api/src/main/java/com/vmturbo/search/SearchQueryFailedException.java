package com.vmturbo.search;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Thrown when error occurs parsing/translating Search requests.
 */
@ResponseStatus(value = HttpStatus.BAD_REQUEST,
        reason = "Error occurred processing Searcy Query")
public class SearchQueryFailedException extends Exception {
    /**
     * Constructor.
     * @param errorMessage message added to exception
     */
    public SearchQueryFailedException(String errorMessage) {
        super(errorMessage);
    }
}
