package com.vmturbo.clustermgr;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.client.HttpClientErrorException;

/**
 * This class contains all the Spring Exception Handlers for the Clutermgr REST API. A Handler
 * defined here will be called automatically, by Spring, in cases where the Exception named
 * in the @ExceptionHandler annotation is thrown. The purpose is to map the Exception into
 * a more useful HttpResponseEntity, with a StatusCode and a Message.
 */
@ControllerAdvice
public class ClusterMgrRestExceptionHandler {

    /**
     * Return a response based on any HttpClientErrorException passing the exception and the message.
     *
     * @param ex the exception being handled
     * @return a new ResponseEntity with response code and error message from the exception
     */
    @ExceptionHandler(HttpClientErrorException.class)
    @ResponseBody
    public ResponseEntity<String> handleHttpClientErrorException(HttpClientErrorException ex) {
        return new ResponseEntity<>(ex.getLocalizedMessage(), ex.getStatusCode());
    }
}

