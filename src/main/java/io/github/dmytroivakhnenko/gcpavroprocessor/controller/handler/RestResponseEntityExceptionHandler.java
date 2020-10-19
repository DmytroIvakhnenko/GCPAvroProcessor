package io.github.dmytroivakhnenko.gcpavroprocessor.controller.handler;

import io.github.dmytroivakhnenko.gcpavroprocessor.exception.AvroFileValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@ControllerAdvice
public class RestResponseEntityExceptionHandler extends ResponseEntityExceptionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ResponseEntityExceptionHandler.class);

    /**
     * This method handles error when file doesn't follow schema.
     *
     * @param ex      - thrown exception
     * @param request - web request
     * @return Ok status (200) to tell PubSub subscription that message was acknowledged
     */
    @ExceptionHandler(value = AvroFileValidationException.class)
    protected ResponseEntity<Object> avroFileValidationHandler(RuntimeException ex, WebRequest request) {
        LOG.error("Exception occurs during avro file validation", ex);
        return handleExceptionInternal(ex, null, new HttpHeaders(), HttpStatus.OK, request);
    }

    /**
     * This method handles all exceptions from the application
     *
     * @param ex      - thrown exception
     * @param request - web request
     * @return BadRequest status (400) to allow failed message be saved in dead letter topic
     */
    @ExceptionHandler(value = Exception.class)
    protected ResponseEntity<Object> defaultHandler(RuntimeException ex, WebRequest request) {
        LOG.error("Some exception occurs", ex);
        return handleExceptionInternal(ex, null, new HttpHeaders(), HttpStatus.BAD_REQUEST, request);
    }


}