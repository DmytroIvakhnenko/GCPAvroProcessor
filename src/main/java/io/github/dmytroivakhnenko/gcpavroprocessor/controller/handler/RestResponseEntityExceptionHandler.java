package io.github.dmytroivakhnenko.gcpavroprocessor.controller.handler;

import io.github.dmytroivakhnenko.gcpavroprocessor.exception.AvroFileGenerationException;
import io.github.dmytroivakhnenko.gcpavroprocessor.exception.AvroFileValidationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@ControllerAdvice
@Slf4j
public class RestResponseEntityExceptionHandler extends ResponseEntityExceptionHandler {
    /**
     * This method handles error when file doesn't follow schema.
     *
     * @param ex      - thrown exception
     * @param request - web request
     * @return Ok status (200) to tell PubSub subscription that message was acknowledged
     */
    @ExceptionHandler(value = AvroFileValidationException.class)
    protected ResponseEntity<Object> avroFileValidationHandler(RuntimeException ex, WebRequest request) {
        var msg = "Exception occurs during avro file validation";
        log.error(msg, ex);
        return handleExceptionInternal(ex, msg, new HttpHeaders(), HttpStatus.OK, request);
    }

    /**
     * This method handles error when file generation failed.
     *
     * @param ex      - thrown exception
     * @param request - web request
     * @return Internal server error (500)
     */
    @ExceptionHandler(value = AvroFileGenerationException.class)
    protected ResponseEntity<Object> avroFileGenerationExceptionHandler(RuntimeException ex, WebRequest request) {
        var msg = "Exception occurs during avro file generation";
        log.error(msg, ex);
        return handleExceptionInternal(ex, msg, new HttpHeaders(), HttpStatus.INTERNAL_SERVER_ERROR, request);
    }

    /**
     * This method handles all exceptions from the application
     *
     * @param ex      - thrown exception
     * @param request - web request
     * @return BadRequest status (400) to allow failed message be saved in dead letter topic
     */
    @ExceptionHandler(value = Exception.class)
    protected ResponseEntity<Object> defaultHandler(Exception ex, WebRequest request) {
        var msg = "Some exception occurs";
        log.error(msg, ex);
        return handleExceptionInternal(ex, msg, new HttpHeaders(), HttpStatus.BAD_REQUEST, request);
    }


}