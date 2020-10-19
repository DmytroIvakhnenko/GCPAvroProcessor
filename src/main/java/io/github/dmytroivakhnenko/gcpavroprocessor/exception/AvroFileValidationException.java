package io.github.dmytroivakhnenko.gcpavroprocessor.exception;

public class AvroFileValidationException extends RuntimeException {
    public AvroFileValidationException() {
    }

    public AvroFileValidationException(String message) {
        super(message);
    }

    public AvroFileValidationException(String message, Throwable cause) {
        super(message, cause);
    }

    public AvroFileValidationException(Throwable cause) {
        super(cause);
    }

    public AvroFileValidationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
