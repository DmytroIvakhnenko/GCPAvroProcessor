package io.github.dmytroivakhnenko.gcpavroprocessor.exception;

public class AvroFileGenerationException extends RuntimeException {
    public AvroFileGenerationException() {
    }

    public AvroFileGenerationException(String message) {
        super(message);
    }

    public AvroFileGenerationException(String message, Throwable cause) {
        super(message, cause);
    }

    public AvroFileGenerationException(Throwable cause) {
        super(cause);
    }

    public AvroFileGenerationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
