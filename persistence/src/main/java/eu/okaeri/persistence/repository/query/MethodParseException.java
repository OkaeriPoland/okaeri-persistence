package eu.okaeri.persistence.repository.query;

/**
 * Exception thrown when a repository method name cannot be parsed
 * or validation fails during parsing.
 */
public class MethodParseException extends RuntimeException {

    public MethodParseException(String message) {
        super(message);
    }

    public MethodParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
