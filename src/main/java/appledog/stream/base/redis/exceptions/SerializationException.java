package appledog.stream.base.redis.exceptions;

public class SerializationException extends RuntimeException {

    public SerializationException(final Throwable cause) {
        super(cause);
    }

    public SerializationException(final String message) {
        super(message);
    }

    public SerializationException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
