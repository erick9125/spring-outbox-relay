package io.github.erick9125.outbox.exception;

public class PermanentPublicationException extends RuntimeException {

  public PermanentPublicationException(String message) {
    super(message);
  }

  public PermanentPublicationException(String message, Throwable cause) {
    super(message, cause);
  }
}
