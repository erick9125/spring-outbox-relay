package io.github.erick9125.outbox.exception;

public class RetryablePublicationException extends RuntimeException {

  public RetryablePublicationException(String message) {
    super(message);
  }

  public RetryablePublicationException(String message, Throwable cause) {
    super(message, cause);
  }
}
