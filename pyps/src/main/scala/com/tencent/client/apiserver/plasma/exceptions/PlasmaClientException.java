package com.tencent.client.apiserver.plasma.exceptions;

public class PlasmaClientException extends RuntimeException {

  public PlasmaClientException(String message) {
    super(message);
  }

  public PlasmaClientException(String message, Throwable t) {
    super(message, t);
  }
}
