package com.tencent.client.apiserver.plasma.exceptions;

public class PlasmaOutOfMemoryException extends RuntimeException {

  public PlasmaOutOfMemoryException() {
    super("The plasma store ran out of memory.");
  }

  public PlasmaOutOfMemoryException(Throwable t) {
    super("The plasma store ran out of memory.", t);
  }
}
