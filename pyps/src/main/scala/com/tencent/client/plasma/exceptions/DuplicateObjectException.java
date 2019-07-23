package com.tencent.client.plasma.exceptions;

public class DuplicateObjectException extends RuntimeException {

  public DuplicateObjectException(String objectId) {
    super("An object with ID " + objectId + " already exists in the plasma store.");
  }

  public DuplicateObjectException(String objectId, Throwable t) {
    super("An object with ID " + objectId + " already exists in the plasma store.", t);
  }
}
