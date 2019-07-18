package com.tencent.client.apiserver.plasma;

import com.tencent.client.apiserver.plasma.exceptions.DuplicateObjectException;
import com.tencent.client.apiserver.plasma.exceptions.PlasmaOutOfMemoryException;

import java.nio.ByteBuffer;

public class PlasmaClientJNI {

  public static native long connect(String storeSocketName, String managerSocketName, int releaseDelay);

  public static native void disconnect(long conn);

  public static native ByteBuffer create(long conn, byte[] objectId, int size, byte[] metadata)
          throws DuplicateObjectException, PlasmaOutOfMemoryException;

  public static native byte[] hash(long conn, byte[] objectId);

  public static native void seal(long conn, byte[] objectId);

  public static native void release(long conn, byte[] objectId);

  public static native ByteBuffer[][] get(long conn, byte[][] objectIds, int timeoutMs);
  
  public static native void delete(long conn, byte[] objectId);

  public static native boolean contains(long conn, byte[] objectId);

  public static native void fetch(long conn, byte[][] objectIds);

  public static native byte[][] wait(long conn, byte[][] objectIds, int timeoutMs,
      int numReturns);

  public static native long evict(long conn, long numBytes);

}
