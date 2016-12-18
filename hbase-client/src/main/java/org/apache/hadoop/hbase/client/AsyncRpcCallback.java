package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

import com.google.protobuf.RpcCallback;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface AsyncRpcCallback<T> extends RpcCallback<T> {
  /**
   * Called when the whole call is completed successfully
   * @param result the return result of remote call
   */
  @Override
  public void run(T result);

  /**
   * Called when the whole call is failed with given exception
   */
  public void onError(Throwable exception);

  /**
   * Set region location for current call, for single request only
   * @param location the region location for current call
   */
  public void setLocation(HRegionLocation location);
}
