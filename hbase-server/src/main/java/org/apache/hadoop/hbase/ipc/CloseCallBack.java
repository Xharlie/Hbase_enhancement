package org.apache.hadoop.hbase.ipc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CloseCallBack implements RpcCallback {
  private static final Log LOG = LogFactory.getLog(CloseCallBack.class);

  private final List<RpcCallback> callbacks = new ArrayList<RpcCallback>();

  public void addCallback(RpcCallback callback) {
    this.callbacks.add(callback);
  }

  @Override
  public void run() {
    for (RpcCallback callback : callbacks) {
      try {
        callback.run();
      } catch (IOException e) {
        LOG.error("Exception while running the callback " + callback, e);
      }
    }
  }

}
