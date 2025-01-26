package org.apache.gravitino.file;

import org.apache.gravitino.annotation.Evolving;

@Evolving
public interface JobHandle {

  enum State {
    STARTED,
    FAILED,
    SUCCEEDED;
  }

  interface Listener {

    void onJobStarted();

    void onJobFailed();

    void onJobSucceeded();
  }


  State getState();

  void addListener(Listener l);
}
