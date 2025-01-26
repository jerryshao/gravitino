package org.apache.gravitino.file;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableScheduledFuture;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public abstract class AbstractJobHandle implements JobHandle {

  protected final List<Listener> listeners;

  protected volatile State state;

  protected ScheduledExecutorService executorService;

  protected final long initialPollIntervalInMs;

  protected final long maxPollIntervalInMs;

  protected abstract State pollState();

  protected AbstractJobHandle(long initialPollIntervalInMs, long maxPollIntervalInMs) {
    this.listeners = Lists.newArrayList();
    this.state = State.STARTED;

    Preconditions.checkArgument(initialPollIntervalInMs > 0, "Initial poll interval must be positive");
    Preconditions.checkArgument(
        maxPollIntervalInMs > 0 && maxPollIntervalInMs >= initialPollIntervalInMs,
        "Max poll interval must be positive and greater than or equal to initial poll interval");

    this.initialPollIntervalInMs = initialPollIntervalInMs;
    this.maxPollIntervalInMs = maxPollIntervalInMs;
    this.executorService = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "JobHandler");
      t.setDaemon(true);
      return t;
    });
    executorService.schedule(
        new StatePollTask(initialPollIntervalInMs), initialPollIntervalInMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public void addListener(Listener l) {
    synchronized (listeners) {
      listeners.add(l);
      fireStateChange(state, l);
    }
  }

  protected void changeState(State newState) {
    synchronized (listeners) {
      if (newState.ordinal() > state.ordinal() && state.ordinal() < State.FAILED.ordinal()) {
        state = newState;
        for (Listener l : listeners) {
          fireStateChange(newState, l);
        }
      }
    }
  }

  private void fireStateChange(State state, Listener l) {
    switch (state) {
      case STARTED:
        l.onJobStarted();
        break;
      case FAILED:
        l.onJobFailed();
        break;
      case SUCCEEDED:
        l.onJobSucceeded();
        break;
      default:
        throw new IllegalStateException("Unknown state: " + state);
    }
  }

  private class StatePollTask implements Runnable {

    private long currentIntervalInMs;

    StatePollTask(long currentIntervalInMs) {
      this.currentIntervalInMs = currentIntervalInMs;
    }

    @Override
    public void run() {
      try {
        State currentState = pollState();
        if (currentState != state) {
          changeState(currentState);
        }

        if (state == State.FAILED || state == State.SUCCEEDED) {
          executorService.shutdown();
        } else {
          currentIntervalInMs = Math.min(currentIntervalInMs * 2, maxPollIntervalInMs);
          executorService.schedule(this, currentIntervalInMs, TimeUnit.MILLISECONDS);
        }

      } catch (Exception e) {
        changeState(State.FAILED);
        executorService.shutdown();
      }
    }
  }
}
