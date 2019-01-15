package com.github.msemys.esjc.subscription;

import com.github.msemys.esjc.AllEventsSlice;
import com.github.msemys.esjc.CatchUpSubscriptionListener;
import com.github.msemys.esjc.EventStore;
import com.github.msemys.esjc.Position;
import com.github.msemys.esjc.UserCredentials;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

public class FilteredAllCatchUpSubscription extends AllCatchUpSubscription {
  private final int maxSearchWindow;
  private final Iterable<String> allowedEventTypes;

  public FilteredAllCatchUpSubscription(EventStore eventstore,
                                Position position,
                                boolean resolveLinkTos,
                                CatchUpSubscriptionListener listener,
                                UserCredentials userCredentials,
                                int readBatchSize,
                                int maxPushQueueSize,
                                Executor executor,
                                int maxSearchWindow,
                                Iterable<String> allowedEventTypes) {
    super(eventstore, position, resolveLinkTos, listener, userCredentials, readBatchSize, maxPushQueueSize, executor);
    this.maxSearchWindow = maxSearchWindow;
    this.allowedEventTypes = allowedEventTypes;
  }

  @Override
  protected AllEventsSlice getNextSlice(EventStore eventstore, boolean resolveLinkTos, Position nextReadPosition, UserCredentials userCredentials) throws InterruptedException, ExecutionException {
    return eventstore.readAllEventsForwardFiltered(
        nextReadPosition,
        readBatchSize,
        maxSearchWindow,
        resolveLinkTos,
        allowedEventTypes,
        userCredentials).get();
  }
}
