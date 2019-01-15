package com.github.msemys.esjc;

import com.github.msemys.esjc.proto.EventStoreClientMessages;

import java.util.List;

public class AllEventsFilteredSlice extends AllEventsSlice {
  private final boolean isEndOfStream;

  public AllEventsFilteredSlice(ReadDirection readDirection,
                        Position fromPosition,
                        Position nextPosition,
                        List<EventStoreClientMessages.ResolvedEvent> events,
                        boolean isEndOfStream) {
    super(readDirection, fromPosition, nextPosition, events);
    this.isEndOfStream = isEndOfStream;
  }

  @Override
  public boolean isEndOfStream() {
    return this.isEndOfStream;
  }
}
