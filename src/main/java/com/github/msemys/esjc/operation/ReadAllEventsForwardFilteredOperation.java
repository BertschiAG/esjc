package com.github.msemys.esjc.operation;

import com.github.msemys.esjc.AllEventsFilteredSlice;
import com.github.msemys.esjc.AllEventsSlice;
import com.github.msemys.esjc.Position;
import com.github.msemys.esjc.ReadDirection;
import com.github.msemys.esjc.UserCredentials;
import com.github.msemys.esjc.proto.EventStoreClientMessages;
import com.github.msemys.esjc.tcp.TcpCommand;
import com.google.protobuf.MessageLite;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static com.github.msemys.esjc.util.Strings.defaultIfEmpty;

public class ReadAllEventsForwardFilteredOperation extends AbstractOperation<AllEventsFilteredSlice, EventStoreClientMessages.ReadAllEventsFilteredCompleted> {

  private final Position position;
  private final int maxCount;
  private final int maxSearchWindow;
  private final boolean resolveLinkTos;
  private final boolean requireMaster;
  private final Iterable<String> allowedEventTypes;

  public ReadAllEventsForwardFilteredOperation(CompletableFuture<AllEventsFilteredSlice> result,
                                       Position position,
                                       int maxCount,
                                       int maxSearchWindow,
                                       boolean resolveLinkTos,
                                       boolean requireMaster,
                                       UserCredentials userCredentials,
                                       Iterable<String> allowedEventTypes) {
    super(result, TcpCommand.ReadAllEventsForwardFiltered, TcpCommand.ReadAllEventsForwardFilteredCompleted, userCredentials);
    this.position = position;
    this.maxCount = maxCount;
    this.maxSearchWindow = maxSearchWindow;
    this.resolveLinkTos = resolveLinkTos;
    this.requireMaster = requireMaster;
    this.allowedEventTypes = allowedEventTypes == null ? Collections.emptyList() : allowedEventTypes;
  }

  @Override
  protected MessageLite createRequestMessage() {
    return EventStoreClientMessages.ReadAllEventsFiltered.newBuilder()
        .setCommitPosition(position.commitPosition)
        .setPreparePosition(position.preparePosition)
        .setMaxCount(maxCount)
        .setMaxSearchWindow(maxSearchWindow)
        .setResolveLinkTos(resolveLinkTos)
        .setRequireMaster(requireMaster)
        .addAllAllowedEventTypes(this.allowedEventTypes)
        .build();
  }

  @Override
  protected EventStoreClientMessages.ReadAllEventsFilteredCompleted createResponseMessage() {
    return EventStoreClientMessages.ReadAllEventsFilteredCompleted.getDefaultInstance();
  }

  @Override
  protected InspectionResult inspectResponseMessage(EventStoreClientMessages.ReadAllEventsFilteredCompleted response) {
    switch (response.getResult()) {
      case Success:
        succeed();
        return InspectionResult.newBuilder()
            .decision(InspectionDecision.EndOperation)
            .description("Success")
            .build();
      case Error:
        fail(new ServerErrorException(defaultIfEmpty(response.getError(), "<no message>")));
        return InspectionResult.newBuilder()
            .decision(InspectionDecision.EndOperation)
            .description("Error")
            .build();
      case AccessDenied:
        fail(new AccessDeniedException("Read access denied for $all."));
        return InspectionResult.newBuilder()
            .decision(InspectionDecision.EndOperation)
            .description("Error")
            .build();
      default:
        throw new IllegalArgumentException(String.format("Unexpected ReadAllResult: %s.", response.getResult()));
    }
  }

  @Override
  protected AllEventsFilteredSlice transformResponseMessage(EventStoreClientMessages.ReadAllEventsFilteredCompleted response) {
    return new AllEventsFilteredSlice(
        ReadDirection.Forward,
        new Position(response.getCommitPosition(), response.getPreparePosition()),
        new Position(response.getNextCommitPosition(), response.getNextPreparePosition()),
        response.getEventsList(),
        response.getIsEndOfStream());
  }

  @Override
  public String toString() {
    return String.format("position: %s, maxCount: %d, maxSearchWindow: %d, resolveLinkTos: %s, requireMaster: %s, allowedTypes: %s",
        position, maxCount, maxSearchWindow, resolveLinkTos, requireMaster, allowedEventTypes);
  }

}
