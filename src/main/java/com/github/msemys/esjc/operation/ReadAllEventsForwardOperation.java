package com.github.msemys.esjc.operation;

import com.github.msemys.esjc.AllEventsSlice;
import com.github.msemys.esjc.Position;
import com.github.msemys.esjc.ReadDirection;
import com.github.msemys.esjc.UserCredentials;
import com.github.msemys.esjc.proto.EventStoreClientMessages.ReadAllEvents;
import com.github.msemys.esjc.proto.EventStoreClientMessages.ReadAllEventsCompleted;
import com.github.msemys.esjc.tcp.TcpCommand;
import com.google.protobuf.MessageLite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;

import static com.github.msemys.esjc.util.Strings.defaultIfEmpty;

public class ReadAllEventsForwardOperation extends AbstractOperation<AllEventsSlice, ReadAllEventsCompleted> {

    private final Position position;
    private final int maxCount;
    private final boolean resolveLinkTos;
    private final boolean requireMaster;
    private final Iterable<String> allowedEventTypes;

    public ReadAllEventsForwardOperation(CompletableFuture<AllEventsSlice> result,
                                         Position position,
                                         int maxCount,
                                         boolean resolveLinkTos,
                                         boolean requireMaster,
                                         UserCredentials userCredentials,
                                         Iterable<String> allowedEventTypes) {
        super(result, TcpCommand.ReadAllEventsForward, TcpCommand.ReadAllEventsForwardCompleted, userCredentials);
        this.position = position;
        this.maxCount = maxCount;
        this.resolveLinkTos = resolveLinkTos;
        this.requireMaster = requireMaster;
        this.allowedEventTypes = allowedEventTypes == null ? Collections.emptyList() : allowedEventTypes;
    }

    @Override
    protected MessageLite createRequestMessage() {
        return ReadAllEvents.newBuilder()
                .setCommitPosition(position.commitPosition)
                .setPreparePosition(position.preparePosition)
                .setMaxCount(maxCount)
                .setResolveLinkTos(resolveLinkTos)
                .setRequireMaster(requireMaster)
                .addAllAllowedEventTypes(this.allowedEventTypes)
                .build();
    }

    @Override
    protected ReadAllEventsCompleted createResponseMessage() {
        return ReadAllEventsCompleted.getDefaultInstance();
    }

    @Override
    protected InspectionResult inspectResponseMessage(ReadAllEventsCompleted response) {
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
    protected AllEventsSlice transformResponseMessage(ReadAllEventsCompleted response) {
        return new AllEventsSlice(
                ReadDirection.Forward,
                new Position(response.getCommitPosition(), response.getPreparePosition()),
                new Position(response.getNextCommitPosition(), response.getNextPreparePosition()),
                response.getEventsList());
    }

    @Override
    public String toString() {
        return String.format("position: %s, maxCount: %d, resolveLinkTos: %s, requireMaster: %s",
                position, maxCount, resolveLinkTos, requireMaster);
    }

}
