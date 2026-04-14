package com.jagame.eventflow;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Stream;

public interface VesselConsumer<R> {

    String vessel();

    CompletableFuture<R> next();

    CompletableFuture<List<R>> next(int numMessages);

    default Stream<R> stream() {
        return stream(false);
    }

    default Stream<R> parallelStream() {
        return stream(true);
    }

    default Stream<R> stream(boolean parallel) {
        return stream(VesselConsumer::next, parallel)
                .map(CompletableFuture::join);
    }

    default Stream<List<R>> stream(int chunkSize) {
        return stream(chunkSize, false);
    }

    default Stream<List<R>> parallelStream(int chunkSize) {
        return stream(chunkSize, true);
    }

    default Stream<List<R>> stream(int chunkSize, boolean parallel) {
        return stream(c -> c.next(chunkSize), parallel)
                .map(CompletableFuture::join);
    }

    default <U> Stream<U> stream(Function<? super VesselConsumer<R>, U> fetchMethod, boolean parallel) {
        Stream<U> dataStream = Stream.generate(() -> fetchMethod.apply(this));
        if(parallel) {
            dataStream = dataStream.unordered().parallel();
        }
        return dataStream;
    }

}
