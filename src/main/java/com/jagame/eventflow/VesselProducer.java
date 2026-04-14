package com.jagame.eventflow;

import com.jagame.common.ChunkSpliterator;

import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface VesselProducer<T> {

    String vessel();

    CompletableFuture<Void> send(T events);

    CompletableFuture<Void> send(List<T> events);

    default Stream<T> send(Stream<T> dataStream) {
        return send(dataStream, 1)
                .map(l -> l.get(0));
    }

    default Stream<List<T>> send(Stream<T> dataStream, int chunkSize) {
        Spliterator<T> dataSpliterator = dataStream.spliterator();
        Spliterator<List<T>> chunkSpliterator = new ChunkSpliterator<>(dataSpliterator, chunkSize);
        return StreamSupport.stream(chunkSpliterator, dataStream.isParallel());
    }

}
