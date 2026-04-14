package com.jagame.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

public class ChunkSpliterator<T> implements Spliterator<List<T>> {
    private final Spliterator<T> originalSpliterator;
    private final int chunkSize;

    public ChunkSpliterator(Spliterator<T> originalSpliterator, int chunkSize) {
        this.originalSpliterator = originalSpliterator;
        this.chunkSize = chunkSize;
        if(chunkSize <= 0) {
            throw new IllegalArgumentException("chunk size must be greater than 0");
        }
    }

    @Override
    @SuppressWarnings("StatementWithEmptyBody")
    public boolean tryAdvance(Consumer<? super List<T>> action) {
        List<T> chunk = new ArrayList<>(chunkSize);
        for (int i = chunkSize; i > 0 && originalSpliterator.tryAdvance(chunk::add); i--);
        if(chunk.isEmpty()) {
            return false;
        }
        action.accept(chunk);
        return true;
    }

    @Override
    public Spliterator<List<T>> trySplit() {
        Spliterator<T> originalSplit = originalSpliterator.trySplit();
        if(originalSplit == null) {
            return null;
        }
        return new ChunkSpliterator<>(originalSplit, chunkSize);
    }

    @Override
    public long estimateSize() {
        if ((originalSpliterator.characteristics() & Spliterator.SIZED) == 0) {
            return Long.MAX_VALUE;
        }
        return (long) Math.ceil(originalSpliterator.estimateSize() / (double) chunkSize);
    }

    @Override
    public int characteristics() {
        return originalSpliterator.characteristics() & ~Spliterator.SORTED;
    }

}
