package org.apache.jackrabbit.oak.index.indexer.document.IndexStore;

import org.apache.jackrabbit.oak.index.indexer.document.CompositeException;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;

public interface IndexStoreSortStrategy {

    String getStrategyName();

    String getBeforeCheckpoint();
    String getAfterCheckpoint();

    List<String> getPreferredPaths();

    Predicate<String> getPathPredicate();
    File createSortedStoreFile() throws IOException, CompositeException;

    long getEntryCount();
}
