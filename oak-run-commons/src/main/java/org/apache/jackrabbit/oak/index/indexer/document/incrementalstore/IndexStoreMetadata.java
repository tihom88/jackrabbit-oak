package org.apache.jackrabbit.oak.index.indexer.document.incrementalstore;

import org.apache.jackrabbit.oak.index.indexer.document.IndexStore.IndexStoreSortStrategy;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.SortStrategy;

import java.util.List;
import java.util.function.Predicate;

public class IndexStoreMetadata {

    private String oldCheckpoint;
    private String newCheckpoint;
     private String storeType;

    private List<String> preferredPaths;
    private Predicate<String> pathPredicate;

    public IndexStoreMetadata(String oldCheckpoint, String newCheckpoint, String storeType, List<String> preferredPaths, Predicate<String> pathPredicate) {
        this.oldCheckpoint = oldCheckpoint;
        this.newCheckpoint = newCheckpoint;
        this.storeType = storeType;
        this.preferredPaths = preferredPaths;
        this.pathPredicate = pathPredicate;
    }

    public IndexStoreMetadata(IndexStoreSortStrategy indexStoreSortStrategy) {
        this.oldCheckpoint = indexStoreSortStrategy.getBeforeCheckpoint();
        this.newCheckpoint = indexStoreSortStrategy.getAfterCheckpoint();
        this.storeType = indexStoreSortStrategy.getStrategyName();
        this.preferredPaths = indexStoreSortStrategy.getPreferredPaths();
        this.pathPredicate = indexStoreSortStrategy.getPathPredicate();
    }

    public String getOldCheckpoint() {
        return oldCheckpoint;
    }

    public String getNewCheckpoint() {
        return newCheckpoint;
    }

    public String getStoreType() {
        return storeType;
    }

    public List<String> getPreferredPaths() {
        return preferredPaths;
    }

    public Predicate<String> getPathPredicate() {
        return pathPredicate;
    }
}
