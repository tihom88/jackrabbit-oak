package org.apache.jackrabbit.oak.index.indexer.document;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.TraversingRange;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.NodeTraversalCallback;
import org.apache.jackrabbit.oak.plugins.index.progress.IndexingProgressReporter;
import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;
public class MongoNodeStateEntryTraverserFactory implements NodeStateEntryTraverserFactory {

    /**
     * This counter is part of this traverser's id and is helpful in identifying logs from different traversers that
     * run concurrently.
     */
    private static final AtomicInteger traverserInstanceCounter = new AtomicInteger(0);
    /**
     * An prefix for ID of traversers (value is acronym for NodeStateEntryTraverser).
     */
    private static final String TRAVERSER_ID_PREFIX = "NSET";
    private final RevisionVector rootRevision;
    private final DocumentNodeStore documentNodeStore;
    private final MongoDocumentStore documentStore;
    private final Logger traversalLogger;
    private final CompositeIndexer indexer;

    public MongoNodeStateEntryTraverserFactory(RevisionVector rootRevision, DocumentNodeStore documentNodeStore,
                                                MongoDocumentStore documentStore, Logger traversalLogger, CompositeIndexer indexer) {
        this.rootRevision = rootRevision;
        this.documentNodeStore = documentNodeStore;
        this.documentStore = documentStore;
        this.traversalLogger = traversalLogger;
        this.indexer = indexer;
    }

    @Override
    public NodeStateEntryTraverser create(TraversingRange traversingRange) {
        IndexingProgressReporter progressReporterPerTask =
                new IndexingProgressReporter(IndexUpdateCallback.NOOP, NodeTraversalCallback.NOOP);
        String entryTraverserID = TRAVERSER_ID_PREFIX + traverserInstanceCounter.incrementAndGet();
        //As first traversal is for dumping change the message prefix
        progressReporterPerTask.setMessagePrefix("Dumping from " + entryTraverserID);
        return new NodeStateEntryTraverser(entryTraverserID, rootRevision,
                documentNodeStore, documentStore, traversingRange)
                .withProgressCallback((id) -> {
                    try {
                        progressReporterPerTask.traversedNode(() -> id);
                    } catch (CommitFailedException e) {
                        throw new RuntimeException(e);
                    }
                    traversalLogger.trace(id);
                });
    }

    public static void main(String[] args) {
        System.out.println("Hello, World!");
        MongoNodeStateEntryTraverserFactory h = new MongoNodeStateEntryTraverserFactory(null, null, null, null, null);

    }
}
