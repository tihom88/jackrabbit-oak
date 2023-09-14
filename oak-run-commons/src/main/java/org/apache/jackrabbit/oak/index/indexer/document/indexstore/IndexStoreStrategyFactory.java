package org.apache.jackrabbit.oak.index.indexer.document.indexstore;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.index.IndexHelper;
import org.apache.jackrabbit.oak.index.IndexerSupport;
import org.apache.jackrabbit.oak.index.indexer.document.CompositeIndexer;
import org.apache.jackrabbit.oak.index.indexer.document.DocumentStoreIndexerBase;
import org.apache.jackrabbit.oak.index.indexer.document.MongoNodeStateEntryTraverserFactory;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverser;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverserFactory;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateIndexerProvider;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.MemoryManager;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.MultithreadedTraverseWithSortStrategy;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryWriter;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.PathElementComparator;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.SortStrategy;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.StoreAndSortStrategy;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.TraverseWithSortStrategy;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedStrategy;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.TraversingRange;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.NodeTraversalCallback;
import org.apache.jackrabbit.oak.plugins.index.progress.IndexingProgressReporter;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.apache.jackrabbit.guava.common.base.Preconditions.checkNotNull;

public class IndexStoreStrategyFactory {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Logger traversalLog = LoggerFactory.getLogger(DocumentStoreIndexerBase.class.getName() + ".traversal");

    private Set<String> preferredPathElements = Collections.emptySet();
    private BlobStore blobStore;
    private NodeStateEntryWriter entryWriter;
    private long entryCount = 0;
    private File flatFileStoreDir;

    private Predicate<String> pathPredicate = path -> true;

    protected final IndexHelper indexHelper;
    protected List<NodeStateIndexerProvider> indexerProviders;
    protected final IndexerSupport indexerSupport;
    private final Compression algorithm;

    File workDir;

    public IndexStoreStrategyFactory(IndexHelper indexHelper, IndexerSupport indexerSupport, Compression algorithm) throws IOException {
        this.indexHelper = indexHelper;
        this.indexerSupport = indexerSupport;
        this.algorithm = algorithm;
        createStoreDir();
    }

    File createStoreDir() throws IOException {
        flatFileStoreDir = Files.createTempDirectory(workDir.toPath(), FlatFileNodeStoreBuilder.FLAT_FILE_STORE_DIR_NAME_PREFIX).toFile();
        return flatFileStoreDir;
    }

    public File getFlatFileStoreDir() {
        return flatFileStoreDir;
    }

    private MongoDocumentStore getMongoDocumentStore() {
        return checkNotNull(indexHelper.getService(MongoDocumentStore.class));
    }

    SortStrategy createStrategy(FlatFileNodeStoreBuilder.SortStrategyType sortStrategyType){
        NodeState checkpointedState = indexerSupport.retrieveNodeStateForCheckpoint();
        DocumentNodeStore nodeStore = (DocumentNodeStore) indexHelper.getNodeStore();
        DocumentNodeState rootDocumentState = (DocumentNodeState) checkpointedState;
        switch (sortStrategyType) {
            case STORE_AND_SORT:
                log.info("Using StoreAndSortStrategy");
                entryWriter = new NodeStateEntryWriter(blobStore);
                return new StoreAndSortStrategy(getMongoNodeStateEntryTraverserFactory(nodeStore, rootDocumentState), getComparator(), entryWriter, getFlatFileStoreDir(), algorithm, pathPredicate);
            case TRAVERSE_WITH_SORT:
                log.info("Using TraverseWithSortStrategy");
                return new TraverseWithSortStrategy(getMongoNodeStateEntryTraverserFactory(nodeStore, rootDocumentState), getComparator(), entryWriter, getFlatFileStoreDir(), algorithm, pathPredicate);
//            case MULTITHREADED_TRAVERSE_WITH_SORT:
//                log.info("Using MultithreadedTraverseWithSortStrategy");
//                return new MultithreadedTraverseWithSortStrategy(nodeStateEntryTraverserFactory, lastModifiedBreakPoints, comparator,
//                        blobStore, dir, existingDataDumpDirs, algorithm, memoryManager, dumpThreshold, pathPredicate);
            case PIPELINED:
                log.info("Using PipelinedStrategy");
                return new PipelinedStrategy(getMongoDocumentStore(), nodeStore, rootDocumentState.getRootRevision(),
                        preferredPathElements, blobStore, getFlatFileStoreDir(), algorithm, pathPredicate);
        }
        throw new IllegalStateException("Not a valid sort strategy value " + sortStrategyType);
    }

    @NotNull
    private PathElementComparator getComparator() {
        PathElementComparator comparator = new PathElementComparator(preferredPathElements);
        return comparator;
    }

    @NotNull
    private MongoNodeStateEntryTraverserFactory getMongoNodeStateEntryTraverserFactory(DocumentNodeStore nodeStore, DocumentNodeState rootDocumentState) {
        MongoNodeStateEntryTraverserFactory nodeStateEntryTraverserFactory = new MongoNodeStateEntryTraverserFactory(rootDocumentState.getRootRevision(),
                nodeStore, getMongoDocumentStore(), traversalLog, null); // indexer = null
        return nodeStateEntryTraverserFactory;
    }

    private static class MongoNodeStateEntryTraverserFactory implements NodeStateEntryTraverserFactory {

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

        private MongoNodeStateEntryTraverserFactory(RevisionVector rootRevision, DocumentNodeStore documentNodeStore,
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
    }

}
