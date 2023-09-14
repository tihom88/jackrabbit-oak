package org.apache.jackrabbit.oak.index.indexer.document.indexstore;

import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.index.IndexHelper;
import org.apache.jackrabbit.oak.index.IndexerSupport;
import org.apache.jackrabbit.oak.index.indexer.document.CompositeException;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverserFactory;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.MemoryManager;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.MultithreadedTraverseWithSortStrategy;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryWriter;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.PathElementComparator;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.SortStrategy;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.StoreAndSortStrategy;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.TraverseWithSortStrategy;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedStrategy;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.function.Predicate;

public class IndexStoreCreator  implements SortStrategy{


    private Set<String> preferredPathElements = Collections.emptySet();
    private BlobStore blobStore;
    private PathElementComparator comparator;
    private NodeStateEntryWriter entryWriter;
    private NodeStateEntryTraverserFactory nodeStateEntryTraverserFactory;
    private long entryCount = 0;
    private File flatFileStoreDir;
    private final MemoryManager memoryManager;
    private Predicate<String> pathPredicate = path -> true;



    public IndexStoreCreator(IndexHelper indexHelper, IndexerSupport indexerSupport, Compression algorithm,FlatFileNodeStoreBuilder.SortStrategyType sortStrategyType) {

    }

    @Override
    public File createSortedStoreFile() throws IOException, CompositeException {
        return null;
    }

    @Override
    public long getEntryCount() {
        return 0;
    }
}
