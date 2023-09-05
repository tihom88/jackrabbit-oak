/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.index.indexer.document.incrementalstore;

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.index.IndexHelper;
import org.apache.jackrabbit.oak.index.indexer.document.CompositeException;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.LZ4Compression;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.MemoryManager;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryReader;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryWriter;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.PathElementComparator;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.SortStrategy;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Set;
import java.util.function.Predicate;

import static java.util.Collections.unmodifiableSet;

public class IncrementalStoreBuilder {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String INCREMENTAL_STORE_DIR_NAME_PREFIX = "inc-store";

    private final File workDir;
    private final MemoryManager memoryManager;
    private final IndexHelper indexHelper;

    private String initialCheckpoint;
    private String incrementalsFFSOutputFile;
    private String finalCheckpoint;
    /**
     * System property name for sort strategy. This takes precedence over {@link #INCREMENTAL_SORT_STRATEGY_TYPE}.
     * Allowed values are the values from enum {@link IncrementalStoreBuilder.IncrementalSortStrategyType}
     */
    public static final String INCREMENTAL_SORT_STRATEGY_TYPE = "oak.indexer.incrementalSortStrategyType";


    private final String sortStrategyTypeString = System.getProperty(INCREMENTAL_SORT_STRATEGY_TYPE);
    private IncrementalStoreBuilder.IncrementalSortStrategyType sortStrategyType = sortStrategyTypeString != null
            ? IncrementalStoreBuilder.IncrementalSortStrategyType.valueOf(sortStrategyTypeString)
            : IncrementalSortStrategyType.INCREMENTAL_FFS_STORE;


    public enum IncrementalSortStrategyType {
        /**
         * Incremental store having nodes updated between initial and final checkpoint
         */

        INCREMENTAL_FFS_STORE
    }

    public IncrementalStoreBuilder(File workDir, MemoryManager memoryManager, IndexHelper indexHelper) {
        this.workDir = workDir;
        this.memoryManager = memoryManager;
        this.indexHelper = indexHelper;
    }

    public IncrementalStoreBuilder withInitialCheckpoint(String checkpoint) {
        this.initialCheckpoint = checkpoint;
        return this;
    }

    public IncrementalStoreBuilder withFinalCheckpoint(String checkpoint) {
        this.finalCheckpoint = checkpoint;
        return this;
    }

    public IncrementalStoreBuilder withOutputFile(String incrementalsFFSOutputFile) {
        this.incrementalsFFSOutputFile = incrementalsFFSOutputFile;
        return this;
    }

    public IncrementalStoreBuilder withPreferredPathElements(Set<String> preferredPathElements) {
        this.preferredPathElements = preferredPathElements;
        return this;
    }

    public IncrementalStoreBuilder withSortStrategyType(IncrementalStoreBuilder.IncrementalSortStrategyType sortStrategyType) {
        this.sortStrategyType = sortStrategyType;
        return this;
    }

    private Predicate<String> pathPredicate = path -> true;

    public IncrementalStoreBuilder withPathPredicate(Predicate<String> pathPredicate) {
        this.pathPredicate = pathPredicate;
        return this;
    }


    private Set<String> preferredPathElements = Collections.emptySet();
    private BlobStore blobStore;
    private PathElementComparator comparator;
    private NodeStateEntryWriter entryWriter;
    private long entryCount = 0;

    private final boolean compressionEnabled = Boolean.parseBoolean(System.getProperty(FlatFileNodeStoreBuilder.OAK_INDEXER_USE_ZIP, "true"));
    private final boolean useLZ4 = Boolean.parseBoolean(System.getProperty(FlatFileNodeStoreBuilder.OAK_INDEXER_USE_LZ4, "false"));

    private final Compression algorithm = compressionEnabled ? (useLZ4 ? new LZ4Compression() : Compression.GZIP) :
            Compression.NONE;

    public IncrementalStore build() throws IOException, CompositeException {
        logFlags();
        File dir = createStoreDir();

        switch (sortStrategyType) {
            case INCREMENTAL_FFS_STORE:
                comparator = new PathElementComparator(preferredPathElements);
                entryWriter = new NodeStateEntryWriter(blobStore);
                SortStrategy strategy = new IncrementalFlatFileStoreStrategy(indexHelper.getNodeStore().retrieve(initialCheckpoint),
                        indexHelper.getNodeStore().retrieve(finalCheckpoint),
                        dir, comparator, algorithm, pathPredicate, entryWriter);
                File result = strategy.createSortedStoreFile();
                entryCount = strategy.getEntryCount();
                IncrementalStore store = new IncrementalFlatFileStore(blobStore, result,
                        new NodeStateEntryReader(blobStore),
                        unmodifiableSet(preferredPathElements), algorithm);
                if (entryCount > 0) {
                    ((IncrementalFlatFileStore) store).setEntryCount(entryCount);
                }
                return store;
        }
        throw new IllegalStateException("Not a valid sort strategy value " + sortStrategyType);
    }

    private File createStoreDir() throws IOException {
        File flatFileStoreDir = Files.createTempDirectory(workDir.toPath(), getDirNamePrefix()).toFile();
        return flatFileStoreDir;
    }

    private String getDirNamePrefix() {
        return INCREMENTAL_STORE_DIR_NAME_PREFIX + sortStrategyTypeString;
    }

    private void logFlags() {
        log.info("Preferred path elements are {}", Iterables.toString(preferredPathElements));
        log.info("Compression enabled while sorting : {} ({})", compressionEnabled, FlatFileNodeStoreBuilder.OAK_INDEXER_USE_ZIP);
        log.info("LZ4 enabled for compression algorithm : {} ({})", useLZ4, FlatFileNodeStoreBuilder.OAK_INDEXER_USE_LZ4);
//        log.info("Sort strategy : {} ({})", sortStrategyType, FlatFileNodeStoreBuilder.OAK_INDEXER_TRAVERSE_WITH_SORT);
    }


}
