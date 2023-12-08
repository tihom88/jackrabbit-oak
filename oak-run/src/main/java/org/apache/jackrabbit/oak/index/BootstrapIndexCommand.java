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

package org.apache.jackrabbit.oak.index;

import com.codahale.metrics.MetricRegistry;
import joptsimple.OptionParser;
import org.apache.commons.io.FileUtils;
import org.apache.felix.inventory.Format;
import org.apache.jackrabbit.guava.common.base.Joiner;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.guava.common.collect.ImmutableMap;
import org.apache.jackrabbit.guava.common.collect.Sets;
import org.apache.jackrabbit.guava.common.io.Closer;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.index.async.AsyncIndexerLucene;
import org.apache.jackrabbit.oak.index.indexer.document.DocumentStoreIndexer;
import org.apache.jackrabbit.oak.index.indexer.document.LuceneIndexer;
import org.apache.jackrabbit.oak.index.indexer.document.LuceneIndexerProvider;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.NodeTraversalCallback;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexDefinitionUpdater;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexWriterFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.DefaultIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriter;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.progress.IndexingProgressReporter;
import org.apache.jackrabbit.oak.plugins.index.progress.MetricRateEstimator;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.spi.binary.FulltextBinaryTextExtractor;
import org.apache.jackrabbit.oak.plugins.index.search.util.NodeStateCopyUtils;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.apache.jackrabbit.oak.query.QueryEngineImpl;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.run.LuceneRepositoryFixture;
import org.apache.jackrabbit.oak.run.cli.CommonOptions;
import org.apache.jackrabbit.oak.run.cli.DocumentBuilderCustomizer;
import org.apache.jackrabbit.oak.run.cli.IndexRepositoryFixture;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.run.commons.LoggingInitializer;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.spi.QNodeDefinition;
import org.apache.jackrabbit.util.ISO8601;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;
import javax.jcr.query.RowIterator;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static org.apache.jackrabbit.guava.common.base.Preconditions.checkArgument;
import static org.apache.jackrabbit.guava.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.guava.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_SORTED_FILE_PATH;
import static org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider.compose;

public class BootstrapIndexCommand implements Command {
    private static final Logger log = LoggerFactory.getLogger(BootstrapIndexCommand.class);
    private static final String LOG_SUFFIX = "indexing";

    public static final String NAME = "bootstrap-index";
    public static final String INDEX_DEFINITIONS_JSON = "index-definitions.json";
    public static final String INDEX_INFO_TXT = "index-info.txt";
    public static final String INDEX_CONSISTENCY_CHECK_TXT = "index-consistency-check-report.txt";

    private final String summary = "Provides index management related operations";

    private File info;
    private File definitions;
    private File consistencyCheckReport;
    private Options opts;
    private IndexOptions indexOpts;
    private static boolean disableExitOnError;

    private final ExecutorService executorService = Executors.newFixedThreadPool(2);

    public void checkTikaDependency() throws ClassNotFoundException {
        Class.forName("org.apache.tika.parser.pdf.PDFParser");
    }

    // to be overridden by test cases that want to check the return value
    public void exit(int status) {
        System.exit(status);
    }

    private long resultCount(QueryEngine qe, String query) throws ParseException {
        long count = 0;
        for (ResultRow row : qe.executeQuery(query, "JCR-SQL2", NO_BINDINGS, emptyMap()).getRows()) {
            count++;
        }
        return count;
    }

    private long resultCount(QueryManager qm, String query) throws ParseException, RepositoryException {
        long count = 0;
        Query q = qm.createQuery(query, Query.JCR_SQL2);
        QueryResult result = q.execute();
        for (RowIterator it = result.getRows(); it.hasNext(); ) {
            Row row = (Row) it.next();
            count++;
        }
        return count;
    }

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();

        opts = new Options();
        opts.setCommandName(NAME);
        opts.setSummary(summary);
        opts.setConnectionString(CommonOptions.DEFAULT_CONNECTION_STRING);
        opts.registerOptionsFactory(IndexOptions.FACTORY);
        opts.parseAndConfigure(parser, args);

        indexOpts = opts.getOptionBean(IndexOptions.class);

//        if (indexOpts.isReindex() && !opts.getCommonOpts().isHelpRequested() && !indexOpts.isIgnoreMissingTikaDep()) {
//            try {
//                checkTikaDependency();
//            } catch (Throwable e) {
//                System.err.println("Missing tika parser dependencies, use --ignore-missing-tika-dep to force continue");
//                exit(1);
//                return;
//            }
//        }

        //Clean up before setting up NodeStore as the temp
        //directory might be used by NodeStore for cache stuff like persistentCache
//        setupDirectories(indexOpts);
        setupLogging(indexOpts);

        logCliArgs(args);

        boolean success = false;
//        NodeStoreFixture fixture;
        try {
            if (indexOpts.isBootstrapIndex()) {
                Closer closer = Closer.create();
                /*

                NodeStoreFixture fixturex = NodeStoreFixtureProvider.create(opts);
                ExtendedIndexHelper extendedIndexHelper = createIndexHelper(fixturex, indexOpts, closer);

                IndexerSupport indexerSupport = createIndexerSupport(extendedIndexHelper, indexOpts.getCheckpoint());
                NodeStore ns  = fixturex.getStore();
                IndexRepositoryFixture luceneFixture = new LuceneRepositoryFixture(indexOpts.getWorkDir(), ns);
                Session sessionx = luceneFixture.getAdminSession();
                QueryManager qm = sessionx.getWorkspace().getQueryManager();
                long counttraversal = resultCount(qm, "SELECT * FROM [nt:base] as a WHERE a.[foo]='bar' option (traversal fail)");
                boolean istraverse = false;
                long count = resultCount(qm, "SELECT * FROM [nt:base] as a WHERE a.[boot]='bar' option (traversal fail)");
                if (count !=2 ){
                    throw new RuntimeException("count should be zero for boot");
                }
//                String query = "SELECT * FROM [nt:base] as a WHERE a.[boot] is not null option(index tag [bootstrap])"; //----------------------- hardcoded boot
                Query q = qm.createQuery("SELECT * FROM [nt:base] as a WHERE a.[boot] is not null option(index tag [bootstrap])", Query.JCR_SQL2);
                //QueryResult result = q.execute();


                try (DocumentStoreIndexer indexer = new DocumentStoreIndexer(extendedIndexHelper, indexerSupport)) {
                    IndexCopier copier = null;
                    try {
                        copier = new IndexCopier(executorService, indexOpts.getWorkDir());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    LuceneIndexEditorProvider editorProvider = new LuceneIndexEditorProvider(copier, new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
//                    LuceneIndexProvider indexProvider = new LuceneIndexProvider(copier);
                    indexer.bootstrapIndex(ns, qm, q, editorProvider);
                }
                //ns.getRoot().getChildNode("oak:index").getChildNode("fooIndex").getChildNode("indexRules").getChildNode("nt:base").getChildNode("properties").getChildNode("boot")
//                assertNotNull(ns.getRoot().getChildNode("oak:index").getChildNode("fooIndex").getChildNode("indexRules"));
//                assertNotNull(ns.getRoot().getChildNode("oak:index").getChildNode("bootstrap").getChildNode("indexRules"));

                //long bootcount = resultCount(qm, "SELECT * FROM [nt:base] as a WHERE a.[boot]='bar' option (traversal fail)");

                sessionx.save();
                sessionx.logout();

                NodeStoreFixture fixturens = NodeStoreFixtureProvider.create(opts);
                NodeStore nsx = fixturens.getStore();
                IndexRepositoryFixture luceneFixture1 = new LuceneRepositoryFixture(indexOpts.getWorkDir(), nsx);
                Session session1 = luceneFixture1.getAdminSession();
                QueryManager qm1 = session1.getWorkspace().getQueryManager();
                long bootcount1 = resultCount(qm1, "SELECT * FROM [nt:base] as a WHERE a.[boot]='bar' option (traversal fail)");
                long test = bootcount1;

                session1.save();
                session1.logout();

*/




//--------------------------
                System.out.println("creating nodestorefixture");
                NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts);
                System.out.println("created nodestorefixture");
                IndexCopier copier = null;
                try {
                    copier = new IndexCopier(executorService, indexOpts.getWorkDir());
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
                System.out.println("created indexcopier");

                LuceneIndexEditorProvider editorProvider = new LuceneIndexEditorProvider(copier, new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
                LuceneIndexProvider indexProvider = new LuceneIndexProvider(copier);

//                 resultCountingIndexProvider = new ResultCountingIndexProvider(indexProvider);
                QueryEngineSettings queryEngineSettings = new QueryEngineSettings();
//                 optionalEditorProvider = new TestUtil.OptionalEditorProvider();
//                 asyncIndexUpdate.setCorruptIndexHandler(trackingCorruptIndexHandler);
                SecurityProvider securityProvider = new OpenSecurityProvider();
                IndexEditorProvider indexEditorProvider = new PropertyIndexEditorProvider();
                QueryIndexProvider nodeTypeIndexProvider = new NodeTypeIndexProvider();
                QueryIndexProvider propertyIndexProvider = new PropertyIndexProvider();
                //QueryIndexProvider luceneIndexProvider = new LuceneIndexProvider();

                System.out.println("creating oak");
                Oak oak = new Oak(fixture.getStore())
                        .with(securityProvider)
//                         .with(resultCountingIndexProvider)
                        .with((Observer) indexProvider)
                        .with((QueryIndexProvider) indexProvider)
                        .with(editorProvider)
//                         .with(optionalEditorProvider)
                        .with(indexEditorProvider)
                        .with(propertyIndexProvider)
                        .with(nodeTypeIndexProvider)
                        .with(queryEngineSettings);
                ContentRepository repository = oak.createContentRepository();
                System.out.println("created oak");

                Whiteboard wb = fixture.getWhiteboard();
                @NotNull ContentSession session = repository.login(null, null);
                @NotNull Root root = session.getLatestRoot();
                @NotNull QueryEngine qe = root.getQueryEngine();
                String checkpoint = connectInReadWriteModeAndCreateCheckPoint(indexOpts);
                ExtendedIndexHelper extendedIndexHelper = createIndexHelper(fixture, indexOpts, closer);
                IndexerSupport indexerSupport = createIndexerSupport(extendedIndexHelper, checkpoint);
                LuceneIndexerProvider luceneIndexerProvider = new LuceneIndexerProvider(extendedIndexHelper, indexerSupport);
//                 String query = "SELECT * FROM [nt:base] as a WHERE a.[foo] is not null option(index tag [bootstrap])";

//                 LuceneIndexDefinition idxDefinition = LuceneIndexDefinition.newBuilder(root, definition.getNodeState(), indexPath).reindex().build();
//                 LuceneIndexDefinition idxDefinition = new LuceneIndexDefinition.Builder().(root, definition.getNodeState(), indexPath).reindex().build();
                NodeBuilder idxBuilder = IndexerSupport.childBuilder(fixture.getStore().getRoot().builder(), indexOpts.getIndexPaths().get(0), false);
                LuceneIndexDefinition idxDefinition = new LuceneIndexDefinition(fixture.getStore().getRoot(),
                        idxBuilder.getNodeState(), indexOpts.getIndexPaths().get(0)); //---------------------- working on one index only

                LuceneIndexWriterFactory indexWriterFactory = luceneIndexerProvider.getIndexWriterFactory();

                LuceneIndexWriter luceneIndexWriter = indexWriterFactory.newInstance(idxDefinition, idxBuilder, null, false);
//                 FulltextBinaryTextExtractor textExtractor = new FulltextBinaryTextExtractor(textCache, idxDefinition, true);
                IndexingProgressReporter progressReporter =
                        new IndexingProgressReporter(IndexUpdateCallback.NOOP, NodeTraversalCallback.NOOP);
                configureEstimators(extendedIndexHelper, progressReporter);
                progressReporter.registerIndex(indexOpts.getIndexPaths().get(0), false, 0); //-----------------working on one index only

                LuceneIndexer luceneIndexer = new LuceneIndexer(
                        idxDefinition,
                        luceneIndexWriter,
                        idxBuilder,
//                         textExtractor,
                        null,
                        progressReporter
                );
                System.out.println("created lucene indexer");

/*
                long counttraversal1 = resultCount(qe, "SELECT * FROM [nt:base] as a WHERE a.[foo]='bar' option (traversal fail)");
                boolean istraverse1 = false;

                long countx = resultCount(qe, "SELECT * FROM [nt:base] as a WHERE a.[boot]='bar' option (traversal fail)");
                if (countx !=2 ){
                    throw new RuntimeException("count should be zer for boot");
                }
                    //throw new Throwable("failed");

*/
//                 String query = "SELECT * FROM [nt:base] as a WHERE a.[foo] is not null";
                String query = "SELECT * FROM [nt:base] as a WHERE a.[boot] is not null option(index tag [bootstrap])"; //----------------------- hardcoded boot
                System.out.println("executing query");
                Result result = qe.executeQuery(query, "JCR-SQL2", NO_BINDINGS, emptyMap());
                Iterator<? extends ResultRow> resultIter = result.getRows().iterator();
                List<String> ans = new LinkedList<>();
                for (ResultRow row : result.getRows()) {
                    String path = row.getPath();
                    ans.add(path);
                    NodeBuilder nodeBuilder = IndexerSupport.childBuilder(fixture.getStore().getRoot().builder(), path, false);
                    luceneIndexer.index(
                            new NodeStateEntry.NodeStateEntryBuilder(nodeBuilder.getNodeState(), path).build());
                    System.out.println("nodes from querycount:" + ans.size());
                }
                System.out.println("indexing done");
                /*
                long count1 = resultCount(qe, "SELECT * FROM [nt:base] as a WHERE a.[boot]='bar' option (traversal fail)");
                if (count1 == 0){
                    throw new RuntimeException("count should not be zero");
                }
*/
                //---------------------------------------




//                AsyncIndexerLucene asyncIndexerService = new AsyncIndexerLucene(extendedIndexHelper, indexOpts.isCowCorEnabled(), closer,
//                        indexOpts.getAsyncLanes(), indexOpts.aysncDelay());
//                closer.register(asyncIndexerService);

//                closer.register(fixturex);
//                closer.register(luceneFixture);

//                asyncIndexerService.execute();
            } else {
                try (Closer closer = Closer.create()) {
                    configureCustomizer(opts, closer, true);
                    NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts);
                    closer.register(fixture);
                    execute(fixture, indexOpts, closer);
                    tellReportPaths();
                }
            }
            success = true;
        } catch (Throwable e) {
            log.error("Error occurred while performing index tasks", e);
            e.printStackTrace(System.err);
            if (disableExitOnError) {
                throw e;
            }
        } finally {
            shutdownLogging();
        }

        if (!success) {
            System.exit(1);
        }
    }

    private void configureEstimators(ExtendedIndexHelper indexHelper, IndexingProgressReporter progressReporter) {
        StatisticsProvider statsProvider = indexHelper.getStatisticsProvider();
        if (statsProvider instanceof MetricStatisticsProvider) {
            MetricRegistry registry = ((MetricStatisticsProvider) statsProvider).getRegistry();
            progressReporter.setTraversalRateEstimator(new MetricRateEstimator("async", registry));
        }
        long nodesCount = getEstimatedDocumentCount(indexHelper);
        if (nodesCount > 0) {
            progressReporter.setNodeCountEstimator((String basePath, Set<String> indexPaths) -> nodesCount);
            progressReporter.setEstimatedCount(nodesCount);
            log.info("Estimated number of documents in Mongo are {}", nodesCount);
        }
    }

    private long getEstimatedDocumentCount(ExtendedIndexHelper indexHelper) {
        MongoConnection mongoConnection = indexHelper.getService(MongoConnection.class);
        if (mongoConnection != null) {
            return mongoConnection.getDatabase().getCollection("nodes").count();
        }
        return 0;
    }

    public static void setDisableExitOnError(boolean disableExitOnError) {
        BootstrapIndexCommand.disableExitOnError = disableExitOnError;
    }

    private void tellReportPaths() {
        if (info != null) {
            System.out.printf("Index stats stored at %s%n", getPath(info));
        }

        if (definitions != null) {
            System.out.printf("Index definitions stored at %s%n", getPath(definitions));
        }

        if (consistencyCheckReport != null) {
            System.out.printf("Index consistency check report stored at %s%n", getPath(consistencyCheckReport));
        }
    }

    private void execute(NodeStoreFixture fixture, IndexOptions indexOpts, Closer closer)
            throws IOException, CommitFailedException {
        ExtendedIndexHelper extendedIndexHelper = createIndexHelper(fixture, indexOpts, closer);

        dumpIndexStats(indexOpts, extendedIndexHelper);
        dumpIndexDefinitions(indexOpts, extendedIndexHelper);
        performConsistencyCheck(indexOpts, extendedIndexHelper);
        dumpIndexContents(indexOpts, extendedIndexHelper);
        reindexOperation(indexOpts, extendedIndexHelper);
        importIndexOperation(indexOpts, extendedIndexHelper);
    }

    private ExtendedIndexHelper createIndexHelper(NodeStoreFixture fixture,
                                                  IndexOptions indexOpts, Closer closer) throws IOException {
        ExtendedIndexHelper extendedIndexHelper = new ExtendedIndexHelper(fixture.getStore(), fixture.getBlobStore(), fixture.getWhiteboard(),
                indexOpts.getOutDir(), indexOpts.getWorkDir(), computeIndexPaths(indexOpts));

        configurePreExtractionSupport(indexOpts, extendedIndexHelper);

        closer.register(extendedIndexHelper);
        return extendedIndexHelper;
    }

    private List<String> computeIndexPaths(IndexOptions indexOpts) throws IOException {
        //Combine the indexPaths from json and cli args
        Set<String> indexPaths = new LinkedHashSet<>(indexOpts.getIndexPaths());
        File definitions = indexOpts.getIndexDefinitionsFile();
        if (definitions != null) {
            IndexDefinitionUpdater updater = new IndexDefinitionUpdater(definitions);
            Set<String> indexPathsFromJson = updater.getIndexPaths();
            Set<String> diff = Sets.difference(indexPathsFromJson, indexPaths);
            if (!diff.isEmpty()) {
                log.info("Augmenting the indexPaths with {} which are present in {}", diff, definitions);
            }
            indexPaths.addAll(indexPathsFromJson);
        }
        return new ArrayList<>(indexPaths);
    }

    private void configurePreExtractionSupport(IndexOptions indexOpts, ExtendedIndexHelper extendedIndexHelper) throws IOException {
        File preExtractedTextDir = indexOpts.getPreExtractedTextDir();
        if (preExtractedTextDir != null) {
            extendedIndexHelper.setPreExtractedTextDir(preExtractedTextDir);
            log.info("Using pre-extracted text directory {}", getPath(preExtractedTextDir));
        }
    }

    private void reindexOperation(IndexOptions indexOpts, ExtendedIndexHelper extendedIndexHelper) throws IOException, CommitFailedException {
        if (!indexOpts.isReindex()) {
            return;
        }

        String checkpoint = indexOpts.getCheckpoint();
        File destDir = reindex(indexOpts, extendedIndexHelper, checkpoint);
        log.info("To complete indexing import the created index files via IndexerMBean#importIndex operation with " +
                "[{}] as input", getPath(destDir));
    }

    private void importIndexOperation(IndexOptions indexOpts, ExtendedIndexHelper extendedIndexHelper) throws IOException, CommitFailedException {
        if (indexOpts.isImportIndex()) {
            File importDir = indexOpts.getIndexImportDir();
            importIndex(extendedIndexHelper, importDir);
        }
    }

    private File reindex(IndexOptions idxOpts, ExtendedIndexHelper extendedIndexHelper, String checkpoint) throws IOException, CommitFailedException {
        checkNotNull(checkpoint, "Checkpoint value is required for reindexing done in read only mode");

        Stopwatch reindexWatch = Stopwatch.createStarted();
        IndexerSupport indexerSupport = createIndexerSupport(extendedIndexHelper, checkpoint);
        log.info("Proceeding to index {} upto checkpoint {} {}", extendedIndexHelper.getIndexPaths(), checkpoint,
                indexerSupport.getCheckpointInfo());

        if (opts.getCommonOpts().isMongo() && idxOpts.isDocTraversalMode()) {
            log.info("Using Document order traversal to perform reindexing");
            try (DocumentStoreIndexer indexer = new DocumentStoreIndexer(extendedIndexHelper, indexerSupport)) {
                if (idxOpts.buildFlatFileStoreSeparately()) {
                    FlatFileStore ffs = indexer.buildFlatFileStore();
                    String pathToFFS = ffs.getFlatFileStorePath();
                    System.setProperty(OAK_INDEXER_SORTED_FILE_PATH, pathToFFS);
                }
                indexer.reindex();
            }
        } else {
            try (OutOfBandIndexer indexer = new OutOfBandIndexer(extendedIndexHelper, indexerSupport)) {
                indexer.reindex();
            }
        }

        indexerSupport.writeMetaInfo(checkpoint);
        File destDir = indexerSupport.copyIndexFilesToOutput();
        log.info("Indexing completed for indexes {} in {} ({} ms) and index files are copied to {}",
                extendedIndexHelper.getIndexPaths(), reindexWatch, reindexWatch.elapsed(TimeUnit.MILLISECONDS), BootstrapIndexCommand.getPath(destDir));
        return destDir;
    }

    private void importIndex(ExtendedIndexHelper extendedIndexHelper, File importDir) throws IOException, CommitFailedException {
        new IndexImporterSupport(extendedIndexHelper).importIndex(importDir);
    }

    private void performReindexInReadWriteMode(IndexOptions indexOpts) throws Exception {
        Stopwatch w = Stopwatch.createStarted();
        //TODO To support restart we need to store this checkpoint somewhere
        String checkpoint = connectInReadWriteModeAndCreateCheckPoint(indexOpts);
        log.info("Created checkpoint [{}] for indexing", checkpoint);

        log.info("Proceeding to reindex with read only access to NodeStore");
        File indexDir = performReindexInReadOnlyMode(indexOpts, checkpoint);

        Stopwatch importWatch = Stopwatch.createStarted();
        log.info("Proceeding to import index data from [{}] by connecting to NodeStore in read-write mode", getPath(indexDir));
        connectInReadWriteModeAndImportIndex(indexOpts, indexDir);
        log.info("Indexes imported successfully in {} ({} ms)", importWatch, importWatch.elapsed(TimeUnit.MILLISECONDS));

        log.info("Indexing completed and imported successfully in {} ({} ms)", w, w.elapsed(TimeUnit.MILLISECONDS));
    }

    private File performReindexInReadOnlyMode(IndexOptions indexOpts, String checkpoint) throws Exception {
        try (Closer closer = Closer.create()) {
            configureCustomizer(opts, closer, true);
            NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts, true);
            closer.register(fixture);
            ExtendedIndexHelper extendedIndexHelper = createIndexHelper(fixture, indexOpts, closer);
            reindex(indexOpts, extendedIndexHelper, checkpoint);
            return new File(indexOpts.getOutDir(), OutOfBandIndexer.LOCAL_INDEX_ROOT_DIR);
        }
    }

    private String connectInReadWriteModeAndCreateCheckPoint(IndexOptions indexOpts) throws Exception {
        String checkpoint = indexOpts.getCheckpoint();
        if (checkpoint != null) {
            log.info("Using provided checkpoint [{}]", checkpoint);
            return checkpoint;
        }

        try (NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts)) {
            return fixture.getStore().checkpoint(TimeUnit.DAYS.toMillis(100), ImmutableMap.of(
                    "creator", BootstrapIndexCommand.class.getSimpleName(),
                    "created", now()));
        }
    }

    private void connectInReadWriteModeAndImportIndex(IndexOptions indexOpts, File indexDir) throws Exception {
        try (Closer closer = Closer.create()) {
            configureCustomizer(opts, closer, false);
            NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts);
            closer.register(fixture);
            ExtendedIndexHelper extendedIndexHelper = createIndexHelper(fixture, indexOpts, closer);
            importIndex(extendedIndexHelper, indexDir);
        }
    }

    private void dumpIndexContents(IndexOptions indexOpts, ExtendedIndexHelper extendedIndexHelper) throws IOException {
        if (indexOpts.dumpIndex()) {
            new IndexDumper(extendedIndexHelper, indexOpts.getOutDir()).dump();
        }
    }

    private void performConsistencyCheck(IndexOptions indexOpts, ExtendedIndexHelper extendedIndexHelper) throws IOException {
        if (indexOpts.checkConsistency()) {
            IndexConsistencyCheckPrinter printer =
                    new IndexConsistencyCheckPrinter(extendedIndexHelper, indexOpts.consistencyCheckLevel());
            PrinterDumper dumper = new PrinterDumper(extendedIndexHelper.getOutputDir(), INDEX_CONSISTENCY_CHECK_TXT,
                    false, Format.TEXT, printer);
            dumper.dump();
            consistencyCheckReport = dumper.getOutFile();
        }
    }

    private void dumpIndexDefinitions(IndexOptions indexOpts, ExtendedIndexHelper extendedIndexHelper) throws IOException {
        if (indexOpts.dumpDefinitions()) {
            PrinterDumper dumper = new PrinterDumper(extendedIndexHelper.getOutputDir(), INDEX_DEFINITIONS_JSON,
                    false, Format.JSON, extendedIndexHelper.getIndexDefnPrinter());
            dumper.dump();
            definitions = dumper.getOutFile();
        }
    }

    private void dumpIndexStats(IndexOptions indexOpts, ExtendedIndexHelper extendedIndexHelper) throws IOException {
        if (indexOpts.dumpStats()) {
            PrinterDumper dumper = new PrinterDumper(extendedIndexHelper.getOutputDir(), INDEX_INFO_TXT,
                    true, Format.TEXT, extendedIndexHelper.getIndexPrinter());
            dumper.dump();
            info = dumper.getOutFile();
        }
    }

    private IndexerSupport createIndexerSupport(ExtendedIndexHelper extendedIndexHelper, String checkpoint) {
        IndexerSupport indexerSupport = new IndexerSupport(extendedIndexHelper, checkpoint)
                .withExistingDataDumpDir(indexOpts.getExistingDataDumpDir());

        File definitions = indexOpts.getIndexDefinitionsFile();
        if (definitions != null) {
            checkArgument(definitions.exists(), "Index definitions file [%s] not found", getPath(definitions));
            indexerSupport.setIndexDefinitions(definitions);
        }
        return indexerSupport;
    }

    private static void setupDirectories(IndexOptions indexOpts) throws IOException {
        if (indexOpts.getOutDir().exists()) {
            if (indexOpts.isImportIndex() &&
                    FileUtils.directoryContains(indexOpts.getOutDir(), indexOpts.getIndexImportDir())) {
                //Do not clean directory in this case
            } else {
                FileUtils.cleanDirectory(indexOpts.getOutDir());
            }
        }
        cleanWorkDir(indexOpts.getWorkDir());
    }

    private static void cleanWorkDir(File workDir) throws IOException {
        //TODO Do not clean if restarting
        String[] dirListing = workDir.list();
        if (dirListing != null && dirListing.length != 0) {
            FileUtils.cleanDirectory(workDir);
        }
    }

    private static void setupLogging(IndexOptions indexOpts) throws IOException {
        new LoggingInitializer(indexOpts.getWorkDir(), LOG_SUFFIX).init();
    }

    private void shutdownLogging() {
        LoggingInitializer.shutdownLogging();
    }

    private static String now() {
        return ISO8601.format(Calendar.getInstance());
    }

    private static void logCliArgs(String[] args) {
        log.info("Command line arguments used for indexing [{}]", Joiner.on(' ').join(args));
        List<String> inputArgs = ManagementFactory.getRuntimeMXBean().getInputArguments();
        if (!inputArgs.isEmpty()) {
            log.info("System properties and vm options passed {}", inputArgs);
        }
    }

    static Path getPath(File file) {
        return file.toPath().normalize().toAbsolutePath();
    }

    private static void configureCustomizer(Options opts, Closer closer, boolean readOnlyAccess) {
        if (opts.getCommonOpts().isDocument()) {
            IndexOptions indexOpts = opts.getOptionBean(IndexOptions.class);
            if (indexOpts.isReindex()) {
                IndexDocumentBuilderCustomizer customizer = new IndexDocumentBuilderCustomizer(opts, readOnlyAccess);
                Registration reg = opts.getWhiteboard().register(DocumentBuilderCustomizer.class, customizer, emptyMap());
                closer.register(reg::unregister);
            }
        }
    }
}
