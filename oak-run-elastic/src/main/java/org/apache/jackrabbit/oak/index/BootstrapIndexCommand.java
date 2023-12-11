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

import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch.core.CreateRequest;
import co.elastic.clients.elasticsearch.core.PutScriptRequest;
import co.elastic.clients.elasticsearch.indices.CloneIndexRequest;
import co.elastic.clients.elasticsearch.indices.GetAliasResponse;
import co.elastic.clients.elasticsearch.indices.IndexSettingBlocks;
import co.elastic.clients.elasticsearch.indices.IndexSettings;
import co.elastic.clients.elasticsearch.indices.PutIndicesSettingsRequest;
import co.elastic.clients.elasticsearch.indices.PutMappingRequest;
import co.elastic.clients.elasticsearch.indices.UpdateAliasesRequest;
import co.elastic.clients.elasticsearch.indices.UpdateAliasesResponse;
import com.codahale.metrics.MetricRegistry;
import joptsimple.OptionParser;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.guava.common.base.Joiner;
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
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.index.indexer.document.ElasticIndexerProvider;

import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateIndexer;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexName;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.NodeTraversalCallback;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexNameHelper;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexTracker;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticMetricHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexDefinitionUpdater;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.progress.IndexingProgressReporter;
import org.apache.jackrabbit.oak.plugins.index.progress.MetricRateEstimator;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.apache.jackrabbit.oak.plugins.migration.NodeStateCopier;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.run.cli.CommonOptions;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.run.commons.LoggingInitializer;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.util.ISO8601;
import org.h2.engine.Setting;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static org.apache.jackrabbit.guava.common.base.Preconditions.checkArgument;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;

public class BootstrapIndexCommand implements Command {
    private static final Logger log = LoggerFactory.getLogger(BootstrapIndexCommand.class);
    private static final String LOG_SUFFIX = "indexing";

    public static final String NAME = "bootstrap-index";
    private final String summary = "Provides index management related operations";

    private Options opts;
    private IndexOptions indexOpts;
    private static boolean disableExitOnError;

    @Override
    public void execute(String... args) throws Exception {


        OptionParser parser = new OptionParser();

        opts = new Options();
        opts.setCommandName(NAME);
        opts.setSummary(summary);
        opts.setConnectionString(CommonOptions.DEFAULT_CONNECTION_STRING);
        opts.registerOptionsFactory(IndexOptions.FACTORY);
        opts.parseAndConfigure(parser, args);

        indexOpts = opts.getOptionBean(ElasticIndexOptions.class);
        setupLogging(indexOpts);

        logCliArgs(args);

        boolean success = false;
        Closer closer = Closer.create();
        try {
            if (indexOpts.isBootstrapIndex()) {
                System.out.println("Starting to execute bootstrap indexing command.");
                NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts);
                closer.register(fixture);
                //System.out.println("created nodestorefixture");

                ElasticConnection esConn = ElasticConnection.newBuilder().withIndexPrefix("oak-elastic").withConnectionParameters("http", "localhost", 9200).build();

                String propAddFilePath = indexOpts.getPropAddFilePath();

                JsonObject propAdditionsJson = JsonObject.fromJson(FileUtils.readFileToString(new File(propAddFilePath), StandardCharsets.UTF_8), true);
                String sourceIndexNameStr = propAdditionsJson.getProperties().get("baseIndex");
                sourceIndexNameStr = sourceIndexNameStr.replace("\"", "");
                IndexName sourceIndexNameObj = IndexName.parse(sourceIndexNameStr);
                String targetIndexNameStr = sourceIndexNameObj.nextCustomizedName();

                Map<String, Set<String>> propAdditions = new LinkedHashMap<>();
                for (String indexRule : propAdditionsJson.getChildren().get("indexRules").getChildren().keySet()) {
                    propAdditions.put(indexRule, propAdditionsJson.getChildren().get("indexRules").getChildren().get(indexRule).getChildren().get("properties").getChildren().keySet());
                }
                copyAndAugmentIndexWithNewProperties(esConn, fixture.getStore(), sourceIndexNameStr, targetIndexNameStr, propAdditions);


                ElasticMetricHandler handler = new ElasticMetricHandler(StatisticsProvider.NOOP);
                ElasticIndexTracker tracker = new ElasticIndexTracker(esConn, handler);
                ElasticIndexProvider indexProvider = new ElasticIndexProvider(tracker);

                ElasticIndexEditorProvider editorProvider = new ElasticIndexEditorProvider(tracker, esConn, null);
                QueryEngineSettings queryEngineSettings = new QueryEngineSettings();
                SecurityProvider securityProvider = new OpenSecurityProvider();
                IndexEditorProvider pIndexEditorProvider = new PropertyIndexEditorProvider();
                IndexEditorProvider indexEditorProvider = new CompositeIndexEditorProvider(pIndexEditorProvider, new ReferenceEditorProvider(), editorProvider);

                QueryIndexProvider nodeTypeIndexProvider = new NodeTypeIndexProvider();
                QueryIndexProvider propertyIndexProvider = new PropertyIndexProvider();

                //System.out.println("creating oak");
                Oak oak = new Oak(fixture.getStore())
                        .with(securityProvider)
                        .with(indexProvider)
                        .with(new ReferenceIndexProvider())
                        .with(editorProvider)
                        .with(tracker)
                        .with(indexEditorProvider)
                        .with(propertyIndexProvider)
                        .with(nodeTypeIndexProvider)
                        .with(queryEngineSettings);
                ContentRepository repository = oak.createContentRepository();
                //System.out.println("created oak");

                @NotNull ContentSession session = repository.login(null, null);
                @NotNull Root root = session.getLatestRoot();
                @NotNull QueryEngine qe = root.getQueryEngine();
                IndexHelper extendedIndexHelper = createIndexHelper(fixture, indexOpts, closer);

                ElasticIndexerProvider elasticIndexerProvider = new ElasticIndexerProvider(extendedIndexHelper, esConn, false);
                NodeBuilder rootBuilder = fixture.getStore().getRoot().builder();
                NodeBuilder idxBuilder = IndexerSupport.childBuilder(rootBuilder, targetIndexNameStr, false);
                IndexingProgressReporter progressReporter =
                        new IndexingProgressReporter(IndexUpdateCallback.NOOP, NodeTraversalCallback.NOOP);
                NodeStateIndexer elasticIndexer = elasticIndexerProvider.getIndexer("elasticsearch",
                        targetIndexNameStr, idxBuilder, rootBuilder.getNodeState(), progressReporter);

                configureEstimators(extendedIndexHelper, progressReporter);
                progressReporter.registerIndex(targetIndexNameStr, false, 0); //-----------------working on one index only

                //System.out.println("created elastic indexer");

                Set<String> pathsToUpdate = new HashSet<>();
                long start = System.currentTimeMillis();

                for (String rule : propAdditions.keySet()) {
                    for (String addedProp : propAdditions.get(rule)) {
                        // TODO : make this query for all properties being added instead of multiple queries.
                        String query = "SELECT * FROM [" + rule + "] as a WHERE a.[" + addedProp+ "] is not null option(index tag [bootstrap])";
                        System.out.println("Executing Query" + query + " to get results from bootstrap index.");
                        Result result = qe.executeQuery(query, "JCR-SQL2", NO_BINDINGS, emptyMap());
                        int count = 0;
                        for (ResultRow row : result.getRows()) {
                            String path = row.getPath();
                            System.out.println("Updating index for path " + path);
                            NodeBuilder nodeBuilder = IndexerSupport.childBuilder(fixture.getStore().getRoot().builder(), path, false);
                            elasticIndexer.index(
                                    new NodeStateEntry.NodeStateEntryBuilder(nodeBuilder.getNodeState(), path).build());

                            //pathsToUpdate.add(path);
                            count++;
                        }
                        System.out.println("Number of nodes updated using bootstrap indexing process - " + count);
                    }
                }
                //System.out.println("Total paths to be updated in index" + pathsToUpdate.size());
                /*for (String path : pathsToUpdate) {
                    NodeBuilder nodeBuilder = IndexerSupport.childBuilder(fixture.getStore().getRoot().builder(), path, false);
                    elasticIndexer.index(
                            new NodeStateEntry.NodeStateEntryBuilder(nodeBuilder.getNodeState(), path).build());
                }*/
                elasticIndexer.close();
                System.out.println("Time taken to index using bootstrap process " + (System.currentTimeMillis() - start));

                //System.out.println("indexing done");
            }
            success = true;
        } catch (Throwable e) {
            log.error("Error occurred while performing index tasks", e);
            e.printStackTrace(System.err);
            if (disableExitOnError) {
                throw e;
            }
        } finally {
            closer.close();
            //System.out.println("in finally...");
            shutdownLogging();
            //System.out.println("ended...");
        }
        System.exit(1);
        /*if (!success) {
            System.exit(1);
        }*/
    }


    private void copyAndAugmentIndexWithNewProperties(ElasticConnection esConn, NodeStore store, String existingIndexPath, String newIndexPath, Map<String, Set<String>> propAdditions) throws CommitFailedException, IOException {

        // Clone

        NodeState root = store.getRoot();
        NodeState sourceIdx = getNodeStateFromPath(root, existingIndexPath);
        ElasticIndexDefinition idx = new ElasticIndexDefinition(root, sourceIdx, existingIndexPath, "oak-elastic");

        String remoteSourceIndexName = ElasticIndexNameHelper.
                getRemoteIndexName(esConn.getIndexPrefix(), idx.getIndexPath(), sourceIdx.builder());
        long targetIndexSeed = UUID.randomUUID().getMostSignificantBits();
        String remoteTargetIndexName = ElasticIndexNameHelper.
                getRemoteIndexName(esConn.getIndexPrefix(), newIndexPath, targetIndexSeed);


        long start = System.currentTimeMillis();
        //System.out.println("Starting to clone index : " + existingIndexPath);
        // Block write on index to be cloned
        IndexSettingBlocks indexSettingBlocks = new IndexSettingBlocks.Builder().write(true).build();
        IndexSettings idxSettings = new IndexSettings.Builder().blocks(indexSettingBlocks).build();
        PutIndicesSettingsRequest putIndicesSettingsRequest = new PutIndicesSettingsRequest.Builder().index(remoteSourceIndexName).preserveExisting(true).settings(idxSettings).build();
        esConn.getClient().indices().putSettings(putIndicesSettingsRequest);
        /// TODO : need to block elastic-async lane as well.
        esConn.getClient().indices().clone(new CloneIndexRequest.Builder().index(remoteSourceIndexName).target(remoteTargetIndexName).build());
        System.out.println("Cloned Index from " + existingIndexPath + " to " + newIndexPath);
        System.out.println("Time to clone - " + (System.currentTimeMillis() - start));


        // Revert block write on both source and copied index
        indexSettingBlocks = new IndexSettingBlocks.Builder().write(false).build();
        idxSettings = new IndexSettings.Builder().blocks(indexSettingBlocks).build();
        putIndicesSettingsRequest = new PutIndicesSettingsRequest.Builder().index(Arrays.asList(remoteSourceIndexName, remoteTargetIndexName)).preserveExisting(true).settings(idxSettings).build();
        esConn.getClient().indices().putSettings(putIndicesSettingsRequest);


        //System.out.println("Starting to update mapping");

        // Update mapping
        Property property = new Property.Builder().keyword(b1 -> b1.ignoreAbove(256)).build();
        PutMappingRequest.Builder putMappingRequestBuilder = new PutMappingRequest.Builder();
        putMappingRequestBuilder.index(remoteTargetIndexName);
        for (String indexRule : propAdditions.keySet()) {
            Set<String> properties = propAdditions.get(indexRule);
            for (String prop : properties) {
                putMappingRequestBuilder.properties(prop, property);
            }
        }
        esConn.getClient().indices().putMapping(putMappingRequestBuilder.build());

        NodeStateCopier.Builder copyBuilder = NodeStateCopier.builder();
        NodeBuilder rootBuilder = root.builder();
        NodeBuilder targetBuilder = getNodeBuilderFromPath(rootBuilder, newIndexPath);

        copyBuilder.copy(sourceIdx, targetBuilder);

        targetBuilder.setProperty(ElasticIndexDefinition.PROP_INDEX_NAME_SEED, targetIndexSeed);

        IndexDefinitionBuilder indexDefinitionBuilder = new IndexDefinitionBuilder(targetBuilder, false);
        Tree idxTree = indexDefinitionBuilder.getBuilderTree();
        idxTree.setProperty("type", sourceIdx.builder().getProperty("type").getValue(Type.STRING));
        idxTree.setProperty("indexNameSeed", targetIndexSeed);
        //idxTree.setProperty(ElasticIndexDefinition.PROP_INDEX_NAME_SEED, targetIndexSeed);
        idxTree.setProperty("merges", Arrays.asList("/oak:index/testIndex"), Type.STRINGS);
        idxTree.setProperty("refresh", true);
        indexDefinitionBuilder.async("elastic-async");

        for (String indexRule : propAdditions.keySet()) {
            Set<String> properties = propAdditions.get(indexRule);
            for (String prop : properties) {
                indexDefinitionBuilder.indexRule(indexRule).property(prop).propertyIndex();
            }
        }
        indexDefinitionBuilder.build();


        // Update alias for target index
        ElasticIndexDefinition destIdxDef = new ElasticIndexDefinition(root, targetBuilder.getNodeState(), newIndexPath, "oak-elastic");

        GetAliasResponse aliasResponse = esConn.getClient().indices().getAlias(garb ->
                garb.index(destIdxDef.getIndexAlias()).ignoreUnavailable(true));

        UpdateAliasesRequest updateAliasesRequest = UpdateAliasesRequest.of(rb -> {
            aliasResponse.result().forEach((idx_, idxAliases) -> rb.actions(ab -> // remove old aliases
                    ab.remove(rab -> rab.index(idx_).aliases(new ArrayList<>(idxAliases.aliases().keySet()))))
            );
            return rb.actions(ab -> ab.add(aab -> aab.index(remoteTargetIndexName).alias(destIdxDef.getIndexAlias()))); // add new one
        });
        UpdateAliasesResponse updateAliasesResponse = esConn.getClient().indices().updateAliases(updateAliasesRequest);

        store.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static NodeState getNodeStateFromPath(NodeState root, String path) {
        NodeState parent = root;
        NodeState state = null;
        for (String pathElement : PathUtils.elements(path)) {
            //System.out.println("Getting source state - " + pathElement);
            state = parent.getChildNode(pathElement);
            parent = state;
        }
        return state;
    }

    private static NodeBuilder getNodeBuilderFromPath(NodeBuilder rootBuilder, String path) {
        NodeBuilder parentBuilder = rootBuilder;
        NodeBuilder builder = null;
        for (String pathElement: PathUtils.elements(path)) {
            //System.out.println("Getting dest builder - " + path);
            builder = parentBuilder.child(pathElement);
            parentBuilder = builder;
        }
        return builder;
    }

    private void configureEstimators(IndexHelper indexHelper, IndexingProgressReporter progressReporter) {
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

    private long getEstimatedDocumentCount(IndexHelper indexHelper) {
        MongoConnection mongoConnection = indexHelper.getService(MongoConnection.class);
        if (mongoConnection != null) {
            return mongoConnection.getDatabase().getCollection("nodes").count();
        }
        return 0;
    }

    private IndexHelper createIndexHelper(NodeStoreFixture fixture,
                                          IndexOptions indexOpts, Closer closer) throws IOException {
        IndexHelper extendedIndexHelper = new IndexHelper(fixture.getStore(), fixture.getBlobStore(), fixture.getWhiteboard(),
                indexOpts.getOutDir(), indexOpts.getWorkDir(), computeIndexPaths(indexOpts));
        //configurePreExtractionSupport(indexOpts, extendedIndexHelper);
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

    private IndexerSupport createIndexerSupport(IndexHelper extendedIndexHelper, String checkpoint) {
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
}
