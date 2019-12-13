package org.apache.jackrabbit.oak.indexversion;

import com.google.common.io.Closer;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexPathService;
import org.apache.jackrabbit.oak.plugins.index.IndexPathServiceImpl;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.property.RecursiveDelete;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.IndexName;
import org.apache.jackrabbit.oak.run.Utils;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

public class PurgeOldIndexVersion {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private static String INDEX_DEFINITION_NODE = ":index-definition";
    private static String REINDEX_COMPLETION_TIMESTAMP = "reindexCompletionTimestamp";

    /**
     * These operations are in ascending order and only one will be executed at last
     */
    enum Operand {
        NOOP, DELETE_HIDDEN_AND_DISABLE, DELETE
    }

    private class IndexNameWithOperation {
        private IndexName indexName;

        public void setOperation(Operand operation) {
            this.operation = operation;
        }

        public Operand getOperation() {
            return this.operation;
        }

        public IndexName getIndexName() {
            return this.indexName;
        }

        private Operand operation;

        IndexNameWithOperation(IndexName indexName) {
            this.indexName = indexName;
            this.operation = Operand.NOOP;
        }


    }

    public static void main(String[] args) throws Exception {


        String source = "/home/mokatari/adobe/aem/aemlatest-1.21-snap/author/crx-quickstart/repository/segmentstore";
        Closer closer = Utils.createCloserWithShutdownHook();
        FileStore fileStore = fileStoreBuilder(new File(source)).withStrictVersionCheck(true).build();
        //marker = new SegmentBlobReferenceRetriever(fileStore);
        closer.register(fileStore);
        NodeStore nodeStore =
                SegmentNodeStoreBuilders.builder(fileStore).build();
        PurgeOldIndexVersion p = new PurgeOldIndexVersion();
        p.parseCommandLineParams(args);
        //p.execute(nodeStore);
    }

    public void execute(String... args) throws Exception {
        Options opts = parseCommandLineParams(args);
        try (NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts, false)) {
            NodeStore nodeStore = fixture.getStore();
            List<String> indexPathList = getIndexPathList(nodeStore);
            Map<String, List<String>> segregateIndexes = segregateIndexes(indexPathList);
            execute2(nodeStore, segregateIndexes);
        }
    }

    private Map<String, List<String>> segregateIndexes(List<String> indexPathList) {

        Map<String, List<String>> segregatedIndexes = new HashMap<>();
        for (String path : indexPathList) {
            String baseIndexPath = getBaseIndexPath(path);
            List<String> indexPaths = segregatedIndexes.get(baseIndexPath);
            if (indexPaths == null) {
                indexPaths = new LinkedList<>();
            }
            indexPaths.add(PurgeOldVersionUtils.trimSlash(path));
            segregatedIndexes.put(baseIndexPath, indexPaths);
        }
        return segregatedIndexes;
    }

    public List<String> getIndexPathList(NodeStore store) throws CommitFailedException, IOException {
        IndexPathService indexPathService = new IndexPathServiceImpl(store);

        Iterable<String> indexPaths = indexPathService.getIndexPaths();
        List<String> indexPathsList = new LinkedList<>();
        for (String indexPath : indexPaths) {
            indexPathsList.add(PurgeOldVersionUtils.trimSlash(indexPath));
        }
        return indexPathsList;
    }

    StringBuilder stringJoiner(String[] stringArray, int arrayIndexToJoin, String delimiter) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int index = 0; index <= arrayIndexToJoin; index++) {
            stringBuilder.append(stringArray[index]);
            stringBuilder.append(delimiter);
        }
        return stringBuilder;
    }

    private String getBaseIndexPath(String path) {
        String trimmedPath = PurgeOldVersionUtils.trimSlash(path);
        String[] fragmentedPath = trimmedPath.split("/");
        String indexName = fragmentedPath[fragmentedPath.length - 1];
        String baseIndexName = PurgeOldVersionUtils.getBaseIndexName(indexName);

        return stringJoiner(fragmentedPath,
                fragmentedPath.length - 2, "/")
                .append(baseIndexName).toString();
    }

    private void execute2(NodeStore store, Map<String, List<String>> segregateIndexes) throws CommitFailedException {
        for (Map.Entry<String, List<String>> entry : segregateIndexes.entrySet()) {
            execute3(store, entry.getValue());
        }
    }

    private List<String> getIndexNameVersionList(List<String> versionedIndexPathList) {
        List<String> indexNameVersionList = new LinkedList<>();
        for (String indexPath : versionedIndexPathList) {
            String trimmedIndexPath = PurgeOldVersionUtils.trimSlash(indexPath);
            String[] splitPathArray = trimmedIndexPath.split("/");
            String indexName = splitPathArray[splitPathArray.length - 1];
            indexNameVersionList.add(indexName);
        }
        return indexNameVersionList;
    }

    private void execute3(NodeStore store, List<String> versionedIndexPathList) throws CommitFailedException {
        if (versionedIndexPathList.size() != 0) {
            log.info("Versioned list present for index " + versionedIndexPathList.get(0));
            String trimmedIndexPath = PurgeOldVersionUtils.trimSlash(versionedIndexPathList.get(0));
            String[] splitPathArray = trimmedIndexPath.split("/");
            NodeState indexDefParentNode = NodeStateUtils.getNode(store.getRoot(),
                    "/" + trimmedIndexPath.substring(0, trimmedIndexPath.lastIndexOf('/'))); //getIndexDefinitionNodeParentFromPath(store, trimmedIndexPath);
            String indexName = splitPathArray[splitPathArray.length - 1];
            String baseIndexName = PurgeOldVersionUtils.getBaseIndexName(indexName);
//            Iterable<String> allIndexes = getAllIndexes(indexDefParentNode);
            //List<String> allIndexNameVersions = getAllIndexVersions(baseIndexName, allIndexes);
            List<String> allIndexNameVersions = getIndexNameVersionList(versionedIndexPathList);
            List<IndexName> sortedIndexNameObjectList = getSortedVersionedIndexNameObjects(allIndexNameVersions);
            List<IndexNameWithOperation> toDeleteIndexNameObjectList =
                    getToDeleteIndexNameObjectList(indexDefParentNode, sortedIndexNameObjectList, 1);
            purgeOldIndexVersion(store, toDeleteIndexNameObjectList, trimmedIndexPath);
        }

    }

    private List<IndexNameWithOperation> getToDeleteIndexNameObjectList(NodeState indexDefParentNode,
                                                                        List<IndexName> indexNameObjectList, int threshold) {
        List<IndexNameWithOperation> toDeleteIndexNameObjectList = new LinkedList<>();
        for (int i = indexNameObjectList.size() - 1; i >= 0; i--) {
            NodeState indexNode = indexDefParentNode
                    .getChildNode(indexNameObjectList.get(i).getNodeName());
            toDeleteIndexNameObjectList.add(new IndexNameWithOperation(indexNameObjectList.get(i)));
            if (indexNode.hasChildNode(INDEX_DEFINITION_NODE)) {
                if (indexNode.getChildNode(INDEX_DEFINITION_NODE)
                        .getProperty(REINDEX_COMPLETION_TIMESTAMP) != null) {
                    String reindexCompletionTime = indexDefParentNode
                            .getChildNode(indexNameObjectList.get(i).getNodeName())
                            .getChildNode(INDEX_DEFINITION_NODE)
                            .getProperty(REINDEX_COMPLETION_TIMESTAMP).getValue(Type.DATE);
                    long reindexCompletionTimeInMillis = PurgeOldVersionUtils.getMillisFromString(reindexCompletionTime);
                    long currentTimeInMillis = System.currentTimeMillis();
                    if (currentTimeInMillis - reindexCompletionTimeInMillis > threshold) {
                        if (i > 0) {
                            IndexNameWithOperation lastBaseIndexNameWithOperation = null;
                            for (int j = 0; j < i; j++) {

                                if (indexNameObjectList.get(j).getCustomerVersion() == 0) {
                                    if (lastBaseIndexNameWithOperation != null) {
                                        lastBaseIndexNameWithOperation.setOperation(Operand.DELETE);
                                    }
                                    lastBaseIndexNameWithOperation = new IndexNameWithOperation(indexNameObjectList.get(j));
                                    lastBaseIndexNameWithOperation.setOperation(Operand.DELETE_HIDDEN_AND_DISABLE);
                                    toDeleteIndexNameObjectList.add(lastBaseIndexNameWithOperation);
                                }
                                else {
                                    IndexNameWithOperation customIndexName = new IndexNameWithOperation(indexNameObjectList.get(j));
                                    customIndexName.setOperation(Operand.DELETE);
                                    toDeleteIndexNameObjectList.add(customIndexName);
                                }
                            }
                            break;
                        }
                    }
                } else {
                    log.warn(REINDEX_COMPLETION_TIMESTAMP
                            + " property is not set for index " + indexNameObjectList.get(i).getNodeName());
                }
            }
        }
        return toDeleteIndexNameObjectList;
    }

    private void recursiveDeleteHiddenChildNodes(NodeStore store, String trimmedPath) throws CommitFailedException {
        NodeState nodeState = NodeStateUtils.getNode(store.getRoot(), "/" + trimmedPath);
        Iterable<String> childNodeNames = nodeState.getChildNodeNames();

        for (String childNodeName : childNodeNames) {
            if (NodeStateUtils.isHidden(childNodeName)) {
                RecursiveDelete recursiveDelete = new RecursiveDelete(store, EmptyHook.INSTANCE, () -> CommitInfo.EMPTY);
                recursiveDelete.run("/"+trimmedPath + "/" + childNodeName);
            }
        }


    }

    private void purgeOldIndexVersion(NodeStore store,
                                      List<IndexNameWithOperation> toDeleteIndexNameObjectList, String trimmedIndexPath) throws CommitFailedException {
        String indexPathParent = "/" + trimmedIndexPath.substring(0, trimmedIndexPath.lastIndexOf('/'));
        for (IndexNameWithOperation toDeleteIndexNameObject : toDeleteIndexNameObjectList) {
            NodeState root = store.getRoot();
            NodeBuilder rootBuilder = root.builder();
//            NodeBuilder nodeBuilderParent = PurgeOldVersionUtils.getNode(rootBuilder, indexPathParent + "/" + toDeleteIndexNameObject.getNodeName());
            NodeBuilder nodeBuilder = PurgeOldVersionUtils.getNode(rootBuilder, indexPathParent + "/" + toDeleteIndexNameObject.getIndexName().getNodeName());
            if (nodeBuilder.exists()) {

                if (toDeleteIndexNameObject.getOperation() == Operand.NOOP) {

                } else if (toDeleteIndexNameObject.getOperation() == Operand.DELETE_HIDDEN_AND_DISABLE) {
                    nodeBuilder.setProperty("type", "disabled", Type.STRING);
                    recursiveDeleteHiddenChildNodes(store, trimmedIndexPath.substring(0, trimmedIndexPath.lastIndexOf('/')) + "/" + toDeleteIndexNameObject.getIndexName().getNodeName());
                } else if (toDeleteIndexNameObject.getOperation() == Operand.DELETE) {
                    nodeBuilder.remove();
                }


                EditorHook hook = new EditorHook(
                        new IndexUpdateProvider(new PropertyIndexEditorProvider()));

                store.merge(rootBuilder, hook, CommitInfo.EMPTY);
                //RecursiveDelete recursiveDelete = new RecursiveDelete(store, EmptyHook.INSTANCE, () -> CommitInfo.EMPTY);
                //recursiveDelete.run(indexPathParent + "/" + toDeleteIndexNameObject.getNodeName());
            } else {
                log.error("nodebuilder null for path " + indexPathParent + "/" + toDeleteIndexNameObject.getIndexName().getNodeName());
            }

        }
        store.getRoot();

    }

    private List<IndexName> getSortedVersionedIndexNameObjects(List<String> allIndexes) {
        List<IndexName> indexNameObjectList = new ArrayList<>();
        for (String indexNameString : allIndexes) {
            indexNameObjectList.add(IndexName.parse(indexNameString));
        }
        indexNameObjectList.sort(new Comparator<IndexName>() {
            @Override
            public int compare(IndexName indexNameObj1, IndexName indexNameObj2) {
                return indexNameObj1.compareTo(indexNameObj2);
            }
        });
        return indexNameObjectList;
    }

    private boolean quiet;

    private Options parseCommandLineParams(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<Void> quietOption = parser.accepts("quiet", "be less chatty");
        Options opts = new Options();
        OptionSet options = opts.parseAndConfigure(parser, args);
        quiet = options.has(quietOption);
        boolean isReadWrite = opts.getCommonOpts().isReadWrite();
        boolean success = true;
        if (!isReadWrite) {
            log.debug("Repository connected in read-only mode. Use '--read-write' for write operations");
        }
//        try (NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts)) {
//            NodeStore nodeStore = fixture.getStore();
//            return nodeStore;
//        }
        return opts;
    }


}
