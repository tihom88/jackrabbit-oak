package org.apache.jackrabbit.oak.indexversion;

import com.google.common.io.Closer;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexPathService;
import org.apache.jackrabbit.oak.plugins.index.IndexPathServiceImpl;
import org.apache.jackrabbit.oak.plugins.index.lucene.property.RecursiveDelete;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.IndexName;
import org.apache.jackrabbit.oak.run.Utils;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.util.ISO8601;
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

    private String trimSlash(String str) {
        int startIndex = 0;
        int endIndex = str.length() - 1;
        if (str.charAt(startIndex) == '/') {
            startIndex++;
        }
        if (str.charAt(endIndex) == '/') {
            endIndex--;
        }
        return str.substring(startIndex, endIndex + 1);
    }

    private NodeState getIndexDefinitionNodeParentFromPath(NodeStore store, String trimmedIndexPath) {
        String[] splitPathArray = getsplitPathArray(trimmedIndexPath);
        NodeState root = store.getRoot();
        int i = 0;
        NodeState node = root;
        while (i < splitPathArray.length - 1) {
            node = node.getChildNode(splitPathArray[i]);
            i++;
        }
        return node;
    }

    private String[] getsplitPathArray(String trimmedIndexPath) {
        return trimmedIndexPath.split("/");
    }

    public void execute(String... args) throws Exception {
        Options opts = parseCommandLineParams(args);
        try (NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts)) {
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
            indexPaths.add(trimSlash(path));
            segregatedIndexes.put(baseIndexPath, indexPaths);
        }

        return segregatedIndexes;
    }


/*
    private static JsonObject getIndexDefinitions(NodeStore nodeStore) throws IOException {
        IndexPathService imageIndexPathService = new IndexPathServiceImpl(nodeStore);
        IndexDefinitionPrinter indexDefinitionPrinter = new IndexDefinitionPrinter(nodeStore, imageIndexPathService);
        StringWriter writer = new StringWriter();
        PrintWriter printWriter = new PrintWriter(writer);
        indexDefinitionPrinter.print(printWriter, Format.JSON, false);
        printWriter.flush();
        writer.flush();
        StringReader reader = new StringReader(writer.toString());
        return new Gson().fromJson(reader, JsonObject.class);
    }*/

    private static IndexPathService getIndexDefinitions(NodeStore nodeStore) throws IOException {
        IndexPathService imageIndexPathService = new IndexPathServiceImpl(nodeStore);
        //IndexDefinitionPrinter indexDefinitionPrinter = new IndexDefinitionPrinter(nodeStore, imageIndexPathService);
        return imageIndexPathService;
        //StringWriter writer = new StringWriter();
        //PrintWriter printWriter = new PrintWriter(writer);
        //indexDefinitionPrinter.print(printWriter, Format.JSON, false);
        //printWriter.flush();
        //writer.flush();
        //    StringReader reader = new StringReader(writer.toString());
        //    return new Gson().fromJson(reader, JsonObject.class);
    }


    public List<String> getIndexPathList(NodeStore store) throws CommitFailedException, IOException {
        IndexPathService indexPathService = getIndexDefinitions(store);
        Iterable<String> indexPaths = indexPathService.getIndexPaths();
        List<String> indexPathsList = new LinkedList<>();
        for (String indexPath : indexPaths) {
            indexPathsList.add(indexPath);
        }

        return indexPathsList;
    }

    StringBuilder stringJoiner(String[] stringArray, int arrayIndexToJoin, String delimiter) {
        StringBuilder stringBuilder = new StringBuilder();
        int index = 0;
        for (index = 0; index <= arrayIndexToJoin; index++) {
            stringBuilder.append(stringArray[index]);
            stringBuilder.append(delimiter);
        }
        //stringBuilder.append(stringArray[index]);
        return stringBuilder;
    }

    private String getBaseIndexPath(String path) {
        String trimmedPath = trimSlash(path);
        String[] fragmentedPath = trimmedPath.split("/");
        String indexName = fragmentedPath[fragmentedPath.length - 1];
        String baseIndexName = getBaseIndexName(indexName);

        return stringJoiner(fragmentedPath,
                fragmentedPath.length - 2, "/")
                .append(baseIndexName).toString();
    }

    private void getBaseIndexPathList(List<String> pathList) {
        Set<String> baseIndexPathSet = new HashSet<>();
        for (String path : pathList) {
            String[] fragmentedPath = trimSlash(path).split("/");
            String indexName = fragmentedPath[fragmentedPath.length - 1];
            String baseIndexName = getBaseIndexName(indexName);
            baseIndexPathSet.add(getBaseIndexPath(path));
        }
    }

    private void execute2(NodeStore store, Map<String, List<String>> segregateIndexes) {
        for (Map.Entry<String, List<String>> entry : segregateIndexes.entrySet()) {
            execute3(store, entry.getValue());
        }
    }

    private List<String> getIndexNameVersionList(List<String> versionedIndexPathList) {
        List<String> indexNameVersionList = new LinkedList<>();
        for (String indexPath : versionedIndexPathList) {
            String trimmedIndexPath = trimSlash(indexPath);
            String[] splitPathArray = getsplitPathArray(trimmedIndexPath);
            String indexName = splitPathArray[splitPathArray.length - 1];
            indexNameVersionList.add(indexName);
        }
        return indexNameVersionList;
    }


    private void execute3(NodeStore store, List<String> versionedIndexPathList) {
        if (versionedIndexPathList.size() != 0) {
            String trimmedIndexPath = trimSlash(versionedIndexPathList.get(0));
            String[] splitPathArray = getsplitPathArray(trimmedIndexPath);
            NodeState indexDefParentNode = getIndexDefinitionNodeParentFromPath(store, trimmedIndexPath);
            String indexName = splitPathArray[splitPathArray.length - 1];
            String baseIndexName = getBaseIndexName(indexName);
//            Iterable<String> allIndexes = getAllIndexes(indexDefParentNode);
            //List<String> allIndexNameVersions = getAllIndexVersions(baseIndexName, allIndexes);
            List<String> allIndexNameVersions = getIndexNameVersionList(versionedIndexPathList);
            List<IndexName> indexNameObjectList = getVersionedIndexNameObjects(allIndexNameVersions);
            indexNameObjectList.sort(new Comparator<IndexName>() {
                @Override
                public int compare(IndexName indexNameObj1, IndexName indexNameObj2) {
                    return indexNameObj1.compareTo(indexNameObj2);
                }
            });
            List<IndexName> toDeleteIndexNameObjectList =
                    getToDeleteIndexNameObjectList(indexDefParentNode, indexNameObjectList, 100000);
            //purgeOldIndexVersion(store, toDeleteIndexNameObjectList);
        }

    }

    private long getMillisFromString(String strDate) {
        long millis = ISO8601.parse(strDate).getTimeInMillis();
        return millis;
    }

    private List<IndexName> getToDeleteIndexNameObjectList(NodeState indexDefParentNode,
                                                           List<IndexName> indexNameObjectList, int threshold) {
        List<IndexName> toDeleteIndexNameObjectList = new LinkedList<>();
        for (int i = 0; i < indexNameObjectList.size(); i++) {
            NodeState indexNode = indexDefParentNode
                    .getChildNode(indexNameObjectList.get(i).getNodeName());
            if (indexNode.hasChildNode(INDEX_DEFINITION_NODE)) {
                String reindexCompletionTime = indexDefParentNode
                        .getChildNode(indexNameObjectList.get(i).getNodeName())
                        .getChildNode(INDEX_DEFINITION_NODE)
                        .getProperty(REINDEX_COMPLETION_TIMESTAMP).getValue(Type.DATE);
                long reindexCompletinTimeInMillis = getMillisFromString(reindexCompletionTime);
                long currentTimeInMillis = System.currentTimeMillis();
                if (currentTimeInMillis - reindexCompletinTimeInMillis > threshold) {
                    if (i > 0) {
                        toDeleteIndexNameObjectList.add(indexNameObjectList.get(i - 1));
                    }
                }
            }


        }
        return toDeleteIndexNameObjectList;
    }

    private void purgeOldIndexVersion(NodeStore store,
                                      List<IndexName> toDeleteIndexNameObjectList) throws CommitFailedException {
        String trimmedIndexPath = trimSlash(getIndexPath());
        String indexPathParent = "/" + trimmedIndexPath.substring(0, trimmedIndexPath.lastIndexOf('/'));

        for (IndexName toDeleteIndexNameObject : toDeleteIndexNameObjectList) {
            RecursiveDelete recursiveDelete = new RecursiveDelete(store, EmptyHook.INSTANCE, () -> CommitInfo.EMPTY);
            recursiveDelete.run(indexPathParent + "/" + toDeleteIndexNameObject.getNodeName());
        }

    }

    private List<IndexName> getVersionedIndexNameObjects(List<String> allIndexes) {
        List<IndexName> indexNameObjectList = new ArrayList<>();
        for (String indexNameString : allIndexes) {
            indexNameObjectList.add(IndexName.parse(indexNameString));
        }
        return indexNameObjectList;
    }

    List<String> getAllIndexVersions(String baseIndexName, Iterable<String> allIndexes) {
        List<String> allIndexVersions = new LinkedList<>();
        for (String index : allIndexes) {
            if (baseIndexName.equals(getBaseIndexName(index))) {
                allIndexVersions.add(index);
            }
        }
        return allIndexVersions;
    }

    private String getIndexPath() {
        return "/oak:index/lucene";
    }

    private Iterable<String> getAllIndexes(NodeState indexDefNode) {
        /*List<String> indexesDummy = new LinkedList<>();
        indexesDummy.add("lucene");
        indexesDummy.add("lucene-2");
        indexesDummy.add("lucene-2-custom-1");
        indexesDummy.add("lucene-2-custom-2");
        indexesDummy.add("lucene-3-custom-1");
        indexesDummy.add("lucene-3-custom-2");
        indexesDummy.add("lucene-4-custom-1");
        indexesDummy.add("lucene-4-custom-2");
        indexesDummy.add("damAsset");
        indexesDummy.add("damAsset-1");
        indexesDummy.add("damAsset-2");
        indexesDummy.add("damAsset-2-custom-1");
        indexesDummy.add("damAsset-2-custom-2");
        indexesDummy.add("damAsset-3-custom-1");
        return indexesDummy;
*/
        return indexDefNode.getChildNodeNames();
    }


    public String getBaseIndexName(String versionedIndexName) {
        String indexBaseName = versionedIndexName.split("-")[0];
        return indexBaseName;
    }

    //    private void storeIndex(NodeStore ns, String newIndexName, JsonObject indexDef) {
//        NodeBuilder rootBuilder = ns.getRoot().builder();
//        NodeBuilder b = rootBuilder;
//        for (String p : PathUtils.elements(newIndexName)) {
//            b = b.child(p);
//        }
//        build("  ", b, indexDef);
//        EditorHook hook = new EditorHook(
//                new IndexUpdateProvider(new PropertyIndexEditorProvider()));
//        try {
//            ns.merge(rootBuilder, hook, CommitInfo.EMPTY);
//            log("Added index " + newIndexName);
//        } catch (CommitFailedException e) {
//            LOG.error("Failed to add index " + newIndexName, e);
//        }
//    }
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
