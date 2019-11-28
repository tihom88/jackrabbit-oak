package org.apache.jackrabbit.oak.indexversion;

import com.google.common.io.Closer;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.property.RecursiveDelete;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.IndexName;
import org.apache.jackrabbit.oak.run.Utils;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.util.ISO8601;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

public class PurgeOldIndexVersion {
    private static String INDEX_DEFINITION_NODE = ":index-definition";
    private static String REINDEX_COMPLETION_TIMESTAMP = "reindexCompletionTimestamp";

    public static void main(String[] args) throws IOException, InvalidFileStoreVersionException, CommitFailedException {
        String source = "/home/mokatari/adobe/aem/aemlatest-1.21-snap/author/crx-quickstart/repository/segmentstore";
        Closer closer = Utils.createCloserWithShutdownHook();
        FileStore fileStore = fileStoreBuilder(new File(source)).withStrictVersionCheck(true).build();
        //marker = new SegmentBlobReferenceRetriever(fileStore);
        closer.register(fileStore);
        NodeStore nodeStore =
                SegmentNodeStoreBuilders.builder(fileStore).build();
        PurgeOldIndexVersion p = new PurgeOldIndexVersion();
        p.execute(nodeStore);
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

    void execute(NodeStore store) throws CommitFailedException {
        String trimmedIndexPath = trimSlash(getIndexPath());
        String[] splitPathArray = getsplitPathArray(trimmedIndexPath);
        NodeState indexDefParentNode = getIndexDefinitionNodeParentFromPath(store, trimmedIndexPath);
        String indexName = splitPathArray[splitPathArray.length - 1];
        String baseIndexName = getBaseIndexName(indexName);
        Iterable<String> allIndexes = getAllIndexes(indexDefParentNode);
        List<String> allIndexVersions = getAllIndexVersions(baseIndexName, allIndexes);
        List<IndexName> indexNameObjectList = getVersionedIndexNameObjects(indexName, allIndexVersions);
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

    private long getMillisFromString(String strDate) {
        long millis = ISO8601.parse(strDate).getTimeInMillis();
        return millis;
    }

    private List<IndexName> getToDeleteIndexNameObjectList(NodeState indexDefParentNode,
                                                           List<IndexName> indexNameObjectList, int threshold) {
        List<IndexName> toDeleteIndexNameObjectList = new LinkedList<>();
        for (int i = 0; i < indexNameObjectList.size(); i++) {
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

    private List<IndexName> getVersionedIndexNameObjects(String indexName, List<String> allIndexes) {
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

//    private parseCommandLineParams(String... args) {
//        OptionParser parser = new OptionParser();
//        OptionSpec<Void> quietOption = parser.accepts("quiet", "be less chatty");
//        Options opts = new Options();
//        OptionSet options = opts.parseAndConfigure(parser, args);
//        quiet = options.has(quietOption);
//        boolean isReadWrite = opts.getCommonOpts().isReadWrite();
//        boolean success = true;
//        if (!isReadWrite) {
//            log("Repository connected in read-only mode. Use '--read-write' for write operations");
//        }
//        try (NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts)) {
//        }
//    }



}
