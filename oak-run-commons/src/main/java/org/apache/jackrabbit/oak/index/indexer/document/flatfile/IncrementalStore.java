package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import com.google.common.base.Stopwatch;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.VisibleEditor;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import static com.google.common.base.StandardSystemProperty.LINE_SEPARATOR;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;

public class IncrementalStore implements SortStrategy {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final NodeState before;
    private final NodeState after;
    private final PathElementComparator comparator;
    private final NodeStateEntryWriter entryWriter;
    private final File storeDir;
    private final boolean compressionEnabled;
    private Predicate<String> pathPredicate;
    private long entryCount;
    private long textSize;
    private static final int LINE_SEP_LENGTH = LINE_SEPARATOR.value().length();

    public IncrementalStore(@NotNull NodeState before, @NotNull NodeState after, File storeDir,
                            PathElementComparator comparator, boolean compressionEnabled, Predicate<String> pathPredicate, NodeStateEntryWriter entryWriter) {
        this.before = before;
        this.after = after;
        this.comparator = comparator;
        this.storeDir = storeDir;
        this.compressionEnabled = compressionEnabled;
        this.pathPredicate = pathPredicate;
        this.entryWriter = entryWriter;
    }

    @Override
    public File createSortedStoreFile() throws IOException {

        Map<NodeState, String> pathMap = new HashMap();

        EditorDiff.process(VisibleEditor.wrap(new DeltaFFSEditor(pathMap)), before, after);

        return writeToStore(pathMap, storeDir, getStoreFileName());
    }

    @Override
    public long getEntryCount() {
        return 0;
    }


    private File writeToStore(Map<NodeState, String> deltaContent, File dir, String fileName) throws IOException {
        entryCount = 0;
        File file = new File(dir, fileName);
        Stopwatch sw = Stopwatch.createStarted();
        try (BufferedWriter w = FlatFileStoreUtils.createWriter(file, compressionEnabled)) {
            for (NodeState e : deltaContent.keySet()) {
                String path = e.toString().split(",")[0].split("'")[1].replace("'","");
                if (!NodeStateUtils.isHiddenPath(path) && pathPredicate.test(path)) {
                    String line = deltaContent.get(e) + " | " + path + " | " + entryWriter.asJson(e);
                    w.append(line);
                    w.newLine();
                    textSize += line.length() + LINE_SEP_LENGTH;
                    entryCount++;
                }
            }
        }
        String sizeStr = compressionEnabled ? String.format("compressed/%s actual size", humanReadableByteCount(textSize)) : "";
        log.info("Dumped {} nodestates in json format in {} ({} {})",entryCount, sw, humanReadableByteCount(file.length()), sizeStr);
        return file;
    }

    private String getStoreFileName() {
        return compressionEnabled ? "store.json.gz" : "store.json";
    }
}
