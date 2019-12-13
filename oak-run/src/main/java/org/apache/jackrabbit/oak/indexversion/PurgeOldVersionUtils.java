package org.apache.jackrabbit.oak.indexversion;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.util.ISO8601;
import org.jetbrains.annotations.NotNull;

import static com.google.common.base.Preconditions.checkNotNull;

public class PurgeOldVersionUtils {

public static String trimSlash(String str) {
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

public static long getMillisFromString(String strDate) {
        long millis = ISO8601.parse(strDate).getTimeInMillis();
        return millis;
    }

    public static String getBaseIndexName(String versionedIndexName) {
        String indexBaseName = versionedIndexName.split("-")[0];
        return indexBaseName;
    }

    public static NodeBuilder getNode(@NotNull NodeBuilder nodeBuilder, @NotNull String path) {
        for (String name : PathUtils.elements(checkNotNull(path))) {
            nodeBuilder = nodeBuilder.getChildNode(checkNotNull(name));
        }
        return nodeBuilder;
    }

}
