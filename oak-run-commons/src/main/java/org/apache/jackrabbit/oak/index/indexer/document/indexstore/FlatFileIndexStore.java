package org.apache.jackrabbit.oak.index.indexer.document.indexstore;

import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStore;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryReader;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

import java.io.File;
import java.util.Set;

public class FlatFileIndexStore extends FlatFileStore implements IndexStore {
    public FlatFileIndexStore(BlobStore blobStore, File storeFile, Set<String> preferredPathElements, Compression algorithm) {
        super(blobStore, storeFile, preferredPathElements, algorithm);
    }

}
