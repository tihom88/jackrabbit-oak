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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryWriter;
import org.apache.jackrabbit.oak.index.indexer.document.incrementalstore.IncrementalFlatFileStoreStrategy;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.util.function.Predicate;

import static com.google.common.base.StandardSystemProperty.LINE_SEPARATOR;

public class DeltaFFSEditor implements Editor {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final BufferedWriter w;
    private final NodeStateEntryWriter entryWriter;
    private final Predicate<String> predicate;
    private final IncrementalFlatFileStoreStrategy incrementalFlatFileStoreStrategy;
    private static final int LINE_SEP_LENGTH = LINE_SEPARATOR.value().length();

    public DeltaFFSEditor(BufferedWriter w, NodeStateEntryWriter entryWriter, Predicate<String> predicate, IncrementalFlatFileStoreStrategy incrementalFlatFileStoreStrategy) {
        this.w = w;
        this.entryWriter = entryWriter;
        this.predicate = predicate;
        this.incrementalFlatFileStoreStrategy = incrementalFlatFileStoreStrategy;
    }

    @Override
    public void enter(NodeState before, NodeState after) throws CommitFailedException {
    }

    @Override
    public void leave(NodeState before, NodeState after) throws CommitFailedException {
    }

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
    }

    @Override
    public @Nullable Editor childNodeAdded(String name, NodeState after) throws CommitFailedException {
        writeToFile(after, IncrementalOperand.ADD);
        return this;
    }

    @Override
    public @Nullable Editor childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        writeToFile(after, IncrementalOperand.MODIFY);
        return this;
    }

    @Override
    public @Nullable Editor childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        writeToFile(before, IncrementalOperand.DELETE);
        return this;
    }

    private String getPath(NodeState e) {
        String path;
        if (e instanceof DocumentNodeState) {
            path = ((DocumentNodeState)e).getPath().toString();
        } else {
            log.error("DeltaFFSEditor implementation is only for DocumentNodeState");
            throw new RuntimeException("DeltaFFSEditor implementation is only for DocumentNodeState");
        }
        return path;
    }

    private void writeToFile(NodeState e, IncrementalOperand action) {
        try {
            String path = getPath(e);
            if (!NodeStateUtils.isHiddenPath(path) && predicate.test(path)) {
                String line =  path + "|" + entryWriter.asJson(e) + "|" + action;
                w.append(line);
                w.newLine();
                incrementalFlatFileStoreStrategy.incrementEntryCount();
                incrementalFlatFileStoreStrategy.setTextSize(incrementalFlatFileStoreStrategy.getTextSize() + line.length() + LINE_SEP_LENGTH);
            }
        } catch (Exception exp) {
            log.error("Error:", exp);
        }
    }
}
