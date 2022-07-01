package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DeltaFFSEditor implements Editor {

    private final Logger log = LoggerFactory.getLogger(getClass());

    Map<NodeState, String> pathMap;
    public DeltaFFSEditor(Map<NodeState, String> pathMap) {
        this.pathMap = pathMap;
    }

    @Override
    public void enter(NodeState before, NodeState after) throws CommitFailedException {
        log.info("inside enter");
    }

    @Override
    public void leave(NodeState before, NodeState after) throws CommitFailedException {
        log.info("inside leave");
    }

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        log.info("inside property added {}", after.getName() );
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        log.info("inside property changed {}", after.getName() );
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        log.info("inside property deleted {}", before.getName() );
    }

    @Override
    public @Nullable Editor childNodeAdded(String name, NodeState after) throws CommitFailedException {
        log.info("inside child node  added {}", name);
        pathMap.put(after, "ADDED");
        return this;
    }

    @Override
    public @Nullable Editor childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        log.info("inside child node  changed {}", name);
        pathMap.put(after, "MODIFIED");
        return this;
    }

    @Override
    public @Nullable Editor childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        log.info("inside child node deleted {}", name);
        pathMap.put(before, "DELETED");
        return this;
    }
}
