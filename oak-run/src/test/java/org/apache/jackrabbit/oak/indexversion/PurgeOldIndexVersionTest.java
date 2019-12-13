package org.apache.jackrabbit.oak.indexversion;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.index.AbstractIndexCommandTest;
import org.apache.jackrabbit.oak.index.RepositoryFixture;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.run.PurgeOldIndexVersionCommand;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import java.io.File;
import java.io.IOException;

import static org.apache.jackrabbit.commons.JcrUtils.getOrCreateByPath;

public class PurgeOldIndexVersionTest extends AbstractIndexCommandTest {

//@Rule
//    public final TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));


    private void createCustomFooIndex(int ootbVersion, int customVersion, boolean asyncIndex) throws IOException, RepositoryException {
        IndexDefinitionBuilder idxBuilder = new IndexDefinitionBuilder();
        if (!asyncIndex) {
            idxBuilder.noAsync();
        }
        idxBuilder.indexRule("nt:base").property("foo").propertyIndex();

        Session session = fixture.getAdminSession();
        String indexName = customVersion != 0
                ? TEST_INDEX_PATH + "-" + ootbVersion + "-custom-" + customVersion
                : TEST_INDEX_PATH + "-" + ootbVersion;
        Node fooIndex = getOrCreateByPath(indexName,
                "oak:QueryIndexDefinition", session);

        idxBuilder.build(fooIndex);
        session.save();
        session.logout();
    }

    @Test
    public void deleteOldIndexCompletely() throws Exception {
        createTestData(false);
        createCustomFooIndex(2, 1, false);
        createCustomFooIndex(3, 0, false);
        createCustomFooIndex(3, 1, false);
        createCustomFooIndex(3, 2, false);
        createCustomFooIndex(4, 0, false);
        createCustomFooIndex(4, 1, false);
        createCustomFooIndex(4, 2, false);
        fixture.getAsyncIndexUpdate("async").run();
        fixture.close();
        PurgeOldIndexVersionCommand command = new PurgeOldIndexVersionCommand();

        // File outDir = temporaryFolder.newFolder();
        File storeDir = fixture.getDir();

        // purge-index-versions /home/mokatari/adobe/aem/aemlatest-1.21-snap/author/crx-quickstart/repository/segmentstore --quiet --read-write
        String[] args = {
                storeDir.getAbsolutePath()
        };

        command.execute(args);
        /*
        new fixture defined to
         */
        fixture = new RepositoryFixture(storeDir);
        fixture.close();

        Assert.assertEquals(fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-4").getProperty("type").getValue(Type.STRING),"disabled");

        //Assert.assertEquals(fixture.getNodeStore().getRoot().getChildNode("oak:index").getChildNode("fooIndex-4-custom-2").getProperty("type"),"disabled");
    }
}
