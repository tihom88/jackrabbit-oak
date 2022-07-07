package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class MergeIncrementalFFSTest {

    private static final String BUILD_TARGET_FOLDER = "target";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File(BUILD_TARGET_FOLDER));

    @Ignore
    @Test
    public void test2() throws IOException {
        File base = new File("/Users/nitigup/garageweek_2022/baseFFS2.json");
        File incremental = new File("/Users/nitigup/garageweek_2022/incrementalFFS2.json");
        File merged = new File("/Users/nitigup/garageweek_2022/merged3.json");

        MergeIncrementalFFS merge = new MergeIncrementalFFS(Collections.emptySet(), base, incremental, merged);

        merge.doMerge();

    }


    @Test
    public void test1() throws IOException {

        File base = folder.newFile("base.txt");
        File inc = folder.newFile("inc.txt");
        File merged = folder.newFile("merged.txt");

        try(BufferedWriter baseBW = new BufferedWriter(new FileWriter(base))) {
            baseBW.write("/tmp|{prop1=\"foo\"}");
            baseBW.newLine();
            baseBW.write("/tmp/a|{prop2=\"foo\"}");
            baseBW.newLine();
            baseBW.write("/tmp/a/b|{prop3=\"foo\"}");
            baseBW.newLine();
            baseBW.write("/tmp/b|{prop1=\"foo\"}");
            baseBW.newLine();
            baseBW.write("/tmp/b/c|{prop2=\"foo\"}");
            baseBW.newLine();
            baseBW.write("/tmp/c|{prop3=\"foo\"}");
        }

        try(BufferedWriter baseInc = new BufferedWriter(new FileWriter(inc))) {
            baseInc.write("/tmp/a|{prop2=\"fooModified\"}|M");
            baseInc.newLine();
            baseInc.write("/tmp/b|{prop1=\"foo\"}|D");
            baseInc.newLine();
            baseInc.write("/tmp/b/c/d|{prop2=\"fooNew\"}|A");
            baseInc.newLine();
            baseInc.write("/tmp/c|{prop3=\"fooModified\"}|M");
            baseInc.newLine();
            baseInc.write("/tmp/d|{prop3=\"bar\"}|A");
            baseInc.newLine();
            baseInc.write("/tmp/e|{prop3=\"bar\"}|A");
        }

        List<String> expectedList = new LinkedList<>();

        expectedList.add("/tmp|{prop1=\"foo\"}");
        expectedList.add("/tmp/a|{prop2=\"fooModified\"}");
        expectedList.add("/tmp/a/b|{prop3=\"foo\"}");
        expectedList.add("/tmp/b/c|{prop2=\"foo\"}");
        expectedList.add("/tmp/b/c/d|{prop2=\"fooNew\"}");
        expectedList.add("/tmp/c|{prop3=\"fooModified\"}");
        expectedList.add("/tmp/d|{prop3=\"bar\"}");
        expectedList.add("/tmp/e|{prop3=\"bar\"}");

        MergeIncrementalFFS merge = new MergeIncrementalFFS(Collections.emptySet(), base, inc, merged);

        merge.doMerge();

        try(BufferedReader br = new BufferedReader(new FileReader(merged))) {
            for (String line : expectedList) {
                String actual = br.readLine();
                System.out.println(actual);
                Assert.assertEquals(line, actual);

            }

            Assert.assertEquals(null, br.readLine());
        }
    }


}
