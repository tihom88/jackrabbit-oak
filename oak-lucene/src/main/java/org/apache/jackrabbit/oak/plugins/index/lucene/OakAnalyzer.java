/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

/**
 * The default Lucene Analyzer used in Oak.
 * Updated for Lucene 10.x APIs.
 */
public class OakAnalyzer extends Analyzer {

    private final boolean preserveOriginal;

    /**
     * Creates a new {@link OakAnalyzer}
     */
    public OakAnalyzer() {
        this(false);
    }

    /**
     * Create a new {@link OakAnalyzer} with configurable flag to preserve
     * original term being analyzed too.
     * @param indexOriginalTerm flag to setup analyzer such that
     *                          {@link WordDelimiterGraphFilter#PRESERVE_ORIGINAL}
     *                          is set to configure word delimiter
     */
    public OakAnalyzer(boolean indexOriginalTerm) {
        this.preserveOriginal = indexOriginalTerm;
    }

    @Override
    protected TokenStreamComponents createComponents(final String fieldName) {
        // In Lucene 10.x, createComponents no longer takes a Reader parameter
        Tokenizer src = new StandardTokenizer();
        TokenStream tok = new LowerCaseFilter(src);
        
        // Configure WordDelimiterGraphFilter flags
        int flags = WordDelimiterGraphFilter.GENERATE_WORD_PARTS
                | WordDelimiterGraphFilter.GENERATE_NUMBER_PARTS
                | WordDelimiterGraphFilter.STEM_ENGLISH_POSSESSIVE;
        
        if (preserveOriginal) {
            flags |= WordDelimiterGraphFilter.PRESERVE_ORIGINAL;
        }
        
        tok = new WordDelimiterGraphFilter(tok, flags, null);
        
        return new TokenStreamComponents(src, tok);
    }
}
