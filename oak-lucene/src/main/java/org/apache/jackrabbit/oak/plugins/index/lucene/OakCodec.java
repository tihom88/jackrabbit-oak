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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;


/**
 * Oak specific {@link org.apache.lucene.codecs.Codec}.
 *
 * Uses modern Lucene 10.x codec components.
 */
public class OakCodec extends Codec {

    public OakCodec() {
        super("oakCodec");
    }

    @Override
    public PostingsFormat postingsFormat() {
        return Codec.getDefault().postingsFormat();
    }

    @Override
    public DocValuesFormat docValuesFormat() {
        return Codec.getDefault().docValuesFormat();
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return Codec.getDefault().storedFieldsFormat();
    }

    @Override
    public TermVectorsFormat termVectorsFormat() {
        return Codec.getDefault().termVectorsFormat();
    }

    @Override
    public FieldInfosFormat fieldInfosFormat() {
        return Codec.getDefault().fieldInfosFormat();
    }

    @Override
    public SegmentInfoFormat segmentInfoFormat() {
        return Codec.getDefault().segmentInfoFormat();
    }

    @Override
    public NormsFormat normsFormat() {
        return Codec.getDefault().normsFormat();
    }

    @Override
    public LiveDocsFormat liveDocsFormat() {
        return Codec.getDefault().liveDocsFormat();
    }

    @Override
    public KnnVectorsFormat knnVectorsFormat() {
        return Codec.getDefault().knnVectorsFormat();
    }

    @Override
    public PointsFormat pointsFormat() {
        return Codec.getDefault().pointsFormat();
    }

    @Override
    public CompoundFormat compoundFormat() {
        return Codec.getDefault().compoundFormat();
    }
}
