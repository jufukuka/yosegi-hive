/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jp.co.yahoo.yosegi.hive.io;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedSupport;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class YosegiHiveLineInputFormat extends FileInputFormat<NullWritable,ColumnAndIndex>
    implements VectorizedInputFormatInterface {

  private static final long MIN_SPLIT_BLOCK_SIZE = 1024 * 1024 * 512;
  private static final Logger LOG = LoggerFactory.getLogger( YosegiHiveLineInputFormat.class );
  private final SpreadCounter spreadCounter = new SpreadCounter();

  @Override
  public InputSplit[] getSplits( final JobConf job , final int numSplits ) throws IOException {
    super.setMinSplitSize( MIN_SPLIT_BLOCK_SIZE );
    return super.getSplits( job , numSplits );
  }

  @Override
  public RecordReader<NullWritable,ColumnAndIndex> getRecordReader(
        final InputSplit split, final JobConf job, final Reporter reporter ) throws IOException {
    FileSplit fileSplit = (FileSplit)split;
    Path path = fileSplit.getPath();
    FileSystem fs = path.getFileSystem( job );
    long fileLength = fs.getLength( path );
    long start = fileSplit.getStart();
    long length = fileSplit.getLength();
    InputStream in = fs.open( path );
    IJobReporter jobReporter = new HadoopJobReporter( reporter );
    jobReporter.setStatus( String.format( "Read file : %s" , path.toString() ) );
    HiveReaderSetting hiveConfig = new HiveReaderSetting( fileSplit , job );
    if ( hiveConfig.isVectorMode() ) {
      IVectorizedReaderSetting vectorizedSetting =
          new HiveVectorizedReaderSetting( fileSplit , job , hiveConfig );
      return (RecordReader)new YosegiHiveDirectVectorizedReader(
          in , fileLength , start , length , vectorizedSetting , jobReporter );
    } else {
      return new YosegiHiveLineReader(
          in , fileLength , start , length , hiveConfig , jobReporter , spreadCounter );
    }
  }

  @Override
  public VectorizedSupport.Support[] getSupportedFeatures() {
    return null;
  }

}
