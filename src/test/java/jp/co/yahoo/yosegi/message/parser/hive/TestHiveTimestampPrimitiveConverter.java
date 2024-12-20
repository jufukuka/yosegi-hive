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
package jp.co.yahoo.yosegi.message.parser.hive;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import jp.co.yahoo.yosegi.message.objects.NullObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;

import java.io.IOException;
import java.sql.Timestamp;

public class TestHiveTimestampPrimitiveConverter {

  @Test
  public void T_1() throws IOException{
    HiveTimestampPrimitiveConverter converter = new HiveTimestampPrimitiveConverter(
        PrimitiveObjectInspectorFactory.writableTimestampObjectInspector );

    long t = 1551756114L;
    TimestampWritable timestamp = new TimestampWritable( new Timestamp( t ) );
    PrimitiveObject tObj = converter.get( timestamp );
    assertEquals( t , tObj.getLong() );
  }

  @Test
  public void T_2() throws IOException{
    HiveTimestampPrimitiveConverter converter = new HiveTimestampPrimitiveConverter(
        PrimitiveObjectInspectorFactory.writableTimestampObjectInspector );

    PrimitiveObject tObj = converter.get( null );
    assertTrue( ( tObj instanceof NullObj ) );
  }

}
