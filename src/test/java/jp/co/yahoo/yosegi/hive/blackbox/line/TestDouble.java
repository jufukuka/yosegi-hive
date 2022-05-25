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
package jp.co.yahoo.yosegi.hive.blackbox.line;

import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.hive.blackbox.ColumnBinaryTestCase;
import jp.co.yahoo.yosegi.hive.blackbox.YosegiFileTestCase;
import jp.co.yahoo.yosegi.hive.YosegiObjectInspectorFactory;
import jp.co.yahoo.yosegi.hive.io.ColumnAndIndex;
import jp.co.yahoo.yosegi.hive.io.YosegiHiveLineReader;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public final class TestDouble {

  public static void createTestCase(List<Arguments> args, double[] data, boolean[] isNullArray) throws IOException {
    String[] testClassArray = ColumnBinaryTestCase.doubleClassNames();
    for (int i = 0; i < testClassArray.length; i++) {
      args.add(arguments(testClassArray[i], data, isNullArray, isNullArray.length));
    }
  }

  public static Stream<Arguments> data() throws IOException {
    // 通常のデータ
    List<Arguments> args = new ArrayList<Arguments>();
    createTestCase(args, new double[]{(double) 10, (double) 20, (double) 30, (double) 40, (double) 50, (double) 10, (double) 10, (double) 20, (double) 20, (double) 30}, new boolean[]{false, false, false, false, false, false, false, false, false, false});
    createTestCase(args, new double[]{(double) 0, (double) 20, (double) 0, (double) 40, (double) 0, (double) 10, (double) 0, (double) 20, (double) 0, (double) 30}, new boolean[]{true, false, true, false, true, false, true, false, true, false});
    createTestCase(args, new double[]{(double) 0, (double) 0, (double) 0, (double) 0, (double) 0, (double) 10, (double) 10, (double) 20, (double) 20, (double) 30}, new boolean[]{true, true, true, true, true, false, false, false, false, false});
    createTestCase(args, new double[]{(double) 10, (double) 10, (double) 20, (double) 20, (double) 30, (double) 0, (double) 0, (double) 0, (double) 0, (double) 0}, new boolean[]{false, false, false, false, false, true, true, true, true, true});
    createTestCase(args, new double[]{(double) 10, (double) 0, (double) 0, (double) 0, (double) 0, (double) 0, (double) 0, (double) 0, (double) 0, (double) 10}, new boolean[]{false, true, true, true, true, true, true, true, true, false});

    // 固定の値のデータ
    createTestCase(args, new double[]{(double) 10, (double) 10, (double) 10, (double) 10, (double) 10, (double) 10, (double) 10, (double) 10, (double) 10, (double) 10}, new boolean[]{false, false, false, false, false, false, false, false, false, false});
    createTestCase(args, new double[]{(double) 0, (double) 10, (double) 0, (double) 10, (double) 0, (double) 10, (double) 0, (double) 10, (double) 0, (double) 10}, new boolean[]{true, false, true, false, true, false, true, false, true, false});
    createTestCase(args, new double[]{(double) 0, (double) 0, (double) 0, (double) 0, (double) 0, (double) 10, (double) 10, (double) 10, (double) 10, (double) 10}, new boolean[]{true, true, true, true, true, false, false, false, false, false});
    createTestCase(args, new double[]{(double) 10, (double) 10, (double) 10, (double) 10, (double) 10, (double) 0, (double) 0, (double) 0, (double) 0, (double) 0}, new boolean[]{false, false, false, false, false, true, true, true, true, true});
    createTestCase(args, new double[]{(double) 10, (double) 0, (double) 0, (double) 0, (double) 0, (double) 0, (double) 0, (double) 0, (double) 0, (double) 10}, new boolean[]{false, true, true, true, true, true, true, true, true, false});
    createTestCase(args, new double[]{(double) 0, (double) 0, (double) 0, (double) 0, (double) 0, (double) 0, (double) 0, (double) 0, (double) 0, (double) 10}, new boolean[]{true, true, true, true, true, true, true, true, true, false});
    createTestCase(args, new double[]{(double) 10, (double) 0, (double) 0, (double) 0, (double) 0, (double) 0, (double) 0, (double) 0, (double) 0, (double) 0}, new boolean[]{false, true, true, true, true, true, true, true, true, true});
    return args.stream();
  }

  public YosegiHiveLineReader createReader( final ColumnBinary columnBinary , final int spreadSize ) throws IOException {
    return YosegiFileTestCase.createYosegiFileBinaryAndCreateLineReader(
        Arrays.asList( Arrays.asList( columnBinary ) ) ,
        Arrays.asList( spreadSize ) ,
        new Configuration() ,
        null );
  }

  public StructObjectInspector createObjectInspector(){
    StructTypeInfo info = new StructTypeInfo();
    ArrayList<String> nameList = new ArrayList<String>();
    nameList.add( "col" ); 

    ArrayList<TypeInfo> typeInfoList = new ArrayList<TypeInfo>( Arrays.asList( TypeInfoFactory.doubleTypeInfo ) );

    info.setAllStructFieldNames( nameList );
    info.setAllStructFieldTypeInfos( typeInfoList );
    return (StructObjectInspector)( YosegiObjectInspectorFactory.craeteObjectInspectorFromTypeInfo( info ) );
  }

  @ParameterizedTest
  @MethodSource("data")
  public void T_load_equalsSetValue( String targetClassName , double[] data , boolean[] isNullArray , int spreadSize ) throws IOException {
    ColumnBinary columnBinary = ColumnBinaryTestCase.createDoubleColumnBinaryFromDouble(targetClassName, data, isNullArray, "col");
    YosegiHiveLineReader reader = createReader( columnBinary , spreadSize );
    StructObjectInspector ins = createObjectInspector();
    NullWritable key = reader.createKey();
    ColumnAndIndex value = reader.createValue();
    int i = 0;
    while ( reader.next( key , value ) ) {
      Object f = ins.getStructFieldData( value , ins.getStructFieldRef( "col" ) );
      if ( isNullArray[i] ) {
        assertNull( f );
      } else {
        assertTrue( f instanceof DoubleWritable );
        assertEquals( data[i] , ( (DoubleWritable) f ).get() );
      }
      i++;
    }
    reader.close();
  }
}
