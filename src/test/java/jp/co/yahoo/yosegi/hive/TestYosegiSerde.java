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
package jp.co.yahoo.yosegi.hive;

import java.util.List;
import java.util.Properties;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.*;
import org.apache.hadoop.hive.serde2.objectinspector.*;

import jp.co.yahoo.yosegi.hive.io.ParserWritable;

public class TestYosegiSerde{

  @Test
  public void T_newInstance_1(){
    YosegiSerde serde = new YosegiSerde();
  }

  @Test
  public void T_initialize_1() throws SerDeException{
    YosegiSerde serde = new YosegiSerde();
    Configuration conf = new Configuration();
    conf.set( ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR , "" );
    Properties table = new Properties();
    table.setProperty( serdeConstants.LIST_COLUMNS , "str,num,arry,nest" );
    table.setProperty( serdeConstants.LIST_COLUMN_TYPES , "string,int,array<string>,struct<a:string,b:int>" );
    serde.initialize( conf , table );
  }

  @Test
  public void T_initialize_2() throws SerDeException{
    YosegiSerde serde = new YosegiSerde();
    Configuration conf = new Configuration();
    conf.set( ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR , "" );

    Properties table = new Properties();
    Properties part = new Properties();
    table.setProperty( serdeConstants.LIST_COLUMNS , "str,num,arry,nest" );
    table.setProperty( serdeConstants.LIST_COLUMN_TYPES , "string,int,array<string>,struct<a:string,b:int>" );

    serde.initialize( conf , table , part );
    StructObjectInspector inspector = (StructObjectInspector)( serde.getObjectInspector() );
    List<? extends StructField> fieldList = inspector.getAllStructFieldRefs();
    assertEquals( fieldList.get(0).getFieldName() , "str" );
    assertEquals( fieldList.get(1).getFieldName() , "num" );
    assertEquals( fieldList.get(2).getFieldName() , "arry" );
    assertEquals( fieldList.get(3).getFieldName() , "nest" );

    assertEquals( ( fieldList.get(0).getFieldObjectInspector() instanceof PrimitiveObjectInspector ) , true );
    assertEquals( ( fieldList.get(1).getFieldObjectInspector() instanceof PrimitiveObjectInspector ) , true );
    assertEquals( ( fieldList.get(2).getFieldObjectInspector() instanceof ListObjectInspector ) , true );
    assertEquals( ( fieldList.get(3).getFieldObjectInspector() instanceof StructObjectInspector ) , true );
  }

  @Test
  public void T_initialize_3() throws SerDeException{
    YosegiSerde serde = new YosegiSerde();
    Configuration conf = new Configuration();
    conf.set( ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR , "num" );

    Properties table = new Properties();
    Properties part = new Properties();
    table.setProperty( serdeConstants.LIST_COLUMNS , "str,num,arry,nest" );
    table.setProperty( serdeConstants.LIST_COLUMN_TYPES , "string,int,array<string>,struct<a:string,b:int>" );

    serde.initialize( conf , table , part );
    StructObjectInspector inspector = (StructObjectInspector)( serde.getObjectInspector() );
    List<? extends StructField> fieldList = inspector.getAllStructFieldRefs();
    assertEquals( fieldList.get(0).getFieldName() , "num" );

    assertEquals( ( fieldList.get(0).getFieldObjectInspector() instanceof PrimitiveObjectInspector ) , true );
  }

  @Test
  public void T_deserialize_1() throws SerDeException{
    YosegiSerde serde = new YosegiSerde();
    Writable a = new Text( "a" );
    Writable b = (Writable)( serde.deserialize( a ) );
    assertEquals( a , b );
  }

  @Test
  public void T_getSerializedClass_1() throws SerDeException{
    YosegiSerde serde = new YosegiSerde();
    assertEquals( ParserWritable.class , serde.getSerializedClass() );
  }



}
