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
package jp.co.yahoo.yosegi.hive.io.vector;

import java.io.*;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.PrimitiveColumn;
import jp.co.yahoo.yosegi.spread.expression.AllExpressionIndex;
import jp.co.yahoo.yosegi.spread.expression.IExpressionIndex;

import org.apache.hadoop.hive.ql.exec.vector.*;

import jp.co.yahoo.yosegi.message.objects.*;

import jp.co.yahoo.yosegi.*;

public class TestLongColumnVectorAssignor{

  @Test
  public void T_set_1() throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.LONG , "t" );
    for( int i = 0 ; i < 2000 ; i++ ){
      column.add( ColumnType.LONG , new LongObj( i ) , i );
    }
    IExpressionIndex index = new AllExpressionIndex( column.size() );

    LongColumnVector vector = new LongColumnVector( 1024 );
    IColumnVectorAssignor assignor = new LongColumnVectorAssignor( LongPrimitiveSetter.getInstance() );
    assignor.setColumn( column.size() , column );

    for( int i = 0 ; i < 3 ; i++ ){
      int start = i * 1024;
      assignor.setColumnVector( vector , index , start , 1024 );
      for( int n = 0 ; n < 1024 ; n++ ){
        if( ( n + start ) < 2000 ){
          assertEquals( vector.vector[n] , ( n + start ) );
        }
        else{
          assertTrue( vector.isNull[n] );
        }
      }
    }
  }

}
