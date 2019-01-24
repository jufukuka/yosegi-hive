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

package jp.co.yahoo.yosegi.hive.pushdown;

import jp.co.yahoo.yosegi.spread.column.filter.IFilter;
import jp.co.yahoo.yosegi.spread.column.filter.StringDictionaryFilter;
import jp.co.yahoo.yosegi.spread.expression.ExecuterNode;
import jp.co.yahoo.yosegi.spread.expression.IExpressionNode;
import jp.co.yahoo.yosegi.spread.expression.IExtractNode;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableVoidObjectInspector;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class InHiveExpr implements IHiveExprNode {

  private final List<ExprNodeDesc> nodeDescList;

  public InHiveExpr( final List<ExprNodeDesc> nodeDescList ) {
    this.nodeDescList = nodeDescList;
  }

  /**
   * Initialize by setting Hive filter condition.
   */
  public static PrimitiveObjectInspector getPrimitiveObjectInspector(
      final ExprNodeDesc exprNode ) {
    if ( ! ( exprNode instanceof ExprNodeConstantDesc ) ) {
      return null;
    }
    ExprNodeConstantDesc constDesc = (ExprNodeConstantDesc)exprNode;
    ObjectInspector objectInspector = constDesc.getWritableObjectInspector();
    if ( objectInspector.getCategory() != ObjectInspector.Category.PRIMITIVE ) {
      return null;
    }
    return (PrimitiveObjectInspector)objectInspector;
  }

  /**
   * Convert to Yosegi's filter condition.
   */
  public static IFilter getEqualsExecuter(
      final List<ExprNodeDesc> nodeDescList , final int start ) {
    PrimitiveObjectInspector rootPrimitiveObjectInspector =
        getPrimitiveObjectInspector( nodeDescList.get( start ) );
    if ( rootPrimitiveObjectInspector == null ) {
      return null;
    }
    PrimitiveObjectInspector.PrimitiveCategory primitiveCategory =
        rootPrimitiveObjectInspector.getPrimitiveCategory();
    IFilter filter = null;
    switch ( rootPrimitiveObjectInspector.getPrimitiveCategory() ) {
      case STRING:
        Set<String> stringDic = new HashSet<String>();
        for ( int i = start ; i < nodeDescList.size() ; i++ ) {
          PrimitiveObjectInspector primitiveObjectInspector  =
              getPrimitiveObjectInspector( nodeDescList.get( i ) );
          if ( primitiveObjectInspector == null
              || primitiveObjectInspector.getPrimitiveCategory()
              != rootPrimitiveObjectInspector.getPrimitiveCategory() ) {
            return null;
          }
          stringDic.add(
              ( (WritableConstantStringObjectInspector)primitiveObjectInspector )
              .getWritableConstantValue().toString() );
        }
        return new StringDictionaryFilter( stringDic );
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      default:
        return null;
    }
  }

  @Override
  public void addChildNode( final ExprNodeGenericFuncDesc exprNodeDesc ) {
    throw new UnsupportedOperationException( "IHiveExprNode node can not have child node." );
  }

  @Override
  public IExpressionNode getPushDownFilterNode() {
    if ( nodeDescList.size() < 2 ) {
      return null;
    }
    ExprNodeDesc columnDesc = nodeDescList.get( 0 );
    IExtractNode extractNode = CreateExtractNodeUtil.getExtractNode( columnDesc );
    if ( extractNode == null ) {
      return null;
    }
    IFilter filter = getEqualsExecuter( nodeDescList , 1 );
    if ( filter == null ) {
      return null;
    }
    return new ExecuterNode( extractNode , filter );
  }

}
