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

import jp.co.yahoo.yosegi.hive.YosegiColumnTypeUtil;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.filter.NotNullFilter;
import jp.co.yahoo.yosegi.spread.expression.ExecuterNode;
import jp.co.yahoo.yosegi.spread.expression.IExpressionNode;
import jp.co.yahoo.yosegi.spread.expression.IExtractNode;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

import java.util.List;

public class NotNullHiveExpr implements IHiveExprNode {

  private final List<ExprNodeDesc> nodeDescList;

  public NotNullHiveExpr( final List<ExprNodeDesc> nodeDescList ) {
    this.nodeDescList = nodeDescList;
  }

  @Override
  public void addChildNode( final ExprNodeGenericFuncDesc exprNodeDesc ) {
    throw new UnsupportedOperationException( "IHiveExprNode node can not have child node." );
  }

  @Override
  public IExpressionNode getPushDownFilterNode() {
    if ( nodeDescList.size() != 1 ) {
      return null;
    }
    ExprNodeDesc columnDesc = nodeDescList.get( 0 );

    if ( ! ( columnDesc instanceof ExprNodeColumnDesc ) ) {
      return null;
    } 

    IExtractNode extractNode = CreateExtractNodeUtil.getExtractNode( columnDesc ); 
    if ( extractNode == null ) {
      return null;
    }

    ColumnType targetColumnType =
        YosegiColumnTypeUtil.typeInfoToColumnType( columnDesc.getTypeInfo() );

    return new ExecuterNode( extractNode , new NotNullFilter( targetColumnType ) );
  }

}
