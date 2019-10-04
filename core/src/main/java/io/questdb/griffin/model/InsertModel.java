/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package io.questdb.griffin.model;

import io.questdb.std.*;
import io.questdb.std.str.CharSink;

public class InsertModel implements ExecutionModel, Mutable, Sinkable {
    public static final ObjectFactory<InsertModel> FACTORY = InsertModel::new;
    private final CharSequenceHashSet columnSet = new CharSequenceHashSet();
    private final ObjList<ExpressionNode> columnValues = new ObjList<>();
    private final IntList columnPositions = new IntList();
    private ExpressionNode tableName;
    private QueryModel queryModel;
    private int selectKeywordPosition;

    private InsertModel() {
    }

    public boolean addColumn(CharSequence columnName, int columnPosition) {
        if (columnSet.add(columnName)) {
            columnPositions.add(columnPosition);
            return true;
        }
        return false;
    }

    public void addColumnValue(ExpressionNode value) {
        columnValues.add(value);
    }

    @Override
    public void clear() {
        this.tableName = null;
        this.queryModel = null;
        this.columnSet.clear();
        this.columnPositions.clear();
        this.columnValues.clear();
        this.selectKeywordPosition = 0;
    }

    public int getColumnPosition(int columnIndex) {
        return columnPositions.getQuick(columnIndex);
    }

    public CharSequenceHashSet getColumnSet() {
        return columnSet;
    }

    public ObjList<ExpressionNode> getColumnValues() {
        return columnValues;
    }

    public int getSelectKeywordPosition() {
        return selectKeywordPosition;
    }

    public void setSelectKeywordPosition(int selectKeywordPosition) {
        this.selectKeywordPosition = selectKeywordPosition;
    }

    @Override
    public int getModelType() {
        return INSERT;
    }

    public QueryModel getQueryModel() {
        return queryModel;
    }

    public void setQueryModel(QueryModel queryModel) {
        this.queryModel = queryModel;
    }

    public ExpressionNode getTableName() {
        return tableName;
    }

    public void setTableName(ExpressionNode tableName) {
        this.tableName = tableName;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put("insert into ").put(tableName.token).put(' ');
        int n = columnSet.size();
        if (n > 0) {
            sink.put('(');
            for (int i = 0; i < n; i++) {
                if (i > 0) {
                    sink.put(", ");
                }
                sink.put(columnSet.get(i));
            }
            sink.put(") ");
        }
        if (queryModel != null) {
            queryModel.toSink(sink);
        } else {
            sink.put("values (");

            for (int i = 0, m = columnValues.size(); i < m; i++) {
                if (i > 0) {
                    sink.put(", ");
                }
                sink.put(columnValues.getQuick(i));
            }

            sink.put(')');
        }
    }
}
