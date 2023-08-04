/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.transform.column2row;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableTransform;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableFactoryContext;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;

@AutoService(Factory.class)
public class ColumnToRowTransformFactory implements TableTransformFactory {
    @Override
    public String factoryIdentifier() {
        return "ColumnToRow";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().required(ColumnToRowTransformConfig.KEY_BASE_FIELD).build();
    }

    @Override
    public TableTransform createTransform(TableFactoryContext context) {
        CatalogTable catalogTable = context.getCatalogTable();
        return () -> new ColumnToRowTransform(context.getOptions(), catalogTable);
    }
}
