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

package org.apache.seatunnel.transform.donothing;

import com.google.auto.service.AutoService;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.table.catalog.*;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportTransform;

import java.util.List;
import java.util.stream.Collectors;

@AutoService(SeaTunnelTransform.class)
@NoArgsConstructor
public class DoNothingTransform extends AbstractCatalogSupportTransform {
    private ReadonlyConfig config;

    public DoNothingTransform(@NonNull ReadonlyConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        this.config = config;
    }

    @Override
    public String getPluginName() {
        return "DoNothing";
    }

    @Override
    protected void setConfig(Config pluginConfig) {
        ConfigValidator.of(ReadonlyConfig.fromConfig(pluginConfig)).validate(new DoNothingTransformFactory().optionRule());
        this.config = ReadonlyConfig.fromConfig(pluginConfig);
    }


    @Override
    protected TableSchema transformTableSchema() {
        List<ConstraintKey> copyConstraintKeys = inputCatalogTable.getTableSchema().getConstraintKeys().stream().map(ConstraintKey::copy).collect(Collectors.toList());
        PrimaryKey copiedPrimaryKey = inputCatalogTable.getTableSchema().getPrimaryKey() == null ? null : inputCatalogTable.getTableSchema().getPrimaryKey().copy();
        return TableSchema.builder().columns(inputCatalogTable.getTableSchema().getColumns()).primaryKey(copiedPrimaryKey).constraintKey(copyConstraintKeys).build();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }

    @Override
    protected SeaTunnelRowType transformRowType(SeaTunnelRowType inputRowType) {
        return inputRowType;
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        return inputRow;
    }
}
