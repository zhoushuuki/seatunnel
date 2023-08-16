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

package org.apache.seatunnel.example.engine;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.seatunnel.core.starter.SeaTunnel;
import org.apache.seatunnel.core.starter.enums.MasterType;
import org.apache.seatunnel.core.starter.exception.CommandException;
import org.apache.seatunnel.core.starter.result.ReturnResult;
import org.apache.seatunnel.core.starter.seatunnel.args.ClientCommandArgs;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

public class SeaTunnelEngineExample {

    public static void main(String[] args)
            throws CommandException {
        String content = "{\n" +
                "\t\"env\": {\n" +
                "\t\t\"job.mode\": \"BATCH\",\n" +
                "\t\t\"parallelism\": 1,\n" +
                "\t\t\"job.name\": \"任务测试复杂过滤\",\n" +
                "\t\t\"checkpoint.interval\": \"1000\"\n" +
                "\t},\n" +
                "\t\"source\": [\n" +
                "\t\t{\n" +
                "\t\t\t\"database\": \"warehouse\",\n" +
                "\t\t\t\"password\": \"MyNewPass4!\",\n" +
                "\t\t\t\"host\": \"192.168.10.200:8123\",\n" +
                "\t\t\t\"result_table_name\": \"xf1234\",\n" +
                "\t\t\t\"plugin_name\": \"Clickhouse\",\n" +
                "\t\t\t\"username\": \"default\",\n" +
                "\t\t\t\"sql\": \"SELECT wxZag.id,wxZag.name,wxZag.code,WyHnE.url FROM (SELECT name,id,code,description FROM `xodb_new_test_data_catalog_class` LIMIT 0, 12) wxZag LEFT JOIN (SELECT name,id,url,resource_code FROM `xodb_new_test_data_resource`) WyHnE ON wxZag.id=WyHnE.id GROUP BY code,name,id,url\"\n" +
                "\t\t}\n" +
                "\t],\n" +
                "\t\"transform\": [\n" +
                "\t\t{\n" +
                "\t\t\t\"input_field\": \"name\",\n" +
                "\t\t\t\"output_field\": \"xxx\",\n" +
                "\t\t\t\"source_table_name\": [\n" +
                "\t\t\t\t\"xf1234\"\n" +
                "\t\t\t],\n" +
                "\t\t\t\"result_table_name\": \"xf222\",\n" +
                "\t\t\t\"plugin_name\": \"StrLength\"\n" +
                "\t\t}\n" +
                "\t],\n" +
                "\t\"sink\": [\n" +
                "\t\t{\n" +
                "\t\t\t\"password\": \"MyNewPass4!\",\n" +
                "\t\t\t\"xa_data_source_class_name\": \"com.mysql.cj.jdbc.MysqlXADataSource\",\n" +
                "\t\t\t\"driver\": \"com.mysql.cj.jdbc.Driver\",\n" +
                "\t\t\t\"query\": \"INSERT INTO `liulin0602`(bame,id,name,status,length_) VALUES(?,?,?,?,?)\",\n" +
                "\t\t\t\"source_table_name\": \"xf222\",\n" +
                "\t\t\t\"plugin_name\": \"Jdbc\",\n" +
                "\t\t\t\"user\": \"root\",\n" +
                "\t\t\t\"url\": \"jdbc:mysql://192.168.10.200:3306/warehouse\",\n" +
                "\t\t\t\"is_exactly_once\": true\n" +
                "\t\t}\n" +
                "\t]\n" +
                "}";
        String configurePath = args.length > 0 ? args[0] : "/examples/2.json";
        // String configFile = getTestConfigFile(configurePath);
        ClientCommandArgs clientCommandArgs = new ClientCommandArgs();
        // clientCommandArgs.setConfigFile(configFile);
        clientCommandArgs.setCheckConfig(false);
//        clientCommandArgs.setJobName(Paths.get(configFile).getFileName().toString());
        clientCommandArgs.setJobName("任务测试");
        // Change Execution Mode to CLUSTER to use client mode, before do this, you should start
        // SeaTunnelEngineServerExample
        clientCommandArgs.setMasterType(MasterType.LOCAL);
        clientCommandArgs.setContent(content);
        ReturnResult result = new ReturnResult();
        clientCommandArgs.setCallBackFunc((item) -> {
            try {
                BeanUtils.copyProperties(result, item);
            } catch (Exception e) {
                // ignored
            }
        });

        SeaTunnel.run(clientCommandArgs.buildCommand());
        System.out.println(result);

    }

    public static String getTestConfigFile(String configFile)
            throws FileNotFoundException, URISyntaxException {
        URL resource = SeaTunnelEngineExample.class.getResource(configFile);
        if (resource == null) {
            throw new FileNotFoundException("Can't find config file: " + configFile);
        }
        return Paths.get(resource.toURI()).toString();
    }
}
