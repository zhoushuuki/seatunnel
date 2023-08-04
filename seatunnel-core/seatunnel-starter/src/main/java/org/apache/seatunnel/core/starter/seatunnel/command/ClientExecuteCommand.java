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

package org.apache.seatunnel.core.starter.seatunnel.command;

import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.StringFormatUtils;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.enums.MasterType;
import org.apache.seatunnel.core.starter.exception.CommandExecuteException;
import org.apache.seatunnel.core.starter.result.ReturnResult;
import org.apache.seatunnel.core.starter.seatunnel.args.ClientCommandArgs;
import org.apache.seatunnel.core.starter.utils.FileUtils;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.client.job.JobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.JobMetricsRunner;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelNodeContext;

import org.apache.commons.lang3.StringUtils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.seatunnel.core.starter.utils.FileUtils.checkConfigExist;

/** This command is used to execute the SeaTunnel engine job by SeaTunnel API. */
@Slf4j
public class ClientExecuteCommand implements Command<ClientCommandArgs> {

    private final ClientCommandArgs clientCommandArgs;

    private JobStatus jobStatus;
    private SeaTunnelClient engineClient;
    private HazelcastInstance instance;
    private ScheduledExecutorService executorService;
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public ClientExecuteCommand(ClientCommandArgs clientCommandArgs) {
        this.clientCommandArgs = clientCommandArgs;
    }

    @SuppressWarnings({"checkstyle:RegexpSingleline", "checkstyle:MagicNumber"})
    @Override
    public void execute() throws CommandExecuteException {
        JobMetricsRunner.JobMetricsSummary jobMetricsSummary = null;
        LocalDateTime startTime = LocalDateTime.now();
        LocalDateTime endTime = LocalDateTime.now();
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        try {
            String clusterName = clientCommandArgs.getClusterName();
            if (clientCommandArgs.getMasterType().equals(MasterType.LOCAL)) {
                clusterName =
                        creatRandomClusterName(
                                StringUtils.isNotEmpty(clusterName)
                                        ? clusterName
                                        : Constant.DEFAULT_SEATUNNEL_CLUSTER_NAME);
                instance = createServerInLocal(clusterName, seaTunnelConfig);
            }
            if (StringUtils.isNotEmpty(clusterName)) {
                seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
            }
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            if (StringUtils.isNotEmpty(clusterName)) {
                clientConfig.setClusterName(clusterName);
            }
            engineClient = new SeaTunnelClient(clientConfig);
            if (clientCommandArgs.isListJob()) {
                String jobStatus = engineClient.getJobClient().listJobStatus(true);
                System.out.println(jobStatus);
            } else if (null != clientCommandArgs.getJobId()) {
                String jobState =
                        engineClient
                                .getJobClient()
                                .getJobDetailStatus(Long.parseLong(clientCommandArgs.getJobId()));
                System.out.println(jobState);
            } else if (null != clientCommandArgs.getCancelJobId()) {
                engineClient
                        .getJobClient()
                        .cancelJob(Long.parseLong(clientCommandArgs.getCancelJobId()));
            } else if (null != clientCommandArgs.getMetricsJobId()) {
                String jobMetrics =
                        engineClient
                                .getJobClient()
                                .getJobMetrics(Long.parseLong(clientCommandArgs.getMetricsJobId()));
                System.out.println(jobMetrics);
            } else if (null != clientCommandArgs.getSavePointJobId()) {
                engineClient
                        .getJobClient()
                        .savePointJob(Long.parseLong(clientCommandArgs.getSavePointJobId()));
            } else {
                JobConfig jobConfig = new JobConfig();
                JobExecutionEnvironment jobExecutionEnv;
                jobConfig.setName(clientCommandArgs.getJobName());
                String content = clientCommandArgs.getContent();
                if (StringUtils.isNotBlank(content)) { // json字符串的任务
                    if (null != clientCommandArgs.getRestoreJobId()) {
                        jobExecutionEnv = engineClient.restoreExecutionContext(content, jobConfig, Long.parseLong(clientCommandArgs.getRestoreJobId()));
                    } else {
                        jobExecutionEnv = engineClient.createExecutionContext(content, jobConfig);
                    }
                } else { // 文件类型的任务
                    Path configFile = FileUtils.getConfigPath(clientCommandArgs);
                    checkConfigExist(configFile);
                    if (null != clientCommandArgs.getRestoreJobId()) {
                        jobExecutionEnv = engineClient.restoreExecutionContext(configFile.toString(), jobConfig, Long.parseLong(clientCommandArgs.getRestoreJobId()));
                    } else {
                        jobExecutionEnv = engineClient.createExecutionContext(configFile.toString(), jobConfig);
                    }
                }

                // get job start time
                startTime = LocalDateTime.now();
                // create job proxy
                ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
                if (clientCommandArgs.isAsync()) {
                    if (clientCommandArgs.getMasterType().equals(MasterType.LOCAL)) {
                        log.warn("The job is running in local mode, can not use async mode.");
                    } else {
                        return;
                    }
                }
                // register cancelJob hook
                Runtime.getRuntime()
                        .addShutdownHook(
                                new Thread(
                                        () -> {
                                            CompletableFuture<Void> future =
                                                    CompletableFuture.runAsync(
                                                            () -> {
                                                                log.info(
                                                                        "run shutdown hook because get close signal");
                                                                shutdownHook(clientJobProxy);
                                                            });
                                            try {
                                                future.get(15, TimeUnit.SECONDS);
                                            } catch (Exception e) {
                                                log.error("Cancel job failed.", e);
                                            }
                                        }));
                // get job id
                long jobId = clientJobProxy.getJobId();
                JobMetricsRunner jobMetricsRunner = new JobMetricsRunner(engineClient, jobId);
                executorService =
                        Executors.newSingleThreadScheduledExecutor(
                                new ThreadFactoryBuilder()
                                        .setNameFormat("job-metrics-runner-%d")
                                        .setDaemon(true)
                                        .build());
                executorService.scheduleAtFixedRate(
                        jobMetricsRunner,
                        0,
                        seaTunnelConfig.getEngineConfig().getPrintJobMetricsInfoInterval(),
                        TimeUnit.SECONDS);
                // wait for job complete
                jobStatus = clientJobProxy.waitForJobComplete();
                // get job end time
                endTime = LocalDateTime.now();
                // get job statistic information when job finished
                jobMetricsSummary = engineClient.getJobMetricsSummary(jobId);
            }
        } catch (Exception e) {
            e.printStackTrace();
            // 开始时间
            String var1 = DateTimeUtils.toString(startTime, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS);
            // 结束时间
            String var2 = DateTimeUtils.toString(endTime, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS);
            // 耗时(s)
            Long var3 = Duration.between(LocalDateTime.parse(var1, formatter), LocalDateTime.parse(var2, formatter)).getSeconds();
            ReturnResult result = new ReturnResult(var1, var2, var3, 0L, 0L, 0L);
            result.setErrorInfo("job executed failed:" + e.getMessage());
            Consumer<ReturnResult> callBackFunc = clientCommandArgs.getCallBackFunc();
            if (Objects.nonNull(callBackFunc)) callBackFunc.accept(result);
        } finally {
            if (jobMetricsSummary != null) {
                // 开始时间
                String var1 = DateTimeUtils.toString(startTime, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS);
                // 结束时间
                String var2 = DateTimeUtils.toString(endTime, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS);
                // 耗时(s)
                Long var3 = Duration.between(LocalDateTime.parse(var1, formatter), LocalDateTime.parse(var2, formatter)).getSeconds();
                // 总数
                Long var4 = jobMetricsSummary.getSourceReadCount();
                // 写入数
                Long var5 = jobMetricsSummary.getSinkWriteCount();
                // 失败数
                Long var6 = jobMetricsSummary.getSourceReadCount() - jobMetricsSummary.getSinkWriteCount();

                ReturnResult result = new ReturnResult(var1, var2, var3, var4, var5, var6);
                Consumer<ReturnResult> callBackFunc = clientCommandArgs.getCallBackFunc();
                if (Objects.nonNull(callBackFunc)) callBackFunc.accept(result);

                // print job statistics information when job finished
                log.info(StringFormatUtils.formatTable("Job Statistic Information",
                        "Start Time", var1,
                        "End Time", var2,
                        "Total Time(s)", var3,
                        "Total Read Count", var4,
                        "Total Write Count", var5,
                        "Total Failed Count", var6));
            }
            closeClient();
        }
    }

    private void closeClient() {
        if (engineClient != null) {
            engineClient.close();
            log.info("Closed SeaTunnel client......");
        }
        if (instance != null) {
            instance.shutdown();
            log.info("Closed HazelcastInstance ......");
        }
        if (executorService != null) {
            executorService.shutdownNow();
            log.info("Closed metrics executor service ......");
        }
    }

    private HazelcastInstance createServerInLocal(
            String clusterName, SeaTunnelConfig seaTunnelConfig) {
        seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
        seaTunnelConfig.getHazelcastConfig().getNetworkConfig().setPortAutoIncrement(true);
        return HazelcastInstanceFactory.newHazelcastInstance(
                seaTunnelConfig.getHazelcastConfig(),
                Thread.currentThread().getName(),
                new SeaTunnelNodeContext(seaTunnelConfig));
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private String creatRandomClusterName(String namePrefix) {
        Random random = new Random();
        return namePrefix + "-" + random.nextInt(1000000);
    }

    private void shutdownHook(ClientJobProxy clientJobProxy) {
        if (clientCommandArgs.isCloseJob()) {
            if (clientJobProxy.getJobResultCache() == null
                    && (jobStatus == null || !jobStatus.isEndState())) {
                log.warn("Task will be closed due to client shutdown.");
                clientJobProxy.cancelJob();
            }
        }
    }
}
