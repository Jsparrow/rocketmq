/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.tools.command.offset;

import java.util.Date;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.admin.RollbackStats;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;

public class ResetOffsetByTimeOldCommand implements SubCommand {
    private static final Logger logger = LoggerFactory.getLogger(ResetOffsetByTimeOldCommand.class);

	public static void resetOffset(DefaultMQAdminExt defaultMQAdminExt, String consumerGroup, String topic,
        long timestamp, boolean force,
        String timeStampStr) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        List<RollbackStats> rollbackStatsList = defaultMQAdminExt.resetOffsetByTimestampOld(consumerGroup, topic, timestamp, force);
        logger.info(
            "rollback consumer offset by specified consumerGroup[%s], topic[%s], force[%s], timestamp(string)[%s], timestamp(long)[%s]%n", consumerGroup, topic, force, timeStampStr, timestamp);

        logger.info("%-20s  %-20s  %-20s  %-20s  %-20s  %-20s%n", "#brokerName", "#queueId", "#brokerOffset", "#consumerOffset", "#timestampOffset", "#rollbackOffset"
        );

        rollbackStatsList.forEach(rollbackStats -> logger.info("%-20s  %-20d  %-20d  %-20d  %-20d  %-20d%n",
				UtilAll.frontStringAtLeast(rollbackStats.getBrokerName(), 32), rollbackStats.getQueueId(),
				rollbackStats.getBrokerOffset(), rollbackStats.getConsumerOffset(), rollbackStats.getTimestampOffset(),
				rollbackStats.getRollbackOffset()));
    }

    @Override
    public String commandName() {
        return "resetOffsetByTimeOld";
    }

    @Override
    public String commandDesc() {
        return "Reset consumer offset by timestamp(execute this command required client restart).";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("g", "group", true, "set the consumer group");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "set the topic");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("s", "timestamp", true, "set the timestamp[currentTimeMillis|yyyy-MM-dd#HH:mm:ss:SSS]");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("f", "force", true, "set the force rollback by timestamp switch[true|false]");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            String consumerGroup = StringUtils.trim(commandLine.getOptionValue("g"));
            String topic = StringUtils.trim(commandLine.getOptionValue("t"));
            String timeStampStr = StringUtils.trim(commandLine.getOptionValue("s"));
            long timestamp = 0;
            try {
                timestamp = Long.parseLong(timeStampStr);
            } catch (NumberFormatException e) {

                logger.error(e.getMessage(), e);
				Date date = UtilAll.parseDate(timeStampStr, UtilAll.YYYY_MM_DD_HH_MM_SS_SSS);
                if (date != null) {
                    timestamp = UtilAll.parseDate(timeStampStr, UtilAll.YYYY_MM_DD_HH_MM_SS_SSS).getTime();
                } else {
                    logger.info("specified timestamp invalid.%n");
                    return;
                }

                boolean force = true;
                if (commandLine.hasOption('f')) {
                    force = Boolean.valueOf(StringUtils.trim(commandLine.getOptionValue("f")));
                }

                defaultMQAdminExt.start();
                resetOffset(defaultMQAdminExt, consumerGroup, topic, timestamp, force, timeStampStr);
            }

        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
