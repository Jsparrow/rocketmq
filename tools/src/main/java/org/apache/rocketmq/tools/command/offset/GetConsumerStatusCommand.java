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

import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;

public class GetConsumerStatusCommand implements SubCommand {
    private static final Logger logger = LoggerFactory.getLogger(GetConsumerStatusCommand.class);

	@Override
    public String commandName() {
        return "getConsumerStatus";
    }

    @Override
    public String commandDesc() {
        return "get consumer status from client.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("g", "group", true, "set the consumer group");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "set the topic");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("i", "originClientId", true, "set the consumer clientId");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            String group = StringUtils.trim(commandLine.getOptionValue("g"));
            String topic = StringUtils.trim(commandLine.getOptionValue("t"));
            String originClientId = "";
            if (commandLine.hasOption("i")) {
                originClientId = StringUtils.trim(commandLine.getOptionValue("i"));
            }
            defaultMQAdminExt.start();

            Map<String, Map<MessageQueue, Long>> consumerStatusTable =
                defaultMQAdminExt.getConsumeStatus(topic, group, originClientId);
            logger.info("get consumer status from client. group=%s, topic=%s, originClientId=%s%n", group, topic, originClientId);

            logger.info("%-50s  %-15s  %-15s  %-20s%n", "#clientId", "#brokerName", "#queueId", "#offset");

            consumerStatusTable.entrySet().forEach(entry -> {
                String clientId = entry.getKey();
                Map<MessageQueue, Long> mqTable = entry.getValue();
                mqTable.entrySet().stream().map(Map.Entry::getKey).forEach(mq -> logger.info("%-50s  %-15s  %-15d  %-20d%n", UtilAll.frontStringAtLeast(clientId, 50), mq.getBrokerName(), mq.getQueueId(), mqTable.get(mq)));
            });
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
