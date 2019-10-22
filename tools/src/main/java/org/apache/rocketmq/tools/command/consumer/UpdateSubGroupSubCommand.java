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
package org.apache.rocketmq.tools.command.consumer;

import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;

public class UpdateSubGroupSubCommand implements SubCommand {

    private static final Logger logger = LoggerFactory.getLogger(UpdateSubGroupSubCommand.class);

	@Override
    public String commandName() {
        return "updateSubGroup";
    }

    @Override
    public String commandDesc() {
        return "Update or create subscription group";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerAddr", true, "create subscription group to which broker");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "create subscription group to which cluster");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("g", "groupName", true, "consumer group name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("s", "consumeEnable", true, "consume enable");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "consumeFromMinEnable", true, "from min offset");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("d", "consumeBroadcastEnable", true, "broadcast");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("q", "retryQueueNums", true, "retry queue nums");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("r", "retryMaxTimes", true, "retry max times");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("i", "brokerId", true, "consumer from which broker id");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("w", "whichBrokerWhenConsumeSlowly", true, "which broker id when consume slowly");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("a", "notifyConsumerIdsChanged", true, "notify consumerId changed");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options,
        RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setConsumeBroadcastEnable(false);
            subscriptionGroupConfig.setConsumeFromMinEnable(false);

            // groupName
            subscriptionGroupConfig.setGroupName(StringUtils.trim(commandLine.getOptionValue('g')));

            // consumeEnable
            if (commandLine.hasOption('s')) {
                subscriptionGroupConfig.setConsumeEnable(Boolean.parseBoolean(StringUtils
                    .trim(commandLine.getOptionValue('s'))));
            }

            // consumeFromMinEnable
            if (commandLine.hasOption('m')) {
                subscriptionGroupConfig.setConsumeFromMinEnable(Boolean.parseBoolean(StringUtils.trim(commandLine.getOptionValue('m'))));
            }

            // consumeBroadcastEnable
            if (commandLine.hasOption('d')) {
                subscriptionGroupConfig.setConsumeBroadcastEnable(Boolean.parseBoolean(StringUtils.trim(commandLine.getOptionValue('d'))));
            }

            // retryQueueNums
            if (commandLine.hasOption('q')) {
                subscriptionGroupConfig.setRetryQueueNums(Integer.parseInt(StringUtils
                    .trim(commandLine.getOptionValue('q'))));
            }

            // retryMaxTimes
            if (commandLine.hasOption('r')) {
                subscriptionGroupConfig.setRetryMaxTimes(Integer.parseInt(StringUtils
                    .trim(commandLine.getOptionValue('r'))));
            }

            // brokerId
            if (commandLine.hasOption('i')) {
                subscriptionGroupConfig.setBrokerId(Long.parseLong(StringUtils.trim(commandLine.getOptionValue('i'))));
            }

            // whichBrokerWhenConsumeSlowly
            if (commandLine.hasOption('w')) {
                subscriptionGroupConfig.setWhichBrokerWhenConsumeSlowly(Long.parseLong(StringUtils.trim(commandLine.getOptionValue('w'))));
            }

            // notifyConsumerIdsChanged
            if (commandLine.hasOption('a')) {
                subscriptionGroupConfig.setNotifyConsumerIdsChangedEnable(Boolean.parseBoolean(StringUtils.trim(commandLine.getOptionValue('a'))));
            }

            if (commandLine.hasOption('b')) {
                String addr = StringUtils.trim(commandLine.getOptionValue('b'));

                defaultMQAdminExt.start();

                defaultMQAdminExt.createAndUpdateSubscriptionGroupConfig(addr, subscriptionGroupConfig);
                logger.info("create subscription group to %s success.%n", addr);
                logger.info("%s", subscriptionGroupConfig);
                return;

            } else if (commandLine.hasOption('c')) {
                String clusterName = StringUtils.trim(commandLine.getOptionValue('c'));

                defaultMQAdminExt.start();
                Set<String> masterSet =
                    CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String addr : masterSet) {
                    try {
                        defaultMQAdminExt.createAndUpdateSubscriptionGroupConfig(addr, subscriptionGroupConfig);
                        logger.info("create subscription group to %s success.%n", addr);
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                        Thread.sleep(1000 * 1);
                    }
                }
                logger.info("%s", subscriptionGroupConfig);
                return;
            }

            ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
