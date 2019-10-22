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
package org.apache.rocketmq.tools.command.broker;

import java.util.Properties;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;

public class UpdateBrokerConfigSubCommand implements SubCommand {

    private static final Logger logger = LoggerFactory.getLogger(UpdateBrokerConfigSubCommand.class);

	@Override
    public String commandName() {
        return "updateBrokerConfig";
    }

    @Override
    public String commandDesc() {
        return "Update broker's config";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerAddr", true, "update which broker");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "update which cluster");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("k", "key", true, "config key");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("v", "value", true, "config value");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            String key = StringUtils.trim(commandLine.getOptionValue('k'));
            String value = StringUtils.trim(commandLine.getOptionValue('v'));
            Properties properties = new Properties();
            properties.put(key, value);

            if (commandLine.hasOption('b')) {
                String brokerAddr = StringUtils.trim(commandLine.getOptionValue('b'));

                defaultMQAdminExt.start();

                defaultMQAdminExt.updateBrokerConfig(brokerAddr, properties);
                logger.info("update broker config success, %s\n", brokerAddr);
                return;

            } else if (commandLine.hasOption('c')) {
                String clusterName = StringUtils.trim(commandLine.getOptionValue('c'));

                defaultMQAdminExt.start();

                Set<String> masterSet =
                    CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String brokerAddr : masterSet) {
                    try {
                        defaultMQAdminExt.updateBrokerConfig(brokerAddr, properties);
                        logger.info("update broker config success, %s\n", brokerAddr);
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
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
