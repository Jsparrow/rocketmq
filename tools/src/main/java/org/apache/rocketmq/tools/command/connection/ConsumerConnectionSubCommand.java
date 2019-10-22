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
package org.apache.rocketmq.tools.command.connection;

import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.protocol.body.Connection;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;

public class ConsumerConnectionSubCommand implements SubCommand {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerConnectionSubCommand.class);

	@Override
    public String commandName() {
        return "consumerConnection";
    }

    @Override
    public String commandDesc() {
        return "Query consumer's socket connection, client version and subscription";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("g", "consumerGroup", true, "consumer group name");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();

            String group = StringUtils.trim(commandLine.getOptionValue('g'));

            ConsumerConnection cc = defaultMQAdminExt.examineConsumerConnectionInfo(group);

            int i = 1;
            for (Connection conn : cc.getConnectionSet()) {
                logger.info("%03d  %-32s %-22s %-8s %s%n", i++, conn.getClientId(), conn.getClientAddr(), conn.getLanguage(), MQVersion.getVersionDesc(conn.getVersion())
                );
            }

            logger.info("%nBelow is subscription:");
            Iterator<Entry<String, SubscriptionData>> it = cc.getSubscriptionTable().entrySet().iterator();
            i = 1;
            while (it.hasNext()) {
                Entry<String, SubscriptionData> entry = it.next();
                SubscriptionData sd = entry.getValue();
                logger.info("%03d  Topic: %-40s SubExpression: %s%n", i++, sd.getTopic(), sd.getSubString()
                );
            }

            logger.info("");
            logger.info("ConsumeType: %s%n", cc.getConsumeType());
            logger.info("MessageModel: %s%n", cc.getMessageModel());
            logger.info("ConsumeFromWhere: %s%n", cc.getConsumeFromWhere());
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
