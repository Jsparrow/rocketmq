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
package org.apache.rocketmq.tools.command.message;

import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;

public class CheckMsgSendRTCommand implements SubCommand {
    private static final Logger logger = LoggerFactory.getLogger(CheckMsgSendRTCommand.class);
	private static String brokerName = "";
    private static int queueId = 0;

    @Override
    public String commandName() {
        return "checkMsgSendRT";
    }

    @Override
    public String commandDesc() {
        return "check message send response time";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("a", "amout", true, "message amout | default 100");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("s", "size", true, "message size | default 128 Byte");
        opt.setRequired(true);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQProducer producer = new DefaultMQProducer(rpcHook);
        producer.setProducerGroup(Long.toString(System.currentTimeMillis()));

        try {
            producer.start();
            long start = 0;
            long end = 0;
            long timeElapsed = 0;
            boolean sendSuccess = false;
            String topic = StringUtils.trim(commandLine.getOptionValue('t'));
            long amount = !commandLine.hasOption('a') ? 100 : Long.parseLong(StringUtils.trim(commandLine.getOptionValue('a')));
            long msgSize = !commandLine.hasOption('s') ? 128 : Long.parseLong(StringUtils.trim(commandLine.getOptionValue('s')));
            Message msg = new Message(topic, getStringBySize(msgSize).getBytes(MixAll.DEFAULT_CHARSET));

            logger.info("%-32s  %-4s  %-20s    %s%n", "#Broker Name", "#QID", "#Send Result", "#RT"
            );
            for (int i = 0; i < amount; i++) {
                start = System.currentTimeMillis();
                try {
                    producer.send(msg, (List<MessageQueue> mqs, Message msg1, Object arg) -> {
					    int queueIndex = (Integer) arg % mqs.size();
					    MessageQueue queue = mqs.get(queueIndex);
					    brokerName = queue.getBrokerName();
					    queueId = queue.getQueueId();
					    return queue;
					}, i);
                    sendSuccess = true;
                    end = System.currentTimeMillis();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
					sendSuccess = false;
                    end = System.currentTimeMillis();
                }

                if (i != 0) {
                    timeElapsed += end - start;
                }

                logger.info("%-32s  %-4s  %-20s    %s%n", brokerName, queueId, sendSuccess, end - start
                );
            }

            double rt = (double) timeElapsed / (amount - 1);
            logger.info("Avg RT: %s%n", String.format("%.2f", rt));
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            producer.shutdown();
        }
    }

    public String getStringBySize(long size) {
        StringBuilder res = new StringBuilder();
        for (int i = 0; i < size; i++) {
            res.append('a');
        }
        return res.toString();
    }
}
