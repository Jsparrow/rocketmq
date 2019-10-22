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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageClientExt;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.api.MessageTrack;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryMsgByIdSubCommand implements SubCommand {
    private static final Logger logger = LoggerFactory.getLogger(QueryMsgByIdSubCommand.class);

	public static void queryById(final DefaultMQAdminExt admin, final String msgId) throws MQClientException,
        RemotingException, MQBrokerException, InterruptedException, IOException {
        MessageExt msg = admin.viewMessage(msgId);

        printMsg(admin, msg);
    }

    public static void printMsg(final DefaultMQAdminExt admin, final MessageExt msg) throws IOException {
        if (msg == null) {
            logger.info("%nMessage not found!");
            return;
        }

        String bodyTmpFilePath = createBodyFile(msg);
        String msgId = msg.getMsgId();
        if (msg instanceof MessageClientExt) {
            msgId = ((MessageClientExt) msg).getOffsetMsgId();
        }

        logger.info("%-20s %s%n", "OffsetID:", msgId
        );

        logger.info("%-20s %s%n", "OffsetID:", msgId
        );

        logger.info("%-20s %s%n", "Topic:", msg.getTopic()
        );

        logger.info("%-20s %s%n", "Tags:", new StringBuilder().append("[").append(msg.getTags()).append("]").toString()
        );

        logger.info("%-20s %s%n", "Keys:", new StringBuilder().append("[").append(msg.getKeys()).append("]").toString()
        );

        logger.info("%-20s %d%n", "Queue ID:", msg.getQueueId()
        );

        logger.info("%-20s %d%n", "Queue Offset:", msg.getQueueOffset()
        );

        logger.info("%-20s %d%n", "CommitLog Offset:", msg.getCommitLogOffset()
        );

        logger.info("%-20s %d%n", "Reconsume Times:", msg.getReconsumeTimes()
        );

        logger.info("%-20s %s%n", "Born Timestamp:", UtilAll.timeMillisToHumanString2(msg.getBornTimestamp())
        );

        logger.info("%-20s %s%n", "Store Timestamp:", UtilAll.timeMillisToHumanString2(msg.getStoreTimestamp())
        );

        logger.info("%-20s %s%n", "Born Host:", RemotingHelper.parseSocketAddressAddr(msg.getBornHost())
        );

        logger.info("%-20s %s%n", "Store Host:", RemotingHelper.parseSocketAddressAddr(msg.getStoreHost())
        );

        logger.info("%-20s %d%n", "System Flag:", msg.getSysFlag()
        );

        logger.info("%-20s %s%n", "Properties:", msg.getProperties() != null ? msg.getProperties().toString() : ""
        );

        logger.info("%-20s %s%n", "Message Body Path:", bodyTmpFilePath
        );

        try {
            List<MessageTrack> mtdList = admin.messageTrackDetail(msg);
            if (mtdList.isEmpty()) {
                logger.info("%n%nWARN: No Consumer");
            } else {
                logger.info("%n%n");
                mtdList.forEach(mt -> logger.info("%s", mt));
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private static String createBodyFile(MessageExt msg) throws IOException {
        DataOutputStream dos = null;
        try {
            String bodyTmpFilePath = "/tmp/rocketmq/msgbodys";
            File file = new File(bodyTmpFilePath);
            if (!file.exists()) {
                file.mkdirs();
            }
            bodyTmpFilePath = new StringBuilder().append(bodyTmpFilePath).append("/").append(msg.getMsgId()).toString();
            dos = new DataOutputStream(new FileOutputStream(bodyTmpFilePath));
            dos.write(msg.getBody());
            return bodyTmpFilePath;
        } finally {
            if (dos != null) {
				dos.close();
			}
        }
    }

    @Override
    public String commandName() {
        return "queryMsgById";
    }

    @Override
    public String commandDesc() {
        return "Query Message by Id";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("i", "msgId", true, "Message Id");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("g", "consumerGroup", true, "consumer group name");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("d", "clientId", true, "The consumer's client id");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("s", "sendMessage", true, "resend message");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("u", "unitName", true, "unit name");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("ReSendMsgById");
        defaultMQProducer.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();
            if (commandLine.hasOption('s')) {
                if (commandLine.hasOption('u')) {
                    String unitName = StringUtils.trim(commandLine.getOptionValue('u'));
                    defaultMQProducer.setUnitName(unitName);
                }
                defaultMQProducer.start();
            }

            final String msgIds = StringUtils.trim(commandLine.getOptionValue('i'));
            final String[] msgIdArr = StringUtils.split(msgIds, ",");

            if (commandLine.hasOption('g') && commandLine.hasOption('d')) {
                final String consumerGroup = StringUtils.trim(commandLine.getOptionValue('g'));
                final String clientId = StringUtils.trim(commandLine.getOptionValue('d'));
                for (String msgId : msgIdArr) {
                    if (StringUtils.isNotBlank(msgId)) {
                        pushMsg(defaultMQAdminExt, consumerGroup, clientId, StringUtils.trim(msgId));
                    }
                }
            } else if (commandLine.hasOption('s')) {
                boolean resend = Boolean.parseBoolean(StringUtils.trim(commandLine.getOptionValue('s', "false")));
                if (resend) {
                    for (String msgId : msgIdArr) {
                        if (StringUtils.isNotBlank(msgId)) {
                            sendMsg(defaultMQAdminExt, defaultMQProducer, StringUtils.trim(msgId));
                        }
                    }
                }
            } else {
                for (String msgId : msgIdArr) {
                    if (StringUtils.isNotBlank(msgId)) {
                        queryById(defaultMQAdminExt, StringUtils.trim(msgId));
                    }
                }

            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQProducer.shutdown();
            defaultMQAdminExt.shutdown();
        }
    }

    private void pushMsg(final DefaultMQAdminExt defaultMQAdminExt, final String consumerGroup, final String clientId,
        final String msgId) {
        try {
            ConsumeMessageDirectlyResult result =
                defaultMQAdminExt.consumeMessageDirectly(consumerGroup, clientId, msgId);
            logger.info("%s", result);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void sendMsg(final DefaultMQAdminExt defaultMQAdminExt, final DefaultMQProducer defaultMQProducer,
        final String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            MessageExt msg = defaultMQAdminExt.viewMessage(msgId);
            if (msg != null) {
                // resend msg by id
                logger.info("prepare resend msg. originalMsgId=%s", msgId);
                SendResult result = defaultMQProducer.send(msg);
                logger.info("%s", result);
            } else {
                logger.info("no message. msgId=%s", msgId);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
