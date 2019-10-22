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

package org.apache.rocketmq.tools.command.queue;

import com.alibaba.fastjson.JSON;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.protocol.body.ConsumeQueueData;
import org.apache.rocketmq.common.protocol.body.QueryConsumeQueueResponseBody;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;

public class QueryConsumeQueueCommand implements SubCommand {

    private static final Logger logger = LoggerFactory.getLogger(QueryConsumeQueueCommand.class);

	public static void main(String[] args) {
        QueryConsumeQueueCommand cmd = new QueryConsumeQueueCommand();

        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {"-t TopicTest", "-q 0", "-i 6447", "-b 100.81.165.119:10911"};
        final CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs, cmd.buildCommandlineOptions(options),
                new PosixParser());
        cmd.execute(commandLine, options, null);
    }

    @Override
    public String commandName() {
        return "queryCq";
    }

    @Override
    public String commandDesc() {
        return "Query cq command.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("q", "queue", true, "queue num, ie. 1");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("i", "index", true, "start queue index.");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("c", "count", true, "how many.");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("b", "broker", true, "broker addr.");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("g", "consumer", true, "consumer group.");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();

            String topic = StringUtils.trim(commandLine.getOptionValue("t"));
            int queueId = Integer.valueOf(StringUtils.trim(commandLine.getOptionValue("q")));
            long index = Long.valueOf(StringUtils.trim(commandLine.getOptionValue("i")));
            int count = Integer.valueOf(StringUtils.trim(commandLine.getOptionValue("c", "10")));
            String broker = null;
            if (commandLine.hasOption("b")) {
                broker = StringUtils.trim(commandLine.getOptionValue("b"));
            }
            String consumerGroup = null;
            if (commandLine.hasOption("g")) {
                consumerGroup = StringUtils.trim(commandLine.getOptionValue("g"));
            }

            if (broker == null || broker.equals("")) {
                TopicRouteData topicRouteData = defaultMQAdminExt.examineTopicRouteInfo(topic);

                if (topicRouteData == null || topicRouteData.getBrokerDatas() == null
                    || topicRouteData.getBrokerDatas().isEmpty()) {
                    throw new Exception("No topic route data!");
                }

                broker = topicRouteData.getBrokerDatas().get(0).getBrokerAddrs().get(0L);
            }

            QueryConsumeQueueResponseBody queryConsumeQueueResponseBody = defaultMQAdminExt.queryConsumeQueue(
                broker, topic, queueId, index, count, consumerGroup
            );

            if (queryConsumeQueueResponseBody.getSubscriptionData() != null) {
                logger.info("Subscription data: \n%s\n", JSON.toJSONString(queryConsumeQueueResponseBody.getSubscriptionData(), true));
                logger.info("======================================\n");
            }

            if (queryConsumeQueueResponseBody.getFilterData() != null) {
                logger.info("Filter data: \n%s\n", queryConsumeQueueResponseBody.getFilterData());
                logger.info("======================================\n");
            }

            logger.info("Queue data: \nmax: %d, min: %d\n", queryConsumeQueueResponseBody.getMaxQueueIndex(), queryConsumeQueueResponseBody.getMinQueueIndex());
            logger.info("======================================\n");

            if (queryConsumeQueueResponseBody.getQueueData() != null) {

                long i = index;
                for (ConsumeQueueData queueData : queryConsumeQueueResponseBody.getQueueData()) {
                    StringBuilder stringBuilder = new StringBuilder();
                    stringBuilder.append(new StringBuilder().append("idx: ").append(i).append("\n").toString());

                    stringBuilder.append(queueData.toString() + "\n");

                    stringBuilder.append("======================================\n");

                    logger.info(stringBuilder.toString());
                    i++;
                }

            }

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
