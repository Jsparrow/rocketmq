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

package org.apache.rocketmq.tools.command.cluster;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;

public class CLusterSendMsgRTCommand implements SubCommand {

    private static final Logger logger = LoggerFactory.getLogger(CLusterSendMsgRTCommand.class);

	public static void main(String args[]) {
    }

    @Override
    public String commandName() {
        return "clusterRT";
    }

    @Override
    public String commandDesc() {
        return "List All clusters Message Send RT";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("a", "amout", true, "message amout | default 100");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("s", "size", true, "message size | default 128 Byte");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("c", "cluster", true, "cluster name | default display all cluster");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "print log", true, "print as tlog | default false");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "machine room", true, "machine room name | default noname");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("i", "interval", true, "print interval | default 10 seconds");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        DefaultMQProducer producer = new DefaultMQProducer(rpcHook);
        producer.setProducerGroup(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();
            producer.start();

            ClusterInfo clusterInfoSerializeWrapper = defaultMQAdminExt.examineBrokerClusterInfo();
            HashMap<String, Set<String>> clusterAddr = clusterInfoSerializeWrapper
                .getClusterAddrTable();

            Set<String> clusterNames = null;

            long amount = !commandLine.hasOption('a') ? 50 : Long.parseLong(StringUtils.trim(commandLine.getOptionValue('a')));

            long size = !commandLine.hasOption('s') ? 128 : Long.parseLong(StringUtils.trim(commandLine.getOptionValue('s')));

            long interval = !commandLine.hasOption('i') ? 10 : Long.parseLong(StringUtils.trim(commandLine.getOptionValue('i')));

            boolean printAsTlog = commandLine.hasOption('p') && Boolean.parseBoolean(StringUtils.trim(commandLine.getOptionValue('p')));

            String machineRoom = !commandLine.hasOption('m') ? "noname" : StringUtils.trim(commandLine.getOptionValue('m'));

            if (commandLine.hasOption('c')) {
                clusterNames = new TreeSet<>();
                clusterNames.add(StringUtils.trim(commandLine.getOptionValue('c')));
            } else {
                clusterNames = clusterAddr.keySet();
            }

            if (!printAsTlog) {
                logger.info("%-24s  %-24s  %-4s  %-8s  %-8s%n", "#Cluster Name", "#Broker Name", "#RT", "#successCount", "#failCount"
                );
            }

            while (true) {
                for (String clusterName : clusterNames) {
                    Set<String> brokerNames = clusterAddr.get(clusterName);
                    if (brokerNames == null) {
                        logger.info("cluster [%s] not exist", clusterName);
                        break;
                    }

                    for (String brokerName : brokerNames) {
                        Message msg = new Message(brokerName, getStringBySize(size).getBytes(MixAll.DEFAULT_CHARSET));
                        long start = 0;
                        long end = 0;
                        long elapsed = 0;
                        int successCount = 0;
                        int failCount = 0;

                        for (int i = 0; i < amount; i++) {
                            start = System.currentTimeMillis();
                            try {
                                producer.send(msg);
                                successCount++;
                                end = System.currentTimeMillis();
                            } catch (Exception e) {
                                logger.error(e.getMessage(), e);
								failCount++;
                                end = System.currentTimeMillis();
                            }

                            if (i != 0) {
                                elapsed += end - start;
                            }
                        }

                        double rt = (double) elapsed / (amount - 1);
                        if (!printAsTlog) {
                            logger.info("%-24s  %-24s  %-8s  %-16s  %-16s%n", clusterName, brokerName, String.format("%.2f", rt), successCount, failCount
                            );
                        } else {
                            logger.info("%s", String.format("%s|%s|%s|%s|%s%n", getCurTime(), machineRoom, clusterName, brokerName,
									new BigDecimal(rt).setScale(0, BigDecimal.ROUND_HALF_UP)));
                        }

                    }

                }

                Thread.sleep(interval * 1000);
            }

        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
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

    public String getCurTime() {
        String fromTimeZone = "GMT+8";
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date();
        format.setTimeZone(TimeZone.getTimeZone(fromTimeZone));
        String chinaDate = format.format(date);
        return chinaDate;
    }

}
