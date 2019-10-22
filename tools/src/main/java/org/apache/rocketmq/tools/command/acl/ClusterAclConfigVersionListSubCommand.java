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
package org.apache.rocketmq.tools.command.acl;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.protocol.body.ClusterAclVersionInfo;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;

public class ClusterAclConfigVersionListSubCommand implements SubCommand {

    private static final Logger logger = LoggerFactory.getLogger(ClusterAclConfigVersionListSubCommand.class);

	@Override public String commandName() {
        return "clusterAclConfigVersion";
    }

    @Override public String commandDesc() {
        return "List all of acl config version information in cluster";
    }

    @Override public Options buildCommandlineOptions(Options options) {
        OptionGroup optionGroup = new OptionGroup();

        Option opt = new Option("b", "brokerAddr", true, "query acl config version for which broker");
        optionGroup.addOption(opt);

        opt = new Option("c", "clusterName", true, "query acl config version for specified cluster");
        optionGroup.addOption(opt);

        optionGroup.setRequired(true);
        options.addOptionGroup(optionGroup);
        
        return options;
    }

    @Override public void execute(CommandLine commandLine, Options options,
        RPCHook rpcHook) throws SubCommandException {

        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {

            if (commandLine.hasOption('b')) {
                String addr = StringUtils.trim(commandLine.getOptionValue('b'));
                defaultMQAdminExt.start();
                printClusterBaseInfo(defaultMQAdminExt, addr);

                logger.info("get broker's plain access config version success.%n", addr);
                return;

            } else if (commandLine.hasOption('c')) {
                String clusterName = StringUtils.trim(commandLine.getOptionValue('c'));

                defaultMQAdminExt.start();

                Set<String> masterSet =
                    CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                logger.info("%-16s  %-22s  %-22s  %-20s  %-22s%n", "#Cluster Name", "#Broker Name", "#Broker Addr", "#AclConfigVersionNum", "#AclLastUpdateTime"
                );
                for (String addr : masterSet) {
                    printClusterBaseInfo(defaultMQAdminExt, addr);
                }
                logger.info("get cluster's plain access config version success.%n");

                return;
            }

            ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    private void printClusterBaseInfo(
        final DefaultMQAdminExt defaultMQAdminExt, final String addr) throws
        InterruptedException, MQBrokerException, RemotingException, MQClientException {


        ClusterAclVersionInfo clusterAclVersionInfo = defaultMQAdminExt.examineBrokerClusterAclVersionInfo(addr);
        DataVersion aclDataVersion = clusterAclVersionInfo.getAclConfigDataVersion();
        String versionNum = String.valueOf(aclDataVersion.getCounter());

        DateFormat sdf = new SimpleDateFormat(UtilAll.YYYY_MM_DD_HH_MM_SS);
        String timeStampStr = sdf.format(new Timestamp(aclDataVersion.getTimestamp()));

        logger.info("%-16s  %-22s  %-22s  %-20s  %-22s%n", clusterAclVersionInfo.getClusterName(), clusterAclVersionInfo.getBrokerName(), clusterAclVersionInfo.getBrokerAddr(), versionNum, timeStampStr
        );
    }
}
