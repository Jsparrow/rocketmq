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

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.junit.Assert;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import org.apache.commons.lang3.StringUtils;

public class UpdateAccessConfigSubCommandTest {

    @Test
    public void testExecute() {
        UpdateAccessConfigSubCommand cmd = new UpdateAccessConfigSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {
            "-b 127.0.0.1:10911",
            "-a RocketMQ",
            "-s 12345678",
            "-w 192.168.0.*",
            "-i DENY",
            "-u SUB",
            "-t topicA=DENY;topicB=PUB|SUB",
            "-g groupA=DENY;groupB=SUB",
            "-m true"};
        final CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs, cmd.buildCommandlineOptions(options), new PosixParser());
        assertThat(StringUtils.trim(commandLine.getOptionValue('b'))).isEqualTo("127.0.0.1:10911");
        assertThat(StringUtils.trim(commandLine.getOptionValue('a'))).isEqualTo("RocketMQ");
        assertThat(StringUtils.trim(commandLine.getOptionValue('s'))).isEqualTo("12345678");
        assertThat(StringUtils.trim(commandLine.getOptionValue('w'))).isEqualTo("192.168.0.*");
        assertThat(StringUtils.trim(commandLine.getOptionValue('i'))).isEqualTo("DENY");
        assertThat(StringUtils.trim(commandLine.getOptionValue('u'))).isEqualTo("SUB");
        assertThat(StringUtils.trim(commandLine.getOptionValue('t'))).isEqualTo("topicA=DENY;topicB=PUB|SUB");
        assertThat(StringUtils.trim(commandLine.getOptionValue('g'))).isEqualTo("groupA=DENY;groupB=SUB");
        assertThat(StringUtils.trim(commandLine.getOptionValue('m'))).isEqualTo("true");

        PlainAccessConfig accessConfig = new PlainAccessConfig();

        // topicPerms list value
        if (commandLine.hasOption('t')) {
            String[] topicPerms = StringUtils.trim(commandLine.getOptionValue('t')).split(";");
            List<String> topicPermList = new ArrayList<>();
            if (topicPerms != null) {
                for (String topicPerm : topicPerms) {
                    topicPermList.add(topicPerm);
                }
            }
            accessConfig.setTopicPerms(topicPermList);
        }

        // groupPerms list value
        if (commandLine.hasOption('g')) {
            String[] groupPerms = StringUtils.trim(commandLine.getOptionValue('g')).split(";");
            List<String> groupPermList = new ArrayList<>();
            if (groupPerms != null) {
                for (String groupPerm : groupPerms) {
                    groupPermList.add(groupPerm);
                }
            }
            accessConfig.setGroupPerms(groupPermList);
        }

        Assert.assertTrue(accessConfig.getTopicPerms().contains("topicB=PUB|SUB"));
        Assert.assertTrue(accessConfig.getGroupPerms().contains("groupB=SUB"));

    }
}
