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
package org.apache.rocketmq.tools.command.topic;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import org.apache.commons.lang3.StringUtils;

public class UpdateTopicSubCommandTest {
    @Test
    public void testExecute() {
        UpdateTopicSubCommand cmd = new UpdateTopicSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {
            "-b 127.0.0.1:10911",
            "-t unit-test",
            "-r 8",
            "-w 8",
            "-p 6",
            "-o false",
            "-u false",
            "-s false"};
        final CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs, cmd.buildCommandlineOptions(options), new PosixParser());
        assertThat(StringUtils.trim(commandLine.getOptionValue('b'))).isEqualTo("127.0.0.1:10911");
        assertThat(StringUtils.trim(commandLine.getOptionValue('r'))).isEqualTo("8");
        assertThat(StringUtils.trim(commandLine.getOptionValue('w'))).isEqualTo("8");
        assertThat(StringUtils.trim(commandLine.getOptionValue('t'))).isEqualTo("unit-test");
        assertThat(StringUtils.trim(commandLine.getOptionValue('p'))).isEqualTo("6");
        assertThat(StringUtils.trim(commandLine.getOptionValue('o'))).isEqualTo("false");
        assertThat(StringUtils.trim(commandLine.getOptionValue('u'))).isEqualTo("false");
        assertThat(StringUtils.trim(commandLine.getOptionValue('s'))).isEqualTo("false");
    }
}