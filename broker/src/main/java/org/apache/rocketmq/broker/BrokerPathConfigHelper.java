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

package org.apache.rocketmq.broker;

import java.io.File;

public class BrokerPathConfigHelper {
    private static String brokerConfigPath = new StringBuilder().append(System.getProperty("user.home")).append(File.separator).append("store").append(File.separator).append("config")
			.append(File.separator).append("broker.properties").toString();

    public static String getBrokerConfigPath() {
        return brokerConfigPath;
    }

    public static void setBrokerConfigPath(String path) {
        brokerConfigPath = path;
    }

    public static String getTopicConfigPath(final String rootDir) {
        return new StringBuilder().append(rootDir).append(File.separator).append("config").append(File.separator).append("topics.json")
				.toString();
    }

    public static String getConsumerOffsetPath(final String rootDir) {
        return new StringBuilder().append(rootDir).append(File.separator).append("config").append(File.separator).append("consumerOffset.json")
				.toString();
    }

    public static String getSubscriptionGroupPath(final String rootDir) {
        return new StringBuilder().append(rootDir).append(File.separator).append("config").append(File.separator).append("subscriptionGroup.json")
				.toString();
    }

    public static String getConsumerFilterPath(final String rootDir) {
        return new StringBuilder().append(rootDir).append(File.separator).append("config").append(File.separator).append("consumerFilter.json")
				.toString();
    }
}
