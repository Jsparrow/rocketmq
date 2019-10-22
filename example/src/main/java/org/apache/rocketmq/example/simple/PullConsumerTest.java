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
package org.apache.rocketmq.example.simple;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PullConsumerTest {
    private static final Logger logger = LoggerFactory.getLogger(PullConsumerTest.class);

	public static void main(String[] args) throws MQClientException {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("please_rename_unique_group_name_5");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.start();

        try {
            MessageQueue mq = new MessageQueue();
            mq.setQueueId(0);
            mq.setTopic("TopicTest3");
            mq.setBrokerName("vivedeMacBook-Pro.local");

            long offset = 26;

            long beginTime = System.currentTimeMillis();
            PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, offset, 32);
            logger.info("%s%n", System.currentTimeMillis() - beginTime);
            logger.info("%s%n", pullResult);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        consumer.shutdown();
    }
}
