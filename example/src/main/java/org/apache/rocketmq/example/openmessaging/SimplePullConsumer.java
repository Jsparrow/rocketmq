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
package org.apache.rocketmq.example.openmessaging;

import io.openmessaging.Message;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.OMS;
import io.openmessaging.OMSBuiltinKeys;
import io.openmessaging.consumer.PullConsumer;
import io.openmessaging.producer.Producer;
import io.openmessaging.producer.SendResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;

public class SimplePullConsumer {
    private static final Logger logger = LoggerFactory.getLogger(SimplePullConsumer.class);

	public static void main(String[] args) {
        final MessagingAccessPoint messagingAccessPoint =
            OMS.getMessagingAccessPoint("oms:rocketmq://localhost:9876/default:default");

        messagingAccessPoint.startup();

        final Producer producer = messagingAccessPoint.createProducer();

        final PullConsumer consumer = messagingAccessPoint.createPullConsumer(
            OMS.newKeyValue().put(OMSBuiltinKeys.CONSUMER_ID, "OMS_CONSUMER"));

        messagingAccessPoint.startup();
        logger.info("MessagingAccessPoint startup OK%n");

        final String queueName = "TopicTest";

        producer.startup();
        Message msg = producer.createBytesMessage(queueName, "Hello Open Messaging".getBytes());
        SendResult sendResult = producer.send(msg);
        logger.info("Send Message OK. MsgId: %s%n", sendResult.messageId());
        producer.shutdown();

        consumer.attachQueue(queueName);

        consumer.startup();
        logger.info("Consumer startup OK%n");

        // Keep running until we find the one that has just been sent
        boolean stop = false;
        while (!stop) {
            Message message = consumer.receive();
            if (message != null) {
                String msgId = message.sysHeaders().getString(Message.BuiltinKeys.MESSAGE_ID);
                logger.info("Received one message: %s%n", msgId);
                consumer.ack(msgId);

                if (!stop) {
                    stop = StringUtils.equalsIgnoreCase(msgId, sendResult.messageId());
                }

            } else {
                logger.info("Return without any message%n");
            }
        }

        consumer.shutdown();
        messagingAccessPoint.shutdown();
    }
}
