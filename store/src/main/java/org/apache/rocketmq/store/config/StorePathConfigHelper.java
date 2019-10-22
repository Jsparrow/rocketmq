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
package org.apache.rocketmq.store.config;

import java.io.File;

public class StorePathConfigHelper {

    public static String getStorePathConsumeQueue(final String rootDir) {
        return new StringBuilder().append(rootDir).append(File.separator).append("consumequeue").toString();
    }

    public static String getStorePathConsumeQueueExt(final String rootDir) {
        return new StringBuilder().append(rootDir).append(File.separator).append("consumequeue_ext").toString();
    }

    public static String getStorePathIndex(final String rootDir) {
        return new StringBuilder().append(rootDir).append(File.separator).append("index").toString();
    }

    public static String getStoreCheckpoint(final String rootDir) {
        return new StringBuilder().append(rootDir).append(File.separator).append("checkpoint").toString();
    }

    public static String getAbortFile(final String rootDir) {
        return new StringBuilder().append(rootDir).append(File.separator).append("abort").toString();
    }

    public static String getLockFile(final String rootDir) {
        return new StringBuilder().append(rootDir).append(File.separator).append("lock").toString();
    }

    public static String getDelayOffsetStorePath(final String rootDir) {
        return new StringBuilder().append(rootDir).append(File.separator).append("config").append(File.separator).append("delayOffset.json")
				.toString();
    }

    public static String getTranStateTableStorePath(final String rootDir) {
        return new StringBuilder().append(rootDir).append(File.separator).append("transaction").append(File.separator).append("statetable")
				.toString();
    }

    public static String getTranRedoLogStorePath(final String rootDir) {
        return new StringBuilder().append(rootDir).append(File.separator).append("transaction").append(File.separator).append("redolog")
				.toString();
    }

}
