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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.util.parallel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParallelTaskExecutor {
    private static final Logger logger = LoggerFactory.getLogger(ParallelTaskExecutor.class);
	public List<ParallelTask> tasks = new ArrayList<>();
    public ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
    public CountDownLatch latch = null;

    public ParallelTaskExecutor() {

    }

    public void pushTask(ParallelTask task) {
        tasks.add(task);
    }

    public void startBlock() {
        init();
        startTask();
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void startNoBlock() {
        tasks.forEach(cachedThreadPool::execute);
    }

    private void init() {
        latch = new CountDownLatch(tasks.size());
        tasks.forEach(task -> task.setLatch(latch));
    }

    private void startTask() {
        tasks.forEach(ParallelTask::start);
    }
}
