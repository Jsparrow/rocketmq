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

package org.apache.rocketmq.logging.inner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SysLogger {

    private static final Logger logger = LoggerFactory.getLogger(SysLogger.class);

	protected static boolean debugEnabled = false;

    private static boolean quietMode = false;

    private static final String PREFIX = "RocketMQLog: ";
    private static final String ERR_PREFIX = "RocketMQLog:ERROR ";
    private static final String WARN_PREFIX = "RocketMQLog:WARN ";

    public static void setInternalDebugging(boolean enabled) {
        debugEnabled = enabled;
    }

    public static void debug(String msg) {
        if (debugEnabled && !quietMode) {
            logger.info("%s", PREFIX + msg);
        }
    }

    public static void debug(String msg, Throwable t) {
        if (!(debugEnabled && !quietMode)) {
			return;
		}
		logger.info("%s", PREFIX + msg);
		if (t != null) {
		    t.printStackTrace(System.out);
		}
    }

    public static void error(String msg) {
        if (quietMode) {
            return;
        }
        logger.error(ERR_PREFIX + msg);
    }

    public static void error(String msg, Throwable t) {
        if (quietMode) {
            return;
        }

        logger.error(ERR_PREFIX + msg);
        if (t != null) {
            t.printStackTrace();
        }
    }

    public static void setQuietMode(boolean quietMode) {
        SysLogger.quietMode = quietMode;
    }

    public static void warn(String msg) {
        if (quietMode) {
            return;
        }

        logger.error(WARN_PREFIX + msg);
    }

    public static void warn(String msg, Throwable t) {
        if (quietMode) {
            return;
        }

        logger.error(WARN_PREFIX + msg);
        if (t != null) {
            t.printStackTrace();
        }
    }
}
