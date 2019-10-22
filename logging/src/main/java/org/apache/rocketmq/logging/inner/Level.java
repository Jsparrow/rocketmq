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

import java.io.Serializable;

public class Level implements Serializable {

    public static final int OFF_INT = Integer.MAX_VALUE;
	public static final int ERROR_INT = 40000;
	public static final int WARN_INT = 30000;
	public static final int INFO_INT = 20000;
	public static final int DEBUG_INT = 10000;
	public static final int ALL_INT = Integer.MIN_VALUE;
	private static final String ALL_NAME = "ALL";
	private static final String DEBUG_NAME = "DEBUG";
	private static final String INFO_NAME = "INFO";
	private static final String WARN_NAME = "WARN";
	private static final String ERROR_NAME = "ERROR";
	private static final String OFF_NAME = "OFF";
	public static final Level OFF = new Level(OFF_INT, OFF_NAME, 0);
	public static final Level ERROR = new Level(ERROR_INT, ERROR_NAME, 3);
	public static final Level WARN = new Level(WARN_INT, WARN_NAME, 4);
	public static final Level INFO = new Level(INFO_INT, INFO_NAME, 6);
	public static final Level DEBUG = new Level(DEBUG_INT, DEBUG_NAME, 7);
	public static final Level ALL = new Level(ALL_INT, ALL_NAME, 7);
	static final long serialVersionUID = 3491141966387921974L;
	transient int level;
	transient String levelStr;
	transient int syslogEquivalent;

	protected Level(int level, String levelStr, int syslogEquivalent) {
        this.level = level;
        this.levelStr = levelStr;
        this.syslogEquivalent = syslogEquivalent;
    }

	public static Level toLevel(String sArg) {
        return toLevel(sArg, Level.DEBUG);
    }

	public static Level toLevel(int val) {
        return toLevel(val, Level.DEBUG);
    }

	public static Level toLevel(int val, Level defaultLevel) {
        switch (val) {
            case ALL_INT:
                return ALL;
            case DEBUG_INT:
                return Level.DEBUG;
            case INFO_INT:
                return Level.INFO;
            case WARN_INT:
                return Level.WARN;
            case ERROR_INT:
                return Level.ERROR;
            case OFF_INT:
                return OFF;
            default:
                return defaultLevel;
        }
    }

	public static Level toLevel(String sArg, Level defaultLevel) {
        if (sArg == null) {
            return defaultLevel;
        }
        String s = sArg.toUpperCase();

        if (s.equals(ALL_NAME)) {
            return Level.ALL;
        }
        if (s.equals(DEBUG_NAME)) {
            return Level.DEBUG;
        }
        if (s.equals(INFO_NAME)) {
            return Level.INFO;
        }
        if (s.equals(WARN_NAME)) {
            return Level.WARN;
        }
        if (s.equals(ERROR_NAME)) {
            return Level.ERROR;
        }
        if (s.equals(OFF_NAME)) {
            return Level.OFF;
        }

        if (s.equals(INFO_NAME)) {
            return Level.INFO;
        }
        return defaultLevel;
    }

	@Override
	public boolean equals(Object o) {
        if (o instanceof Level) {
            Level r = (Level) o;
            return this.level == r.level;
        } else {
            return false;
        }
    }

	@Override
    public int hashCode() {
        int result = level;
        result = 31 * result + (levelStr != null ? levelStr.hashCode() : 0);
        result = 31 * result + syslogEquivalent;
        return result;
    }

	public boolean isGreaterOrEqual(Level r) {
        return level >= r.level;
    }

	@Override
	public final String toString() {
        return levelStr;
    }

	public final int toInt() {
        return level;
    }

}
