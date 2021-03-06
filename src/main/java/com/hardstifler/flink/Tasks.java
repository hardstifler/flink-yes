/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hardstifler.flink;

import com.hardstifler.flink.tasks.fraud.FrundDetectorTask;
import com.hardstifler.flink.tasks.keyfunc.KeyTask;
import com.hardstifler.flink.tasks.watermarks.WatermarksTask;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * Skeleton code for the datastream walkthrough
 */
public class Tasks {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /***
        new FrundDetectorTask(env);

        new KeyTask(env);
        */
        new WatermarksTask(env);

        env.execute("Fraud Detection");

    }
}
