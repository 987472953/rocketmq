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

package org.apache.rocketmq.client.common;

import java.util.Random;

public class ThreadLocalIndex {
    private final ThreadLocal<Integer> threadLocalIndex = new ThreadLocal<>();
    private final Random random = new Random();
    private final static int POSITIVE_MASK = 0x7FFFFFFF;

    public int incrementAndGet() {
        // 这儿不用加锁，因为每个线程都有自己的 threadLocalIndex
        Integer index = this.threadLocalIndex.get();
        if (null == index) {
            index = random.nextInt();
        }
        this.threadLocalIndex.set(++index);
        // q: & 0x7FFFFFFF 为什么要这样做？
        // a: 因为 index 可能为负数，如果不做处理，会导致 Math.abs(index) 为负数，从而导致消息发送失败
        // Math.abs(Integer.MIN_VALUE) < 0
        return Math.abs(index & POSITIVE_MASK);
    }

    @Override
    public String toString() {
        return "ThreadLocalIndex{" +
            "threadLocalIndex=" + threadLocalIndex.get() +
            '}';
    }
}
