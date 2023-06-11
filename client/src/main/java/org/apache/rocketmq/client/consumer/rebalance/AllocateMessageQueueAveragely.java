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
package org.apache.rocketmq.client.consumer.rebalance;

import org.apache.rocketmq.common.message.MessageQueue;

import java.util.ArrayList;
import java.util.List;

/**
 * 消息队列分配算法, 用于将消息队列分配给消费者
 * Average Hashing queue algorithm
 */
public class AllocateMessageQueueAveragely extends AbstractAllocateMessageQueueStrategy {

    /**
     * Allocating by consumer id
     *
     * @param consumerGroup current consumer group
     * @param currentCID current consumer id
     * @param mqAll message queue set in current topic
     * @param cidAll consumer set in current consumer group
     * @return The allocate result of given strategy
     */
    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
        List<String> cidAll) {

        List<MessageQueue> result = new ArrayList<>();
        if (!check(consumerGroup, currentCID, mqAll, cidAll)) {
            return result;
        }

        // 当前消费者 在 消费者列表 中的索引
        int index = cidAll.indexOf(currentCID);
        int mod = mqAll.size() % cidAll.size();
        int averageSize = mqAll.size() <= cidAll.size() ? 1 :
                (mod > 0 && index < mod ? mqAll.size() / cidAll.size() + 1 :
                        mqAll.size() / cidAll.size());

        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    private static int getAverageSize(List<MessageQueue> mqAll, List<String> cidAll, int index, int mod) {
        // 消息队列数量小于消费者数量, 则每个消费者分配一个消息队列
        if (mqAll.size() <= cidAll.size()) {
            return 1;
        }
        // 余数大于0, 且当前消费者在余数范围内, 则每个消费者分配的消息队列数量为: 消息队列总数 / 消费者总数 + 1
        if (mod > 0 && index < mod) {
            return mqAll.size() / cidAll.size() + 1;
        } else {
            return mqAll.size() / cidAll.size();
        }
    }

    @Override
    public String getName() {
        return "AVG";
    }
}
