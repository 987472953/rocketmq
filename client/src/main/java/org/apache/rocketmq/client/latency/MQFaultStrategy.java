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

package org.apache.rocketmq.client.latency;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class MQFaultStrategy {
    private final static Logger log = LoggerFactory.getLogger(MQFaultStrategy.class);
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    private boolean sendLatencyFaultEnable = false;

    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        // 默认情况下没开
        if (this.sendLatencyFaultEnable) {
            try {
                // 内部使用的 ThreadLocal 每个线程都是独立的
                int index = tpInfo.getSendWhichQueue().incrementAndGet();
                // 遍历所有的队列
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    // 对队列数取模，这样就能实现轮询
                    int pos = index++ % tpInfo.getMessageQueueList().size();
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    // q: 为什么要判断lastBrokerName
                    // a: 因为lastBrokerName是上一次发送消息的brokerName，如果不判断，那么就会出现一直发送到同一个broker的情况
                    // q: 为什么要判断latencyFaultTolerance.isAvailable(mq.getBrokerName())
                    // a: 因为latencyFaultTolerance.isAvailable(mq.getBrokerName())是判断当前broker是否可用
                    if (!StringUtils.equals(lastBrokerName, mq.getBrokerName()) && latencyFaultTolerance.isAvailable(mq.getBrokerName())) {
                        return mq;
                    }
                }

                // 注意: 第一次发送消息的时候，lastBrokerName是null，所以如果有可用Broker不会走到这里
                // 所有的broker都不可用，那么就从latencyFaultTolerance中获取一个broker(从快要结束管制的前一半随机拿一个)
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                // 这个 brokerName 写队列数
                int writeQueueNums = tpInfo.getWriteQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().incrementAndGet() % writeQueueNums);
                    }
                    return mq;
                } else {
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }

            return tpInfo.selectOneMessageQueue();
        }

        // 这儿如果不开启 也会规避不正常的broker
        //  每次index都加1 => 不会拿到旧的broker
        //  拿到的broker不能是上一次的broker
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        if (this.sendLatencyFaultEnable) {
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    /**
     * 从后往前, 找到对应需要置为不可用的时间
     */
    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
