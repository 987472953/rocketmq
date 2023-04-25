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
package org.apache.rocketmq.client.impl.producer;

import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

import java.util.ArrayList;
import java.util.List;

/**
 * 主题消息发布的相关信息，包括主题名称、主题的消息队列列表、消息队列的路由信息等
 */
public class TopicPublishInfo {
    /**
     * 是否是顺序消息主题
     */
    private boolean orderTopic = false;
    /**
     * 是否已经有了主题路由信息
     */
    private boolean haveTopicRouterInfo = false;
    /**
     * 主题的消息队列列表
     */
    private List<MessageQueue> messageQueueList = new ArrayList<>();
    /**
     * 根据消息的 Key 值，选择发送到哪个消息队列的算法实现
     */
    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();
    /**
     * 路由信息
     */
    private TopicRouteData topicRouteData;

    public boolean isOrderTopic() {
        return orderTopic;
    }

    public void setOrderTopic(boolean orderTopic) {
        this.orderTopic = orderTopic;
    }

    public boolean ok() {
        return null != this.messageQueueList && !this.messageQueueList.isEmpty();
    }

    public List<MessageQueue> getMessageQueueList() {
        return messageQueueList;
    }

    public void setMessageQueueList(List<MessageQueue> messageQueueList) {
        this.messageQueueList = messageQueueList;
    }

    public ThreadLocalIndex getSendWhichQueue() {
        return sendWhichQueue;
    }

    public void setSendWhichQueue(ThreadLocalIndex sendWhichQueue) {
        this.sendWhichQueue = sendWhichQueue;
    }

    public boolean isHaveTopicRouterInfo() {
        return haveTopicRouterInfo;
    }

    public void setHaveTopicRouterInfo(boolean haveTopicRouterInfo) {
        this.haveTopicRouterInfo = haveTopicRouterInfo;
    }

    public MessageQueue selectOneMessageQueue(final String lastBrokerName) {
        if (lastBrokerName == null) {
            return selectOneMessageQueue();
        } else {
            for (int i = 0; i < this.messageQueueList.size(); i++) {
                int index = this.sendWhichQueue.incrementAndGet();
                int pos = index % this.messageQueueList.size();
                MessageQueue mq = this.messageQueueList.get(pos);
                if (!mq.getBrokerName().equals(lastBrokerName)) {
                    return mq;
                }
            }
            return selectOneMessageQueue();
        }
    }

    public MessageQueue selectOneMessageQueue() {
        int index = this.sendWhichQueue.incrementAndGet();
        int pos = index % this.messageQueueList.size();

        return this.messageQueueList.get(pos);
    }

    public int getWriteQueueIdByBroker(final String brokerName) {
        for (int i = 0; i < topicRouteData.getQueueDatas().size(); i++) {
            final QueueData queueData = this.topicRouteData.getQueueDatas().get(i);
            if (queueData.getBrokerName().equals(brokerName)) {
                return queueData.getWriteQueueNums();
            }
        }

        return -1;
    }

    @Override
    public String toString() {
        return "TopicPublishInfo [orderTopic=" + orderTopic + ", messageQueueList=" + messageQueueList
            + ", sendWhichQueue=" + sendWhichQueue + ", haveTopicRouterInfo=" + haveTopicRouterInfo + "]";
    }

    public TopicRouteData getTopicRouteData() {
        return topicRouteData;
    }

    public void setTopicRouteData(final TopicRouteData topicRouteData) {
        this.topicRouteData = topicRouteData;
    }
}
