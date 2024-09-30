package com.jd.platform.hotkey.client.core.key;

import com.jd.platform.hotkey.client.Context;
import com.jd.platform.hotkey.client.core.worker.WorkerInfoHolder;
import com.jd.platform.hotkey.client.log.JdLogger;
import com.jd.platform.hotkey.common.model.HotKeyModel;
import com.jd.platform.hotkey.common.model.HotKeyMsg;
import com.jd.platform.hotkey.common.model.KeyCountModel;
import com.jd.platform.hotkey.common.model.typeenum.MessageType;
import io.netty.channel.Channel;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 将msg推送到netty的pusher
 * @author wuweifeng wrote on 2020-01-06
 * @version 1.0
 */
public class NettyKeyPusher implements IKeyPusher {

    /**
     * 将指定的热点数据列表发送到相应的消息中心
     *
     * @param appName 应用程序名称
     * @param list    待发送的热点数据列表
     */
    @Override
    public void send(String appName, List<HotKeyModel> list) {
        //积攒了半秒的key集合，按照hash分发到不同的worker
        // 获取当前时间，用于设置创建时间
        long now = System.currentTimeMillis();
        // 创建Hash字典，使用Channel作为key，List<HotKeyModel>作为value
        Map<Channel, List<HotKeyModel>> map = new HashMap<>();
        // 遍历list
        for(HotKeyModel model : list) {
            // 设置创建时间
            model.setCreateTime(now);
            // 根据Key值选择一个合适的Worker并取得其对应的通道
            Channel channel = WorkerInfoHolder.chooseChannel(model.getKey());
            // 如果channel不为null，则将该热点数据添加到该channel对应的List<HotKeyModel>中。
            if (channel == null) {
                continue;
            }

            List<HotKeyModel> newList = map.computeIfAbsent(channel, k -> new ArrayList<>());
            newList.add(model);
        }

        // 遍历Map<Channel,List<HotKeyModel>>，依次将每个channel中的HotKeyMsg发送到对应的消息中心。如果出现异常则打印错误日志。
        for (Channel channel : map.keySet()) {
            try {
                List<HotKeyModel> batch = map.get(channel);
                // 创建消息对象HotKeyMsg，并设置其类型以及发送方应用程序名称。
                HotKeyMsg hotKeyMsg = new HotKeyMsg(MessageType.REQUEST_NEW_KEY, Context.APP_NAME);
                hotKeyMsg.setHotKeyModels(batch);
                // 通过channel发送该HotKeyMsg消息
                channel.writeAndFlush(hotKeyMsg).sync();
            } catch (Exception e) {
                try {
                    InetSocketAddress insocket = (InetSocketAddress) channel.remoteAddress();
                    JdLogger.error(getClass(),"flush error " + insocket.getAddress().getHostAddress());
                } catch (Exception ex) {
                    JdLogger.error(getClass(),"flush error");
                }

            }
        }

    }

    @Override
    public void sendCount(String appName, List<KeyCountModel> list) {
        //积攒了10秒的数量，按照hash分发到不同的worker
        long now = System.currentTimeMillis();
        Map<Channel, List<KeyCountModel>> map = new HashMap<>();
        for(KeyCountModel model : list) {
            model.setCreateTime(now);
            Channel channel = WorkerInfoHolder.chooseChannel(model.getRuleKey());
            if (channel == null) {
                continue;
            }

            List<KeyCountModel> newList = map.computeIfAbsent(channel, k -> new ArrayList<>());
            newList.add(model);
        }

        for (Channel channel : map.keySet()) {
            try {
                List<KeyCountModel> batch = map.get(channel);
                HotKeyMsg hotKeyMsg = new HotKeyMsg(MessageType.REQUEST_HIT_COUNT, Context.APP_NAME);
                hotKeyMsg.setKeyCountModels(batch);
                channel.writeAndFlush(hotKeyMsg).sync();
            } catch (Exception e) {
                try {
                    InetSocketAddress insocket = (InetSocketAddress) channel.remoteAddress();
                    JdLogger.error(getClass(),"flush error " + insocket.getAddress().getHostAddress());
                } catch (Exception ex) {
                    JdLogger.error(getClass(),"flush error");
                }

            }
        }
    }

}
