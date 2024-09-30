package com.jd.platform.hotkey.worker.netty.filter;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.date.SystemClock;
import com.jd.platform.hotkey.common.model.HotKeyModel;
import com.jd.platform.hotkey.common.model.HotKeyMsg;
import com.jd.platform.hotkey.common.model.typeenum.MessageType;
import com.jd.platform.hotkey.common.tool.NettyIpUtil;
import com.jd.platform.hotkey.worker.keydispatcher.KeyProducer;
import com.jd.platform.hotkey.worker.netty.holder.WhiteListHolder;
import com.jd.platform.hotkey.worker.starters.EtcdStarter;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 热key消息，包括从netty来的和mq来的。收到消息，都发到队列去
 *
 * @author wuweifeng wrote on 2019-12-11
 * @version 1.0
 */
@Component
@Order(3)
public class HotKeyFilter implements INettyMsgFilter {
    @Resource
    private KeyProducer keyProducer;

    public static AtomicLong totalReceiveKeyCount = new AtomicLong();

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public boolean chain(HotKeyMsg message, ChannelHandlerContext ctx) {
        // 如果是请求新 key，则进行响应处理
        if (MessageType.REQUEST_NEW_KEY == message.getMessageType()) {
            // 收到新的 key 后自增计数器
            totalReceiveKeyCount.incrementAndGet();
            // 发布消息给下游处理
            publishMsg(message, ctx);

            return false;
        }

        return true;
    }

    private void publishMsg(HotKeyMsg message, ChannelHandlerContext ctx) {
        //老版的用的单个HotKeyModel，新版用的数组
        List<HotKeyModel> models = message.getHotKeyModels();
        long now = SystemClock.now();
        // 如果模型列表为空则返回
        if (CollectionUtil.isEmpty(models)) {
            return;
        }
        // 遍历 hot key 列表，将每个 key 发送给队列处理，并根据白名单确定是否需要进一步处理
        for (HotKeyModel model : models) {
            //白名单key不处理
            if (WhiteListHolder.contains(model.getKey())) {
                continue;
            }
            // 计算当前 key 距离创建时间的超时时长，如果超过 1000ms 则输出日志
            long timeOut = now - model.getCreateTime();
            if (timeOut > 1000) {
                if (EtcdStarter.LOGGER_ON) {
                    logger.info("key timeout " + timeOut + ", from ip : " + NettyIpUtil.clientIp(ctx));
                }
            }
            // 将当前 key 发送到队列处理
            keyProducer.push(model, now);
        }

    }

}