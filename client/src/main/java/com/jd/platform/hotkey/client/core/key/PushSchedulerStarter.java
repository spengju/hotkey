package com.jd.platform.hotkey.client.core.key;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.thread.NamedThreadFactory;
import com.jd.platform.hotkey.client.Context;
import com.jd.platform.hotkey.common.model.HotKeyModel;
import com.jd.platform.hotkey.common.model.KeyCountModel;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 定时推送一批key到worker
 * @author wuweifeng wrote on 2020-01-06
 * @version 1.0
 */
public class PushSchedulerStarter {

    /**
     * 每0.5秒推送一次待测key
     */
    public static void startPusher(Long period) {
        // 如果周期为null或小于等于0，则设置默认值500L
        if (period == null || period <= 0) {
            period = 500L;
        }
        @SuppressWarnings("PMD.ThreadPoolCreationRule")
        // 创建ScheduledExecutorService对象，该对象用于定时执行任务
        ScheduledExecutorService scheduledExecutorService =
                // 单线程池，用于顺序执行任务并保证线程安全
                Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("hotkey-pusher-service-executor", true));
        // 执行定时任务，返回一个ScheduledFuture<?>对象。
        // 使用ScheduledExecutorService.scheduleAtFixedRate()方法可以按固定的时间间隔周期性地执行指定的任务。
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            // 获取KeyCollector实例，并调用lockAndGetResult()方法获取热点数据的列表
            IKeyCollector<HotKeyModel, HotKeyModel> collectHK = KeyHandlerFactory.getCollector();
            List<HotKeyModel> hotKeyModels = collectHK.lockAndGetResult();
            // 如果当前列表不为空，则调用Pusher发送该列表中的所有热点数据
            if(CollectionUtil.isNotEmpty(hotKeyModels)){
                KeyHandlerFactory.getPusher().send(Context.APP_NAME, hotKeyModels);
                // 调用finishOnce()方法表示当前批次已经完成
                collectHK.finishOnce();
            }

        },0, period, TimeUnit.MILLISECONDS);
    }

    /**
     * 每10秒推送一次数量统计
     */

    public static void startCountPusher(Integer period) {
        if (period == null || period <= 0) {
            period = 10;
        }
        @SuppressWarnings("PMD.ThreadPoolCreationRule")
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("hotkey-count-pusher-service-executor", true));
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            IKeyCollector<KeyHotModel, KeyCountModel> collectHK = KeyHandlerFactory.getCounter();
            List<KeyCountModel> keyCountModels = collectHK.lockAndGetResult();
            if(CollectionUtil.isNotEmpty(keyCountModels)){
                KeyHandlerFactory.getPusher().sendCount(Context.APP_NAME, keyCountModels);
                collectHK.finishOnce();
            }
        },0, period, TimeUnit.SECONDS);
    }

}
