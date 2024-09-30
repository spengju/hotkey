package com.jd.platform.hotkey.worker.keydispatcher;

import com.jd.platform.hotkey.common.model.HotKeyModel;
import com.jd.platform.hotkey.worker.keylistener.IKeyListener;
import com.jd.platform.hotkey.worker.keylistener.KeyEventOriginal;


import static com.jd.platform.hotkey.worker.keydispatcher.DispatcherConfig.QUEUE;
import static com.jd.platform.hotkey.worker.tool.InitConstant.totalDealCount;

/**
 * @author wuweifeng
 * @version 1.0
 * @date 2020-06-09
 */
public class KeyConsumer {

    // 消费者对象
    private IKeyListener iKeyListener;

    // 注入实现IKeyListener接口的对象
    public void setKeyListener(IKeyListener iKeyListener) {
        this.iKeyListener = iKeyListener;
    }

    // 开始消费，处理从队列中获取HotKeyModel对象
    public void beginConsume() {
        while (true) {
            try {
                // 从队列中获取HotKeyModel对象，如无则等待
                HotKeyModel model = QUEUE.take();
                // 如果该HotKeyModel对象的isRemove字段为true，则调用IKeyListener对象的removeKey方法进行处理；
                // 否则调用newKey方法进行处理
                if (model.isRemove()) {
                    iKeyListener.removeKey(model, KeyEventOriginal.CLIENT);
                } else {
                    iKeyListener.newKey(model, KeyEventOriginal.CLIENT);
                }

                //处理完毕，将数量加1
                totalDealCount.increment();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}
