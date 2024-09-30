package com.jd.platform.hotkey.client.core.key;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.StrUtil;
import com.jd.platform.hotkey.common.model.HotKeyModel;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 轮流提供读写、暂存key的操作。
 * 上报时譬如采用定时器，每隔0.5秒调度一次push方法。在上报过程中，
 * 不应阻塞写入操作。所以计划采用2个HashMap加一个atomicLong，如奇数时写入map0，为1写入map1，上传后会清空该map。
 *
 * @author wuweifeng wrote on 2020-01-06
 * @version 1.0
 */
public class TurnKeyCollector implements IKeyCollector<HotKeyModel, HotKeyModel> {
    private ConcurrentHashMap<String, HotKeyModel> map0 = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, HotKeyModel> map1 = new ConcurrentHashMap<>();

    private AtomicLong atomicLong = new AtomicLong(0);

    @Override
    public List<HotKeyModel> lockAndGetResult() {
        //自增后，对应的map就会停止被写入，等待被读取
        atomicLong.addAndGet(1);

        List<HotKeyModel> list;
        if (atomicLong.get() % 2 == 0) {
            list = get(map1);
            map1.clear();
        } else {
            list = get(map0);
            map0.clear();
        }
        return list;
    }

    /**
     * 获取指定Map中的所有值，并返回作为集合。
     *
     * @param map 要获取值的Map对象
     * @return Map中所有值的集合
     */
    private List<HotKeyModel> get(ConcurrentHashMap<String, HotKeyModel> map) {
        return CollectionUtil.list(false, map.values());
    }

    /**
     * 将HotKeyModel对象添加到缓存中。
     *
     * @param hotKeyModel 待添加的HotKeyModel对象
     */
    @Override
    public void collect(HotKeyModel hotKeyModel) {
        String key = hotKeyModel.getKey();
        if (StrUtil.isEmpty(key)) {
            return;
        }
        // 判断当前缓存的数量是否为偶数
        if (atomicLong.get() % 2 == 0) {
            //不存在时返回null并将key-value放入，已有相同key时，返回该key对应的value，并且不覆盖
            // 当前缓存数量为偶数时，将数据存储在map0中
            // 如果map0中不存在该key，则将该key-value加入map0并返回null；
            // 如果map0中已存在相同的key，则返回该key对应的value，并累加count值
            HotKeyModel model = map0.putIfAbsent(key, hotKeyModel);
            if (model != null) {
                model.add(hotKeyModel.getCount());
            }
        } else {
            // 当前缓存数量为奇数时，将数据存储在map1中
            HotKeyModel model = map1.putIfAbsent(key, hotKeyModel);
            if (model != null) {
                model.add(hotKeyModel.getCount());
            }
        }

    }

    @Override
    public void finishOnce() {

    }

}
