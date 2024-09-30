package com.jd.platform.hotkey.demo.controller;

import com.jd.platform.hotkey.client.ClientStarter;
import com.jd.platform.hotkey.client.callback.JdHotKeyStore;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;

@RestController
@RequestMapping("/index")
public class IndexController {


    @PostConstruct
    public void initHotkey() {
        ClientStarter.Builder builder = new ClientStarter.Builder();
        // 注意，setAppName很重要，它和dashboard中相关规则是关联的。
        ClientStarter starter = builder.setAppName("demo")
                .setEtcdServer("http://127.0.0.1:2379")
                .setCaffeineSize(10)
                .build();
        starter.startPipeline();
    }

    @RequestMapping("/get/{key}")
    public Object get(@PathVariable String key) {
        //key skuId__1
        String cacheKey = "skuId__" + key;
        if (JdHotKeyStore.isHotKey(cacheKey)) {
            System.out.println("hotkey:"+ cacheKey);
            //注意是get，不是getValue。getValue会获取并上报，get是纯粹的本地获取
            Object skuInfo = JdHotKeyStore.get(cacheKey);
            if (skuInfo == null) {
                Object theSkuInfo = "123" + "[" + key + "]" + key;
                JdHotKeyStore.smartSet(cacheKey, theSkuInfo);
                return theSkuInfo;
            } else {
                //使用缓存好的value即可
                return skuInfo;
            }
            //["skuId__1","skuId__2","skuId__3"]
        } else {
            System.out.println("not hot:"+ cacheKey);
            return "123" + "[" + key + "]" + key;
            //从redis当中获取数据
            //mysql当中获取数据
        }
    }

    @RequestMapping("/get/info")
    public Object getGoodsInfo(){
        //127.0.0.1/get/info/1
        //key skuId__1
        // /get/info/1
        //127.0.0.1
        String cacheKey = "as_" + 1;
        //
        if (JdHotKeyStore.isHotKey(cacheKey)) {
            System.out.println("hot:"+ cacheKey);
            return "访问次数太多请稍后再试！";
        } else {
            System.out.println("not hot:"+ cacheKey);
            return "ok";
        }
    }
}
