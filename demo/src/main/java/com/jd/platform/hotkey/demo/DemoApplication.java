package com.jd.platform.hotkey.demo;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;


@EnableAsync
@EnableScheduling
@SpringBootApplication
public class DemoApplication{

    public static void main(String[] args) {
        try {
            SpringApplication.run(DemoApplication.class, args);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
