package com.example.smlc_sync_vit;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.text.SimpleDateFormat;
import java.util.Date;

@SpringBootApplication
public class SmlcSyncVitApplication {

    public static void main(String[] args) {
        String timestamp = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date());
        System.setProperty("logging.file.name", "logs/sync-" + timestamp + ".log");
        SpringApplication.run(SmlcSyncVitApplication.class, args);
    }

}
