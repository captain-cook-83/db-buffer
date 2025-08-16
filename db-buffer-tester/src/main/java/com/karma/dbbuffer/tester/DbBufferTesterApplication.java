package com.karma.dbbuffer.tester;

import com.karma.dbbuffer.client.EnableBufferedBDWriter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@EnableBufferedBDWriter
@SpringBootApplication
public class DbBufferTesterApplication {

    public static void main(String[] args) {
        SpringApplication.run(DbBufferTesterApplication.class, args);
    }

}
