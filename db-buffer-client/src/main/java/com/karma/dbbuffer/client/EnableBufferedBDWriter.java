package com.karma.dbbuffer.client;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Import(BufferedDBWriterRegistrar.class)
@Documented
public @interface EnableBufferedBDWriter {
}
