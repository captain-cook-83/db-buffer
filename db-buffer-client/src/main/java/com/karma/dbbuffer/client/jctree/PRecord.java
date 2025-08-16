package com.karma.dbbuffer.client.jctree;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.SOURCE)
public @interface PRecord {

    String name();

    byte id();

    byte version();

    boolean sortingOrder() default false;
}
