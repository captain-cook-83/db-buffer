package com.karma.commons.utils;

import java.util.HashMap;
import java.util.Map;

public class MapUtils {

    public static <K, V> Map<K, V> createSingle(K k, V v) {
        Map<K, V> map = new HashMap<>(1);
        map.put(k, v);
        return map;
    }
}
