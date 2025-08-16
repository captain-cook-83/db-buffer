package com.karma.commons.utils;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class ListUtils {

    public static <T> T random(List<T> list) {
        return list.get(ThreadLocalRandom.current().nextInt(list.size()));
    }

    public static <T> void batchAdd(List<T> pool, T item, int num) {
        for (int i = 0; i < num; i++) {
            pool.add(item);
        }
    }

    public static String join(List list, String sep) {
        return join(list, sep, null);
    }

    public static String join(List list, String sep, StringBuilder builder) {
        if (list == null) {
            return null;
        }

        if (builder == null) {
            builder = new StringBuilder();
        }
        for (Object element : list) {
            if (builder.length() > 0) {
                builder.append(sep);
            }
            builder.append(element);
        }
        return builder.toString();
    }

    public static <T> Set<T> list2Set(List<T> list) {
        Set<T> result = new HashSet<T>();
        result.addAll(list);
        return result;
    }

    public static List<String> convertStringList(List<?> longList) {

        return longList.stream().map(v -> String.valueOf(v)).collect(Collectors.toList());
    }

    public static <T> String[] convertList2Array(List<T> list) {

        String[] arr = new String[list.size()];
        for (int i = 0; i < list.size(); i++) {
            arr[i] = String.valueOf(list.get(i));
        }
        return arr;
    }

    public static <T> List<T> convertFromIterator(Iterator<T> iterator) {
        List<T> resultList = new ArrayList<>();
        while (iterator.hasNext()) {
            resultList.add(iterator.next());
        }
        return resultList;
    }

    public static <T, S> Set<S> convert2PropertySet(List<T> list, PropertyGetter<T, S> propertyGetter) {
        Set<S> propertySet = new HashSet<>();
        for (T entity : list) {
            S propertyValue = propertyGetter.getProperty(entity);
            if (propertyValue != null) {
                propertySet.add(propertyValue);
            }
        }
        return propertySet;
    }

    public static <T, S> Map<S, List<T>> convert2GroupMap(List<T> list, PropertyGetter<T, S> propertyGetter) {
        Map<S, List<T>> groupMap = new HashMap<>();
        for (T entity : list) {
            S propertyValue = propertyGetter.getProperty(entity);
            if (propertyValue != null) {
                List<T> collection = groupMap.computeIfAbsent(propertyValue, k -> new ArrayList<>());
                collection.add(entity);
            }
        }
        return groupMap;
    }

    public static <T, S> Map<S, T> convert2PropertyMap(List<T> list, PropertyGetter<T, S> propertyGetter) {
        Map<S, T> map = new HashMap<>(list.size());
        for (T item : list) {
            S propertyValue = propertyGetter.getProperty(item);
            if (propertyValue != null) {
                map.put(propertyValue, item);
            }
        }
        return map;
    }

    public static <T, S, V> Map<S, V> convert2PropertyMap(List<T> list, PropertyGetter<T, S> propertyGetter, PropertyGetter<T, V> valueGetter) {
        Map<S, V> map = new HashMap<>(list.size());
        for (T item : list) {
            S propertyValue = propertyGetter.getProperty(item);
            if (propertyValue != null) {
                map.put(propertyValue, valueGetter.getProperty(item));
            }
        }
        return map;
    }

    public static <T> void deleteIntersection(List<T> listA, List<T> listB, PropertyGetter<T, String> propertyGetter) {
        Map<String, T> map = new HashMap<>();
        for (T item : listA) {
            map.put(propertyGetter.getProperty(item), item);
        }
        for (int i = listB.size() - 1; i >= 0; i--) {
            T item = listB.get(i);
            if (map.remove(propertyGetter.getProperty(item)) != null) {
                listB.remove(i);
            }
        }
        for (int i = listA.size() - 1; i >= 0; i--) {
            T item = listA.get(i);
            if (map.get(propertyGetter.getProperty(item)) == null) {
                listA.remove(i);
            }
        }
    }

    public interface PropertyGetter<T, S> {

        S getProperty(T entity);
    }
}
