package com.flink.source;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

import java.awt.*;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;


public class DataSourceJava {
    public static void main(String[] args) {
        String filePath = "data/wc.txt";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

       //readFile 按照指定格式读取文件。 测试 里面穿四个参数
//        env.readFile(new TextInputFormat(new Path(filePath)),
//                filePath,
//                FileProcessingMode.PROCESS_ONCE,
//                1,
//                BasicTypeInfo.STRING_TYPE_INFO).print("readFile");
//        try {
//            env.execute();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }


        //*1. fromCollection(Collection)**：基于集合构建，集合中的所有元素必须是同一类型
        env.fromCollection(Arrays.asList(1,6,8)).print("fromCollection");


        // 2 fromElements(T ...)**： 基于元素构建，所有元素必须是同一类型
        env.fromElements(1,3,5).print("fromElements");


        //3. generateSequence(from, to)**：基于给定的序列区间进行构建。示例如下：
        env.generateSequence(0,20).print("generateSequence");

        //**4. fromCollection(Iterator, Class)**：基于迭代器进行构建。
        // 第一个参数用于定义迭代器，第二个参数用于定义输出元素的类型。使用示例如下

        env.fromCollection(new CustomIterator(),BasicTypeInfo.INT_TYPE_INFO).print("Iterator");


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class CustomIterator implements Iterator<Integer>, Serializable
    {
        Integer i = 0;
        @Override
        public boolean hasNext() {
            return i < 10;
        }

        @Override
        public Integer next() {
            i++;
            return i;
        }
    }
}
