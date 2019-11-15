/*
 *  Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.siddhi.extension.map.protobuf.sinkmapper;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.transport.InMemoryBroker;
import io.siddhi.extension.map.protobuf.grpc.Request;
import io.siddhi.extension.map.protobuf.grpc.RequestWithList;
import io.siddhi.extension.map.protobuf.grpc.RequestWithMap;
import io.siddhi.extension.map.protobuf.grpc.ResponseWithList;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TestCases for protobuf-sink-mapper.
 */
public class TestCaseOfProtobufSinkMapper {
    private static final Logger log = Logger.getLogger(TestCaseOfProtobufSinkMapper.class);
    private AtomicInteger count = new AtomicInteger();
    private String packageName = "io.siddhi.extension.map.protobuf.grpc";

    @BeforeMethod
    public void init() {
        count.set(0);
    }

    @Test
    public void protobuSinkMapperDefaultTestCase() throws InterruptedException {

        InMemoryBroker.Subscriber subscriber = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object o) {
                Request request = Request.newBuilder()
                        .setBooleanValue(true)
                        .setStringValue("Test 01")
                        .setDoubleValue(34.5668)
                        .setFloatValue(522.7586f)
                        .setIntValue(60)
                        .setLongValue(10000L)
                        .build();
                AssertJUnit.assertEquals(request.toByteArray(), (byte[]) o);
            }

            @Override
            public String getTopic() {
                return "test01";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriber);
        String streams = "" +
                "@App:name('TestSiddhiApp1')" +
                "define stream FooStream (intValue int, stringValue string,longValue long,booleanValue bool," +
                "floatValue float,doubleValue double); " +
                "@sink(type='inMemory', topic='test01'," +
                "@map(type='protobuf', class='" + packageName + ".Request')) " +
                "define stream BarStream (intValue int, stringValue string,longValue long,booleanValue bool," +
                "floatValue float,doubleValue double); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(toMap(events));
            }
        });
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        Object[] data1 = {60, "Test 01", 10000L, true, 522.7586f, 34.5668};
        stockStream.send(data1);
        siddhiAppRuntime.shutdown();
        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriber);
    }

    @Test
    public void protobuSinkMapperTestCaseWithKeyValue() throws InterruptedException {
        InMemoryBroker.Subscriber subscriber = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object o) {
                Request request = Request.newBuilder()
                        .setBooleanValue(true)
                        .setStringValue("Test 01")
                        .setDoubleValue(34.5668 * 2)
                        .setFloatValue(522.7586f * 2)
                        .setIntValue(60 * 2)
                        .setLongValue(10000L * 2)
                        .build();
                AssertJUnit.assertEquals(request.toByteArray(), (byte[]) o);
            }

            @Override
            public String getTopic() {
                return "test01";
            }
        };
        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriber);
        String streams = "" +
                "@App:name('TestSiddhiApp1')" +
                "define stream FooStream (a string, b long,c int,d bool,e float,f double); " +
                "@sink(type='inMemory', topic='test01' ," +
                "@map(type='protobuf' , class='" + packageName + ".Request', " +
                "@payload(stringValue='a',intValue='c',longValue='b', booleanValue='d',floatValue = 'e', doubleValue" +
                " = 'f'))) " +
                "define stream BarStream (a string, b long, c int,d bool,e float,f double); ";
        String query = "" +
                "from FooStream " +
                "select a,b*2 as b,c*2 as c, d ,e*2 as e,f*2 as f " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(toMap(events));
            }
        });
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        Object[] data1 = {"Test 01", 10000L, 60, true, 522.7586f, 34.5668};
        fooStream.send(data1);
        siddhiAppRuntime.shutdown();
        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriber);
    }

    @Test
    public void protobuSinkMapperTestCaseWithMapObject() throws InterruptedException {
        Map<String, String> map1 = new HashMap<>();
        map1.put("0001", "Barry Allen");
        InMemoryBroker.Subscriber subscriber = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object o) {
                RequestWithMap requestWithMap = RequestWithMap.newBuilder()
                        .setIntValue(60000)
                        .setStringValue("Test 01")
                        .putAllMap(map1)
                        .build();
                AssertJUnit.assertEquals(requestWithMap.toByteArray(), (byte[]) o);

            }

            @Override
            public String getTopic() {
                return "test01";
            }
        };
        InMemoryBroker.subscribe(subscriber);
        String streams = "" +
                "@App:name('TestSiddhiApp1')" +
                "define stream FooStream (stringValue string,intValue int,map object); " +
                "@sink(type='inMemory', topic='test01'," +
                "@map(type='protobuf', class='" + packageName + ".RequestWithMap')) " +
                "define stream BarStream (stringValue string,intValue int,map object); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(toMap(events));
            }
        });
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        Object[] data1 = {"Test 01", 60000, map1};
        stockStream.send(data1);
        siddhiAppRuntime.shutdown();
        InMemoryBroker.unsubscribe(subscriber);
    }

    @Test
    public void protobuSinkMapperTestCaseKeyValueAndMapObject() throws InterruptedException {
        Map<String, String> map1 = new HashMap<>();
        map1.put("0001", "Barry Allen");
        InMemoryBroker.Subscriber subscriber = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object o) {
                RequestWithMap requestWithMap = RequestWithMap.newBuilder()
                        .setIntValue(60000)
                        .setStringValue("Test 01")
                        .putAllMap(map1)
                        .build();
                AssertJUnit.assertEquals(requestWithMap.toByteArray(), (byte[]) o);

            }

            @Override
            public String getTopic() {
                return "test01";
            }
        };
        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriber);
        String streams = "" +
                "@App:name('TestSiddhiApp1')" +
                "define stream FooStream (a string, b int,c object); " +
                "@sink(type='inMemory', topic='test01'," +
                "@map(type='protobuf' , class='" + packageName + ".RequestWithMap', " +
                "@payload(stringValue='a', map='c',intValue='b'))) " +
                "define stream BarStream (a string,b int,c object); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(toMap(events));
            }
        });
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        Object[] data1 = {"Test 01", 60000, map1};
        stockStream.send(data1);
        siddhiAppRuntime.shutdown();
        InMemoryBroker.unsubscribe(subscriber);
    }

    @Test
    public void protobuSinkMapperTestCaseWithLists() throws InterruptedException {
        log.info("ProtobufSinkMapperTestCase 6");
        List<String> stringList = new ArrayList<>();
        stringList.add("Test1");
        stringList.add("Test2");
        stringList.add("Test3");
        List<Integer> integerList = new ArrayList<>();
        integerList.add(100);
        integerList.add(200);
        integerList.add(300);
        InMemoryBroker.Subscriber subscriber = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object o) {
                RequestWithList requestWithList = RequestWithList.newBuilder()
                        .addAllStringList(stringList)
                        .addAllIntList(integerList)
                        .setIntValue(60)
                        .setStringValue("Test 01")
                        .build();
                AssertJUnit.assertEquals(requestWithList.toByteArray(), (byte[]) o);
            }

            @Override
            public String getTopic() {
                return "test01";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriber);
        String streams = "" +
                "@App:name('TestSiddhiApp1')" +
                "define stream FooStream (intValue int, stringValue string, stringList object, intList object); " +
                "@sink(type='inMemory', topic='test01'," +
                "@map(type='protobuf', class='" + packageName + ".RequestWithList')) " +
                "define stream BarStream (intValue int, stringValue string, stringList object, intList object); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(toMap(events));
            }
        });
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        Object[] data1 = {60, "Test 01", stringList, integerList};
        stockStream.send(data1);
        stockStream.send(data1);
        siddhiAppRuntime.shutdown();
        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriber);
    }
    @Test
    public void protobuSinkMapperTestCaseWithListsAndKeyValue() throws InterruptedException {
        log.info("ProtobufSinkMapperTestCase 7");
        List<String> stringList = new ArrayList<>();
        stringList.add("Test1");
        stringList.add("Test2");
        stringList.add("Test3");
        List<Integer> integerList = new ArrayList<>();
        integerList.add(100);
        integerList.add(200);
        integerList.add(300);
        InMemoryBroker.Subscriber subscriber = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object o) {
                ResponseWithList requestWithList = ResponseWithList.newBuilder()
                        .addAllStringList(stringList)
                        .addAllIntList(integerList)
                        .build();
                AssertJUnit.assertEquals(requestWithList.toByteArray(), (byte[]) o);
            }

            @Override
            public String getTopic() {
                return "test01";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriber);
        String streams = "" +
                "@App:name('TestSiddhiApp1')" +
                "define stream FooStream (a object,b object); " +
                "@sink(type='inMemory', topic='test01'," +
                "@map(type='protobuf', class='" + packageName + ".ResponseWithList', " +
                "@payload(stringList='a', intList='b'))) " +
                "define stream BarStream (a object,b object); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(toMap(events));
            }
        });
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        Object[] data1 = {stringList, integerList};
        stockStream.send(data1);
        siddhiAppRuntime.shutdown();
        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriber);
    }


}
