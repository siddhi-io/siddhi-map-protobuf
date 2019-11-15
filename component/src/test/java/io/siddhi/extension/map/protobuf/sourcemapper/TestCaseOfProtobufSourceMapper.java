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

package io.siddhi.extension.map.protobuf.sourcemapper;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.core.util.transport.InMemoryBroker;
import io.siddhi.extension.map.protobuf.grpc.OtherMessageTypes;
import io.siddhi.extension.map.protobuf.grpc.Request;
import io.siddhi.extension.map.protobuf.grpc.RequestWithList;
import io.siddhi.extension.map.protobuf.grpc.RequestWithMap;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * TestCases for protobuf-source-mapper.
 */
public class TestCaseOfProtobufSourceMapper {

    private static Logger log = Logger.getLogger(TestCaseOfProtobufSourceMapper.class.getName());
    private final int waitTime = 500;
    private final int timeout = 500;
    private AtomicInteger count = new AtomicInteger();
    private String packageName = "io.siddhi.extension.map.protobuf.grpc";

    @BeforeMethod
    public void init() {

        count.set(0);
    }

    @Test
    public void protobufSourceMapperTest() throws Exception {

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='test01'," +
                " @map(type='protobuf', class='" + packageName + ".Request')) " +
                "define stream FooStream (stringValue string, intValue int,longValue long,booleanValue bool," +
                "floatValue float,doubleValue double); " +
                "define stream BarStream (stringValue string, intValue int,longValue long,booleanValue bool," +
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

                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            AssertJUnit.assertEquals("Test 01", event.getData(0));
                            AssertJUnit.assertEquals(100, event.getData(1));
                            AssertJUnit.assertEquals(1000000L, event.getData(2));
                            AssertJUnit.assertEquals(false, event.getData(3));
                            AssertJUnit.assertEquals(45.345f, event.getData(4));
                            AssertJUnit.assertEquals(168.4567, event.getData(5));
                            break;
                        case 2:
                            AssertJUnit.assertEquals("Test 02", event.getData(0));
                            AssertJUnit.assertEquals(520, event.getData(1));
                            AssertJUnit.assertEquals(3456445L, event.getData(2));
                            AssertJUnit.assertEquals(true, event.getData(3));
                            AssertJUnit.assertEquals(88.235f, event.getData(4));
                            AssertJUnit.assertEquals(523.455, event.getData(5));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        Request response1 = Request.newBuilder()
                .setStringValue("Test 01")
                .setIntValue(100)
                .setBooleanValue(false)
                .setDoubleValue(168.4567)
                .setFloatValue(45.345f)
                .setLongValue(1000000L)
                .build();
        Request response2 = Request.newBuilder()
                .setStringValue("Test 02")
                .setIntValue(520)
                .setBooleanValue(true)
                .setDoubleValue(523.455)
                .setFloatValue(88.235f)
                .setLongValue(3456445L)
                .build();
        InMemoryBroker.publish("test01", response1.toByteArray());
        InMemoryBroker.publish("test01", response2.toByteArray());
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 2, count.get());
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void protobufSourceMapperTestWithKeyValue() throws Exception {

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='test01'," +
                " @map(type='protobuf', class='" + packageName + ".Request'," +
                " @attributes(a = 'stringValue', b = 'intValue', c = 'longValue',d = 'booleanValue', e = " +
                "'floatValue', f ='doubleValue'))) " +
                "define stream FooStream (a string ,c long,b int, d bool,e float,f double); " +

                "define stream BarStream (a string,c long,b int,d bool,e float,f double); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {

                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            AssertJUnit.assertEquals("Test 01", event.getData(0));
                            AssertJUnit.assertEquals(100, event.getData(2));
                            AssertJUnit.assertEquals(1000000L, event.getData(1));
                            AssertJUnit.assertEquals(false, event.getData(3));
                            AssertJUnit.assertEquals(45.345f, event.getData(4));
                            AssertJUnit.assertEquals(168.4567, event.getData(5));
                            break;
                        case 2:
                            AssertJUnit.assertEquals("Test 02", event.getData(0));
                            AssertJUnit.assertEquals(520, event.getData(2));
                            AssertJUnit.assertEquals(3456445L, event.getData(1));
                            AssertJUnit.assertEquals(true, event.getData(3));
                            AssertJUnit.assertEquals(88.235f, event.getData(4));
                            AssertJUnit.assertEquals(523.455, event.getData(5));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        Request response1 = Request.newBuilder()
                .setStringValue("Test 01")
                .setIntValue(100)
                .setBooleanValue(false)
                .setDoubleValue(168.4567)
                .setFloatValue(45.345f)
                .setLongValue(1000000L)
                .build();
        Request response2 = Request.newBuilder()
                .setStringValue("Test 02")
                .setIntValue(520)
                .setBooleanValue(true)
                .setDoubleValue(523.455)
                .setFloatValue(88.235f)
                .setLongValue(3456445L)
                .build();
        InMemoryBroker.publish("test01", response1.toByteArray());
        InMemoryBroker.publish("test01", response2.toByteArray());
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 2, count.get());
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void protobufSourceMapperTestWithMapObject() throws Exception {

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='test01',  " +
                " @map(type='protobuf', class='" + packageName + ".RequestWithMap')) " +
                "define stream FooStream (stringValue string,intValue int,map object); " +

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

                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1: {
                            Map<String, String> output = new HashMap<>();
                            output.put("Key 01", "Value 01");
                            output.put("Key 02", "Value 02");
                            AssertJUnit.assertEquals("Barry Allen", event.getData(0));
                            AssertJUnit.assertEquals(100, event.getData(1));
                            AssertJUnit.assertEquals(output, event.getData(2));
                            break;
                        }
                        default:
                            AssertJUnit.fail();
                    }
                }

            }
        });
        siddhiAppRuntime.start();
        RequestWithMap response1 = RequestWithMap.newBuilder()
                .setIntValue(100)
                .setStringValue("Barry Allen")
                .putMap("Key 01", "Value 01")
                .putMap("Key 02", "Value 02")
                .build();
        InMemoryBroker.publish("test01", response1.toByteArray());
        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 1, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void protobufSourceMapperTestKeyValueAndMapObject() throws Exception {

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='test01',  " +
                " @map(type='protobuf', class='" + packageName + ".RequestWithMap'," +
                " @attributes(a = 'stringValue' ,b = 'intValue', c  ='map'))) " +
                "define stream FooStream (a string,b int,c object); " +

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

                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1: {
                            Map<String, String> output = new HashMap<>();
                            output.put("Key 01", "Value 01");
                            output.put("Key 02", "Value 02");
                            AssertJUnit.assertEquals("Barry Allen", event.getData(0));
                            AssertJUnit.assertEquals(100, event.getData(1));
                            AssertJUnit.assertEquals(output, event.getData(2));
                            break;
                        }
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        RequestWithMap response1 = RequestWithMap.newBuilder()
                .setIntValue(100)
                .setStringValue("Barry Allen")
                .putMap("Key 01", "Value 01")
                .putMap("Key 02", "Value 02")
                .build();
        InMemoryBroker.publish("test01", response1.toByteArray());
        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 1, count.get());
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void protobufSourceMapperTestWithLists() throws Exception {

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='test01'," +
                " @map(type='protobuf', class='" + packageName + ".RequestWithList')) " +
                "define stream FooStream (stringList object,intList object, stringValue string, intValue int);" +
                "define stream BarStream (stringList object,intList object, stringValue string, intValue int);";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        List<String> stringList = new ArrayList<>();
        stringList.add("Test1");
        stringList.add("Test2");
        stringList.add("Test3");
        List<Integer> integerList = new ArrayList<>();
        integerList.add(100);
        integerList.add(200);
        integerList.add(300);

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {

                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            AssertJUnit.assertEquals(stringList, event.getData(0));
                            AssertJUnit.assertEquals(integerList, event.getData(1));
                            AssertJUnit.assertEquals("String value", event.getData(2));
                            AssertJUnit.assertEquals(2000, event.getData(3));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();

        RequestWithList requestWithList = RequestWithList.newBuilder()
                .addAllStringList(stringList)
                .addAllIntList(integerList)
                .setStringValue("String value")
                .setIntValue(2000)
                .build();
        InMemoryBroker.publish("test01", requestWithList.toByteArray());
        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 1, count.get());
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void protobufSourceMapperTestWithListsAndKeyValue() throws Exception {

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='test01',  " +
                " @map(type='protobuf', class='" + packageName + ".RequestWithList'," +
                " @attributes(a = 'stringValue' ,b = 'intValue', c  ='stringList', d = 'intList'))) " +
                "define stream FooStream (a string,b int,c object,d object); " +
                "define stream BarStream (a string,b int,c object, d object); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        List<String> stringList = new ArrayList<>();
        stringList.add("Test1");
        stringList.add("Test2");
        stringList.add("Test3");
        List<Integer> integerList = new ArrayList<>();
        integerList.add(100);
        integerList.add(200);
        integerList.add(300);

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1: {
                            AssertJUnit.assertEquals("Barry Allen", event.getData(0));
                            AssertJUnit.assertEquals(100, event.getData(1));
                            AssertJUnit.assertEquals(stringList, event.getData(2));
                            AssertJUnit.assertEquals(integerList, event.getData(3));
                            break;
                        }
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        RequestWithList requestWithList = RequestWithList.newBuilder()
                .setStringValue("Barry Allen")
                .setIntValue(100)
                .addAllIntList(integerList)
                .addAllStringList(stringList)
                .build();
        InMemoryBroker.publish("test01", requestWithList.toByteArray());
        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 1, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void protobufSourceMapperTestToMapOtherMessagesTypes() throws Exception {

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='test01',  " +
                " @map(type='protobuf', class='" + packageName + ".OtherMessageTypes')) " +
                "define stream FooStream (request object, requestWithMap object, requestWithList object, " +
                "requestList object); " +
                "define stream BarStream (request object, requestWithMap object, requestWithList object, " +
                "requestList object ); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        List<String> stringList = new ArrayList<>();
        stringList.add("Test1");
        stringList.add("Test2");
        stringList.add("Test3");
        List<Integer> integerList = new ArrayList<>();
        integerList.add(100);
        integerList.add(200);
        integerList.add(300);
        Request request = Request.newBuilder()
                .setBooleanValue(true)
                .setStringValue("Test 1")
                .setIntValue(1000)
                .setDoubleValue(1000.6745)
                .setFloatValue(1022.43f)
                .setLongValue(10000000L)
                .build();
        Map<String, String> map = new HashMap<>();
        map.put("key 01", "Value 01");
        map.put("key 02", "Value 02");
        RequestWithMap requestWithMap = RequestWithMap.newBuilder()
                .setIntValue(2000)
                .setStringValue("Test 2")
                .putAllMap(map)
                .build();
        RequestWithList requestWithList = RequestWithList.newBuilder()
                .setStringValue("Barry Allen")
                .setIntValue(100)
                .addAllIntList(integerList)
                .addAllStringList(stringList)
                .build();
        List<Request> requests = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Request req = Request.newBuilder()
                    .setBooleanValue(true)
                    .setStringValue("Test " + i)
                    .setIntValue(1000 + i)
                    .setDoubleValue(1000.6745 + i)
                    .setFloatValue(1022.43f + i)
                    .setLongValue(10000000L + i)
                    .build();
            requests.add(req);
        }

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1: {
                            AssertJUnit.assertEquals(request, event.getData(0));
                            AssertJUnit.assertEquals(requestWithMap, event.getData(1));
                            AssertJUnit.assertEquals(requestWithList, event.getData(2));
                            AssertJUnit.assertEquals(requests, event.getData(3));
                            break;
                        }
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        OtherMessageTypes otherMessageTypes = OtherMessageTypes.newBuilder()
                .setRequest(request)
                .setRequestWithMap(requestWithMap)
                .setRequestWithList(requestWithList)
                .addAllRequestList(requests)
                .build();
        InMemoryBroker.publish("test01", otherMessageTypes.toByteArray());
        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 1, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void protobufSourceMapperTestToMapOtherMessagesTypesWithKeyAndValue() throws Exception {

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='test01',  " +
                " @map(type='protobuf', class='" + packageName + ".OtherMessageTypes'," +
                " @attributes(a = 'request' ,b = 'requestWithMap', c  ='requestWithList', d = 'requestList'))) " +
                "define stream FooStream (a object, b object, c object, d object); " +
                "define stream BarStream (a object, b object, c object, d object); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        List<String> stringList = new ArrayList<>();
        stringList.add("Test1");
        stringList.add("Test2");
        stringList.add("Test3");
        List<Integer> integerList = new ArrayList<>();
        integerList.add(100);
        integerList.add(200);
        integerList.add(300);
        Request request = Request.newBuilder()
                .setBooleanValue(true)
                .setStringValue("Test 1")
                .setIntValue(1000)
                .setDoubleValue(1000.6745)
                .setFloatValue(1022.43f)
                .setLongValue(10000000L)
                .build();
        Map<String, String> map = new HashMap<>();
        map.put("key 01", "Value 01");
        map.put("key 02", "Value 02");
        RequestWithMap requestWithMap = RequestWithMap.newBuilder()
                .setIntValue(2000)
                .setStringValue("Test 2")
                .putAllMap(map)
                .build();
        RequestWithList requestWithList = RequestWithList.newBuilder()
                .setStringValue("Barry Allen")
                .setIntValue(100)
                .addAllIntList(integerList)
                .addAllStringList(stringList)
                .build();
        List<Request> requests = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Request req = Request.newBuilder()
                    .setBooleanValue(true)
                    .setStringValue("Test " + i)
                    .setIntValue(1000 + i)
                    .setDoubleValue(1000.6745 + i)
                    .setFloatValue(1022.43f + i)
                    .setLongValue(10000000L + i)
                    .build();
            requests.add(req);
        }

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1: {
                            AssertJUnit.assertEquals(request, event.getData(0));
                            AssertJUnit.assertEquals(requestWithMap, event.getData(1));
                            AssertJUnit.assertEquals(requestWithList, event.getData(2));
                            AssertJUnit.assertEquals(requests, event.getData(3));
                            break;
                        }
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        OtherMessageTypes otherMessageTypes = OtherMessageTypes.newBuilder()
                .setRequest(request)
                .setRequestWithMap(requestWithMap)
                .setRequestWithList(requestWithList)
                .addAllRequestList(requests)
                .build();
        InMemoryBroker.publish("test01", otherMessageTypes.toByteArray());
        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 1, count.get());
        siddhiAppRuntime.shutdown();
    }

}
