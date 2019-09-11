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
import io.siddhi.extension.map.protobuf.grpc.RequestWithMap;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TestCaseOfProtobufSinkMapper {
    private static final Logger log = Logger.getLogger(TestCaseOfProtobufSinkMapper.class);
    private AtomicInteger count = new AtomicInteger();

    @BeforeMethod
    public void init() {
        count.set(0);
    }

    @Test
    public void protobuSinkMapperTestCase1() throws InterruptedException {
        log.info("ProtobufSinkMapperTestCase 1");

        InMemoryBroker.Subscriber subscriber = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object o) {
                AssertJUnit.assertEquals(Request.class, o.getClass());
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
                "define stream FooStream (stringValue string, intValue int,longValue long,booleanValue bool," +
                "floatValue float,doubleValue double); " +
                "@sink(type='inMemory', topic='test01', publisher.url = 'grpc://localhost:2000/io.siddhi.extension." +
                "map.protobuf.grpc.MyService/process' ," +
                "@map(type='protobuf')) " +
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
                EventPrinter.print(toMap(events));
            }
        });
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        Object[] data1 = {"Test 01", 60, 10000L, true, 522.7586f, 34.5668};
        stockStream.send(data1);
        siddhiAppRuntime.shutdown();
        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriber);
    }

    @Test
    public void protobuSinkMapperTestCase2() throws InterruptedException {
        log.info("ProtobufSinkMapperTestCase 2");
        InMemoryBroker.Subscriber subscriber = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object o) {
                AssertJUnit.assertEquals(Request.class, o.getClass());
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
                "@sink(type='inMemory', topic='test01', publisher.url = 'grpc://localhost:2000/io.siddhi.extension." +
                "map.protobuf.grpc.MyService/process' ," +
                "@map(type='protobuf' , " +
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


    @Test //when user send the class name with the mapper
    public void protobuSinkMapperTestCase3() throws InterruptedException {
        log.info("ProtobufSinkMapperTestCase 3");
        InMemoryBroker.Subscriber subscriber = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object o) {
                AssertJUnit.assertEquals(Request.class, o.getClass());
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
                "@sink(type='inMemory', topic='test01', publisher.url = 'grpc://localhost:2000/io.siddhi.extension." +
                "map.protobuf.grpc.MyService/process' ," +
                "@map(type='protobuf' , class='io.siddhi.extension.map.protobuf.grpc.Request', " +
                "@payload(stringValue='a',longValue='b',intValue='c',booleanValue='d',floatValue = 'e', doubleValue =" +
                " 'f'))) " +
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
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        Object[] data1 = {"Test 01", 10000L, 60, true, 522.7586f, 34.5668};
        stockStream.send(data1);
        siddhiAppRuntime.shutdown();
        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriber);
    }

    @Test
    public void protobuSinkMapperTestCase4() throws InterruptedException {
        log.info("ProtobufSinkMapperTestCase 4");
        InMemoryBroker.Subscriber subscriber = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object o) {
                AssertJUnit.assertEquals(RequestWithMap.class, o.getClass());

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
                "@sink(type='inMemory', topic='test01', publisher.url = 'grpc://localhost:2000/io.siddhi.extension." +
                "map.protobuf.grpc.MyService/testMap' ," +
                "@map(type='protobuf' , " +
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
        Map<String, String> map1 = new HashMap<>();
        map1.put("0001", "Barry Allen");
        Object[] data1 = {"Test 01", 60000, map1};
        stockStream.send(data1);
        siddhiAppRuntime.shutdown();
        InMemoryBroker.unsubscribe(subscriber);
    }

    @Test
    public void protobuSinkMapperTestCase5() throws InterruptedException {
        log.info("ProtobufSinkMapperTestCase 5");

        InMemoryBroker.Subscriber subscriber = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object o) {
                log.info("Request :" + o);
                AssertJUnit.assertEquals(RequestWithMap.class, o.getClass());

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
                "@sink(type='inMemory', topic='test01', publisher.url = 'grpc://localhost:2000/io.siddhi.extension." +
                "map.protobuf.grpc.MyService/testMap' ," +
                "@map(type='protobuf')) " +
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
        Map<String, String> map1 = new HashMap<>();
        map1.put("0001", "Barry Allen");
        Object[] data1 = {"Test 01", 60000, map1};
        stockStream.send(data1);
        siddhiAppRuntime.shutdown();
        InMemoryBroker.unsubscribe(subscriber);
    }

    @Test
    public void protobuSinkMapperTestCaseWithoutPublisherUrl_01() throws InterruptedException {
        log.info("ProtobufSinkMapperTestCase 1");

        InMemoryBroker.Subscriber subscriber = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object o) {
                log.info("Request :\n" + o);
                AssertJUnit.assertEquals(Request.class, o.getClass());
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
                "define stream FooStream (stringValue string, intValue int,longValue long,booleanValue bool," +
                "floatValue float,doubleValue double); " +
                "@sink(type='inMemory', topic='test01'," +
                "@map(type='protobuf', class='io.siddhi.extension.map.protobuf.grpc.Request')) " +
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
                EventPrinter.print(toMap(events));
            }
        });
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        Object[] data1 = {"Test 01", 60, 10000L, true, 522.7586f, 34.5668};
        stockStream.send(data1);
        siddhiAppRuntime.shutdown();
        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriber);
    }

    @Test
    public void protobuSinkMapperTestCaseWithoutPublisherUrl_02() throws InterruptedException {
        log.info("ProtobufSinkMapperTestCase 2");
        InMemoryBroker.Subscriber subscriber = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object o) {
                AssertJUnit.assertEquals(Request.class, o.getClass());
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
                "@sink(type='inMemory', topic='test01', " +
                "@map(type='protobuf', class='io.siddhi.extension.map.protobuf.grpc.Request'," +
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
}
