package io.siddhi.extension.map.protobuf.sourcemapper;


import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.core.util.transport.InMemoryBroker;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.grpc.test.Request;
import org.wso2.grpc.test.RequestWithMap;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;


public class TestCaseOfProtobufSourceMapper {

    private static Logger log = Logger.getLogger(TestCaseOfProtobufSourceMapper.class.getName());
    private final int waitTime = 500;
    private final int timeout = 500;
    private AtomicInteger count = new AtomicInteger();

    @BeforeMethod
    public void init() {
        count.set(0);
    }


    @Test
    public void protobufSourceMapperTest1() throws Exception {
        log.info("ProtobufSourceMapper 1");
        String streams = "" +
                "@App:name('TestSiddhiApp')" + //todo change and check with request types
                "@source(type='inMemory', topic='test01',  receiver.url = 'grpc://localhost:2000/org.wso2.grpc.test" +
                ".MyService/process'," +
                " @map(type='protobuf')) " +
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

        InMemoryBroker.publish("test01", response1);
        InMemoryBroker.publish("test01", response2);
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 2, count.get());
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void protobufSourceMapperTest2() throws Exception {
        log.info("ProtobufSourceMapper 2");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='test01',  receiver.url = 'grpc://localhost:2000/org.wso2.grpc.test" +
                ".MyService/process'," +
                " @map(type='protobuf'," +
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

        InMemoryBroker.publish("test01", response1);
        InMemoryBroker.publish("test01", response2);
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 2, count.get());
        siddhiAppRuntime.shutdown();

    }


    @Test
    public void protobufSourceMapperTest3() throws Exception { // testcase for class parameter
        log.info("ProtobufSourceMapper 3");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='test01',  receiver.url = 'grpc://localhost:2000/org.wso2.grpc.test" +
                ".MyService/process'," +
                " @map(type='protobuf', class='org.wso2.grpc.test.Request'," +
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
                            AssertJUnit.assertEquals(1000000000000000000L, event.getData(1));
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
                .setLongValue(1000000000000000000L)
                .build();
        Request response2 = Request.newBuilder()
                .setStringValue("Test 02")
                .setIntValue(520)
                .setBooleanValue(true)
                .setDoubleValue(523.455)
                .setFloatValue(88.235f)
                .setLongValue(3456445L)
                .build();
        InMemoryBroker.publish("test01", response1);
        InMemoryBroker.publish("test01", response2);
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 2, count.get());
        siddhiAppRuntime.shutdown();

    }


    @Test
    public void protobufSourceMapperTest4() throws Exception {
        log.info("ProtobufSourceMapper 2");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='test01',  " +
                "receiver.url = 'grpc://localhost:" + 2000 + "/org.wso2.grpc.test.MyService/testMap'," +
                " @map(type='protobuf')) " +
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
        InMemoryBroker.publish("test01", response1);
        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 1, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void protobufSourceMapperTest6() throws Exception {
        log.info("ProtobufSourceMapper 2");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='test01',  " +
                "receiver.url = 'grpc://localhost:" + 2000 + "/org.wso2.grpc.test.MyService/testMap'," +
                " @map(type='protobuf'," +
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

        InMemoryBroker.publish("test01", response1);
        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 1, count.get());
        siddhiAppRuntime.shutdown();

    }
}
