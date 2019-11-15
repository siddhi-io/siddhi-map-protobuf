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

import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.MapField;
import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.stream.output.sink.SinkListener;
import io.siddhi.core.stream.output.sink.SinkMapper;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.core.util.transport.TemplateBuilder;
import io.siddhi.extension.map.protobuf.utils.GrpcConstants;
import io.siddhi.extension.map.protobuf.utils.ProtobufUtils;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.apache.log4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.siddhi.extension.map.protobuf.utils.ProtobufUtils.getMethodName;
import static io.siddhi.extension.map.protobuf.utils.ProtobufUtils.getRPCmethodList;
import static io.siddhi.extension.map.protobuf.utils.ProtobufUtils.getServiceName;
import static io.siddhi.extension.map.protobuf.utils.ProtobufUtils.protobufFieldsWithTypes;
import static io.siddhi.extension.map.protobuf.utils.ProtobufUtils.toLowerCamelCase;

/**
 * Protobuf SinkMapper converts siddhi events in to protobuf message objects.
 */
@Extension(
        name = "protobuf",
        namespace = "sinkMapper",

        description = "" +
                "This output mapper allows you to convert Events to protobuf messages before publishing them." +
                " To work with this mapper you have to add auto-generated protobuf classes to the project classpath." +
                " When you use this output mapper, you can either define stream attributes as the same names as the " +
                "protobuf message attributes or you can use custom mapping to map stream definition attributes with " +
                "the protobuf attributes..Please find the sample proto definition [here](https://github.com/siddhi-io" +
                "/siddhi-map-protobuf/tree/master/component/src/main/resources/sample.proto) "
        ,
        parameters = {
                @Parameter(name = "class",
                        description = "" +
                                "This specifies the class name of the protobuf message class, If sink type is grpc " +
                                "then it's not necessary to provide this parameter.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "-"),
        },
        examples = {
                @Example(
                        syntax = "@sink(type='grpc',  url = 'grpc://localhost:2000/org.wso2.grpc.test" +
                                ".MyService/process \n" +
                                "@map(type='protobuf')) \n" +
                                "define stream BarStream (stringValue string, intValue int,longValue long," +
                                "booleanValue bool,floatValue float,doubleValue double)",
                        description = "Above definition will map BarStream values into the protobuf message type of " +
                                "the 'process' method in 'MyService' service"
                ),
                @Example(
                        syntax = "@sink(type='grpc', url = 'grpc://localhost:2000/org.wso2.grpc.test" +
                                ".MyService/process\n" +
                                "@map(type='protobuf'), \n" +
                                "@payload(stringValue='a',longValue='b',intValue='c',booleanValue='d',floatValue = " +
                                "'e', doubleValue  = 'f'))) \n" +
                                "define stream BarStream (a string, b long, c int,d bool,e float,f double);",

                        description = "The above definition will map BarStream values to request message type of the " +
                                "'process' method in 'MyService' service. and stream values will map like this, \n" +
                                "- value of 'a' will be assign 'stringValue' variable in the message class \n" +
                                "- value of 'b' will be assign 'longValue' variable in the message class \n" +
                                "- value of 'c' will be assign 'intValue' variable in the message class \n" +
                                "- value of 'd' will be assign 'booleanValue' variable in the message class \n" +
                                "- value of 'e' will be assign 'floatValue' variable in the message class \n" +
                                "- value of 'f' will be assign 'doubleValue' variable in the message class \n" +
                                ""
                ),
                @Example(
                        syntax = "@sink(type='grpc', url = 'grpc://localhost:2000/org.wso2.grpc.test" +
                                ".MyService/testMap' \n" +
                                "@map(type='protobuf')) \n" +
                                " define stream BarStream (stringValue string,intValue int,map object);",

                        description = "The above definition will map BarStream values to request message type of the " +
                                "'testMap' method in 'MyService' service and since there is an object data type is in" +
                                "the stream(map object) , mapper will assume that 'map' is an instance of  " +
                                "'java.util.Map' class, otherwise it will throws and error. \n" +
                                ""
                ),
                @Example(
                        syntax = "@sink(type='inMemory', topic='test01', \n" +
                                "@map(type='protobuf', class='org.wso2.grpc.test.Request'))\n" +
                                "define stream BarStream (stringValue string, intValue int,longValue long," +
                                "booleanValue bool,floatValue float,doubleValue double);",

                        description = "The above definition will map BarStream values to 'org.wso2.grpc.test.Request'" +
                                "protobuf class type. If sink type is not a grpc, sink is expecting to get the" +
                                " mapping protobuf class from the 'class' parameter in the @map extension" +
                                ""

                )
        }
)
public class ProtobufSinkMapper extends SinkMapper {
    private static final Logger log = Logger.getLogger(ProtobufSinkMapper.class);
    private Object messageBuilderObject;
    private List<MappingPositionData> mappingPositionDataList;
    private String siddhiAppName;
    private String streamID;

    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[0];
    }

    @Override
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder, Map<String, TemplateBuilder>
            templateBuilderMap, ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        this.siddhiAppName = siddhiAppContext.getName();
        this.streamID = streamDefinition.getId();
        mappingPositionDataList = new ArrayList<>();
        String userProvidedClassName = null;
        if (optionHolder.isOptionExists(GrpcConstants.CLASS_OPTION_HOLDER)) {
            userProvidedClassName = optionHolder.validateAndGetOption(GrpcConstants.CLASS_OPTION_HOLDER).getValue();
        }
        Class messageObjectClass;
        if (sinkType.toLowerCase().startsWith(GrpcConstants.GRPC_PROTOCOL_NAME)) {
            if (GrpcConstants.GRPC_SERVICE_RESPONSE_SINK_NAME.equalsIgnoreCase(sinkType)
                    && templateBuilderMap.size() == 0) {
                throw new SiddhiAppCreationException(" No mapping found at @Map, mapping is required to continue " +
                        "for Siddhi App " + siddhiAppName); //grpc-service-response should have a mapping
            }
            String url = null;
            if (sinkOptionHolder.isOptionExists(GrpcConstants.PUBLISHER_URL)) {
                url = sinkOptionHolder.validateAndGetStaticValue(GrpcConstants.PUBLISHER_URL);
            }
            if (url != null) {
                URL aURL;
                try {
                    if (!url.toLowerCase().startsWith(GrpcConstants.GRPC_PROTOCOL_NAME)) {
                        throw new SiddhiAppValidationException(siddhiAppName + ": " + streamID + ": The url must " +
                                "begin with \"" + GrpcConstants.GRPC_PROTOCOL_NAME + "\" for all grpc sinks");
                    }
                    aURL = new URL(GrpcConstants.DUMMY_PROTOCOL_NAME + url.substring(4));
                } catch (MalformedURLException e) {
                    throw new SiddhiAppValidationException(siddhiAppName + ": " + streamID + ": Error in URL format." +
                            " Expected format is `grpc://0.0.0.0:9763/<serviceName>/<methodName>` but the provided " +
                            "url" +
                            " is '" + url + "'," + e.getMessage(), e);
                }
                String methodReference = getMethodName(aURL.getPath(), siddhiAppName, streamID);
                String fullQualifiedServiceReference = getServiceName(aURL.getPath(), siddhiAppName, streamID);
                try {
                    String capitalizedFirstLetterMethodName = methodReference.substring(0, 1).toUpperCase() +
                            methodReference.substring(1);
                    Field methodDescriptor = Class.forName(fullQualifiedServiceReference
                            + GrpcConstants.GRPC_PROTOCOL_NAME_UPPERCAMELCASE).getDeclaredField
                            (GrpcConstants.GETTER + capitalizedFirstLetterMethodName + GrpcConstants.METHOD_NAME);
                    ParameterizedType parameterizedType = (ParameterizedType) methodDescriptor.getGenericType();
                    if (GrpcConstants.GRPC_SERVICE_RESPONSE_SINK_NAME.equalsIgnoreCase(sinkType)) {
                        messageObjectClass = (Class) parameterizedType.
                                getActualTypeArguments()[GrpcConstants.RESPONSE_CLASS_POSITION];
                    } else {
                        messageObjectClass = (Class) parameterizedType.
                                getActualTypeArguments()[GrpcConstants.REQUEST_CLASS_POSITION];
                    }
                    if (userProvidedClassName != null) {
                        if (url.toLowerCase().startsWith(GrpcConstants.GRPC_PROTOCOL_NAME)) { /*only if sink is a grpc
                        type, check for both user provided class name and the required class name*/
                            if (!messageObjectClass.getName().equals(userProvidedClassName)) {
                                throw new SiddhiAppCreationException(siddhiAppName + ": " + streamID +
                                        ": provided class name does not match with the original mapping class, " +
                                        "provided class: '" + userProvidedClassName + "' , expected: '" +
                                        messageObjectClass.getName() + "'");
                            }
                        }
                    }
                    Method builderMethod = messageObjectClass.getDeclaredMethod(GrpcConstants.NEW_BUILDER_NAME); //to
                    // create an builder object of message class
                    messageBuilderObject = builderMethod.invoke(messageObjectClass); // create the object
                } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException |
                        NoSuchFieldException e) {
                    throw new SiddhiAppCreationException(siddhiAppName + ": " + streamID + ": Invalid method name " +
                            "provided in the url, provided method name : '" + methodReference + "' expected one of " +
                            "these methods : " + getRPCmethodList(fullQualifiedServiceReference, siddhiAppName,
                            streamID) + "," + e.getMessage(), e);
                } catch (ClassNotFoundException e) {
                    throw new SiddhiAppCreationException(siddhiAppName + ": " + streamID + ": Invalid service name " +
                            "provided in url, provided service name : '" + fullQualifiedServiceReference + "'," +
                            e.getMessage(), e);
                }
            } else {
                throw new SiddhiAppValidationException(siddhiAppName + ": " + streamID + ": publisher.url should be " +
                        "given.");
            }
        } else {
            log.debug(siddhiAppName + ": Not a grpc sink, getting the protobuf class name from 'class' parameter");
            if (userProvidedClassName == null) {
                throw new SiddhiAppCreationException(siddhiAppName + ": " + streamID + "No class name provided in " +
                        "the @map, you should provide the protobuf class name within the 'class' parameter");
            }
            try {
                messageObjectClass = Class.forName(userProvidedClassName);
                Method builderMethod = messageObjectClass.getDeclaredMethod(GrpcConstants.NEW_BUILDER_NAME); //to
                // create an builder object of message class
                messageBuilderObject = builderMethod.invoke(messageObjectClass); // create the  builder object
            } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException |
                    InvocationTargetException e) {
                throw new SiddhiAppCreationException(siddhiAppName + ": " + streamID + ": Invalid class name provided" +
                        " in the 'class' parameter, provided class name: '" + userProvidedClassName + "'," +
                        e.getMessage(), e);
            }
        }
        initializeSetterMethods(streamDefinition, templateBuilderMap);
    }

    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{GeneratedMessageV3.class};
    }

    @Override
    public void mapAndSend(Event[] events, OptionHolder optionHolder, Map<String, TemplateBuilder> templateBuilderMap,
                           SinkListener sinkListener) {
        for (Event event : events) {
            mapAndSend(event, optionHolder, templateBuilderMap, sinkListener);
        }
    }

    @Override
    public void mapAndSend(Event event, OptionHolder optionHolder, Map<String, TemplateBuilder> templateBuilderMap,
                           SinkListener sinkListener) {
        for (MappingPositionData mappingPositionData : mappingPositionDataList) {
            Object data = mappingPositionData.getData(event);
            try {
                mappingPositionData.getMessageObjectSetterMethod().invoke(messageBuilderObject, data);
            } catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException e) {
                String nameOfExpectedClass =
                        mappingPositionData.getMessageObjectSetterMethod().getParameterTypes()[0].getName();
                String nameOfFoundClass = data.getClass().getName();
                String[] foundClassnameArray = nameOfFoundClass.split("\\.");
                nameOfFoundClass = foundClassnameArray[foundClassnameArray.length - 1]; // to get the last name
                throw new SiddhiAppRuntimeException(siddhiAppName + ": " + streamID + " : Data type does not match. " +
                        "Expected data type: '" + nameOfExpectedClass + "' found: '" + nameOfFoundClass + "'," +
                        e.getMessage(), e);
            }
        }
        try {
            Method buildMethod = messageBuilderObject.getClass().getDeclaredMethod(GrpcConstants.BUILD_METHOD);
            Object messageObject = buildMethod.invoke(messageBuilderObject); //get the message object by invoking
            // build() method
            if (sinkType.toLowerCase().startsWith(GrpcConstants.GRPC_PROTOCOL_NAME)) {
                sinkListener.publish(messageObject);
            } else {
                byte[] messageObjectByteArray = (byte[]) AbstractMessageLite.class
                        .getDeclaredMethod(GrpcConstants.TO_BYTE_ARRAY).invoke(messageObject);
                sinkListener.publish(messageObjectByteArray);
                clearMessageBuilderObject();
            }
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new SiddhiAppRuntimeException(siddhiAppName + ": " + streamID + " Unknown error occurred during " +
                    "runtime," + e.getMessage(), e);
            // this error will not throw, All possible scenarios are handled in the init() method.
        }
    }

    private void initializeSetterMethods(StreamDefinition streamDefinition, Map<String, TemplateBuilder>
            templateBuilderMap) {
        Attribute.Type attributeType;
        String attributeName = null;
        try {
            if (templateBuilderMap == null) {
                for (int i = 0; i < streamDefinition.getAttributeList().size(); i++) {
                    attributeType = streamDefinition.getAttributeList().get(i).getType(); //get attribute type
                    attributeName = streamDefinition.getAttributeNameArray()[i]; //get attribute name
                    Method setterMethod = setSetterMethod(attributeType, attributeName);
                    mappingPositionDataList.add(new MappingPositionData(setterMethod, i));
                }
            } else {
                List<String> mapKeySetList = new ArrayList<>(templateBuilderMap.keySet()); //convert keyset to a
                // list, to get keys by index
                for (int i = 0; i < templateBuilderMap.size(); i++) {
                    attributeName = mapKeySetList.get(i); //get attribute name
                    attributeType = templateBuilderMap.get(attributeName).getType();
                    Method setterMethod = setSetterMethod(attributeType, attributeName);
                    mappingPositionDataList.add(new MappingPositionDataWithTemplateBuilder(setterMethod,
                            templateBuilderMap.get(mapKeySetList.get(i))));
                }
            }
        } catch (NoSuchMethodException | NoSuchFieldException e) {
            Field[] fields = messageBuilderObject.getClass().getDeclaredFields(); //get all available attributes
            throw new SiddhiAppCreationException(siddhiAppName + ": " + streamID + "Attribute name or type does " +
                    "not match with protobuf variable or type. provided attribute '" + attributeName +
                     "'. Expected one of these attributes " + protobufFieldsWithTypes(fields) + ".",
                    e);
        }
    }

    private Method setSetterMethod(Attribute.Type attributeType, String attributeName) throws NoSuchFieldException,
            NoSuchMethodException {
        if (attributeType == Attribute.Type.OBJECT) {
            if (List.class.isAssignableFrom(messageBuilderObject.getClass().getDeclaredField(
                    attributeName + "_").getType())) { // check if list or not
                return messageBuilderObject.getClass().getDeclaredMethod(GrpcConstants
                        .ADDALL_METHOD + toLowerCamelCase(attributeName), Iterable.class);
            } else if (MapField.class.isAssignableFrom(messageBuilderObject.getClass().getDeclaredField(
                    attributeName + "_").getType())) { //check if map or not
                return messageBuilderObject.getClass().getDeclaredMethod(GrpcConstants
                        .PUTALL_METHOD + toLowerCamelCase(attributeName), java.util.Map.class);
            } else {
                throw new SiddhiAppCreationException("Unknown data type. You should provide either 'map' " +
                        "or 'list' with 'object' data type");
            }
        } else {
            return messageBuilderObject.getClass().getDeclaredMethod(GrpcConstants.SETTER + toLowerCamelCase(
                    attributeName), ProtobufUtils.getDataType(attributeType));
        }
    }

    private void clearMessageBuilderObject() {
        try {
            messageBuilderObject.getClass().getDeclaredMethod("clear").invoke(messageBuilderObject);
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new SiddhiAppRuntimeException(siddhiAppName + ": " + streamID + " : Unable to find 'clear()' " +
                    "method." , e);
        }
    }

    private static class MappingPositionData {
        private Method messageObjectSetterMethod;
        private int position; //this attribute can be removed

        private MappingPositionData(Method messageObjectSetterMethod, int position) {
            this.messageObjectSetterMethod = messageObjectSetterMethod;
            this.position = position; //if mapping is not available
        }

        private MappingPositionData(Method messageObjectSetterMethod) {
            this.messageObjectSetterMethod = messageObjectSetterMethod;
        }

        private Method getMessageObjectSetterMethod() {
            return messageObjectSetterMethod;
        }

        protected Object getData(Event event) {
            return event.getData(position);
        }
    }

    private static class MappingPositionDataWithTemplateBuilder extends MappingPositionData {
        private TemplateBuilder templateBuilder;

        private MappingPositionDataWithTemplateBuilder(Method messageObjectSetterMethod,
                                                       TemplateBuilder templateBuilder) {
            super(messageObjectSetterMethod);
            this.templateBuilder = templateBuilder;
        }

        @Override
        protected Object getData(Event event) {
            return templateBuilder.build(event);
        }
    }
}
