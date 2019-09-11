/*
 * Copyright (c)  2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.siddhi.extension.map.protobuf.utils;

import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Class to get service name, methodNames and the package name from url
 */
public class ProtobufUtils {
    public static String getServiceName(String path) {
        List<String> urlParts = new ArrayList<>(Arrays.asList(path.substring(1).split(GrpcConstants
                .PORT_SERVICE_SEPARATOR)));
        if (urlParts.contains(GrpcConstants.EMPTY_STRING)) {
            throw new SiddhiAppValidationException("Malformed URL. There should not be any empty parts in the URL " +
                    "between two '/'");
        }
        if (urlParts.size() < 2) {
            throw new SiddhiAppValidationException("Malformed URL. After port number at least two sections should " +
                    "be available separated by '/' as in 'grpc://<host>:<port>/<ServiceName>/<MethodName>'");
        }
        return urlParts.get(GrpcConstants.PATH_SERVICE_NAME_POSITION);

    }

    public static String getMethodName(String path) {
        List<String> urlParts = new ArrayList<>(Arrays.asList(path.split(GrpcConstants.PORT_SERVICE_SEPARATOR)));
        urlParts.removeAll(Collections.singletonList(GrpcConstants.EMPTY_STRING));
        if (urlParts.size() < 2) {
            throw new SiddhiAppValidationException("Malformed URL. After port number at least two sections should " +
                    "be available separated by '/' as in 'grpc://<host>:<port>/<ServiceName>/<MethodName>'");
        }

        return urlParts.get(GrpcConstants.PATH_METHOD_NAME_POSITION);
    }

    public static Class getDataType(Attribute.Type type) { // TODO: 9/11/19 use switch case
        if (type == Attribute.Type.STRING) {
            return String.class;
        } else if (type == Attribute.Type.INT) {
            return Integer.TYPE;
        } else if (type == Attribute.Type.DOUBLE) {
            return Double.TYPE;
        } else if (type == Attribute.Type.LONG) {
            return Long.TYPE;
        } else if (type == Attribute.Type.FLOAT) {
            return Float.TYPE;
        } else if (type == Attribute.Type.BOOL) {
            return Boolean.TYPE;
        }
        return null; // won't reach here
    }

    public static  String protobufFieldsWithTypes(Field[] protobufFields) {
        StringBuilder variableNamesWithType = new StringBuilder("{ ");
        for (Field field : protobufFields) {
            if (field.getName().equals("bitField0_")) {
                continue;
            }
            String name = field.getName().substring(0, field.getName().length() - 1);
            String[] tempFieldNameArray = field.getType().getTypeName().split("\\.");
            String type = tempFieldNameArray[tempFieldNameArray.length - 1];
            if (type.equals("Object")) {
                type = "String";
                // as Object
            } else if (type.equals("MapField")) {
                type = "Map";
            }
            variableNamesWithType.append("\"'").append(name).append("' : ").append(type).append("\" , ");

        }
        variableNamesWithType = new StringBuilder(variableNamesWithType.substring(0,
                variableNamesWithType.length() - 2));
        variableNamesWithType.append(" }");
        return variableNamesWithType.toString();
    }

    public static List<String> getRPCmethodList(String serviceReference, String siddhiAppName) { //require full
        // serviceName
        List<String> rpcMethodNameList = new ArrayList<>();
        String[] serviceReferenceArray = serviceReference.split("\\.");
        String serviceName = serviceReferenceArray[serviceReferenceArray.length - 1];
        String stubReference = serviceReference + GrpcConstants.GRPC_PROTOCOL_NAME_UPPERCAMELCASE
                + GrpcConstants.DOLLAR_SIGN + serviceName + GrpcConstants.STUB_NAME;
        Method[] methodsInStub;
        try {
            methodsInStub = Class.forName(stubReference).getMethods(); //get all methods in stub Inner class
        } catch (ClassNotFoundException e) {
            throw new SiddhiAppCreationException(siddhiAppName + ": " +
                    "Invalid service name provided in url, provided service name : '" + serviceName + "'", e);
        }
        // ClassNotFound Exception will be thrown
        for (Method method : methodsInStub) {
            if (method.getDeclaringClass().getName().equals(stubReference)) { // check if the method belongs
                // to blocking stub, other methods that does not belongs to blocking stub are not rpc methods
                rpcMethodNameList.add(method.getName());
            }
        }
        return rpcMethodNameList;
    }



}
