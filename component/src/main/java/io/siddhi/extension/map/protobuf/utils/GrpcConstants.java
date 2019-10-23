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

/**
 * Class to hold the constants used by protobuf source and sink mapper
 */
public class GrpcConstants {

    public static final String PUBLISHER_URL = "publisher.url";
    public static final String RECEIVER_URL = "receiver.url";
    public static final String PORT_SERVICE_SEPARATOR = "/";
    public static final String EMPTY_STRING = "";
    public static final String GRPC_PROTOCOL_NAME = "grpc";
    public static final String DUMMY_PROTOCOL_NAME = "http";
    public static final String GRPC_SERVICE_RESPONSE_SINK_NAME = "grpc-service-response";
    public static final String GRPC_CALL_RESPONSE_SOURCE_NAME = "grpc-call-response";
    public static final String GRPC_SERVICE_SOURCE_NAME = "grpc-service-source";
    public static final String CLASS_OPTION_HOLDER = "class";
    public static final String STUB_NAME = "Stub";
    public static final String GRPC_PROTOCOL_NAME_UPPERCAMELCASE = "Grpc";
    public static final String DOLLAR_SIGN = "$";
    public static final String NEW_BUILDER_NAME = "newBuilder";
    public static final String PARSE_FROM_NAME = "parseFrom";
    public static final String TO_BYTE_ARRAY = "toByteArray";
    public static final String SETTER = "set";
    public static final String GETTER = "get";
    public static final String MAP_NAME = "Map";
    public static final String BUILD_METHOD = "build";
    public static final String PUTALL_METHOD = "putAll";
    public static final String METHOD_NAME = "Method";
    public static final int PATH_SERVICE_NAME_POSITION = 0;
    public static final int PATH_METHOD_NAME_POSITION = 1;
    public static final int REQUEST_CLASS_POSITION = 0;
    public static final int RESPONSE_CLASS_POSITION = 1;
    private GrpcConstants() {
    }


}
