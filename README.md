siddhi-map-protobuf
======================================

 [![Jenkins Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-map-protobuf/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-map-protobuf/)
  [![GitHub Release](https://img.shields.io/github/release/siddhi-io/siddhi-map-protobuf.svg)](https://github.com/siddhi-io/siddhi-map-protobuf/releases)
  [![GitHub Release Date](https://img.shields.io/github/release-date/siddhi-io/siddhi-map-protobuf.svg)](https://github.com/siddhi-io/siddhi-map-protobuf/releases)
  [![GitHub Open Issues](https://img.shields.io/github/issues-raw/siddhi-io/siddhi-map-protobuf.svg)](https://github.com/siddhi-io/siddhi-map-protobuf/issues)
  [![GitHub Last Commit](https://img.shields.io/github/last-commit/siddhi-io/siddhi-map-protobuf.svg)](https://github.com/siddhi-io/siddhi-map-protobuf/commits/master)
  [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The **siddhi-map-protobuf extension** is an extension that converts Protobuf messages to/form Siddhi evnets.

For information on <a target="_blank" href="https://siddhi.io/">Siddhi</a> and it's features refer <a target="_blank" href="https://siddhi.io/redirect/docs.html">Siddhi Documentation</a>. 

## Download

* from <a target="_blank" href="https://mvnrepository.com/artifact/io.siddhi.extension.map.protobuf/siddhi-map-protobuf/">here</a>.

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://siddhi-io.github.io/siddhi-map-protobuf/api/1.0.5">1.0.5</a>.

## Features

* <a target="_blank" href="https://siddhi-io.github.io/siddhi-map-protobuf/api/1.0.5/#protobuf-sink-mapper">protobuf</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#sink-mapper">Sink Mapper</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This output mapper allows you to convert Events to protobuf messages before publishing them. To work with this mapper you have to add auto-generated protobuf classes to the project classpath. When you use this output mapper, you can either define stream attributes as the same names as the protobuf message attributes or you can use custom mapping to map stream definition attributes with the protobuf attributes. Please find the sample proto definition [here](https://github.com/siddhi-io/siddhi-map-protobuf/tree/master/component/src/main/resources/sample.proto). When you use this mapper with <code>siddhi-io-grpc</code> you don't have to provide the protobuf message class in the <code>class</code> parameter. </p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-map-protobuf/api/1.0.5/#protobuf-source-mapper">protobuf</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#source-mapper">Source Mapper</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This input mapper allows you to convert protobuf messages into Events. To work with this input mapper you have to add auto-generated protobuf classes to the project classpath. When you use this input mapper, you can either define stream attributes as the same names as the protobuf message attributes or you can use custom mapping to map stream definition attributes with the protobuf attributes.Please find the sample proto definition [here](https://github.com/siddhi-io/siddhi-map-protobuf/tree/master/component/src/main/resources/sample.proto). When you use this mapper with <code>siddhi-io-grpc</code> you don't have to provide the protobuf message class in the <code>class</code> parameter.  </p></p></div>

## Dependencies 

Add following protobuf jar into {SIDDHI_HOME}/bundles
* <a target="_blank" href="https://mvnrepository.com/artifact/com.google.protobuf/protobuf-java/3.9.1">protobuf-java-3.9.1.jar</a>

## Installation

For installing this extension on various siddhi execution environments refer Siddhi documentation section on <a target="_blank" href="https://siddhi.io/redirect/add-extensions.html">adding extensions</a>.

## Support and Contribution

* We encourage users to ask questions and get support via <a target="_blank" href="https://stackoverflow.com/questions/tagged/siddhi">StackOverflow</a>, make sure to add the `siddhi` tag to the issue for better response.

* If you find any issues related to the extension please report them on <a target="_blank" href="https://github.com/siddhi-io/siddhi-execution-string/issues">the issue tracker</a>.

* For production support and other contribution related information refer <a target="_blank" href="https://siddhi.io/community/">Siddhi Community</a> documentation.
