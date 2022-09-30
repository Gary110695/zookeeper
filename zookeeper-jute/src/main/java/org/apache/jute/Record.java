/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jute;

import org.apache.yetus.audience.InterfaceAudience;

import java.io.IOException;

/**
 * Interface that is implemented by generated classes.
 *
 * Record接口是Jute序列化的核心接口，每个需要被序列化的实例都需要实现这个接口，该接口定义了序列化和反序列的方法
 *
 * zookeeper采用一种名为jute的方式进行序列化，jute本质上使用的是JDK自带的序列化方式，那么相对现在流行的protobuf等
 * 方式显得笨重了许多。但是总体看来，序列化并不是zookeeper的性能瓶颈，且为了兼容，所以zookeeper一直没有更换这种序列化方式
 */
@InterfaceAudience.Public
public interface Record {
    public void serialize(OutputArchive archive, String tag)
        throws IOException;
    public void deserialize(InputArchive archive, String tag)
        throws IOException;
}
