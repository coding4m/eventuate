/*
 * Copyright 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rbmhtechnology.eventuate

import MultiNodeConfigRocksdb._

class BasicReplicationSpecRocksdb extends BasicReplicationSpec(new BasicReplicationConfig(providerConfig)) with MultiNodeSupportRocksdb
class BasicReplicationSpecRocksdbMultiJvmNode1 extends BasicReplicationSpecRocksdb
class BasicReplicationSpecRocksdbMultiJvmNode2 extends BasicReplicationSpecRocksdb
class BasicReplicationSpecRocksdbMultiJvmNode3 extends BasicReplicationSpecRocksdb

class BasicReplicationThroughputSpecRocksdb extends BasicReplicationThroughputSpec(new BasicReplicationThroughputConfig(providerConfig)) with MultiNodeSupportRocksdb
class BasicReplicationThroughputSpecRocksdbMultiJvmNode1 extends BasicReplicationThroughputSpecRocksdb
class BasicReplicationThroughputSpecRocksdbMultiJvmNode2 extends BasicReplicationThroughputSpecRocksdb
class BasicReplicationThroughputSpecRocksdbMultiJvmNode3 extends BasicReplicationThroughputSpecRocksdb
class BasicReplicationThroughputSpecRocksdbMultiJvmNode4 extends BasicReplicationThroughputSpecRocksdb
class BasicReplicationThroughputSpecRocksdbMultiJvmNode5 extends BasicReplicationThroughputSpecRocksdb
class BasicReplicationThroughputSpecRocksdbMultiJvmNode6 extends BasicReplicationThroughputSpecRocksdb

class BasicCausalitySpecRocksdb extends BasicCausalitySpec(new BasicCausalityConfig(providerConfig)) with MultiNodeSupportRocksdb
class BasicCausalitySpecRocksdbMultiJvmNode1 extends BasicCausalitySpecRocksdb
class BasicCausalitySpecRocksdbMultiJvmNode2 extends BasicCausalitySpecRocksdb

class BasicPersistOnEventSpecRocksdb extends BasicPersistOnEventSpec(new BasicPersistOnEventConfig(providerConfig)) with MultiNodeSupportRocksdb
class BasicPersistOnEventSpecRocksdbMultiJvmNode1 extends BasicPersistOnEventSpecRocksdb
class BasicPersistOnEventSpecRocksdbMultiJvmNode2 extends BasicPersistOnEventSpecRocksdb

class FailureDetectionSpecRocksdb extends FailureDetectionSpec(new FailureDetectionConfig(providerConfig)) with MultiNodeSupportRocksdb
class FailureDetectionSpecRocksdbMultiJvmNode1 extends FailureDetectionSpecRocksdb
class FailureDetectionSpecRocksdbMultiJvmNode2 extends FailureDetectionSpecRocksdb




