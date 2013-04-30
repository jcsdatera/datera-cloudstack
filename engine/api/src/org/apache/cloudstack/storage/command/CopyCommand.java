// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package org.apache.cloudstack.storage.command;

import org.apache.cloudstack.engine.subsystem.api.storage.DataTO;

import com.cloud.agent.api.Command;

public class CopyCommand extends Command implements StorageSubSystemCommand {
    private DataTO srcTO;
    private DataTO destTO;
    private DataTO cacheTO;

    public CopyCommand(DataTO srcData, DataTO destData, int timeout) {
        super();
        this.srcTO = srcData;
        this.destTO = destData;
        this.setWait(timeout);
    }
    
    public DataTO getDestTO() {
        return this.destTO;
    }
    
    public DataTO getSrcTO() {
        return this.srcTO;
    }

    @Override
    public boolean executeInSequence() {
        return true;
    }

    public DataTO getCacheTO() {
        return cacheTO;
    }

    public void setCacheTO(DataTO cacheTO) {
        this.cacheTO = cacheTO;
    }

}
