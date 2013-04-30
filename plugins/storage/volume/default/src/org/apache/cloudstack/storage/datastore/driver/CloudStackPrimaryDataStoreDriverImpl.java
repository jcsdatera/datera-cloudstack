/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cloudstack.storage.datastore.driver;

import java.util.Set;

import javax.inject.Inject;

import org.apache.cloudstack.engine.subsystem.api.storage.CommandResult;
import org.apache.cloudstack.engine.subsystem.api.storage.CopyCommandResult;
import org.apache.cloudstack.engine.subsystem.api.storage.CreateCmdResult;
import org.apache.cloudstack.engine.subsystem.api.storage.DataObject;
import org.apache.cloudstack.engine.subsystem.api.storage.DataObjectType;
import org.apache.cloudstack.engine.subsystem.api.storage.DataStore;
import org.apache.cloudstack.engine.subsystem.api.storage.DataTO;
import org.apache.cloudstack.engine.subsystem.api.storage.EndPoint;
import org.apache.cloudstack.engine.subsystem.api.storage.PrimaryDataStoreDriver;
import org.apache.cloudstack.engine.subsystem.api.storage.SnapshotInfo;
import org.apache.cloudstack.engine.subsystem.api.storage.VolumeInfo;
import org.apache.cloudstack.framework.async.AsyncCompletionCallback;
import org.apache.cloudstack.storage.command.CreateObjectCommand;
import org.apache.cloudstack.storage.datastore.db.PrimaryDataStoreDao;
import org.apache.cloudstack.storage.to.SnapshotObjectTO;
import org.apache.cloudstack.storage.volume.VolumeObject;
import org.apache.log4j.Logger;

import com.cloud.agent.api.Answer;
import com.cloud.agent.api.ManageSnapshotAnswer;
import com.cloud.agent.api.ManageSnapshotCommand;
import com.cloud.agent.api.storage.CreateAnswer;
import com.cloud.agent.api.storage.CreateCommand;
import com.cloud.agent.api.storage.DestroyCommand;
import com.cloud.agent.api.storage.ResizeVolumeAnswer;
import com.cloud.agent.api.storage.ResizeVolumeCommand;
import com.cloud.agent.api.to.DataStoreTO;
import com.cloud.agent.api.to.StorageFilerTO;
import com.cloud.exception.StorageUnavailableException;
import com.cloud.host.dao.HostDao;
import com.cloud.storage.DiskOfferingVO;
import com.cloud.storage.ResizeVolumePayload;
import com.cloud.storage.SnapshotVO;
import com.cloud.storage.StorageManager;
import com.cloud.storage.StoragePool;
import com.cloud.storage.VolumeManager;
import com.cloud.storage.VolumeVO;
import com.cloud.storage.dao.DiskOfferingDao;
import com.cloud.storage.dao.SnapshotDao;
import com.cloud.storage.dao.VMTemplateDao;
import com.cloud.storage.dao.VolumeDao;
import com.cloud.storage.snapshot.SnapshotManager;
import com.cloud.vm.DiskProfile;
import com.cloud.vm.dao.VMInstanceDao;

public class CloudStackPrimaryDataStoreDriverImpl implements PrimaryDataStoreDriver {
	private static final Logger s_logger = Logger
			.getLogger(CloudStackPrimaryDataStoreDriverImpl.class);
	@Inject DiskOfferingDao diskOfferingDao;
	@Inject VMTemplateDao templateDao;
	@Inject VolumeDao volumeDao;
	@Inject HostDao hostDao;
	@Inject StorageManager storageMgr;
	@Inject VolumeManager volumeMgr;
	@Inject VMInstanceDao vmDao;
	@Inject SnapshotDao snapshotDao;
	@Inject PrimaryDataStoreDao primaryStoreDao;
	@Inject SnapshotManager snapshotMgr;
	@Override
	public String grantAccess(DataObject data, EndPoint ep) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
    public DataTO getTO(DataObject data) {
        return null;
    }


	@Override
    public DataStoreTO getStoreTO(DataStore store) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
	public boolean revokeAccess(DataObject data, EndPoint ep) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Set<DataObject> listObjects(DataStore store) {
		// TODO Auto-generated method stub
		return null;
	}

	public Answer createVolume(
			VolumeInfo volume) throws StorageUnavailableException {
		if (s_logger.isDebugEnabled()) {
			s_logger.debug("Creating volume: " + volume);
		}
		
		CreateObjectCommand cmd = new CreateObjectCommand(volume.getTO());
		Answer answer = storageMgr.sendToPool((StoragePool)volume.getDataStore(), null, cmd);
		return answer;
	}

	@Override
	public void createAsync(DataObject data,
			AsyncCompletionCallback<CreateCmdResult> callback) {
		// TODO Auto-generated method stub
		String errMsg = null;
		Answer answer = null;
		if (data.getType() == DataObjectType.VOLUME) {
			try {
				answer = createVolume((VolumeInfo)data);
			} catch (StorageUnavailableException e) {
				s_logger.debug("failed to create volume", e);
				errMsg = e.toString();
			} catch (Exception e) {
				s_logger.debug("failed to create volume", e);
				errMsg = e.toString();
			}
		}
		CreateCmdResult result = new CreateCmdResult(null, answer);
		if (errMsg != null) {
			result.setResult(errMsg);
		}

		callback.complete(result);
	}

	@Override
	public void deleteAsync(DataObject data,
			AsyncCompletionCallback<CommandResult> callback) {

		String vmName = null;
		VolumeVO vol = this.volumeDao.findById(data.getId());


		StoragePool pool = (StoragePool)data.getDataStore();

		DestroyCommand cmd = new DestroyCommand(pool, vol, vmName);

		CommandResult result = new CommandResult();
		try {
			Answer answer = this.storageMgr.sendToPool(pool, cmd);
			if (answer != null && !answer.getResult()) {
				result.setResult(answer.getDetails());
				s_logger.info("Will retry delete of " + vol + " from " + pool.getId());
			}
		} catch (StorageUnavailableException e) {
			s_logger.error("Storage is unavailable currently.  Will retry delete of "
					+ vol + " from " + pool.getId(), e);
			result.setResult(e.toString());
		} catch (Exception ex) {
			s_logger.debug("Unable to destoy volume" + vol + " from " + pool.getId(), ex);
			result.setResult(ex.toString());
		}
		callback.complete(result);
	}

	@Override
	public void copyAsync(DataObject srcdata, DataObject destData,
			AsyncCompletionCallback<CopyCommandResult> callback) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean canCopy(DataObject srcData, DataObject destData) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void takeSnapshot(SnapshotInfo snapshot,
			AsyncCompletionCallback<CreateCmdResult> callback) {
	    CreateCmdResult result = null;
	    try {
	        DataTO snapshotTO = snapshot.getTO();
	        
	        CreateObjectCommand cmd = new CreateObjectCommand(snapshotTO);
	        Answer answer = storageMgr.sendToPool((StoragePool)snapshot.getDataStore(), null, cmd);
	
	        result = new CreateCmdResult(null, answer);
	        if (answer != null && !answer.getResult()) {
	            result.setResult(answer.getDetails());
	        }

	        callback.complete(result);
	    } catch (Exception e) {
	        s_logger.debug("Failed to take snapshot: " + snapshot.getId(), e);
	        result = new CreateCmdResult(null, null);
	        result.setResult(e.toString());
	    }
        callback.complete(result);
	}

	@Override
	public void revertSnapshot(SnapshotInfo snapshot,
			AsyncCompletionCallback<CommandResult> callback) {
		// TODO Auto-generated method stub

	}

	@Override
	public void resize(DataObject data,
			AsyncCompletionCallback<CreateCmdResult> callback) {
		VolumeObject vol = (VolumeObject)data;
		StoragePool pool = (StoragePool)data.getDataStore();
		ResizeVolumePayload resizeParameter = (ResizeVolumePayload)vol.getpayload();

		ResizeVolumeCommand resizeCmd = new ResizeVolumeCommand(
				vol.getPath(), new StorageFilerTO(pool), vol.getSize(),
				resizeParameter.newSize, resizeParameter.shrinkOk, resizeParameter.instanceName);
		CreateCmdResult result = new CreateCmdResult(null, null);
		try {
			ResizeVolumeAnswer answer = (ResizeVolumeAnswer) this.storageMgr.sendToPool(pool,
					resizeParameter.hosts, resizeCmd);
			if (answer != null && answer.getResult()) {
				long finalSize = answer.getNewSize();
				s_logger.debug("Resize: volume started at size " + vol.getSize()
						+ " and ended at size " + finalSize);

				vol.setSize(finalSize);
				vol.update();
			} else if (answer != null) {
				result.setResult(answer.getDetails());
			} else {
				s_logger.debug("return a null answer, mark it as failed for unknown reason");
				result.setResult("return a null answer, mark it as failed for unknown reason");
			}

		} catch (Exception e) {
			s_logger.debug("sending resize command failed", e);
			result.setResult(e.toString());
		}

		callback.complete(result);
	}

}
