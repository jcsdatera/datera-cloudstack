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

package org.apache.cloudstack.storage.datastore.driver;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import org.apache.cloudstack.engine.subsystem.api.storage.ChapInfo;
import org.apache.cloudstack.engine.subsystem.api.storage.CopyCommandResult;
import org.apache.cloudstack.engine.subsystem.api.storage.CreateCmdResult;
import org.apache.cloudstack.engine.subsystem.api.storage.DataStoreCapabilities;
import org.apache.cloudstack.engine.subsystem.api.storage.DataObject;
import org.apache.cloudstack.engine.subsystem.api.storage.DataStore;
import org.apache.cloudstack.engine.subsystem.api.storage.PrimaryDataStoreDriver;
import org.apache.cloudstack.engine.subsystem.api.storage.SnapshotInfo;
import org.apache.cloudstack.engine.subsystem.api.storage.VolumeInfo;
import org.apache.cloudstack.framework.async.AsyncCompletionCallback;
import org.apache.cloudstack.storage.command.CommandResult;
import org.apache.cloudstack.storage.command.CreateObjectAnswer;
import org.apache.cloudstack.storage.datastore.db.PrimaryDataStoreDao;
import org.apache.cloudstack.storage.datastore.db.StoragePoolDetailVO;
import org.apache.cloudstack.storage.datastore.db.StoragePoolDetailsDao;
import org.apache.cloudstack.storage.datastore.db.StoragePoolVO;
import org.apache.cloudstack.storage.datastore.util.DateraObject;
import org.apache.cloudstack.storage.datastore.util.DateraUtil;
import org.apache.cloudstack.storage.to.SnapshotObjectTO;
import org.apache.log4j.Logger;

import com.cloud.agent.api.Answer;
import com.cloud.agent.api.to.DataObjectType;
import com.cloud.agent.api.to.DataStoreTO;
import com.cloud.agent.api.to.DataTO;
import com.cloud.agent.api.to.DiskTO;
import com.cloud.dc.ClusterVO;
import com.cloud.dc.ClusterDetailsVO;
import com.cloud.dc.ClusterDetailsDao;
import com.cloud.dc.dao.ClusterDao;
import com.cloud.host.Host;
import com.cloud.host.HostVO;
import com.cloud.host.dao.HostDao;
import com.cloud.storage.Storage.StoragePoolType;
import com.cloud.storage.ResizeVolumePayload;
import com.cloud.storage.StoragePool;
import com.cloud.storage.Volume;
import com.cloud.storage.VolumeDetailVO;
import com.cloud.storage.VolumeVO;
import com.cloud.storage.SnapshotVO;
import com.cloud.storage.dao.SnapshotDao;
import com.cloud.storage.dao.SnapshotDetailsDao;
import com.cloud.storage.dao.SnapshotDetailsVO;
import com.cloud.storage.dao.VolumeDao;
import com.cloud.storage.dao.VolumeDetailsDao;
import com.cloud.utils.db.GlobalLock;
import com.cloud.utils.exception.CloudRuntimeException;

public class DateraPrimaryDataStoreDriver implements PrimaryDataStoreDriver {
    private static final Logger s_logger = Logger.getLogger(DateraPrimaryDataStoreDriver.class);
    private static final int s_lockTimeInSeconds = 300;
    private static final int s_lowestHypervisorSnapshotReserve = 10;
    private static final String INITIATOR_GROUP_PREFIX = "CSInitiatorGroup-";
    private static final String INITIATOR_PREFIX = "CSInitiator-";
    private static final String APPINSTANCE_PREFIX = "CSAppInstance-";

    @Inject private ClusterDao _clusterDao;
    @Inject private ClusterDetailsDao _clusterDetailsDao;
    @Inject private HostDao _hostDao;
    @Inject private SnapshotDao _snapshotDao;
    @Inject private SnapshotDetailsDao _snapshotDetailsDao;
    @Inject private PrimaryDataStoreDao _storagePoolDao;
    @Inject private StoragePoolDetailsDao _storagePoolDetailsDao;
    @Inject private VolumeDao _volumeDao;
    @Inject private VolumeDetailsDao _volumeDetailsDao;

    @Override
    public Map<String, String> getCapabilities() {
        Map<String, String> mapCapabilities = new HashMap<String, String>();

        mapCapabilities.put(DataStoreCapabilities.STORAGE_SYSTEM_SNAPSHOT.toString(), Boolean.TRUE.toString());

        return mapCapabilities;
    }

    @Override
    public DataTO getTO(DataObject data) {
        return null;
    }

    @Override
    public DataStoreTO getStoreTO(DataStore store) {
        return null;
    }

    @Override
    public ChapInfo getChapInfo(VolumeInfo volumeInfo) {
        return null;
    }

    @Override
    public boolean grantAccess(DataObject dataObject, Host host, DataStore dataStore)
    {
        if (dataObject == null || host == null || dataStore == null) {
            return false;
        }

        long storagePoolId = dataStore.getId();

        DateraObject.DateraConnection conn = DateraUtil.getDateraConnection(storagePoolId, _storagePoolDetailsDao);

        String appInstanceName = getAppInstanceName(dataObject);
        DateraObject.AppInstance appInstance;

        try {
            appInstance = DateraUtil.getAppInstance(conn, appInstanceName);
        } catch (DateraObject.DateraError dateraError) {
            s_logger.warn("Error getting appInstance " + appInstanceName, dateraError);
            throw new CloudRuntimeException(dateraError.getMessage());
        }

        if (appInstance == null){
            throw new CloudRuntimeException("App instance not found " + appInstanceName);
        }

        long clusterId = host.getClusterId();

        ClusterVO cluster = _clusterDao.findById(clusterId);

        GlobalLock lock = GlobalLock.getInternLock(cluster.getUuid());

        if (!lock.lock(s_lockTimeInSeconds)) {
            s_logger.debug("Couldn't lock the DB (in grantAccess) on the following string: " + cluster.getUuid());
        }

        try {

            DateraObject.InitiatorGroup initiatorGroup = null;
            String initiatorGroupKey = DateraUtil.getInitiatorGroupKey(storagePoolId);

            ClusterDetailsVO clusterDetail = _clusterDetailsDao.findDetail(clusterId, initiatorGroupKey);

            String initiatorGroupName = clusterDetail != null ? clusterDetail.getValue() : null;

            List<HostVO> hosts = _hostDao.findByClusterId(clusterId);

            if (!DateraUtil.hostsSupport_iScsi(hosts)) {
                return false;
            }

            // We don't have the initiator group, create one
            if (initiatorGroupName == null) {
                initiatorGroupName = INITIATOR_GROUP_PREFIX + cluster.getUuid();

                initiatorGroup = DateraUtil.getInitiatorGroup(conn, initiatorGroupName);

                if (initiatorGroup == null) {
                    initiatorGroup = DateraUtil.createInitiatorGroup(conn, initiatorGroupName);
                }

                //Save it to the DB
                clusterDetail = new ClusterDetailsVO(clusterId, initiatorGroupKey, initiatorGroupName);
                _clusterDetailsDao.persist(clusterDetail);
            } else {
                initiatorGroup = DateraUtil.getInitiatorGroup(conn, initiatorGroupName);
            }

            //check if we have an initiator for the host
            String iqn = host.getStorageUrl();

            DateraObject.Initiator initiator = DateraUtil.getInitiator(conn, iqn);

            //initiator not found, create it
            if (initiator == null) {

                String initiatorName = INITIATOR_PREFIX + host.getUuid();
                initiator = DateraUtil.createInitiator(conn, initiatorName, iqn);

            }

            Preconditions.checkNotNull(initiator);
            Preconditions.checkNotNull(initiatorGroup);

            if (!isInitiatorPresentInGroup(initiator, initiatorGroup)){
                DateraUtil.addInitiatorToGroup(conn, initiator.getPath(), initiatorGroupName);
            }

            //assgin the initiatorgroup to appInstance
            if (!isInitiatorGroupAssignedToAppInstance(conn, initiatorGroup, appInstance)) {
                DateraUtil.assignGroupToAppInstance(conn, initiatorGroupName, appInstanceName);
                Thread.sleep(3000);
            }

            return true;
        } catch (DateraObject.DateraError | UnsupportedEncodingException | InterruptedException dateraError) {
            s_logger.warn(dateraError.getMessage(), dateraError );
            throw new CloudRuntimeException("Unable to grant access to volume " + dateraError.getMessage());
        } finally {
            lock.unlock();
            lock.releaseRef();
        }
    }

    private boolean isInitiatorGroupAssignedToAppInstance(DateraObject.DateraConnection conn, DateraObject.InitiatorGroup initiatorGroup, DateraObject.AppInstance appInstance) throws DateraObject.DateraError {

        Map<String, DateraObject.InitiatorGroup> assignedInitiatorGroups = DateraUtil.getAppInstanceInitiatorGroups(conn, appInstance.getName());

        Preconditions.checkNotNull(assignedInitiatorGroups);

        for (String groupName : assignedInitiatorGroups.keySet()) {
            if (initiatorGroup.getName().equals(groupName)) {
                return true;
            }
        }

        return false;
    }

    private boolean isInitiatorPresentInGroup(DateraObject.Initiator initiator, DateraObject.InitiatorGroup initiatorGroup) {

        for (String memberPath : initiatorGroup.getMembers() ) {
            if (memberPath.equals(initiator.getPath())) {
                return true;
            }
        }

        return false;
    }

    @Override
    public void revokeAccess(DataObject dataObject, Host host, DataStore dataStore)
    {
        if (dataObject == null || host == null || dataStore == null) {
            return;
        }

        String appInstanceName = getAppInstanceName(dataObject);
        long clusterId = host.getClusterId();
        long storagePoolId = dataStore.getId();

        ClusterVO cluster = _clusterDao.findById(clusterId);

        GlobalLock lock = GlobalLock.getInternLock(cluster.getUuid());

        if (!lock.lock(s_lockTimeInSeconds)) {
            s_logger.debug("Couldn't lock the DB (in revokeAccess) on the following string: " + cluster.getUuid());
        }

        try {
            ClusterDetailsVO clusterDetail = _clusterDetailsDao.findDetail(clusterId, DateraUtil.getInitiatorGroupKey(storagePoolId));

            String initatorGroupName = clusterDetail != null ? clusterDetail.getValue() : null;

            if (initatorGroupName != null) {
                DateraObject.DateraConnection conn = DateraUtil.getDateraConnection(storagePoolId, _storagePoolDetailsDao);
                DateraUtil.removeGroupFromAppInstance(conn, initatorGroupName, appInstanceName);
            }
        } catch (DateraObject.DateraError | UnsupportedEncodingException dateraError) {
            String errMesg = "Error revoking access for Volume : " + dataObject.getId();
            s_logger.warn(errMesg, dateraError);
            throw new CloudRuntimeException(errMesg);
        } finally {
            lock.unlock();
            lock.releaseRef();
        }
    }

    private String getAppInstanceName(DataObject dataObject) {
        return APPINSTANCE_PREFIX + dataObject.getUuid();
    }

    private long getDefaultMinIops(long storagePoolId) {
        StoragePoolDetailVO storagePoolDetail = _storagePoolDetailsDao.findDetail(storagePoolId, DateraUtil.CLUSTER_DEFAULT_MIN_IOPS);

        String clusterDefaultMinIops = storagePoolDetail.getValue();

        return Long.parseLong(clusterDefaultMinIops);
    }

    private long getDefaultMaxIops(long storagePoolId) {
        StoragePoolDetailVO storagePoolDetail = _storagePoolDetailsDao.findDetail(storagePoolId, DateraUtil.CLUSTER_DEFAULT_MAX_IOPS);

        String clusterDefaultMaxIops = storagePoolDetail.getValue();

        return Long.parseLong(clusterDefaultMaxIops);
    }

    private int getNumReplicas(long storagePoolId) {
        StoragePoolDetailVO storagePoolDetail = _storagePoolDetailsDao.findDetail(storagePoolId, DateraUtil.NUM_REPLICAS);

        String clusterDefaultReplicas = storagePoolDetail.getValue();

        return Integer.parseInt(clusterDefaultReplicas);

    }

    @Override
    public long getUsedBytes(StoragePool storagePool) {
        return getUsedBytes(storagePool, Long.MIN_VALUE);
    }

    private long getUsedBytes(StoragePool storagePool, long volumeIdToIgnore) {
        long usedSpace = 0;

        List<VolumeVO> lstVolumes = _volumeDao.findByPoolId(storagePool.getId(), null);

        if (lstVolumes != null) {
            for (VolumeVO volume : lstVolumes) {
                if (volume.getId() == volumeIdToIgnore) {
                    continue;
                }

                VolumeDetailVO volumeDetail = _volumeDetailsDao.findDetail(volume.getId(), DateraUtil.VOLUME_SIZE);

                if (volumeDetail != null && volumeDetail.getValue() != null) {
                    long volumeSize = Long.parseLong(volumeDetail.getValue());

                    usedSpace += volumeSize;
                }
                else {
                    DateraObject.DateraConnection conn = DateraUtil.getDateraConnection(storagePool.getId(), _storagePoolDetailsDao);
                    try {

                        String appInstanceName = getAppInstanceName((DataObject) volume);
                        DateraObject.AppInstance appInstance = DateraUtil.getAppInstance(conn, appInstanceName);
                        if (appInstance != null) {
                            usedSpace += DateraUtil.gbToBytes(appInstance.getSize());
                        }
                    } catch (DateraObject.DateraError dateraError) {
                        String errMesg = "Error getting used bytes for storage pool : " + storagePool.getId();
                        s_logger.warn(errMesg, dateraError);
                        throw new CloudRuntimeException(errMesg);
                    }
                }
            }
        }


        List<SnapshotVO> lstSnapshots = _snapshotDao.listAll();

        if (lstSnapshots != null) {
            for (SnapshotVO snapshot : lstSnapshots) {
                SnapshotDetailsVO snapshotDetails = _snapshotDetailsDao.findDetail(snapshot.getId(), DateraUtil.STORAGE_POOL_ID);

                // if this snapshot belongs to the storagePool that was passed in
                if (snapshotDetails != null && snapshotDetails.getValue() != null && Long.parseLong(snapshotDetails.getValue()) == storagePool.getId()) {
                    snapshotDetails = _snapshotDetailsDao.findDetail(snapshot.getId(), DateraUtil.VOLUME_SIZE);

                    if (snapshotDetails != null && snapshotDetails.getValue() != null) {
                        long snapshotSize = Long.parseLong(snapshotDetails.getValue());

                        usedSpace += snapshotSize;
                    }
                }
            }
        }

        return usedSpace;
    }

    @Override
    public long getUsedIops(StoragePool storagePool) {
        long usedIops = 0;

        List<VolumeVO> volumes = _volumeDao.findByPoolId(storagePool.getId(), null);

        if (volumes != null) {
            for (VolumeVO volume : volumes) {
                usedIops += volume.getMinIops() != null ? volume.getMinIops() : 0;
            }
        }

        return usedIops;
    }

    @Override
    public long getVolumeSizeIncludingHypervisorSnapshotReserve(Volume volume, StoragePool pool) {
        long volumeSize = volume.getSize();
        Integer hypervisorSnapshotReserve = volume.getHypervisorSnapshotReserve();

        if (hypervisorSnapshotReserve != null) {
            hypervisorSnapshotReserve = Math.max(hypervisorSnapshotReserve, s_lowestHypervisorSnapshotReserve);

            volumeSize += volumeSize * (hypervisorSnapshotReserve / 100f);
        }

        return volumeSize;
    }

    private void deleteVolume(DateraObject.DateraConnection conn, VolumeInfo volumeInfo) {

        Long storagePoolId = volumeInfo.getPoolId();

        if (storagePoolId == null) {
            return; // this volume was never assigned to a storage pool, so no SAN volume should exist for it
        }

        try {
            DateraUtil.deleteAppInstance(conn, getAppInstanceName(volumeInfo));
        } catch (UnsupportedEncodingException | DateraObject.DateraError e) {
            String errMesg = "Error deleting app instance for Volume : " + volumeInfo.getId();
            s_logger.warn(errMesg, e);
            throw new CloudRuntimeException(errMesg);
        }
    }

    private DateraObject.AppInstance createVolume(DateraObject.DateraConnection conn, VolumeInfo volumeInfo) {

         Long storagePoolId = volumeInfo.getPoolId();

        if (storagePoolId == null) {
            return null;
        }

        try {

            int minIops = Ints.checkedCast(volumeInfo.getMinIops());
            int maxIops = Ints.checkedCast(volumeInfo.getMaxIops());

            if (maxIops <= 0) {  // We don't care about min iops for now
                maxIops = Ints.checkedCast(getDefaultMaxIops(storagePoolId));
            }

            int replicas = getNumReplicas(storagePoolId);

            long volumeSizeBytes = getVolumeSizeIncludingHypervisorSnapshotReserve(volumeInfo, _storagePoolDao.findById(storagePoolId));
            int volumeSizeGb = DateraUtil.bytesToGb(volumeSizeBytes);

            return DateraUtil.createAppInstance(conn, getAppInstanceName(volumeInfo),  volumeSizeGb, maxIops, replicas);

        } catch (UnsupportedEncodingException | DateraObject.DateraError e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void createAsync(DataStore dataStore, DataObject dataObject, AsyncCompletionCallback<CreateCmdResult> callback) {
        String iqn = null;
        String errMsg = null;

        String iqnPath = null;
        if (dataObject.getType() == DataObjectType.VOLUME) {
            VolumeInfo volumeInfo = (VolumeInfo) dataObject;

            long storagePoolId = dataStore.getId();

            DateraObject.DateraConnection conn = DateraUtil.getDateraConnection(storagePoolId, _storagePoolDetailsDao);


            DateraObject.AppInstance appInstance = createVolume(conn, volumeInfo);

            Preconditions.checkNotNull(appInstance);

            iqn = appInstance.getIqn();
            iqnPath = DateraUtil.generateIqnPath(iqn);

            VolumeVO volume = _volumeDao.findById(volumeInfo.getId());

            volume.set_iScsiName(iqnPath);
            volume.setFolder(appInstance.getName());
            volume.setPoolType(StoragePoolType.IscsiLUN);
            volume.setPoolId(storagePoolId);

            _volumeDao.update(volume.getId(), volume);

            updateVolumeDetails(volume.getId(), appInstance.getSize());

            StoragePoolVO storagePool = _storagePoolDao.findById(dataStore.getId());

            long capacityBytes = storagePool.getCapacityBytes();
            long usedBytes = getUsedBytes(storagePool);

            storagePool.setUsedBytes(usedBytes > capacityBytes ? capacityBytes : usedBytes);

            _storagePoolDao.update(storagePoolId, storagePool);

        } else {
            errMsg = "Invalid DataObjectType (" + dataObject.getType() + ") passed to createAsync";
        }

        CreateCmdResult result = new CreateCmdResult(iqnPath, new Answer(null, errMsg == null, errMsg));

        result.setResult(errMsg);

        callback.complete(result);
    }

    private void updateVolumeDetails(long volumeId, long volumeSize) {
        VolumeDetailVO volumeDetailVo = _volumeDetailsDao.findDetail(volumeId, DateraUtil.VOLUME_SIZE);

        if (volumeDetailVo == null || volumeDetailVo.getValue() == null) {
            volumeDetailVo = new VolumeDetailVO(volumeId, DateraUtil.VOLUME_SIZE, String.valueOf(volumeSize), false);

            _volumeDetailsDao.persist(volumeDetailVo);
        }
    }

    @Override
    public void deleteAsync(DataStore dataStore, DataObject dataObject, AsyncCompletionCallback<CommandResult> callback) {
        String errMsg = null;

        if (dataObject.getType() == DataObjectType.VOLUME) {
            try {
                VolumeInfo volumeInfo = (VolumeInfo)dataObject;
                long volumeId = volumeInfo.getId();

                long storagePoolId = dataStore.getId();

                DateraObject.DateraConnection conn = DateraUtil.getDateraConnection(storagePoolId, _storagePoolDetailsDao);

                deleteVolume(conn, volumeInfo);

                _volumeDetailsDao.removeDetails(volumeId);

                StoragePoolVO storagePool = _storagePoolDao.findById(storagePoolId);

                long usedBytes = getUsedBytes(storagePool, volumeId);

                storagePool.setUsedBytes(usedBytes < 0 ? 0 : usedBytes);

                _storagePoolDao.update(storagePoolId, storagePool);
            }
            catch (Exception ex) {
                s_logger.debug("Failed to delete volume ", ex);

                errMsg = ex.getMessage();
            }
        } else if (dataObject.getType() == DataObjectType.SNAPSHOT) {

            errMsg = deleteSnapshot((SnapshotInfo)dataObject, dataStore.getId());

        } else {
            errMsg = "Invalid DataObjectType (" + dataObject.getType() + ") passed to deleteAsync";
        }

        CommandResult result = new CommandResult();

        result.setResult(errMsg);

        callback.complete(result);
    }

    @Override
    public void copyAsync(DataObject srcData, DataObject destData, AsyncCompletionCallback<CopyCommandResult> callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean canCopy(DataObject srcData, DataObject destData) {
        return false;
    }

    @Override
    public void takeSnapshot(SnapshotInfo snapshotInfo, AsyncCompletionCallback<CreateCmdResult> callback) {
        CreateCmdResult result;

        try {
            VolumeInfo volumeInfo = snapshotInfo.getBaseVolume();
            VolumeVO volumeVO = _volumeDao.findById(volumeInfo.getId());

            long storagePoolId = volumeVO.getPoolId();

            DateraObject.DateraConnection conn = DateraUtil.getDateraConnection(storagePoolId, _storagePoolDetailsDao);

            String baseAppInstanceName = getAppInstanceName(volumeInfo);

            DateraObject.AppInstance baseAppInstance = DateraUtil.getAppInstance(conn, baseAppInstanceName);

            Preconditions.checkNotNull(baseAppInstance);

            StoragePoolVO storagePool = _storagePoolDao.findById(storagePoolId);

            long capacityBytes = storagePool.getCapacityBytes();
            long usedBytes = getUsedBytes(storagePool);
            int volumeSizeGb = baseAppInstance.getSize();
            long volumeSizeBytes = DateraUtil.gbToBytes(volumeSizeGb);

            usedBytes += volumeSizeBytes;

            if (usedBytes > capacityBytes) {
                throw new CloudRuntimeException("Insufficient amount of space remains in this primary storage to take a snapshot");
            }

            storagePool.setUsedBytes(usedBytes);


            String appInstanceName  = getAppInstanceName(snapshotInfo);
            DateraObject.AppInstance snapshotAppInstance = DateraUtil.createAppInstance(conn, appInstanceName,
                    volumeSizeGb, baseAppInstance.getTotalIops(), getNumReplicas(storagePoolId));

            //update size in storage pool
            _storagePoolDao.update(storagePoolId, storagePool);

            String iqnPath = DateraUtil.generateIqnPath(snapshotAppInstance.getIqn());

            updateSnapshotDetails(snapshotInfo.getId(), snapshotAppInstance.getName(), storagePoolId, snapshotAppInstance.getSize(), iqnPath);

            SnapshotObjectTO snapshotObjectTo = (SnapshotObjectTO)snapshotInfo.getTO();

            snapshotObjectTo.setPath(snapshotAppInstance.getName());

            CreateObjectAnswer createObjectAnswer = new CreateObjectAnswer(snapshotObjectTo);

            result = new CreateCmdResult(null, createObjectAnswer);

            result.setResult(null);
        }
        catch (Exception ex) {
            s_logger.debug("Failed to take CloudStack snapshot: " + snapshotInfo.getId(), ex);

            result = new CreateCmdResult(null, new CreateObjectAnswer(ex.toString()));

            result.setResult(ex.toString());
        }

        callback.complete(result);
    }

    private void updateSnapshotDetails(long csSnapshotId, String snapshotAppInstanceName, long storagePoolId, long snapshotSizeGb, String snapshotIqn) {
        SnapshotDetailsVO snapshotDetail = new SnapshotDetailsVO(csSnapshotId,
                DateraUtil.VOLUME_ID,
                String.valueOf(snapshotAppInstanceName),
                false);

        _snapshotDetailsDao.persist(snapshotDetail);

        snapshotDetail = new SnapshotDetailsVO(csSnapshotId,
                DateraUtil.STORAGE_POOL_ID,
                String.valueOf(storagePoolId),
                false);

        _snapshotDetailsDao.persist(snapshotDetail);

        snapshotDetail = new SnapshotDetailsVO(csSnapshotId,
                DateraUtil.VOLUME_SIZE,
                String.valueOf(snapshotSizeGb),
                false);

        _snapshotDetailsDao.persist(snapshotDetail);

        snapshotDetail = new SnapshotDetailsVO(csSnapshotId,
                DiskTO.IQN,
                snapshotIqn,
                false);

        _snapshotDetailsDao.persist(snapshotDetail);
    }

    // return null for no error message
    private String deleteSnapshot(SnapshotInfo snapshotInfo, long storagePoolId) {
        String errMsg = null;

        long snapshotId = snapshotInfo.getId();

        try {
            DateraObject.DateraConnection conn = DateraUtil.getDateraConnection(storagePoolId, _storagePoolDetailsDao);

            SnapshotDetailsVO snapshotDetails = _snapshotDetailsDao.findDetail(snapshotId, DateraUtil.VOLUME_ID);

            String snapshotAppInstanceName = snapshotDetails.getValue();

            DateraObject.AppInstance snapshotAppInstance = DateraUtil.getAppInstance(conn, snapshotAppInstanceName);

            if (snapshotAppInstance == null) {
                return null;
            }

            DateraUtil.deleteAppInstance(conn, snapshotAppInstanceName);

            _snapshotDetailsDao.removeDetails(snapshotId);

            StoragePoolVO storagePool = _storagePoolDao.findById(storagePoolId);

            // getUsedBytes(StoragePool) will not include the snapshot to delete because it has already been deleted by this point
            long usedBytes = getUsedBytes(storagePool);

            storagePool.setUsedBytes(usedBytes < 0 ? 0 : usedBytes);

            _storagePoolDao.update(storagePoolId, storagePool);
        }
        catch (Exception ex) {
            s_logger.debug("Failed to delete volume. CloudStack snapshot ID: " + snapshotId, ex);

            errMsg = ex.getMessage();
        }

        return errMsg;
    }

    @Override
    public void revertSnapshot(SnapshotInfo snapshot, SnapshotInfo snapshotOnPrimaryStore, AsyncCompletionCallback<CommandResult> callback) {
        throw new UnsupportedOperationException("Reverting not supported. Create a template or volume based on the snapshot instead.");
    }

    @Override
    public void resize(DataObject dataObject, AsyncCompletionCallback<CreateCmdResult> callback) {
        String iqn = null;
        String errMsg = null;

        if (dataObject.getType() == DataObjectType.VOLUME) {
            VolumeInfo volumeInfo = (VolumeInfo)dataObject;
            iqn = volumeInfo.get_iScsiName();
            long storagePoolId = volumeInfo.getPoolId();
            ResizeVolumePayload payload = (ResizeVolumePayload)volumeInfo.getpayload();
            String appInstanceName = getAppInstanceName(volumeInfo);
            int newSize = DateraUtil.bytesToGb(payload.newSize);

            DateraObject.DateraConnection conn = DateraUtil.getDateraConnection(storagePoolId, _storagePoolDetailsDao);

            try {
                DateraObject.AppInstance appInstance = DateraUtil.getAppInstance(conn, appInstanceName);

                Preconditions.checkNotNull(appInstance);

                verifySufficientIopsForStoragePool(storagePoolId, volumeInfo.getId(), payload.newMinIops);

                if (appInstance.getSize() < newSize) {
                    DateraUtil.updateAppInstanceSize(conn, appInstanceName, Ints.checkedCast(newSize));
                }

                if (appInstance.getTotalIops() < payload.newMaxIops) {
                    DateraUtil.updateAppInstanceIops(conn, appInstanceName, Ints.checkedCast(payload.newMaxIops));
                }

                appInstance = DateraUtil.getAppInstance(conn, appInstanceName);

                Preconditions.checkNotNull(appInstance);

                VolumeVO volume = _volumeDao.findById(volumeInfo.getId());

                volume.setMinIops(payload.newMinIops);
                volume.setMaxIops(payload.newMaxIops);

                _volumeDao.update(volume.getId(), volume);

                updateVolumeDetails(volume.getId(), appInstance.getSize());

                Preconditions.checkNotNull(appInstance);

            } catch (DateraObject.DateraError | UnsupportedEncodingException dateraError) {
                dateraError.printStackTrace();
            }

        } else {
            errMsg = "Invalid DataObjectType (" + dataObject.getType() + ") passed to resize";
        }

        CreateCmdResult result = new CreateCmdResult(iqn, new Answer(null, errMsg == null, errMsg));

        result.setResult(errMsg);

        callback.complete(result);
    }

    private void verifySufficientIopsForStoragePool(long storagePoolId, long volumeId, long newMinIops) {
        StoragePoolVO storagePool = _storagePoolDao.findById(storagePoolId);
        VolumeVO volume = _volumeDao.findById(volumeId);

        long currentMinIops = volume.getMinIops();
        long diffInMinIops = newMinIops - currentMinIops;

        // if the desire is for more IOPS
        if (diffInMinIops > 0) {
            long usedIops = getUsedIops(storagePool);
            long capacityIops = storagePool.getCapacityIops();

            if (usedIops + diffInMinIops > capacityIops) {
                throw new CloudRuntimeException("Insufficient number of IOPS available in this storage pool");
            }
        }
    }
}
