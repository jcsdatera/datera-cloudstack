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

import com.cloud.agent.api.Answer;
import com.cloud.agent.api.to.DataObjectType;
import com.cloud.agent.api.to.DataStoreTO;
import com.cloud.agent.api.to.DataTO;
import com.cloud.agent.api.to.DiskTO;
import com.cloud.dc.ClusterDetailsDao;
import com.cloud.dc.ClusterDetailsVO;
import com.cloud.dc.ClusterVO;
import com.cloud.dc.dao.ClusterDao;
import com.cloud.host.Host;
import com.cloud.host.HostVO;
import com.cloud.host.dao.HostDao;
import com.cloud.storage.ResizeVolumePayload;
import com.cloud.storage.SnapshotVO;
import com.cloud.storage.Storage.StoragePoolType;
import com.cloud.storage.StoragePool;
import com.cloud.storage.Volume;
import com.cloud.storage.VolumeDetailVO;
import com.cloud.storage.VolumeVO;
import com.cloud.storage.dao.SnapshotDao;
import com.cloud.storage.dao.SnapshotDetailsDao;
import com.cloud.storage.dao.SnapshotDetailsVO;
import com.cloud.storage.dao.VolumeDao;
import com.cloud.storage.dao.VolumeDetailsDao;
import com.cloud.utils.StringUtils;
import com.cloud.utils.db.GlobalLock;
import com.cloud.utils.exception.CloudRuntimeException;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import org.apache.cloudstack.engine.subsystem.api.storage.ChapInfo;
import org.apache.cloudstack.engine.subsystem.api.storage.CopyCommandResult;
import org.apache.cloudstack.engine.subsystem.api.storage.CreateCmdResult;
import org.apache.cloudstack.engine.subsystem.api.storage.DataObject;
import org.apache.cloudstack.engine.subsystem.api.storage.DataStore;
import org.apache.cloudstack.engine.subsystem.api.storage.DataStoreCapabilities;
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

import javax.inject.Inject;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DateraPrimaryDataStoreDriver implements PrimaryDataStoreDriver {
    private static final Logger s_logger = Logger.getLogger(DateraPrimaryDataStoreDriver.class);
    private static final int s_lockTimeInSeconds = 300;
    private static final int s_lowestHypervisorSnapshotReserve = 10;

    @Inject private ClusterDao _clusterDao;
    @Inject private ClusterDetailsDao _clusterDetailsDao;
    @Inject private HostDao _hostDao;
    @Inject private SnapshotDao _snapshotDao;
    @Inject private SnapshotDetailsDao _snapshotDetailsDao;
    @Inject private PrimaryDataStoreDao _storagePoolDao;
    @Inject private StoragePoolDetailsDao _storagePoolDetailsDao;
    @Inject private VolumeDao _volumeDao;
    @Inject private VolumeDetailsDao _volumeDetailsDao;

    /**
     * Returns a map which lists the capabilities that this storage device can offer. Currently supported
     * STORAGE_SYSTEM_SNAPSHOT: Has the ability to create native snapshots
     *
     * @return a Map<String,String> which determines the capabilities of the driver
     *
     */
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
        // We don't support auth yet
        return null;
    }

    /**
     * Fetches an App Instance from Datera, throws exception if it doesn't find it
     * @param conn Datera Connection
     * @param appInstanceName Name of the Aplication Instance
     * @return application instance
     */
    public DateraObject.AppInstance getDateraAppInstance(DateraObject.DateraConnection conn, String appInstanceName) {

        DateraObject.AppInstance appInstance = null;
        try {
            appInstance = DateraUtil.getAppInstance(conn, appInstanceName);
        } catch (DateraObject.DateraError dateraError) {
            s_logger.warn("Error getting appInstance " + appInstanceName, dateraError);
            throw new CloudRuntimeException(dateraError.getMessage());
        }

        if (appInstance == null){
            throw new CloudRuntimeException("App instance not found " + appInstanceName);
        }

        return appInstance;
    }

    /**
     * Given a {@code dataObject} this function makes sure that the {@code host} has access to it.
     * All hosts which are in the same cluster are added to an initiator group and that group is assigned
     * to the appInstance. If an initiator group does not exist, it is created. If the host does not have
     * an initiator registered on dataera, that is created and added to the initiator group
     *
     * @param dataObject The volume that needs to be accessed
     * @param host  The host which needs to access the volume
     * @param dataStore Identifies which primary storage the volume resides in
     * @return True if access is granted. False otherwise
     */
    @Override
    public boolean grantAccess(DataObject dataObject, Host host, DataStore dataStore) {

        Preconditions.checkArgument(dataObject != null, "'dataObject' should not be 'null'");
        Preconditions.checkArgument(host != null, "'host' should not be 'null'");
        Preconditions.checkArgument(dataStore != null, "'dataStore' should not be 'null'");

        long storagePoolId = dataStore.getId();

        DateraObject.DateraConnection conn = DateraUtil.getDateraConnection(storagePoolId, _storagePoolDetailsDao);

        String appInstanceName = getAppInstanceName(dataObject);
        DateraObject.AppInstance appInstance = getDateraAppInstance(conn, appInstanceName);

        Preconditions.checkArgument(appInstance != null);

        long clusterId = host.getClusterId();

        ClusterVO cluster = _clusterDao.findById(clusterId);

        GlobalLock lock = GlobalLock.getInternLock(cluster.getUuid());

        if (!lock.lock(s_lockTimeInSeconds)) {
            s_logger.debug("Couldn't lock the DB (in grantAccess) on the following string: " + cluster.getUuid());
        }

        try {

            DateraObject.InitiatorGroup initiatorGroup = null;
            String initiatorGroupKey = DateraUtil.getInitiatorGroupKey(storagePoolId);

            List<HostVO> hosts = _hostDao.findByClusterId(clusterId);

            if (!DateraUtil.hostsSupport_iScsi(hosts)) {
                return false;
            }

            // We don't have the initiator group, create one
            String initiatorGroupName = DateraUtil.INITIATOR_GROUP_PREFIX +  "-" + cluster.getUuid();

            initiatorGroup = DateraUtil.getInitiatorGroup(conn, initiatorGroupName);

            if (initiatorGroup == null) {

                initiatorGroup = DateraUtil.createInitiatorGroup(conn, initiatorGroupName);
                //Save it to the DB
                ClusterDetailsVO clusterDetail = new ClusterDetailsVO(clusterId, initiatorGroupKey, initiatorGroupName);
                _clusterDetailsDao.persist(clusterDetail);

            } else {
                initiatorGroup = DateraUtil.getInitiatorGroup(conn, initiatorGroupName);
            }

            Preconditions.checkNotNull(initiatorGroup);

            // We create an initiator for every host in this cluster and add it to the initator group
            addClusterHostsToInitiatorGroup(conn, clusterId, initiatorGroupName);

            //assgin the initiatorgroup to appInstance
            if (!isInitiatorGroupAssignedToAppInstance(conn, initiatorGroup, appInstance)) {
                DateraUtil.assignGroupToAppInstance(conn, initiatorGroupName, appInstanceName);
                int retries = DateraUtil.DEFAULT_RETRIES;
                while (!isInitiatorGroupAssignedToAppInstance(conn, initiatorGroup, appInstance) && retries > 0) {
                    Thread.sleep(DateraUtil.POLL_TIMEOUT_MS);
                    retries--;
                }
                //FIXME: Sleep anyways
                Thread.sleep(9000); // ms
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

    private void addClusterHostsToInitiatorGroup(DateraObject.DateraConnection conn, long clusterId, String initiatorGroupName) throws DateraObject.DateraError, UnsupportedEncodingException {

        List<HostVO> clusterHosts = _hostDao.findByClusterId(clusterId);
        DateraObject.InitiatorGroup initiatorGroup = DateraUtil.getInitiatorGroup(conn, initiatorGroupName);

        for (HostVO host : clusterHosts) {

            //check if we have an initiator for the host
            String iqn = host.getStorageUrl();

            DateraObject.Initiator initiator = DateraUtil.getInitiator(conn, iqn);

            //initiator not found, create it
            if (initiator == null) {

                String initiatorName = DateraUtil.INITIATOR_PREFIX + "-" + host.getUuid();
                initiator = DateraUtil.createInitiator(conn, initiatorName, iqn);
            }

            Preconditions.checkNotNull(initiator);

            if (!DateraUtil.isInitiatorPresentInGroup(initiator, initiatorGroup)) {
                DateraUtil.addInitiatorToGroup(conn, initiator.getPath(), initiatorGroupName);
            }
        }
    }

    /**
     * Checks if an initiator group is assigned to an appInstance
     * @param conn Datera connection
     * @param initiatorGroup Initiator group to check
     * @param appInstance App Instance
     * @return  True if initiator group is assigned to app instnace, false otherwise
     *
     * @throws DateraObject.DateraError
     */

    private boolean isInitiatorGroupAssignedToAppInstance(DateraObject.DateraConnection conn, DateraObject.InitiatorGroup initiatorGroup, DateraObject.AppInstance appInstance) throws DateraObject.DateraError {

        Map<String, DateraObject.InitiatorGroup> assignedInitiatorGroups = DateraUtil.getAppInstanceInitiatorGroups(conn, appInstance.getName());

        Preconditions.checkNotNull(assignedInitiatorGroups);

        for (DateraObject.InitiatorGroup ig : assignedInitiatorGroups.values()) {
            if (initiatorGroup.getName().equals(ig.getName())) {
                return true;
            }
        }

        return false;
    }

    /**
     * Removes access of the initiator group to which {@code host} belongs from the appInstance
     * given by {@code dataObject}
     *
     * @param dataObject Datera volume
     * @param host  the host which is currently having access to the volume
     * @param dataStore The primary store to which volume belongs
     */
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
            String initiatorGroupName = DateraUtil.INITIATOR_GROUP_PREFIX +  "-" + cluster.getUuid();

            DateraObject.DateraConnection conn = DateraUtil.getDateraConnection(storagePoolId, _storagePoolDetailsDao);

            DateraObject.AppInstance appInstance = DateraUtil.getAppInstance(conn, appInstanceName);
            DateraObject.InitiatorGroup initiatorGroup = DateraUtil.getInitiatorGroup(conn, initiatorGroupName);

            if (initiatorGroup != null && appInstance != null) {
                DateraUtil.removeGroupFromAppInstance(conn, initiatorGroupName, appInstanceName);
                int retries = DateraUtil.DEFAULT_RETRIES;
                while (isInitiatorGroupAssignedToAppInstance(conn, initiatorGroup, appInstance) && retries > 0) {
                    Thread.sleep(DateraUtil.POLL_TIMEOUT_MS);
                    retries--;
                }
            }
        } catch (DateraObject.DateraError | UnsupportedEncodingException | InterruptedException dateraError) {
            String errMesg = "Error revoking access for Volume : " + dataObject.getId();
            s_logger.warn(errMesg, dateraError);
            throw new CloudRuntimeException(errMesg);
        } finally {
            lock.unlock();
            lock.releaseRef();
        }
    }

    private String getAppInstanceName(DataObject dataObject) {
        ArrayList<String> name = new ArrayList<>();

        name.add(DateraUtil.APPINSTANCE_PREFIX);
        name.add(dataObject.getType().toString());
        name.add(dataObject.getUuid());

        return StringUtils.join("-", name.toArray());

    }

    // Not being used right now as Datera doesn't support min IOPS
    private long getDefaultMinIops(long storagePoolId) {
        StoragePoolDetailVO storagePoolDetail = _storagePoolDetailsDao.findDetail(storagePoolId, DateraUtil.CLUSTER_DEFAULT_MIN_IOPS);

        String clusterDefaultMinIops = storagePoolDetail.getValue();

        return Long.parseLong(clusterDefaultMinIops);
    }

    /**
     * If user doesn't specify the IOPS, use this IOPS
     * @param storagePoolId the primary storage
     * @return default max IOPS for this storage configured when the storage is added
     */
    private long getDefaultMaxIops(long storagePoolId) {
        StoragePoolDetailVO storagePoolDetail = _storagePoolDetailsDao.findDetail(storagePoolId, DateraUtil.CLUSTER_DEFAULT_MAX_IOPS);

        String clusterDefaultMaxIops = storagePoolDetail.getValue();

        return Long.parseLong(clusterDefaultMaxIops);
    }

     /**
     * Return the default number of replicas to use (configured at storage addition time)
     * @param storagePoolId the primary storage
     * @return the number of replicas to use
     */
    private int getNumReplicas(long storagePoolId) {
        StoragePoolDetailVO storagePoolDetail = _storagePoolDetailsDao.findDetail(storagePoolId, DateraUtil.NUM_REPLICAS);

        String clusterDefaultReplicas = storagePoolDetail.getValue();

        return Integer.parseInt(clusterDefaultReplicas);

    }

    /**
    * Return the default volume placement to use (configured at storage addition time)
    * @param storagePoolId the primary storage
    * @return volume placement string
    */
   private String getVolPlacement(long storagePoolId) {
       StoragePoolDetailVO storagePoolDetail = _storagePoolDetailsDao.findDetail(storagePoolId, DateraUtil.VOL_PLACEMENT);

       String clusterDefaultVolPlacement = storagePoolDetail.getValue();

       return clusterDefaultVolPlacement;

   }

    @Override
    public long getUsedBytes(StoragePool storagePool) {
        return getUsedBytes(storagePool, Long.MIN_VALUE);
    }

    /**
     * Get the total space used by all the entities on the storage.
     *
     * Total space = volume space + snapshot space + template space
     *
     * @param storagePool Primary storage
     * @param volumeIdToIgnore Ignore this volume (used when we delete a volume and want to update the space)
     * @return size in bytes
     */
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
            String volumePlacement = getVolPlacement(storagePoolId);

            long volumeSizeBytes = getVolumeSizeIncludingHypervisorSnapshotReserve(volumeInfo, _storagePoolDao.findById(storagePoolId));
            int volumeSizeGb = DateraUtil.bytesToGb(volumeSizeBytes);

            if (volumePlacement==null) {
                return DateraUtil.createAppInstance(conn, getAppInstanceName(volumeInfo),  volumeSizeGb, maxIops, replicas);
            } else {
                return DateraUtil.createAppInstance(conn, getAppInstanceName(volumeInfo),  volumeSizeGb, maxIops, replicas, volumePlacement);
            }

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

    /**
     * Deletes snapshot on Datera
     * @param snapshotInfo  snapshot information
     * @param storagePoolId primary storage
     * @throws UnsupportedEncodingException
     * @throws DateraObject.DateraError
     */
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

    /**
     * Resizes a volume on Datera, shrinking is not allowed. Resize also takes into account the HSR
     * @param dataObject volume to resize
     * @param callback  async context
     */
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

            long volumeSizeBytes = getVolumeSizeIncludingHypervisorSnapshotReserve(volumeInfo, _storagePoolDao.findById(storagePoolId));
            int newSize = DateraUtil.bytesToGb(volumeSizeBytes);

            DateraObject.DateraConnection conn = DateraUtil.getDateraConnection(storagePoolId, _storagePoolDetailsDao);

            try {
                DateraObject.AppInstance appInstance = DateraUtil.getAppInstance(conn, appInstanceName);

                Preconditions.checkNotNull(appInstance);

                if (appInstance.getSize() < newSize) {
                    DateraUtil.updateAppInstanceSize(conn, appInstanceName, Ints.checkedCast(newSize));
                }

                if (payload.newMaxIops != null && appInstance.getTotalIops() != payload.newMaxIops) {
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
}
