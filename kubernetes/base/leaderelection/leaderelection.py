# Copyright 2021 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import sys
import time
import json
import threading
from .leaderelectionrecord import LeaderElectionRecord
import logging
from kubernetes.client.rest import ApiException
from kubernetes import client, config
from kubernetes.client.api_client import ApiClient
import signal


# if condition to be removed when support for python2 will be removed
if sys.version_info > (3, 0):
    from http import HTTPStatus
else:
    import httplib
logging.basicConfig(level=logging.INFO)

"""
This package implements leader election using an annotation in a Kubernetes object.
The onstarted_leading function is run in a thread and when it returns, if it does 
it might not be safe to run it again in a process.

At first all candidates are considered followers. The one to create a lock or update
an existing lock first becomes the leader and remains so until it keeps renewing its
lease.
"""

def handle_sigint(signal_received, frame):
    print("\nSIGINT received! Cancelling election...")
    if LeaderElection.global_context:
        LeaderElection.global_context.cancel()


class LeaderElectionRecord:
    # Annotation used in the lock object

    def __init__(self, holder_identity: int, lease_duration: int, acquire_time: int, renew_time: int):
        """
        pre: (lease_duration > renew_time)
            (renew_time > 1)
            (lease_duration > 1)
        """
        self.holder_identity = holder_identity
        self.lease_duration = lease_duration
        self.acquire_time = acquire_time
        self.renew_time = renew_time

class ConfigMapLock:
    def __init__(self, name: str, namespace: str, identity: int):
        """
        :param name: name of the lock
        :param namespace: namespace
        :param identity: A unique identifier that the candidate is using
        """
        self.api_instance = client.CoreV1Api()
        self.leader_electionrecord_annotationkey = 'control-plane.alpha.kubernetes.io/leader'
        self.name = name
        self.namespace = namespace
        self.identity = str(identity)
        self.configmap_reference = None
        self.lock_record = {
            'holderIdentity': None,
            'leaseDurationSeconds': None,
            'acquireTime': None,
            'renewTime': None
                            }

    # get returns the election record from a ConfigMap Annotation
    def get(self, name: str, namespace: str):
        """
        :param name: Name of the configmap object information to get
        :param namespace: Namespace in which the configmap object is to be searched
        :return: 'True, election record' if object found else 'False, exception response'
        """
        try:
            api_response = self.api_instance.read_namespaced_config_map(name, namespace)

            # If an annotation does not exist - add the leader_electionrecord_annotationkey
            annotations = api_response.metadata.annotations
            if annotations is None or annotations == '':
                api_response.metadata.annotations = {self.leader_electionrecord_annotationkey: ''}
                self.configmap_reference = api_response
                return True, None

            # If an annotation exists but, the leader_electionrecord_annotationkey does not then add it as a key
            if not annotations.get(self.leader_electionrecord_annotationkey):
                api_response.metadata.annotations = {self.leader_electionrecord_annotationkey: ''}
                self.configmap_reference = api_response
                return True, None

            lock_record = self.get_lock_object(json.loads(annotations[self.leader_electionrecord_annotationkey]))

            self.configmap_reference = api_response
            return True, lock_record
        except ApiException as e:
            return False, e

    def create(self, name: str, namespace: str, election_record: LeaderElectionRecord):
        """
        :param electionRecord: Annotation string
        :param name: Name of the configmap object to be created
        :param namespace: Namespace in which the configmap object is to be created
        :return: 'True' if object is created else 'False' if failed
        """
        body = client.V1ConfigMap(
            metadata={"name": name,
                      "annotations": {self.leader_electionrecord_annotationkey: json.dumps(self.get_lock_dict(election_record))}})

        try:
            api_response = self.api_instance.create_namespaced_config_map(namespace, body, pretty=True)
            return True
        except ApiException as e:
            logging.info("Failed to create lock as {}".format(e))
            return False

    def update(self, name: str, namespace: str, updated_record: LeaderElectionRecord):
        """
        :param name: name of the lock to be updated
        :param namespace: namespace the lock is in
        :param updated_record: the updated election record
        :return: True if update is successful False if it fails
        """
        try:
            # Set the updated record
            self.configmap_reference.metadata.annotations[self.leader_electionrecord_annotationkey] = json.dumps(self.get_lock_dict(updated_record))
            api_response = self.api_instance.replace_namespaced_config_map(name=name, namespace=namespace,
                                                                           body=self.configmap_reference)
            return True
        except ApiException as e:
            logging.info("Failed to update lock as {}".format(e))
            return False

    def get_lock_object(self, lock_record):
        """
        pre: (lock_record['leaseDurationSeconds'] > lock_record['renewTime'])
             (lock_record['renewTime'] > 1)
             (lock_record['leaseDurationSeconds'] > 1)
        """
        leader_election_record = LeaderElectionRecord(None, None, None, None)

        if lock_record.get('holderIdentity'):
            leader_election_record.holder_identity = lock_record['holderIdentity']
        if lock_record.get('leaseDurationSeconds'):
            leader_election_record.lease_duration = lock_record['leaseDurationSeconds']
        if lock_record.get('acquireTime'):
            leader_election_record.acquire_time = lock_record['acquireTime']
        if lock_record.get('renewTime'):
            leader_election_record.renew_time = lock_record['renewTime']

        return leader_election_record

    def get_lock_dict(self, leader_election_record):
        self.lock_record['holderIdentity'] = leader_election_record.holder_identity
        self.lock_record['leaseDurationSeconds'] = leader_election_record.lease_duration
        self.lock_record['acquireTime'] = leader_election_record.acquire_time
        self.lock_record['renewTime'] = leader_election_record.renew_time

        return self.lock_record

class Config:

    # Validate config, exit if an error is detected
    def __init__(self, lock: ConfigMapLock, lease_duration: int, renew_deadline: int, retry_period: int, onstarted_leading, onstopped_leading, context: Context):
        """

        pre: (lease_duration > renew_deadline)
             (renew_deadline > 1)
             (retry_period > 1)
             (renew_deadline > self.jitter_factor * retry_period)
             (lease_duration > 1)
        """
        self.jitter_factor = 2

        if lock is None:
            print("lock error")
            # sys.exit("lock cannot be None")
        self.lock = lock

        if lease_duration <= renew_deadline:
            print("lease less than renew")
            # sys.exit("lease_duration must be greater than renew_deadline")

        if renew_deadline <= self.jitter_factor * retry_period:
            print("renew less than retry")
            # sys.exit("renewDeadline must be greater than retry_period*jitter_factor")

        if lease_duration < 1:
            print("lease less than 1")
            # sys.exit("lease_duration must be greater than one")

        if renew_deadline < 1:
            print("renew less than 1")
            # sys.exit("renew_deadline must be greater than one")

        if retry_period < 1:
            print("retry less than 1")
            # sys.exit("retry_period must be greater than one")

        self.lease_duration = lease_duration
        self.renew_deadline = renew_deadline
        self.retry_period = retry_period

        if onstarted_leading is None:
            sys.exit("callback onstarted_leading cannot be None")
        self.onstarted_leading = onstarted_leading

        if onstopped_leading is None:
            self.onstopped_leading = self.on_stoppedleading_callback
        else:
            self.onstopped_leading = onstopped_leading
        self.context = context

    # Default callback for when the current candidate if a leader, stops leading
    def on_stoppedleading_callback(self):
        logging.info("stopped leading".format(self.lock.identity))


class LeaderElection:
    """

    inv: self.observed_time_milliseconds >= 0
    """
    global_context = None
    def __init__(self, election_config):
        #if election_config is None or not (hasattr(election_config, "lock") and hasattr(election_config, "context") and hasattr(election_config.context, "cancelled")):
        #    sys.exit("Invalid election_config: must have 'lock' and 'context' with 'cancelled'")
        self.observed_record = None
        self.election_config = election_config
        self.observed_time_milliseconds = 0
        LeaderElection.global_context = self.election_config.context

        # Attach signal handler to Ctrl+C (SIGINT)
        signal.signal(signal.SIGINT, handle_sigint)

    # Point of entry to Leader election
    def run(self):
        """
        post: __return__ is None
        """
        # Try to create/ acquire a lock
        if self.acquire():
            logging.info("{} successfully acquired lease".format(self.election_config.lock.identity))

            # Start leading and call OnStartedLeading()
            threading.daemon = True
            threading.Thread(target=self.election_config.onstarted_leading).start()

            self.renew_loop()

            # Failed to update lease, run OnStoppedLeading callback
            self.election_config.onstopped_leading()

    def acquire(self):
        """

        pre: (self.election_config.retry_period > 1)  
        post: __return__ is True
        """
        # Follower
        logging.info("{} is a follower".format(self.election_config.lock.identity))
        retry_period = self.election_config.retry_period

        while True:
            succeeded = self.try_acquire_or_renew()

            if succeeded:
                return True

            time.sleep(retry_period)

    def renew_loop(self):
        """

        pre: (self.election_config.renew_deadline > 1)
             (self.election_config.retry_period > 1)
             (self.election_config.renew_deadline > self.jitter_factor * self.election_config.retry_period)
        post: self.election_config.context.cancelled and (time.time() - start_time > self.election_config.retry_period)
        """
        start_time = time.time()
        logging.info("Leader has entered renew loop and will try to update lease continuously")
        retry_period = self.election_config.retry_period
        renew_deadline = self.election_config.renew_deadline * 1000  # convert to milliseconds

        while True:
            # Check for context cancellation
            if self.election_config.context.cancelled:
                logging.info(f"Context cancelled. Reason: {self.election_config.context.cancel_reason}")
                # If configured to release on cancel, release the lease immediately
                # if getattr(self.election_config, "ReleaseOnCancel", False):
                # self.force_expire_lease()
                return
            
            timeout = int(time.time() * 1000) + renew_deadline
            succeeded = False

            while int(time.time() * 1000) < timeout:
                if self.election_config.context.cancelled:
                    logging.info(f"Context cancelled during renew loop. Reason: {self.election_config.context.cancel_reason}")
                    # self.force_expire_lease()
                    return

                if self.try_acquire_or_renew():
                    succeeded = True
                    break
                time.sleep(retry_period)
            if succeeded:
                time.sleep(retry_period)
                continue
            return

    def try_acquire_or_renew(self):
        """
        post: (__return__ is True and self.observed_record == leader_election_record and self.observed_time_milliseconds > 0) or (__return__ is False)
        """
        now_timestamp = time.time()
        now = datetime.datetime.fromtimestamp(now_timestamp)

        # Check if lock is created
        lock_status, old_election_record = self.election_config.lock.get(self.election_config.lock.name,
                                                                        self.election_config.lock.namespace)

        # create a default Election record for this candidate
        leader_election_record = LeaderElectionRecord(self.election_config.lock.identity,
                                                     str(self.election_config.lease_duration), str(now), str(now))

        # A lock is not created with that name, try to create one
        if not lock_status:
            # To be removed when support for python2 will be removed
            if sys.version_info > (3, 0):
                if json.loads(old_election_record.body)['code'] != HTTPStatus.NOT_FOUND:
                    logging.info("Error retrieving resource lock {} as {}".format(self.election_config.lock.name,
                                                                                  old_election_record.reason))
                    return False
            else:
                if json.loads(old_election_record.body)['code'] != httplib.NOT_FOUND:
                    logging.info("Error retrieving resource lock {} as {}".format(self.election_config.lock.name,
                                                                                  old_election_record.reason))
                    return False

            logging.info("{} is trying to create a lock".format(leader_election_record.holder_identity))
            create_status = self.election_config.lock.create(name=self.election_config.lock.name,
                                                             namespace=self.election_config.lock.namespace,
                                                             election_record=leader_election_record)

            if create_status is False:
                logging.info("{} Failed to create lock".format(leader_election_record.holder_identity))
                return False

            self.observed_record = leader_election_record
            self.observed_time_milliseconds = int(time.time() * 1000)
            return True

        # A lock exists with that name
        # Validate old_election_record
        if old_election_record is None:
            # try to update lock with proper annotation and election record
            return self.update_lock(leader_election_record)

        if (old_election_record.holder_identity is None or old_election_record.lease_duration is None
                or old_election_record.acquire_time is None or old_election_record.renew_time is None):
            # try to update lock with proper annotation and election record
            return self.update_lock(leader_election_record)

        # Report transitions
        if self.observed_record and self.observed_record.holder_identity != old_election_record.holder_identity:
            logging.info("Leader has switched to {}".format(old_election_record.holder_identity))

        if self.observed_record is None or old_election_record.__dict__ != self.observed_record.__dict__:
            self.observed_record = old_election_record
            self.observed_time_milliseconds = int(time.time() * 1000)

        # If This candidate is not the leader and lease duration is yet to finish
        if (self.election_config.lock.identity != self.observed_record.holder_identity
                and self.observed_time_milliseconds + self.election_config.lease_duration * 1000 > int(now_timestamp * 1000)):
            logging.info("yet to finish lease_duration, lease held by {} and has not expired".format(old_election_record.holder_identity))
            return False

        # If this candidate is the Leader
        if self.election_config.lock.identity == self.observed_record.holder_identity:
            # Leader updates renewTime, but keeps acquire_time unchanged
            leader_election_record.acquire_time = self.observed_record.acquire_time

        return self.update_lock(leader_election_record)

    def update_lock(self, leader_election_record: LeaderElectionRecord):
        # Update object with latest election record
        update_status = self.election_config.lock.update(self.election_config.lock.name,
                                                         self.election_config.lock.namespace,
                                                         leader_election_record)

        if update_status is False:
            logging.info("{} failed to acquire lease".format(leader_election_record.holder_identity))
            return False

        self.observed_record = leader_election_record
        self.observed_time_milliseconds = int(time.time() * 1000)
        logging.info("leader {} has successfully acquired lease".format(leader_election_record.holder_identity))
        return True
