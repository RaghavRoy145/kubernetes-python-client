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
from leaderelectionrecord import LeaderElectionRecord
import logging
from kubernetes.client.rest import ApiException
from kubernetes import client, config
from kubernetes.client.api_client import ApiClient
import signal
from typing import Callable


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

class Context:
    def __init__(self, cancelled: bool):
        self.cancelled = cancelled

    def cancel(self):
        self.cancelled = True

class DummyMetadata:
    def __init__(self):
        self.annotations = {}
        # Provide a dummy openapi_types attribute if needed by the client
        self.openapi_types = {}

class DummyConfigMap:
    def __init__(self):
        self.metadata = DummyMetadata()
        # Provide a dummy openapi_types attribute at the top level as well.
        self.openapi_types = {}


class LeaderElectionRecord:
    # Annotation used in the lock object

    def __init__(self, holder_identity: int, lease_duration: int, acquire_time: int, renew_time: int):
        """
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
        self.configmap_reference = DummyConfigMap()
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

    def get_lock_object(self, lock_record: dict):
        """
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

    def get_lock_dict(self, leader_election_record: LeaderElectionRecord):
        self.lock_record['holderIdentity'] = leader_election_record.holder_identity
        self.lock_record['leaseDurationSeconds'] = leader_election_record.lease_duration
        self.lock_record['acquireTime'] = leader_election_record.acquire_time
        self.lock_record['renewTime'] = leader_election_record.renew_time

        return self.lock_record

class Config:

    # Validate config, exit if an error is detected
    def __init__(self, lock: ConfigMapLock, lease_duration: int, renew_deadline: int, retry_period: int, onstarted_leading: Callable[[], None], onstopped_leading: Callable[[], None], context: Context):
        """
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

def make_dummy_config() -> "Config":
    # Provide concrete values that satisfy the preconditions:
    # For example:
    # lease_duration > renew_deadline, renew_deadline > jitter_factor * retry_period, retry_period > 1, etc.
    """
    """
    lock = ConfigMapLock("dummyLock", "default", 1000)
    lease_duration = 10
    renew_deadline = 5
    jitter_factor = 2
    retry_period = 2
    
    # Ensure precondition: renew_deadline > retry_period * jitter_factor
    if not (renew_deadline > retry_period * jitter_factor):
        renew_deadline = retry_period * jitter_factor + 1

    # Use simple lambda callbacks that take no arguments.
    onstarted_leading = lambda: None
    onstopped_leading = lambda: None

    # Create a dummy context that starts with cancelled==False.
    context = Context(True)
    
    return Config(lock, lease_duration, renew_deadline, retry_period, onstarted_leading, onstopped_leading, context)

class LeaderElection:
    global_context = None
    def __init__(self, observed_record: LeaderElectionRecord):
        #if election_config is None or not (hasattr(election_config, "lock") and hasattr(election_config, "context") and hasattr(election_config.context, "cancelled")):
        #    sys.exit("Invalid election_config: must have 'lock' and 'context' with 'cancelled'")
        """
        pre: observed_record.renew_time > 0 and observed_record.lease_duration > 0 and observed_record.acquire_time > 0
        """

        self.election_config = make_dummy_config()
        # self.observed_record = LeaderElectionRecord(
        #     holder_identity = int(self.election_config.lock.identity),  # Ensure a different identity.
        #     lease_duration = 10,  # For example, lease duration is 10 seconds.
        #     acquire_time = 10000, # constant value
        #     renew_time = 10000    # constant value, meaning the lease is not expired.
        # )
        self.observed_record = observed_record
        self.observed_record.lease_duration = 10
        self.observed_time_milliseconds = 0
        self.captured_observed_record_before_update = None
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
            # threading.daemon = True
            # threading.Thread(target=self.election_config.onstarted_leading).start()

            self.renew_loop()

            # Failed to update lease, run OnStoppedLeading callback
            self.election_config.onstopped_leading()

    def acquire(self):
        """

        """
        # Follower
        logging.info("{} is a follower".format(self.election_config.lock.identity))
        retry_period = self.election_config.retry_period
        print(retry_period, "acquire")
        while True:
            succeeded = self.try_acquire_or_renew()

            if succeeded:
                return True

            time.sleep(retry_period)

    def renew_loop(self):
        """
        pre: self.election_config.renew_deadline > 1 and self.election_config.renew_deadline < 10
        pre: self.election_config.retry_period > 1
        pre: self.election_config.renew_deadline > self.election_config.retry_period * self.election_config.jitter_factor
        pre: self.election_config.lease_duration > 0
        """
        start_time = self.observed_record.acquire_time
        logging.info("Leader has entered renew loop and will try to update lease continuously")
        retry_period = self.election_config.retry_period*1000
        renew_deadline = self.election_config.renew_deadline * 1000  # convert to milliseconds

        while True:
            # Check for context cancellation
            if self.election_config.context.cancelled:
                logging.info("Context cancelled")
                print(self.election_config.context.cancelled, "renew_loop+cancelled?")
                # If configured to release on cancel, release the lease immediately
                # if getattr(self.election_config, "ReleaseOnCancel", False):
                # self.force_expire_lease()
                return
            
            # timeout = int(time.time() * 1000) + renew_deadline
            timeout = start_time + renew_deadline
            succeeded = False
            cur_time = start_time
            # while int(time.time() * 1000) < timeout:
            while cur_time < timeout:
                if self.election_config.context.cancelled:
                    # logging.info(f"Context cancelled during renew loop. Reason: {self.election_config.context.cancel_reason}")
                    # self.force_expire_lease()
                    return

                if self.try_acquire_or_renew():
                    succeeded = True
                    break
                time.sleep(retry_period)
                cur_time += retry_period
                print(cur_time, "renew_loop + curtime")
            if succeeded:
                time.sleep(retry_period)
                continue
            return
    # def force_expire_lease(self, max_retries=3):
    #     """
    #     Force the lease to be considered expired by updating the leader election record's renewTime
    #     to a value in the past. Retries the update if a conflict (HTTP 409) is encountered.
    #     """
    #     expired_time = time.time() - self.election_config.lease_duration - 1  # Expired timestamp
    #     retries = 0
    #     while retries < max_retries:
    #         # Re-read the current state of the lock to get the latest version.
    #         lock_status, current_record = self.election_config.lock.get(
    #             self.election_config.lock.name,
    #             self.election_config.lock.namespace
    #         )
    #         # Create a new record using the current record's acquireTime if available.
    #         new_record = LeaderElectionRecord(
    #             self.election_config.lock.identity,
    #             str(self.election_config.lease_duration),
    #             None,
    #             str(expired_time)
    #         )
    #         update_status = self.election_config.lock.update(
    #             self.election_config.lock.name,
    #             self.election_config.lock.namespace,
    #             new_record
    #         )
    #         if update_status:
    #             logging.info("Lease forcibly expired.")
    #             return True
    #         else:
    #             logging.info(f"Conflict encountered, retrying update... (attempt {retries+1})")
    #             retries += 1
    #             time.sleep(0.5)  # wait a bit before retrying
    #     logging.info("Failed to force lease expiration after retries.")
    #     return False

    # def force_expire_lease(self, max_retries=3):
    #     """
    #     Force the lease to be considered expired by updating the observed_record's renew_time.
    #     In this dummy implementation, we simply set observed_record.renew_time such that
    #     observed_record.renew_time + lease_duration*1000 <= 10000.
    #     """
    #     # For example, choose the expired value to be exactly 10000 - lease_duration*1000.
    #     expired_value = 10000 - self.election_config.lease_duration * 1000
    #     if self.observed_record is None:
    #         # If no record exists, create one with expired renew_time.
    #         self.observed_record = LeaderElectionRecord(
    #             self.election_config.lock.identity,
    #             self.election_config.lease_duration,
    #             None,
    #             expired_value
    #         )
    #     else:
    #         # Update the renew_time in the existing record.
    #         self.observed_record.renew_time = expired_value
    #     logging.info("Lease forcibly expired via dummy update. New renew_time: {}".format(self.observed_record.renew_time))

    def try_acquire_or_renew(self):
        """
        post: (not self.election_config.context.cancelled is True) or (self.election_config.context.cancelled is True and (self.captured_observed_record_before_update.renew_time + self.election_config.lease_duration * 1000 == self.observed_record.acquire_time))
        """

        # now_timestamp = time.time()
        # now = datetime.datetime.fromtimestamp(now_timestamp)
        now_timestamp = self.observed_record.acquire_time
        # now = 10000

        # if self.election_config.context.cancelled:
        #     self.force_expire_lease()
        # Check if lock is created
        # lock_status, old_election_record self.observed_record.acquire_time= self.election_config.lock.get(self.election_config.lock.name,
        #                                                                 self.election_config.lock.namespace)
        lock_status = True
        old_election_record = self.observed_record

        # create a default Election record for this candidate
        leader_election_record = LeaderElectionRecord(
            holder_identity = int(self.election_config.lock.identity) + 1,  # Ensure a different identity.
            lease_duration = 10,  # For example, lease duration is 10 seconds.
            acquire_time = 10000, # constant value
            renew_time = 10000    # constant value, meaning the lease is not expired.
        )

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
            self.observed_time_milliseconds = time.time()
            return True

        self.captured_observed_record_before_update = self.observed_record
        logging.info("{}, {}, {}".format(self.election_config.context.cancelled, self.captured_observed_record_before_update.renew_time, self.election_config.lease_duration))
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
            self.observed_time_milliseconds = self.observed_record.acquire_time

        # If This candidate is not the leader and lease duration is yet to finish
        if (self.election_config.lock.identity != self.observed_record.holder_identity
                and self.observed_time_milliseconds + self.election_config.lease_duration * 1000 > int(now_timestamp*1000)):
            logging.info("{}, {}, {}".format(self.observed_time_milliseconds, self.election_config.lease_duration * 1000, int(now_timestamp)))
            logging.info("yet to finish lease_duration, lease held by {} and has not expired".format(old_election_record.holder_identity))
            return False

        # If this candidate is the Leader
        if self.election_config.lock.identity == self.observed_record.holder_identity:
            # Leader updates renewTime, but keeps acquire_time unchanged
            leader_election_record.acquire_time = self.observed_record.acquire_time
        logging.info("{}, {}".format(self.observed_record.renew_time, self.election_config.lease_duration * 1000))
        return self.update_lock(leader_election_record)

    def update_lock(self, leader_election_record: LeaderElectionRecord):

        """
        pre: self.election_config.lock.configmap_reference is not None
        """
        # Update object with latest election record
        # update_status = self.election_config.lock.update(self.election_config.lock.name,
        #                                                  self.election_config.lock.namespace,
        #                                                  leader_election_record)
        update_status = True

        if update_status is False:
            logging.info("{} failed to acquire lease".format(leader_election_record.holder_identity))
            return False
        # leader_election_record = LeaderElectionRecord(self.election_config.lock.identity,
        #                                              self.election_config.lease_duration, 10000, 10000)
        self.observed_record = leader_election_record
        self.observed_time_milliseconds = 10000
        logging.info("leader {} has successfully acquired lease".format(leader_election_record.holder_identity))
        return True