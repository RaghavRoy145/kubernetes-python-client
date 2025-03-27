import datetime
import sys
import time
import json
import threading
import logging

if sys.version_info > (3, 0):
    from http import HTTPStatus
else:
    import httplib

logging.basicConfig(level=logging.INFO)

class LeaderElection:
    """
    inv: self.election_config is not None
    inv: self.observed_time_milliseconds <= 0
    """
    def __init__(self, election_config):
        """
        pre: election_config is not None and hasattr(election_config, "lock") 
        #and hasattr(election_config, "context") and hasattr(election_config.context, "cancelled")
        post: self.election_config == election_config
              self.observed_record is None
              self.observed_time_milliseconds == 0
        """
        #if election_config is None or not (hasattr(election_config, "lock") and hasattr(election_config, "context") and hasattr(election_config.context, "cancelled")):
        #    sys.exit("Invalid election_config: must have 'lock' and 'context' with 'cancelled'")
        self.observed_record = None
        self.election_config = election_config
        self.observed_time_milliseconds = 0

    def run(self):
        """
        pre: hasattr(self.election_config, 'lock')
             callable(self.election_config.onstarted_leading)
             callable(self.election_config.onstopped_leading)
        post: __return__ is None
        """
        if self.acquire():
            logging.info(f"{self.election_config.lock.identity} successfully acquired lease")
            # Start the leader callback in a new daemon thread.
            threading.daemon = True
            threading.Thread(target=self.election_config.onstarted_leading).start()
            self.renew_loop()
            self.election_config.onstopped_leading()

    def acquire(self):
        """
        pre: hasattr(self.election_config, 'lock')
             hasattr(self.election_config, 'retry_period') and self.election_config.retry_period >= 0
        post: __return__ is True
        """
        logging.info(f"{self.election_config.lock.identity} is a follower")
        retry_period = self.election_config.retry_period
        # Note: This loop does not check for context cancellation.
        while True:
            assert self.election_config.retry_period >= 0, "retry_period must be non-negative"
            succeeded = self.try_acquire_or_renew()
            if succeeded:
                return True
            time.sleep(retry_period)

    def renew_loop(self):
        """
        pre: hasattr(self.election_config, 'lock')
             hasattr(self.election_config, 'retry_period') and self.election_config.retry_period >= 0
             hasattr(self.election_config, 'renew_deadline') and self.election_config.renew_deadline > 0
        #pre: hasattr(self.election_config, 'context') and hasattr(self.election_config.context, 'cancelled')
        post: self.election_config.context.cancelled and (time.time() - __old__.start_time < self.election_config.retry_period)
              __return__ is None
        """
        #start_time = time.time()  # Record the start time for measuring immediate exit upon cancellation.
        logging.info("Leader has entered renew loop and will try to update lease continuously")
        retry_period = self.election_config.retry_period
        renew_deadline = self.election_config.renew_deadline * 1000  # convert to milliseconds

        while True:
            timeout = int(time.time() * 1000) + renew_deadline
            succeeded = False
            while int(time.time() * 1000) < timeout:
                # Notice: No check for self.election_config.context.cancelled exists here.
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
        pre: hasattr(self.election_config, 'lock')
        post: (__return__ is True and self.observed_record is not None) or (__return__ is False)
        """
        now_timestamp = time.time()
        now = datetime.datetime.fromtimestamp(now_timestamp)
        lock_status, old_election_record = self.election_config.lock.get(
            self.election_config.lock.name,
            self.election_config.lock.namespace)
        leader_election_record = LeaderElectionRecord(
            self.election_config.lock.identity,
            str(self.election_config.lease_duration),
            str(now),
            str(now))
        if not lock_status:
            if sys.version_info > (3, 0):
                if json.loads(old_election_record.body)['code'] != HTTPStatus.NOT_FOUND:
                    logging.info(f"Error retrieving resource lock {self.election_config.lock.name} as {old_election_record.reason}")
                    return False
            else:
                if json.loads(old_election_record.body)['code'] != httplib.NOT_FOUND:
                    logging.info(f"Error retrieving resource lock {self.election_config.lock.name} as {old_election_record.reason}")
                    return False

            logging.info(f"{leader_election_record.holder_identity} is trying to create a lock")
            create_status = self.election_config.lock.create(
                name=self.election_config.lock.name,
                namespace=self.election_config.lock.namespace,
                election_record=leader_election_record)
            if create_status is False:
                logging.info(f"{leader_election_record.holder_identity} Failed to create lock")
                return False
            self.observed_record = leader_election_record
            self.observed_time_milliseconds = int(time.time() * 1000)
            return True

        return self.update_lock(leader_election_record)

    def update_lock(self, leader_election_record):
        """
        pre: hasattr(self.election_config, 'lock')
             leader_election_record is not None
        post: (__return__ is True and self.observed_record == leader_election_record and self.observed_time_milliseconds > 0) or (__return__ is False)
        """
        update_status = self.election_config.lock.update(
            self.election_config.lock.name,
            self.election_config.lock.namespace,
            leader_election_record)
        if update_status is False:
            logging.info(f"{leader_election_record.holder_identity} failed to acquire lease")
            return False
        self.observed_record = leader_election_record
        self.observed_time_milliseconds = int(time.time() * 1000)
        logging.info(f"leader {leader_election_record.holder_identity} has successfully acquired lease")
        return True


# mock implementation
# Authenticate using config file
#config.load_kube_config(config_file=r"~/.kube/config")

# Parameters required from the user

# A unique identifier for this candidate
#candidate_id = uuid.uuid4()

# Name of the lock object to be created
#lock_name = "examplepython"

# Kubernetes namespace
#lock_namespace = "default"


# The function that a user wants to run once a candidate is elected as a leader
#def example_func():
#    print("I am leader")


    # A user can choose not to provide any callbacks for what to do when a candidate fails to lead - onStoppedLeading()
    # In that case, a default callback function will be used

    # Create config
#    config = electionconfig.Config(ConfigMapLock(lock_name, lock_namespace, candidate_id), lease_duration=17,
#                               renew_deadline=15, retry_period=5, onstarted_leading=example_func,
#                               onstopped_leading=None)

    # Enter leader election
#    leaderelection.LeaderElection(config).run()

    # User can choose to do another round of election or simply exit
#    print("Exited leader election")
#example_func()
