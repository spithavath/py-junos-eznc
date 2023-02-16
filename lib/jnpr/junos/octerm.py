"""
This file defines the 'OCTerm' class.
This has the functionality to communicate to oc-terminator of JCloud over kafka
"""
import json
import re
import sys
import logging

# 3rd-party packages
import time

from ncclient.devices.junos import JunosDeviceHandler
from lxml import etree
from ncclient.xml_ import NCElement
from ncclient.operations.rpc import RPCReply, RPCError
from ncclient.xml_ import to_ele

from jnpr.junos.device import _Connection
from jnpr.junos.rpcmeta import _RpcMetaExec
from jnpr.junos.factcache import _FactCache
from jnpr.junos import exception as EzErrors
from jnpr.junos import jxml as JXML

from jnpr.junos.decorators import ignoreWarnDecorator
# from confluent_kafka import  KafkaError, KafkaException
from kafka import KafkaConsumer

logger = logging.getLogger("jnpr.junos.octerm")


class OCTerm(_Connection):
    # TODO:
    def __init__(self, uuid=None, producer=None, consumer=None, id=None, **kvargs):
        """
        OCTerm object constructor.

        grpc_deps = {
                        "meta_data": meta_data,
                        "stub": css,
                        "uuid": "test1234",
                        "types_pb2": types_pb2,
                        "dcs_pb2": dcs_pb2,
                        "device_info": device_info,
                     }

        :param dict kvargs:
            **REQUIRED** gRPC call dependencies
        """

        # ----------------------------------------
        # setup instance connection/open variables
        # ----------------------------------------
        self._tty = None
        self._ofacts = {}
        self.connected = False
        self.results = dict(changed=False, failed=False, errmsg=None)
        self._hostname = "hostname"
        self._dev_uuid = uuid
        self._producer = producer
        self._id = id
        self._async_consumer = kvargs.get("async_consumer", True)
        self._xslt = kvargs.get("xslt", "") 
        
        if kvargs.get("result"):
            logger.info('Creating oc-term class for consumer')
            self.kafka_result = kvargs["result"]
        else:
            self._request_topic = "oc-cmd-dev"
            if "request_topic" in kvargs:
                self._request_topic = kvargs["request_topic"]
            self._response_topic = "netconf-resp-dev"
            if "response_topic" in kvargs:
                self._response_topic = kvargs["response_topic"]

            if "kafka_brokers" not in kvargs:
                raise Exception("Kafka Brokers should be provided")
            self._kafka_brokers = kvargs["kafka_brokers"]
            if self._producer is None:
                raise Exception("Producer should be initialized")
            if not self._async_consumer:
                self._consumer_lock = kvargs["consumer_lock"]
                self._consumer = consumer
            else:
                self._consumer_lock = None
                self._consumer = None
            self.kafka_result = None

        self.junos_dev_handler = JunosDeviceHandler(
            device_params={"name": "junos", "local": False}
        )
        self.rpc = _RpcMetaExec(self)
        self._conn = None
        self.facts = _FactCache(self)
        self._normalize = kvargs.get("normalize", False)
        self._gather_facts = kvargs.get("gather_facts", False)
        self._fact_style = kvargs.get("fact_style", "new")
        self._use_filter = kvargs.get("use_filter", False)
        self._table_metadata = kvargs.get("t_metadata", None)

    @property
    def timeout(self):
        """
        :returns: current console connection timeout value (int) in seconds.
        """
        return self._timeout

    @timeout.setter
    def timeout(self, value):
        """
        Used to change the console connection timeout value (default=0.5 sec).

        :param int value:
            New timeout value in seconds
        """
        self._timeout = value

    @property
    def transform(self):
        """
        :returns: the current RPC XML Transformation.
        """
        return self.junos_dev_handler.transform_reply

    @transform.setter
    def transform(self, func):
        """
        Used to change the RPC XML Transformation.

        :param lambda value:
            New transform lambda
        """
        self.junos_dev_handler.transform_reply = func

    def open(self, *vargs, **kvargs):
        """
        Opens a connection to the device using existing login/auth
        information.

        :param bool gather_facts:
            If set to ``True``/``False`` will override the device
            instance value for only this open process
        """

        # for now everything is all connection via gRPC
        self.connected = True

        self._nc_transform = self.transform
        self._norm_transform = lambda: JXML.normalize_xslt.encode("UTF-8")

        self._normalize = kvargs.get("normalize", self._normalize)
        if self._normalize is True:
            self.transform = self._norm_transform

        gather_facts = kvargs.get("gather_facts", self._gather_facts)
        if gather_facts is True:
            logger.info("facts: retrieving device facts...")
            self.facts_refresh()
            self.results["facts"] = self.facts
        self._conn = self._tty
        return self

    def close(self):
        """
        Closes the connection to the device.
        """
        # self._grpc_conn_stub.DisconnectDevice(metadata=self._grpc_meta_data)
        self.connected = False

    def acquire_consume_lock(self):
        if not self._async_consumer:
            self._consumer_lock.acquire()

    def release_consume_lock(self):
        if not self._async_consumer:
            self._consumer_lock.release()

    @ignoreWarnDecorator
    def _rpc_reply(self, rpc_cmd_e, *args, **kwargs):
        if self.kafka_result:
            result = self.kafka_result
            reply = RPCReply(result)
            errors = reply.errors
            if len(errors) > 1:
                raise RPCError(to_ele(reply._raw), errs=errors)
            elif len(errors) == 1:
                raise reply.error

            rpc_rsp_e = NCElement(
                reply, self.junos_dev_handler.transform_reply()
            )._NCElement__doc
            return rpc_rsp_e

        encode = None if sys.version < "3" else "unicode"

        rpc_cmd = (
            etree.tostring(rpc_cmd_e, encoding=encode)
            if isinstance(rpc_cmd_e, etree._Element)
            else rpc_cmd_e
        )
        rpc_cmd.encode("unicode_escape")
        if self._async_consumer:
            table_name = self._table_metadata
            req_id = "pyezsched" + ":" + ":" + self._id + ":" + \
                table_name + ":" + str(time.time())
        else:
            req_id = self._dev_uuid + ":" + rpc_cmd + ":" + self._id + str(time.time())
        kafka_cmd = {
            "op": "OC_COMMAND",
            "command": "netconf-rpc",
            "requestID": req_id,
            "resource": self._dev_uuid,
            "id": self._dev_uuid,
            "params": rpc_cmd,
            "netconfCommand": rpc_cmd
        }
        
        if self._xslt != "":
            kafka_cmd["params"] = {
                "netconfCommand" : rpc_cmd,
                "filterXSLT": self._xslt
            }
        
        result = ""
        
        # consumer = KafkaConsumer(
        #     self._response_topic,
        #     bootstrap_servers=self._kafka_brokers.split(","),
        #     auto_offset_reset='latest',
        #     enable_auto_commit=True,
        #     group_id=None,
        #     max_poll_interval_ms=5000,
        # )
        try:
            self.acquire_consume_lock()
            
            self._producer.produce(
                self._request_topic, key="key", value=json.dumps(kafka_cmd))
            time_start = time.time()

            if self._async_consumer:
                # Async consumer enabled. Return immediately.
                raise EzErrors.OCTermProducer(
                    cmd=rpc_cmd,
                    error="message published to kafka",
                    uuid=self._dev_uuid,
                )
            else:
                consumer = self._consumer

            while True:
                result = None
                if time.time() - time_start >= self.timeout:
                    raise EzErrors.OCTermRpcError(
                        cmd=rpc_cmd,
                        error="Timeout waiting for response",
                        uuid=self._dev_uuid,
                    )
                messages = consumer.poll(timeout_ms=5000)
                if not messages or messages is None:
                    logger.info("No messages response from kafka topic")
                    continue
                else:
                    for k, msg in messages.items():
                        for a in msg:
                            try:
                                value = json.loads(a.value.decode('utf-8'))
                            except Exception:
                                continue
                            if value.get("requestID", "") == kafka_cmd["requestID"]:
                                if "Payload" in value and "Output" in value["Payload"]:
                                    result = value["Payload"]["Output"]
                                    break
                                else:
                                    # consumer.close()
                                    self._consumer_lock.release()
                                    raise EzErrors.OCTermRpcError(
                                        cmd=rpc_cmd,
                                        error=value.get("Payload", {}).get(
                                            "Error", "Unknown error"),
                                        uuid=self._dev_uuid,
                                    )
                            else:
                                print("============")
                        if result is not None:
                            break
                if result is not None:
                    break
            self.release_consume_lock()
        except Exception as exp:
            self.release_consume_lock()
            raise exp

        reply = RPCReply(result)
        errors = reply.errors
        if len(errors) > 1:
            raise RPCError(to_ele(reply._raw), errs=errors)
        elif len(errors) == 1:
            raise reply.error

        rpc_rsp_e = NCElement(
            reply, self.junos_dev_handler.transform_reply()
        )._NCElement__doc
        return rpc_rsp_e

    # -----------------------------------------------------------------------
    # Context Manager
    # -----------------------------------------------------------------------

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connected:
            self.close()
