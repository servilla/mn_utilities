#!/usr/bin/env python
# -*- coding: utf-8 -*-

""":Mod: pid_tools

:Synopsis:
    Manage DataONE "persistent identifiers" (pids) from either a Member Node or
    a Coordinating Node using a variety of methods.

:Author:
    servilla

:Created:
    2/5/15

:Requires:
    - Python 2.6 or 2.7.
    - DataONE Common Library for Python
    - DataONE Client Library for Python
"""

from __future__ import print_function

__author__ = "servilla"

# Stdlib
import logging

# D1
import d1_client.solr_client as solr_client
import d1_client.mnclient as mnclient
import d1_client.cnclient as cnclient
import d1_client.objectlistiterator as objectlistiterator
import d1_common.types.generated.dataoneTypes as dataoneTypes

# Site packages
import pyxb


class Pid_Tools(object):
    """Pid management object."""


    def __init__(self, mn_profile, cn_profile):
        """__init__

        :param mn_profile: Member Node profile
        :type mn_profile: node_profile
        :param cn_profile: Coordinating Node profile
        :type cn_profile: node_profile
        :return: None
        """

        self.logger = logging.getLogger('pids.Pids')
        # self.logger.setLevel(logging.DEBUG)
        self._logger_level = self.logger.getEffectiveLevel()

        self._mn_profile = mn_profile
        self._cn_profile = cn_profile

    def get_mn_pids_from_cn(self, source="solrator", max_records=None,
                            type="list"):

        if source == "solrator":
            pids = self._get_cn_pids_solr(max_records, type)
        elif source == "assertarator":
            pids = self._get_cn_pids_assertarator(max_records, type)
        elif source == "comparator":
            pids = self._get_cn_pids_comparator(max_records, type)
        elif source == "iterator":
            pids = self._get_cn_pids_iterator(max_records, type)
        else:
            raise Exception("wtf")

        return pids


    def _get_cn_pids_solr(self, max_records, type="list"):

        mn_id = self._mn_profile.get_id(esc=True)
        cn_domain_name = self._cn_profile.get_domain_name()

        cn_solr_client = solr_client.SolrConnection(host=cn_domain_name)
        solr_iterator = solr_client.SOLRSearchResponseIterator(cn_solr_client,
                                                    q='datasource:' + mn_id,
                                                    fields="identifier",
                                                    max_records=max_records)
        if type == "list":
            pids = []
            while True:
                try:
                    row = solr_iterator.next()
                    pid = row['identifier']
                    pids.append(pid)
                except StopIteration:
                    return pids
        elif type == "dict":
            pids = {}
            while True:
                try:
                    row = solr_iterator.next()
                    pid = row['identifier']
                    pids[pid] = 1
                except StopIteration:
                    return pids
        else:
            raise Exception("wtf")


    def _get_cn_pids_assertarator(self, max_records, type="list"):

        mn_pids = self._get_mn_pids(type="list")
        cn_base_url = self._cn_profile.get_base_url()

        count = 0

        if type == "list":
            pids = []
            for mn_pid in mn_pids:
                try:
                    cn_client = cnclient.CoordinatingNodeClient(
                        base_url=cn_base_url,
                        cert_path=self._mn_profile.get_cert_path())
                    cn_client.getSystemMetadataResponse(mn_pid)
                    pids.append(mn_pid)
                    count += 1
                    if count > max_records:
                        break
                except Exception as e:
                    self.logger.error("%s" % str(e))
        elif type == "dict":
            pids = {}
            for mn_pid in mn_pids:
                try:
                    cn_client = cnclient.CoordinatingNodeClient(
                        base_url=cn_base_url,
                        cert_path=self._mn_profile.get_cert_path())
                    cn_client.getSystemMetadataResponse(mn_pid)
                    pids[mn_pid] = 1
                    count += 1
                    if count > max_records:
                        break
                except Exception as e:
                    self.logger.error("%s" % str(e))
        else:
            raise Exception("wtf")

        return pids


    def _get_cn_pids_comparator(self, max_records, type="list"):

        mn_pids = self._get_mn_pids(type="list")
        cn_base_url = self._cn_profile.get_base_url()
        cn_pids = self._get_cn_pids(type="dict")

        count = 0
        processed = 0

        if type == "list":
            pids = []
            for mn_pid in mn_pids:
                processed += 1
                if not bool(processed % 100):
                    self.logger.info("Processing %d" % processed)
                try:
                    value = cn_pids[mn_pid]
                    pids.append(mn_pid)
                    print("%s" % mn_pid)
                    count += 1
                    if count > max_records:
                        break
                except Exception as e:
                    self.logger.error("%s" % str(e))
        elif type == "dict":
            pids = {}
            for mn_pid in mn_pids:
                processed += 1
                if not bool(processed % 100):
                    self.logger.info("Processing %d" % processed)
                try:
                    value = cn_pids[mn_pid]
                    pids[mn_pid] = 1
                    count += 1
                    if count > max_records:
                        break
                except Exception as e:
                    self.logger.error("%s" % str(e))
        else:
            raise Exception("wtf")

        return pids


    def _get_cn_pids_iterator(self, max_records, type="list"):

        cn_pids = self._get_cn_pids(type="list")
        cn_client = cnclient.CoordinatingNodeClient(
            base_url=self._cn_profile.get_base_url(),
            cert_path=self._mn_profile.get_cert_path())
        mn_node_id = self._cn_profile.get_id()

        count = 0
        processed = 0

        if type == "list":
            pids = []
            for pid in cn_pids:
                processed += 1
                if not bool(processed % 100):
                    self.logger.info("Processing %d" % processed)
                try:
                    system_metadata = self._get_sys_meta(pid, cn_client)
                    authoritative_member_node = system_metadata.authoritativeMemberNode.value()
                    self.logger.debug("From CN %s" % authoritative_member_node)
                    if mn_node_id == authoritative_member_node:
                        pids.append(pid)
                        count += 1
                        if count > max_records:
                            break
                except Exception as e:
                    self.logger.error("%s" % str(e))
        elif type == "dict":
            pids = {}
            for pid in cn_pids:
                processed += 1
                if not bool(processed % 100):
                    self.logger.info("Processing %d" % processed)
                try:
                    system_metadata = self._get_sys_meta(pid, cn_client)
                    authoritative_member_node = system_metadata.authoritativeMemberNode.value()
                    self.logger.debug("From CN %s" % authoritative_member_node)
                    if mn_node_id == authoritative_member_node:
                        pids[pid] = 1
                        count += 1
                        if count > max_records:
                            break
                except Exception as e:
                    self.logger.error("%s" % str(e))
        else:
            raise Exception("wtf")

        return pids


    def get_mn_pids(self, type="list"):

        return self._get_mn_pids(type)


    def _get_mn_pids(self, type="list"):

        mn_client = mnclient.MemberNodeClient(
            base_url=self._mn_profile.get_base_url(),
            cert_path=self._mn_profile.get_cert_path())
        count = 0

        if type == "list":
            pids = []
            for objects in objectlistiterator.ObjectListIterator(mn_client):
                count += 1
                pids.append(objects.identifier.value())
                if not bool(count % 500):
                    self.logger.info("Reading %d from %s" % (
                    count, self._mn_profile.get_base_url()))
                if self._logger_level == logging.DEBUG:
                    if count >= 50:
                        break
        elif type == "dict":
            pids = {}
            for objects in objectlistiterator.ObjectListIterator(mn_client):
                count += 1
                pids[objects.identifier.value()] = 1
                if not bool(count % 500):
                    self.logger.info("Reading %d from %s" % (
                    count, self._mn_profile.get_base_url()))
                if self._logger_level == logging.DEBUG:
                    if count >= 50:
                        break
        else:
            raise Exception("wtf")

        return pids


    def get_cn_pids(self, type="list"):

        return self._get_cn_pids(type)


    def _get_cn_pids(self, type="list"):

        cn_client = cnclient.CoordinatingNodeClient(
            base_url=self._cn_profile.get_base_url(),
            cert_path=self._mn_profile.get_cert_path())
        count = 0
        if type == "list":
            pids = []
            for objects in objectlistiterator.ObjectListIterator(cn_client):
                count += 1
                pids.append(objects.identifier.value())
                if not bool(count % 500):
                    self.logger.info("Reading %d from %s" % (
                    count, self._cn_profile.get_base_url()))
                if self._logger_level == logging.DEBUG:
                    if count >= 50:
                        break
        elif type == "dict":
            pids = {}
            for objects in objectlistiterator.ObjectListIterator(cn_client):
                count += 1
                pids[objects.identifier.value()] = 1
                if not bool(count % 500):
                    self.logger.info("Reading %d from %s" % (
                    count, self._cn_profile.get_base_url()))
                if self._logger_level == logging.DEBUG:
                    if count >= 50:
                        break
        else:
            raise Exception("wtf")

        return pids


    def _get_sys_meta(self, pid, client):
        """Return corrected system metadata as pyxb object

        :param d1_pid:
        The DataONE pid string
        :type:
        String

        :param client:
        Either the DataONE Member Node or Coordinating Node Client object
        :type:
        d1client object

        :return:
        System metadata as pyxb object
        :type:
        pyxb object
        """

        try:
            sys_meta_str = client.getSystemMetadataResponse(pid).read()
            sys_meta_str = sys_meta_str.replace('<accessPolicy/>', '')
            sys_meta_str = sys_meta_str.replace('<blockedMemberNode/>', '')
            sys_meta_str = sys_meta_str.replace(
                '<blockedMemberNode></blockedMemberNode>', '')
            sys_meta_obj = dataoneTypes.CreateFromDocument(sys_meta_str)
            return sys_meta_obj
        except pyxb.UnrecognizedDOMRootNodeError as e:
            self.logger.error("%s" % str(e))
            raise e
        except Exception as e:
            self.logger.error("%s" % str(e))
            raise e