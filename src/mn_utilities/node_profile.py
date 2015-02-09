#!/usr/bin/env python
# -*- coding: utf-8 -*-

""":Mod: mn_profile

:Synopsis:

:Author:
    servilla
  
:Created:
    2/5/15
"""

__author__ = "servilla"

class Node_Profile(object):

    def __init__(self):
        self._base_url = None
        self._domain_name = None
        self._cert_path = None
        self._id = None
        self._mn_version = None


    def set_base_url(self, mn_base_url):
        self._base_url = mn_base_url
        path_parts = mn_base_url.split('/')
        self._domain_name = path_parts[2]

    def get_base_url(self):
        return self._base_url

    def set_cert_path(self,mn_cert_path):
        self._cert_path = mn_cert_path

    def get_cert_path(self):
        return self._cert_path

    def set_id(self, mn_id):
        self._id = mn_id

    def get_id(self, esc=False):
        mn_id = self._id
        if esc:
            mn_id = mn_id.replace(":", "\:")
        return mn_id

    def get_domain_name(self):
        return self._domain_name

    def set_version(self, version):
        self._mn_version = version

    def get_version(self):
        return self._mn_version