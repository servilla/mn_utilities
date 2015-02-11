#!/usr/bin/env python
# -*- coding: utf-8 -*-

""":Mod: mn_profile

:Synopsis:
    Simple node profile object with setters and getters for common node attributes.

:Author:
    servilla
  
:Created:
    2/5/15
"""

__author__ = "servilla"

class NodeProfile(object):
    """Simple DataONE node profile object."""

    def __init__(self):
        """Inits NodeProfile with private attributes.

        :return: None
        """
        self._base_url = None
        self._domain_name = None
        self._cert_path = None
        self._id = None
        self._api_version = None


    def set_base_url(self, base_url):
        """Sets the node base URL.

        Sets the node domain name as a side-effect.

        :param base_url: The node base URL
        :type base_url: str
        :return: None
        """
        self._base_url = base_url
        path_parts = base_url.split('/')
        self._domain_name = path_parts[2]


    def get_base_url(self):
        """Gets the node base URL.

        :return: The node base URL
        :rtype: str
        """
        return self._base_url


    def set_cert_path(self,cert_path):
        """Sets the node x509 certificate path.

        :param cert_path: The x509 certificate path
        :type cert_path: str
        :return: None
        """
        self._cert_path = cert_path


    def get_cert_path(self):
        """Gets the node x509 certificate path.

        :return: The x509 certificate path
        :rtype: str
        """
        return self._cert_path


    def set_id(self, id):
        """Sets the canonical node identifier.

        :param id: The node identifier
        :type id: str
        :return: None
        """
        self._id = id


    def get_id(self, esc=False):
        """Gets the canonical node identifier.

        :param esc: Flag to escape the node identifier colons
        :type esc: bool
        :return: The node identifier
        :rtype: str
        """
        id = self._id
        if esc:
            id = id.replace(":", "\:")
        return id


    def get_domain_name(self):
        """Gets the node's Internet domain name.

        The Internet domain name is set as a side-effect of the set_base_url function.

        :return: The node's domain name
        :rtype: str
        """
        return self._domain_name


    def set_api_version(self, api_version):
        """Sets the node's DataONE API version.

        :param api_version: The node's API version
        :type api_version: str
        :return: None
        """
        self._api_version = api_version


    def get_api_version(self):
        """Gets the node's DataONE API version.

        :return: The node's API version
        :rtype: str
        """
        return self._api_version