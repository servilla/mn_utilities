#!/usr/bin/env python
# -*- coding: utf-8 -*-

""":Mod: list_pids

:Synopsis:

:Author:
    servilla
  
:Created:
    2/5/15
"""

__author__ = "servilla"

import logging

import mn_utilities.pid_tools
import mn_utilities.node_profile
import settings

def main():
    logging.basicConfig()
    logging.getLogger('').setLevel(logging.INFO)

    mn_profile = mn_utilities.node_profile.Node_Profile()
    mn_profile.set_id(settings.MN_ID)
    mn_profile.set_base_url(settings.MN_BASE_URL)
    mn_profile.set_cert_path(settings.MN_CERT_PATH)

    cn_profile = mn_utilities.node_profile.Node_Profile()
    cn_profile.set_base_url(settings.CN_BASE_URL)

    pid_tools = mn_utilities.pid_tools.Pid_Tools(mn_profile, cn_profile)
    cn_pids = pid_tools.get_mn_pids_from_cn(source="comparator", max_records=999999)

    for pid in cn_pids:
        print(pid)

    return 0


if __name__ == "__main__":
    main()