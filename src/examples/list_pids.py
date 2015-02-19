#!/usr/bin/env python
# -*- coding: utf-8 -*-

""":Mod: list_pids

:Synopsis:
    Example driver to generate a list of "persistent identifiers" (pids) on a
    DataONE Coordinating Node using various methods.

:Author:
    servilla
  
:Created:
    2/5/15
"""

__author__ = "servilla"

# System
import logging
import argparse

import mn_utilities.pid_tools
import mn_utilities.node_profile
import settings

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--filename", help="List output file name", default=None)
    parser.add_argument("-a", "--algorithm", help="List source algorithm type", default=None)
    parser.add_argument("-r", "--records", help="Maximum number of records to return", default=999999)
    args = parser.parse_args()

    filename = args.filename
    algorithm = args.algorithm
    records = args.records

    logging.basicConfig()
    logging.getLogger('').setLevel(logging.INFO)

    mn_profile = mn_utilities.node_profile.NodeProfile()
    mn_profile.set_id(settings.MN_ID)
    mn_profile.set_base_url(settings.MN_BASE_URL)
    mn_profile.set_cert_path(settings.MN_CERT_PATH)

    cn_profile = mn_utilities.node_profile.NodeProfile()
    cn_profile.set_base_url(settings.CN_BASE_URL)

    pid_tools = mn_utilities.pid_tools.Pid_Tools(mn_profile, cn_profile)

    if algorithm:
        cn_pids = pid_tools.get_mn_pids_from_cn(source=algorithm,
                                                max_records=records)
    else:
        cn_pids = pid_tools.get_mn_pids_from_cn(max_records=records)

    if filename:
        for pid in cn_pids:
            open(filename, mode="a").write("%s\n" % pid)
    else:
        for pid in cn_pids:
            print(pid)

    return 0


if __name__ == "__main__":
    main()
