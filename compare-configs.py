#!/usr/bin/env python

#
#  compare-configs.py compares a comma seperated list of files which contain ambari blueprints
#  and prints out differences by default.
#

from config_compare import ConfigCompare


if __name__ == "__main__":
    # create the compare instance
    cc_hdl = ConfigCompare()

    # validate args
    args = cc_hdl.validate_args()

    # do the actual comparision of configs
    result = cc_hdl.do_compare()

    # print out differences found
    cc_hdl.print_result(result)
