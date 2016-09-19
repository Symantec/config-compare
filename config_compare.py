import argparse
import collections
import copy
import json
import os
import pprint
import re
import sys
import xmltodict


def _chk_label(cmp_root, test_label, conf_file):
    """
    Check to see if the label for the current value needs to be created.
    :param cmp_root: <dictionary> A dictionary representing the configuration parameters and values
                                  already observered
    :param test_label: <string> the label which is being checked
    :param conf_file: <string> the configuration source from which the label came from
    :return: <dictionary>
    """
    # if branch not processed add dict for holding info
    if cmp_root.get(test_label) is None:
        cmp_root[test_label] = {}
        cmp_root[test_label]['clusters'] = []

    # record the cluster conf_file
    if conf_file not in cmp_root[test_label]['clusters']:
        cmp_root[test_label]['clusters'].append(conf_file)

    return cmp_root


def _do_plain_text(blueprint, label, compare_root, blueprint_branch):
    """
    Record a string in the dictionary representing the configuration parameters ( compare_root )
    :param blueprint: <string> the name of the configuration file the information came from
    :param label: <string> the path to the configuration data in blueprint_branch
    :param compare_root: <dictionary> a dictionary representing the configuration parameters and values already
                                      observered
    :param blueprint_branch: <string> text to be recorded
    :return:
    """
    # do nothing if we are passed an empty string
    if len(blueprint_branch) < 1:
        return

    # remove leading, trailing spaces and tabs
    wrk_blueprint_branch = copy.copy(blueprint_branch)
    clean_blueprint_branch = re.sub("\t", ' ', str(wrk_blueprint_branch))
    clean_blueprint_branch = re.sub("\n", "\\\\n", clean_blueprint_branch)
    clean_blueprint_branch = clean_blueprint_branch.lstrip()
    clean_blueprint_branch = clean_blueprint_branch.rstrip()

    # if branch not processed add dict for holding info
    if compare_root[label].get('values') is None:
        compare_root[label]['values'] = {}
    if compare_root[label]['values'].get(clean_blueprint_branch) is None:
        compare_root[label]['values'][clean_blueprint_branch] = []

    # record the cluster blueprint
    if blueprint not in compare_root[label]['values'][clean_blueprint_branch]:
        compare_root[label]['values'][clean_blueprint_branch].append(blueprint)


class ConfigCompare(object):
    """
    Class to hold the observed configurations and generates report of differences.
    """

    def __init__(self):
        """
        Constructor for ConfigCompare
        :return: <object> ConfigCompare instance
        """
        self.configs = None

        self.short_val_length = 40
        self.verbose_flag = False
        self.same_flag = False
        self.output_file = None
        self.file_hdl = None
        self.include_pattern = None
        self.exclude_pattern = None

    def _validate_config_files(self, args):
        # error out if this was previously done
        if self.configs is not None:
            print("    The configuration files have been previously set.  Something is wrong.")
            sys.exit(1)

        # gather the configs in one list
        blueprints = args.first_conf + args.other_configs

        # check all configs are present before we begin
        err = 0
        bset = set(blueprints)
        distinct_blueprints = list(bset)
        if len(distinct_blueprints) < 2:
            print("  only one unique configuration was provided: %s\n" % blueprints)
            sys.exit(1)

        # ensure the order presented back to the user is the same
        # as passed even if a duplicate file was passed
        distinct_blueprints = []
        for b in blueprints:
            if b not in distinct_blueprints:
                distinct_blueprints.append(b)

        # ensure file exists before proceeding
        for b in distinct_blueprints:
            if not os.path.isfile(b):
                print("    blueprint file %s does not exist" % b)
                err = 1
        if err:
            print("\n")
            sys.exit(1)

        self.configs = distinct_blueprints

    def validate_args(self):
        usage_desc = "\n\nThis tool takes a space seperated list of configuration files and compares them.\n" \
                     "Differences in pathes to values and the values themselves are written out in\n" \
                     "sorted order as determined by the path to the value.\n\n" \
                     "The tool was created with the main purpose of comparing ambari blueprint files, but it works\n" \
                     "reasonablely well with JSON and XML.\n\n" \
                     "A limitation of the tool is its ability to compare plain text found in JSON or XML files.  The\n" \
                     "tool makes an effort to identify variable assignments, but does not attempt to interpret any\n" \
                     "logic that may appear.\n\n" \
                     "Ambari blueprint files can be downloaded from a cluster with curl or saving after opening in a\n" \
                     "browser.  For example, a clustere's ambari blueprint can be found at:\n\n" \
                     '    https://ambari-<cluster_name>.symcpe.net/api/v1/clusters/<cluster_name>?format=blueprint' \
                     "\n\nExample commands:" \
                     "\n  To see the differences bewteen two JSON files:\n\tcompare-configs.py file1.json file2.json" \
                     "\n\n  To see the differences bewteen three XML files:\n\tcompare-configs.py file1.xml file2.xml file3.xml" \
                     "\n\n  To see the values set the same between three XML files:\n\tcompare-configs.py -s file1.xml file2.xml file3.xml" \
                     "\n\n  To save the results to a file:\n\tcompare-configs.py -v -o ~/file1-file2-diffs.tsv file1.json file2.json" \
                     "\n\n  To only see results for a specific value:\n\tcompare-configs.py -i falcon.http.authentication.type file1.json file2.json" \
                     "\n\n  To exclude values:\n\tcompare-configs.py -e .oozie. file1.json file2.json\n\n\n\n "


        parser = argparse.ArgumentParser(description=usage_desc, formatter_class=argparse.RawTextHelpFormatter)
        parser.add_argument('-v', '--verbose', action="store_true",
                            help='display all config values, rather than just values which have a difference')
        parser.add_argument('-s', '--same', action="store_true",
                            help='display only config values which are the same across all configs')
        parser.add_argument('-o', '--output', dest='output_file', metavar='output-file', default=None,
                            help='the file to write results in')
        parser.add_argument('-i', '--include', dest='include_pattern', metavar='include-pattern', default=None,
                            help='a crude mechanism to display only values with the include pattern as part ' \
                                 "of their path.\n   e.g. To display only capacity-scheduler properties one " \
                                 "would use\n\t\t -i \": capacity-scheduler : properties :\"")
        parser.add_argument('-e', '--exclude', dest='exclude_pattern', metavar='exclude-pattern', default=None,
                            help='a crude mechanism to supress the display of values with the exclude pattern as part ' \
                                 "of their path.\n   e.g. To supress the display of all log4j properties one " \
                                 "would use\n\t\t   -e \"log4j\"\n\n")
        parser.add_argument('first_conf', nargs=1, metavar='config1 config2 config3 ...')
        parser.add_argument('other_configs', nargs='+', metavar='conf2 conf3 ...', help=argparse.SUPPRESS)

        t_args = parser.parse_args()

        # options -v (verbose) and -s (same) are mutually exclusive, so error if both are set
        if t_args.verbose is True and t_args.same is True:
            print(
                "    Both the -v (verbose) and -s (same) option were provided.  These options are mutually exclusive.")
            sys.exit(1)

        self._validate_config_files(t_args)

        # TODO: pull blueprint on the fly from Ambari without relying on saved files
        # TODO:    https://<ambari-url>/api/v1/clusters/<cluster_name>?format=blueprint

        self._set_args(t_args)

    def _set_args(self, args):
        """
        Record arguements passed for furute use
        :param args: an instance of ArgumentParser
        """
        if args.output_file is not None and len(args.output_file):
            self.output_file = args.output_file
        if args.include_pattern is not None and len(args.include_pattern):
            self.include_pattern = args.include_pattern
        self.verbose_flag = args.verbose
        if args.exclude_pattern is not None and len(args.exclude_pattern):
            self.exclude_pattern = args.exclude_pattern
        self.same_flag = args.same

    def _do_file(self, blueprint, label, compare_root, blueprint_branch):
        """
        Record a multi-line string in the dictionary representing the configuration parameters ( compare_root )
        :param blueprint: <string> the name of the configuration file the information came from
        :param label: <string> the path to the configuration data in blueprint_branch
        :param compare_root: <dictionary> a dictionary representing the configuration parameters and values already
                                          observered
        :param blueprint_branch: <string> text to be recorded
        """
        # make \\\n in configs to \n for clean splitting
        wrk_blueprint_branch = re.sub('\\\\\\n', '\n', blueprint_branch)

        # get seperate lines for attemps to identify specific attribute / value pairs
        lines = wrk_blueprint_branch.split("\n")
        part_of_list = ''
        for line in lines:
            # skip comments
            if re.match("^\s*#", line):
                continue
            else:
                # see if we have a single line seperated by "\"
                if re.search("\\\\\s*$", line):
                    part_of_list += ' %s' % re.sub("\\\\\s*$", '', line)
                else:
                    # make certain we are not printing a list of variables
                    if len(part_of_list) > 0:
                        # remove tabs and white space
                        part_of_list = re.sub("\t", '', part_of_list)
                        element_array = re.sub("\s+", '', part_of_list).split(",")
                        part_of_list = ''
                        compare_root = self._do_branch(blueprint, label, compare_root, element_array)
                    else:
                        # remove leading white space
                        line = re.sub("^\s*", '', line)
                        # ignore blank lines
                        if len(line) > 0:
                            # remove the export command if present
                            line = re.sub("^\s*export\s*", '', line, flags=re.IGNORECASE)

                            # use a dict for key value pairs
                            # don't parse '==' as if it were the equals in attr /val pair
                            test_line = re.sub('==', 'equalsequals', line)
                            if '=' in test_line:
                                dkey, dval = test_line.split("=", 1)
                                # put back to how recieved
                                dkey = re.sub('equalsequals', '==', dkey)
                                dval = re.sub('equalsequals', '==', dval)
                                # strip out the pesky leading and trailing white space and tabs
                                dkey = re.sub("^\s*", '', dkey)
                                dkey = re.sub("\s*$", '', dkey)
                                dkey = re.sub("\t", '', dkey)
                                dval = re.sub("^\s*", '', dval)
                                dval = re.sub("\s*$", '', dval)
                                dval = re.sub("\t", '', dval)

                                # put the key and value in an associative array for recording
                                ddict = {dkey: dval}
                                compare_root = self._do_branch(blueprint, label, compare_root, ddict)
                            else:
                                compare_root = self._do_branch(blueprint, label, compare_root, line)

    def _do_branch(self, blueprint, label, compare_root, blueprint_branch):
        """
        Record data in blueprint_branch in the dictionary representing the configuration parameters ( compare_root )
        :param blueprint: <string> the name of the configuration file the information came from
        :param label: <string> the path to the configuration data in blueprint_branch
        :param compare_root: <dictionary> a dictionary representing the configuration parameters and values already
                                          observered
        :param blueprint_branch: <string> <dictionary|list|string|unicode|int> configuration information from the
                                                                               configuration source (blueprint)
        :return: <dictionary>
        """

        # see if we were passed parsed XML in an OrderedDict
        if isinstance(blueprint_branch, collections.OrderedDict):
            # make path label for this XML chunk
            pass_label = label
            attr_label = None
            attr_value = None

            # first we process process attributes and simple elements
            for k in sorted(blueprint_branch.keys()):

                # is it something other than attributes or a simple element
                if not isinstance(blueprint_branch[k], unicode): continue

                # remove leading and trailing space and tabs
                clean_k = re.sub("^\s+", '', str(k))
                clean_k = re.sub("\s+$", '', clean_k)
                clean_k = re.sub("\t", '', clean_k)
                if attr_label is not None:
                    attr_label += ' - %s' % clean_k
                    attr_value += ' - %s' % blueprint_branch[k]
                else:
                    attr_label = clean_k
                    attr_value = blueprint_branch[k]

            # build label for tracking paths and values
            if attr_value is not None:
                if pass_label is not None:
                    pass_label += ' : %s' % attr_label
                else:
                    pass_label = attr_label

                compare_root = _chk_label(compare_root, pass_label, blueprint)
                # walk branch to the end
                compare_root = self._do_branch(blueprint, pass_label, compare_root, attr_value)

            # now process each element of the XML
            for k in sorted(blueprint_branch.keys()):

                # skip the elements and attributes already recorded above
                if isinstance(blueprint_branch[k], unicode): continue

                new_label = pass_label
                # remove leading and trailing space and tabs
                clean_k = re.sub("^\s+", '', str(k))
                clean_k = re.sub("\s+$", '', clean_k)
                clean_k = re.sub("\t", '', clean_k)

                if new_label is not None:
                    new_label += ' : %s' % clean_k
                else:
                    new_label = clean_k

                compare_root = _chk_label(compare_root, new_label, blueprint)
                # walk branch to the end
                compare_root = self._do_branch(blueprint, new_label, compare_root, blueprint_branch[k])
        # see if we were passed a dictionary
        elif isinstance(blueprint_branch, dict):
            for k in sorted(blueprint_branch.keys()):
                new_label = label
                # remove leading and trailing space and tabs
                clean_k = re.sub("^\s+", '', str(k))
                clean_k = re.sub("\s+$", '', clean_k)
                clean_k = re.sub("\t", '', clean_k)

                if new_label is not None:
                    new_label += ' : %s' % clean_k
                else:
                    new_label = clean_k

                compare_root = _chk_label(compare_root, new_label, blueprint)
                # walk branch to the end
                compare_root = self._do_branch(blueprint, new_label, compare_root, blueprint_branch[k])
        # see if we were passed a list
        elif isinstance(blueprint_branch, list):
            for e in blueprint_branch:
                new_label = label
                if new_label is not None:
                    new_label += ' : ELEMENT'
                else:
                    new_label = 'ELEMENT'

                compare_root = _chk_label(compare_root, new_label, blueprint)
                # walk each element to the end
                compare_root = self._do_branch(blueprint, new_label, compare_root, e)
        # see if we were passed text
        elif isinstance(blueprint_branch, str) or isinstance(blueprint_branch, unicode) \
                or isinstance(blueprint_branch, int) or isinstance(blueprint_branch, float) or blueprint_branch is None:

            # reformat floats as strings and print to report
            if isinstance(blueprint_branch, float):
                _do_plain_text(blueprint, label, compare_root, str(blueprint_branch))
            # test to see if we have xml-like data
            elif re.match("\s*<\w+>", blueprint_branch) or re.match("\s*<\?xml version", blueprint_branch) \
                    or re.match("\s*<!--", blueprint_branch):

                xml_tree = xmltodict.parse(blueprint_branch)
                # walk xml to the end
                compare_root = self._do_branch(blueprint, label, compare_root, xml_tree)

            # test to see if we have a multiline text config
            elif re.search("\n", blueprint_branch):
                # ensure any text which can be treated as a json doesn't slip by
                try:
                    new_blueprint_branch = json.loads(blueprint_branch)
                    # success, use the JSON for processing
                    compare_root = self._do_branch(blueprint, label, compare_root, new_blueprint_branch)
                except:
                    # we have a text config, first ensure we compare the full text data to the other configs
                    _do_plain_text(blueprint, label, compare_root, blueprint_branch)

                    # now try pulling and attribute value pairs from the text for comparison
                    # exclude commentted out lines and record the vars
                    self._do_file(blueprint, label, compare_root, blueprint_branch)

            else:
                # just a simple string
                _do_plain_text(blueprint, label, compare_root, blueprint_branch)
        else:
            print("\n\n  unknown type found: blueprint_branch type is %s\n\n" % type(blueprint_branch))
            pprint.pprint(blueprint_branch)
            print("\n\n")
            sys.exit(1)

        return compare_root

    def _skip_line(self, config_details):
        """
        Enforce command line option logic.
            If the -s (same) option was passed then return True if one the configs is not recorded is not
                recorded in the config_details list.
            If neither the -s (same) nor the -v (verbose) option was passed we are only displaying values which
                are not the same across all configs, so return True when all of the configs are recorded in the
                config_details list.
            If the -v (verbose) option was passed then never return True

        :param config_details: list of config file the value was found in
        :return: bool
        """
        if self.same_flag:
            # only continue if all configs are in config_details
            if len(config_details) != len(self.configs):
                return True
        elif self.verbose_flag is True:
            # never skip a line for verbose
            return False
        elif len(config_details) == len(self.configs):
            # default behavior, only show if the value is not in all configs
            return True
        else:
            # default behavior, value is not in all configs, so show it
            return False

    def _skip_include_test(self, value):
        """
        Test to see if the value should be skipped based on the include pattern option value
        :param value:
        :return:
        """
        if self.include_pattern is None:
            return False
        elif re.search(self.include_pattern, value):
            return False
        else:
            return True

    def _skip_exclude_test(self, value):
        """
        Test to see if the value should be skipped based on the exclude pattern option value
        :param value:
        :return:
        """
        if self.exclude_pattern is None:
            return False
        elif re.search(self.exclude_pattern, value):
            return True
        else:
            return False

    def _skip_value(self, value):
        """
        Enforce include and exclude patterns
        :param value:
        :return:
        """
        i = self._skip_include_test(value)
        e = self._skip_exclude_test(value)

        if i is True or e is True:
            return True
        else:
            return False

    def _get_spew_line(self, details):
        """
        Generate a tab delminated string of Xs to indicate the presence of the tested configuration information
        :param details: <list> list of configuration files containing the configuration information
        :return: <string>
        """
        spew_line = ''
        for dc in self.configs:
            if dc in details:
                spew_line += '\t X '
            else:
                spew_line += '\t - '
        return spew_line

    def _spew_path_differences(self, key, config_details):
        """
        Writes configuration information results for a path to output
        :param key: <string> the label identifying the configuration information path
        :param config_details: <list> a list containing the configuration sources in which the configuration
                                      information was found
        """
        if self._skip_line(config_details): return
        if self._skip_value(key): return

        # include a space in the value column for pasting into wikis and the like
        line = '%s\t" "%s' % (key, self._get_spew_line(config_details))
        if self.file_hdl is not None:
            self.file_hdl.write("%s\n" % line)
        else:
            print("%s" % line)

    def _spew_value_differences(self, key, config_details):
        """
        Writes configuration information results for a specific value to output
        :param key: <string> the label identifying the configuration information path and value
        :param config_details: <list> a list containing the configuration sources in which the configuration
                                      information was found
        """
        if self._skip_line(config_details): return
        if self._skip_value(key): return

        # put path to value in line first.
        line = '%s' % " : ".join(key.split(" : ")[0:-1])
        # put value in separate var to sort out how much to display
        value = key.split(" : ")[-1].lstrip()
        # present a shorter value if the value is long
        if len(value) > self.short_val_length:
            # remove leading comments
            short_val = ''
            rows = value.split("\\n")  # split on \\n because we substitue \\n for \n to keep Excel formatted nicely
            for r in rows:
                if re.match("^\s*#", r) or r is None or len(r) < 1: continue
                r = r.lstrip()
                if short_val == '':
                    short_val = "%s" % r
                else:
                    short_val += " %s" % r
            line += '\t%s ... ' % short_val[0:(self.short_val_length - 1)]
            # if the value started with a " and had an odd number of them, put a closing quote for Excel
            if re.match('\s*"', short_val[0:(self.short_val_length - 1)]) and (
                        short_val[0:(self.short_val_length - 1)].count('"') % 2):
                line += '"'
        else:
            line += '\t%s' % value
        line += '%s' % self._get_spew_line(config_details)

        # put the full value at the end of the row if it is long
        if len(value) > self.short_val_length:
            line += '\t%s' % value

        if self.file_hdl is not None:
            self.file_hdl.write("%s\n" % line)
        else:
            print("%s" % line)

    def _print_differences(self, key, config_details):
        """
        Walks the dictionary representing the configuration parameters and values found in the configuration sources
        sending information to be printed to _spew_path_differences for path items that are difference and
        :param key: <string> the label identifying the configuration information
        :param config_details: <dictionary|list> contains the configuration sources in which the configuration
                                                 information was found
        """
        if isinstance(config_details, dict):
            # see if it is a value holder or just a part of the path
            if config_details.get('clusters') is not None:
                self._spew_path_differences(key, config_details['clusters'])
                if config_details.get('values') is not None:
                    self._print_differences(key, config_details['values'])
            else:
                # look at each key in the dictionary
                for dk in sorted(config_details.keys()):
                    new_key = key
                    if new_key is None:
                        new_key = dk
                    else:
                        new_key += ' : %s' % dk
                    self._print_differences(new_key, config_details[dk])
        elif isinstance(config_details, list):
            # check if we are at a list of dictionaries in the tree or a leaf
            # if the list is empty treat as a list
            if len(config_details) and isinstance(config_details[0], dict):
                # check to see if this element is unique or repeated in other elements of the list
                print("\n\nWE RAN INTO AN ELEMENT LIST")
                pprint.pprint(config_details)
                sys.exit(1)
            else:
                # we are at a leaf (aka value) in the config tree, see if all configs have the same value
                self._spew_value_differences(key, sorted(config_details))
        else:
            print("\n\n  unknown type found: config_details type is %s\n\n" % type(config_details))
            pprint.pprint(config_details)
            print("\n\n")
            sys.exit(1)

    def do_compare(self):
        """
        Walk the list of configurations from self.configs and record found parameters in a dictionary
        :return: <dictionary>
        """
        # create a dictionary to hold all blueprint info for comparison and
        # a label to track path to subsection
        blueprint_hdl = {}

        # load the blueprints into the
        for b in self.configs:
            # load blueprint
            with open(b, 'r') as f:
                cur_bp = f.read()
                blueprint_hdl = self._do_branch(b, None, blueprint_hdl, cur_bp)

        # return the dictionary of information found in configs
        return blueprint_hdl

    def print_result(self, compare_results):
        """
        Walk the dictionary holding the found configuration information and display it
        :param compare_results: <dictionary > a dictionary representing the configuration parameters and values already
                                              observered
        """
        # check if the output should go to stdout or a file
        if self.output_file is not None:
            self.file_hdl = open(self.output_file, 'w')

        # generate report headers
        headers = "PATH\tVALUE"
        for b in self.configs:
            headers += "\t%s" % b
        headers += "\tCOMPLETE VALUE IF TRUNCATED"

        # write headers
        if self.file_hdl is not None:
            self.file_hdl.write("%s\n" % headers)
        else:
            print("%s" % headers)

        # print out differences
        for k in sorted(compare_results.keys()):
            self._print_differences(k, compare_results[k])
        if self.file_hdl is not None:
            self.file_hdl.close()
