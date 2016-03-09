import os
import sys
import json
import logging
from inspect import getfile, currentframe
from os.path import join, dirname, abspath

from getopt import getopt

def usage(script_name):
    print 'Usage: python ', script_name,    \
                   '-d [logging_level]      \
                    -f [update_frequency]   \
                    -h [help]'


def process_options(script_name):
    '''
        Process command line options
    '''

    options, remainders = getopt(sys.argv[1:], "d:f:h", ["debug=", "frequency=", "help"])

    args = {}
    for (opt, arg) in options:
        if opt in ('-d', '--debug'):
            args['log_level'] = int(arg)
        elif opt in ('-f', '--frequency'):
            args['update_frequency'] = int(arg)
        elif opt in ('-h', '--help'):
            usage(script_name)

    return args

def setup_logging(config_file_path,
                  default_level=logging.INFO):
    '''
        Setup logging configuration
    '''

    if os.path.exists(config_file_path):
        with open(config_file_path, 'rt') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)

def get_full_path(file_name):
    '''
        Get the full path of File %file_name
    '''

    curr_path = dirname(abspath(getfile(currentframe())))
    file_path = join(curr_path, file_name)
    return file_path

