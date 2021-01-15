# import numpy as np
# import pandas as pd

# from pprint import pformat
# from cgi import parse_qsl

# from bson.objectid import ObjectId

# from urllib.parse import quote_plus

import http_procession
import json
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
# import tensorflow as tf
import os

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
filepath = r'C:\Users\Mi\Desktop\КИК.txt'

def t_application(request_type, start_response):

    parameters_string = make_parameters_string_from_json(request_type)

    environ = dict()
    environ['REQUEST_METHOD'] = 'POST'

    environ['wsgi.input'] = [parameters_string]

    output = http_procession.process(environ, start_response)

    return output


def make_parameters_string_from_list(parameters):
    parameters_string = ''
    is_first = False
    for key, value in parameters:
        parameters_string += '' if is_first else '&' + key + '=' + str(value)
        is_first = True

    return parameters_string


def make_parameters_string_from_json(request_type):

    file_path = request_type + '_parameters.json'

    p_file = open(file_path, 'r', encoding="utf-8")
    parameters_string = p_file.read()
    return parameters_string

def t_start_response(status, headers):
    pass


if __name__ == '__main__':

    # print(tf.__version__)
    # print(tf.config.list_physical_devices('GPU'))

    request_types = list()
    # request_types.append('get_fitting_cvs')
    # request_types.append('get_all_cvs')
    # request_types.append('set_cv_vacancy_labels')
    request_types.append('refill_cv_collection')

    for request_type in request_types:
        output = t_application(request_type, t_start_response)

        output_str = output[0].decode()

        output_dict = json.loads(output_str)

        print(output_dict['status'])

        if request_type == 'get_fitting_cvs':
            print(output_dict['fitting_cvs'][0])
        elif request_type == 'get_all_cvs':
            print(output_dict['all_cvs'][0])

