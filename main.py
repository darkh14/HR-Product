# import numpy as np
# import pandas as pd

# from pprint import pformat
# from cgi import parse_qsl

# from bson.objectid import ObjectId

# from urllib.parse import quote_plus

import http_procession
import json
import warnings
# import cProfile
warnings.simplefilter(action='ignore', category=FutureWarning)
# import tensorflow as tf


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
    request_types.append('get_all_cvs')
    # request_types.append('set_vacancies')
    # request_types.append('set_profiles')
    # request_types.append('set_cv_vacancy_labels')
    # request_types.append('refill_cv_collection')
    # request_types.append('check_job_status')
    # request_types.append('delete_jobs')
    # request_types.append('set_filter_collection')

    # pr = cProfile.Profile()
    # pr.enable()

    for request_type in request_types:
        output = t_application(request_type, t_start_response)

        output_str = output[0].decode()

        output_dict = json.loads(output_str)

        if request_type == 'get_fitting_cvs':
            print(len(output_dict['fitting_cvs']))
            if output_dict['fitting_cvs']:
                print(output_dict['fitting_cvs'][0])
        elif request_type == 'get_all_cvs':
            print(output_dict['all_cvs'][0])
        elif request_type == 'refill_cv_collection':
            print(output_dict)
        elif request_type == 'set_vacancies':
            print(output_dict)
        elif request_type == 'check_job_status':
            print(output_dict)
        elif request_type == 'set_filter_collection':
            print(output_dict)

    # pr.disable()  # after your program
    # pr.print_stats(sort="calls")

    # connector = mongo_connection.MongoDBConnector()
    # cv = connector.get_cv()
    #
    # edu = []
    # for cv_line in cv.find():
    #     if cv_line['address'] not in edu:
    #         edu.append(cv_line['address'])
    #
    # print(edu)

