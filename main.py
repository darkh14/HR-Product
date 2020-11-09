# import numpy as np
# import pandas as pd

# from pprint import pformat
# from cgi import parse_qsl

# from bson.objectid import ObjectId

# from urllib.parse import quote_plus

import http_procession
import json

def t_application(start_response):
    parameters_string = make_parameters_string_from_json()
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


def make_parameters_string_from_json():
    p_file = open('HR_Product\parameters.json', 'r', encoding="utf-8")
    parameters_string = p_file.read()
    return parameters_string


def t_start_response(status, headers):
    pass


if __name__ == '__main__':
    output = t_application(t_start_response)


