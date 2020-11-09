import http_procession

def application(environ, start_response):

    output = http_procession.process(environ, start_response)    # output.append('Post:')
    # output.append('<form method="post">')
    # output.append('<input type="text" name = "test">')
    # output.append('<input type="submit" value="Send">')
    # output.append('</form>')

    # d = parse_qsl(environ['QUERY_STRING'])
    # output.append(pformat(environ['wsgi.input'].read()))
    # output.append(str(environ['wsgi.input']))
    # if environ['REQUEST_METHOD'] == 'GET':
    #    if environ['QUERY_STRING'] != '':
    #        output.append('<h1>Get data:</h1>')
    #        for ch in d:
    #            output.append(' = '.join(ch))
    #            output.append('<br>')
    # print(output)

    return output

