import pandas_gbq

from django.shortcuts import render
from google.oauth2 import service_account

# Make sure you have installed pandas-gbq at first;
# You can use the other way to query BigQuery.
# please have a look at
# https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-nodejs
# To get your credential

credentials = service_account.Credentials.from_service_account_file(
    '/home/yerlan/Documents/data_storage/lab4/wide-maxim-376908-dd7e56c6355c.json')


def hello(request):
    context = dict()
    context['content1'] = 'Hello World!'
    return render(request, 'helloworld.html', context)


def dashboard(request):
    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = "wide-maxim-376908"

    SQL = "SELECT time___________________, ai, data, good, movie, spark " \
          "FROM `wide-maxim-376908.datastoragekbtu.rstcnt` " \
          "LIMIT 8"
    df = pandas_gbq.read_gbq(SQL)
    df_list = df.to_dict('records')

    data_list = []
    for df_row in df_list:
        data_row = dict()
        data_row["Time"] = df_row["time___________________"].strftime(format="%H:%M")
        df_row = dict(df_row)
        df_row.pop("time___________________")
        data_row["count"] = df_row
        data_list.append(data_row)

    data = dict()
    data["data"] = data_list

    '''
        TODO: Finish the SQL to query the data, it should be limited to 8 rows. 
        Then process them to format below:
        Format of data:
        {'data': [{'Time': hour:min, 'count': {'ai': xxx, 'data': xxx, 'good': xxx, 'movie': xxx, 'spark': xxx}},
                  {'Time': hour:min, 'count': {'ai': xxx, 'data': xxx, 'good': xxx, 'movie': xxx, 'spark': xxx}},
                  ...
                  ]
        }
    '''

    return render(request, 'dashboard.html', data)


def connection(request):
    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = "wide-maxim-376908"
    SQL1 = 'SELECT node ' \
           'FROM `wide-maxim-376908.datastoragekbtu.nodes`'
    df1 = pandas_gbq.read_gbq(SQL1)

    SQL2 = 'SELECT source, target ' \
           'FROM `wide-maxim-376908.datastoragekbtu.edges`'
    df2 = pandas_gbq.read_gbq(SQL2)

    data = {
        'n': list(df1.T.to_dict().values()),
        'e': list(df2.T.to_dict().values())
    }

    '''
        TODO: Finish the SQL to query the data, it should be limited to 8 rows. 
        Then process them to format below:
        Format of data:
        {
        ‘n’: [{‘node’: 18233},{‘node’: 18234},...]
        'e': [{'source':0, 'target':0},{'source':0, 'target':1},... ]
        }
    '''

    return render(request, 'connection.html', data)
