# coding: utf-8

# ------------------------------------------------------------------------
# Module Name :- HDFS Stats Generator
# Module description : - Generates Graphical Stats for the tables in hdfs directory mentioned in the json file passed through --jsonconfig <db_config.json>
# Parameters required :- --jsonconfig <db_config.json>
# Created by :- Jatin Chauhan
# Created on :- 7 JULY 2019
# Version History :- VERSION 1.4
# ----------------------

import os
import io
import sys
import math
import json
import base64
import smtplib
import argparse
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

parser = argparse.ArgumentParser()
parser.add_argument('-j','--jsonconfig', help='Input the JSON file which servers as a configuratiion to HDFS Stats', default="NA")
parser.add_argument('-c','--cssfile', help='Input the css file', default="NA")
args = parser.parse_args()

#Use this to prevent column from wrapping up data
pd.set_option('display.max_colwidth', -1)
spark = ""
stats, out_dict, df_all_stats = {}, {}, ""

def main():
    module_name = os.path.basename(sys.argv[0])
    try:
        param_str = args.jsonconfig
    except Exception as e:
        param_str = None
    if param_str is not None and type(param_str) == str:
        print("value of run_args for module {0} is: ".format(module_name) + param_str)
        try:
            file_name = param_str
        except Exception as e:
            print("Error in pyspark run_args check  module config file of {}. Following error is detected: ".format(module_name) + str(e))
            raise e
    else:
        pass
    return file_name


# Uses smtp library to send mails
def send_mail(franchise_name, send_from, send_to, subject, email_content, files,
              server_host, server_port):
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = send_to
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject
    msg.preamble = """
    Please open these reports using Google Chrome!"""

    msg.attach(MIMEText(email_content, 'html'))

    f = "{0} HDFS Stats.html".format(franchise_name)
    part = MIMEApplication(files, Name=os.path.basename(f))
    part['Content-Disposition'] = 'attachment; filename="%s"' % os.path.basename(f)
    msg.attach(part)
    smtp = smtplib.SMTP(server_host, server_port)
    smtp.starttls()
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()

def fig_to_base64(fig):
    img = io.BytesIO()
    fig.savefig(img, format='png',
                bbox_inches='tight')
    img.seek(0)
    return base64.b64encode(img.getvalue())

def get_convert_nan_to_zero(dataframe):
    return dataframe.fillna(0)

def get_total_size(dataframe):
    #Get total size in TB
    total = dataframe['totalSize'].sum()
    return round(total/1024,2)

def convert_kb_to_gb(dataframe, column):
    MBFACTOR = float(1<<30) #Converts into GB
    dataframe[column] = dataframe[column].apply(lambda z: z/MBFACTOR,2)
    return dataframe

def get_unique_owner_name(dataframe):
    unique_val = dataframe.Owner.unique()
    return unique_val

def get_plot(dataframe, param_1, param_2):
    print("GET_PLOT_DF: " + str(dataframe.head(1)))
    dataframe = dataframe.sort_values(by='totalSize', ascending=False)
    dataframe = dataframe.head(50)
    ax = plt.subplots()
    ax = dataframe[[param_1,param_2]].plot(kind='bar', title ="Top 50 Tables HDFS Size",legend=True, fontsize=12)
    ax.set_xlabel("Table Name", fontsize=20)
    ax.set_ylabel("Size  on HDFS (GB)", fontsize=20)
    f = ax.figure
    f.set_figheight(9)
    f.set_figwidth(25)
    return f

def get_top_table(dataframe, num_of_rows):
    dataframe = dataframe.sort_values(by ='totalSize', ascending=False)
    dataframe = dataframe.head(num_of_rows)
    dataframe.rename(columns={'totalSize': 'Total Size (GB)'}, inplace=True)
    return dataframe[['Owner', 'Location', 'Total Size (GB)']].to_html(classes='mystyle shadow p-3 mb-5 bg-white rounded')

def get_zerokb_table(dataframe):
    dataframe = dataframe.loc[dataframe['totalSize'] == 0]
    dataframe.rename(columns={'totalSize': 'Total Size (GB)'}, inplace=True)
    return dataframe[['Owner', 'Location']].to_html(classes='mystyle shadow p-3 mb-5 bg-white rounded')

def get_text_formatted_tables(dataframe):
    dataframe = dataframe[~dataframe['Table Name'].str.contains("ref")]
    dataframe = dataframe[dataframe['InputFormat'] == "org.apache.hadoop.mapred.TextInputFormat"]
    dataframe = dataframe.sort_values(['totalSize', 'Owner'], ascending=[False, True])
    dataframe.rename(columns={'totalSize': 'Total Size (GB)'}, inplace=True)
    return dataframe[['Owner', 'Location', 'Total Size (GB)']].to_html(classes='mystyle shadow p-3 mb-5 bg-white rounded')

def get_html_header(dataframe_all_stats, dataframe, franchise):
    input_props = {
    "db_name" : str(franchise).upper(),
    "db_size" : str(get_total_size(dataframe)),
    "db_all_size" : str(get_total_size(dataframe_all_stats)),
    "hdfs_location_all" : get_common_location(dataframe_all_stats),
    "hdfs_location" : get_common_location(dataframe),
    "top_10_tables_style" : get_top_table(dataframe, 10),
    "zerokb_tables_style" : get_zerokb_table(dataframe),
    "style" : style
    }
    html_buffer = '<link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css" integrity="sha384-ggOyR0iXCbMQv3Xipma34MD+dH/1fQ784/j6cY/iJTQUOhcWr7x9JvoRxT2MZw1T" crossorigin="anonymous"><script src="https://code.jquery.com/jquery-3.3.1.slim.min.js" integrity="sha384-q8i/X+965DzO0rT7abK41JStQIAqVgRVzpbzo5smXKp4YfRvH+8abtTE1Pi6jizo" crossorigin="anonymous"></script><script src="https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.14.7/umd/popper.min.js" integrity="sha384-UO2eT0CpHqdSJQ6hJty5KVphtPhzWj9WO1clHTMGa3JDZwrnQq4sF86dIHNDz0W1" crossorigin="anonymous"></script><script src="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/js/bootstrap.min.js" integrity="sha384-JjSmVgyd0p3pXB1rRibZUAYoIIy6OrQ6VrjIEaFf/nJGzIxFDsf4x0xIM+B07jRM" crossorigin="anonymous"></script>'
    html_buffer = html_buffer + '''
    <style>
    {style}
    </style>
    <div class="container">
    <h1 class="display-4 p-4 text-blue"> {db_name} Database HDFS Stats</h1>
    <div class="row">
          <div class="col-sm-6">
            <div class="card shadow-lg p-3 mb-5 bg-white rounded">
              <div class="card-body">
                <h5 class="card-title text-blue">All Database Size</h5>
                <p class="card-text"><p class="font-weight-light">HDFS LOCATION:</p> <h7 class="text-monospace"> {hdfs_location_all}</h7></p>
                <p class="border-text"><span class="text-blue">SIZE:</span> {db_all_size} TB <span class="text-blue">(Without Replication)</span></p>
              </div>
            </div>
          </div>
          <div class="col-sm-6">
            <div class="card shadow-lg p-3 mb-5 bg-white rounded">
              <div class="card-body">
                <h5 class="card-title text-blue">{db_name} Database Size</h5>
                <p class="card-text"><p class="font-weight-light">HDFS LOCATION:</p> <h7 class="text-monospace"> {hdfs_location}</h7></p>
                <p class="border-text"><span class="text-blue">SIZE:</span> {db_size} TB <span class="text-blue">(Without Replication)</span></p>
              </div>
            </div>
          </div>
        </div>
    </div>
    <br>
        '''.format(**input_props)
    return html_buffer

def get_html_footer(dataframe_all_stats, dataframe, franchise):
    input_props = {
    "db_name" : str(franchise).upper(),
    "db_size" : str(get_total_size(dataframe)),
    "db_all_size" : str(get_total_size(dataframe_all_stats)),
    "top_10_tables" : get_top_table(dataframe, 10),
    "zerokb_tables" : get_zerokb_table(dataframe),
    "text_formatted_tables" : get_text_formatted_tables(dataframe)
    }
    html_buffer = '''
        <div class="row">
          <div class="col-sm-12">
            <div class="card">
              <div class="card-body">
                <h5 class="card-title title-text">{db_name} Database TOP 10 Tables</h5>
                <p class="card-text">{top_10_tables}</p>
              </div>
            </div>
          </div>
        </div>
        <div class="row">
          <div class="col-sm-12">
            <div class="card">
              <div class="card-body">
                <h5 class="card-title title-text">{db_name} Database with Empty Tables (0KB)</h5>
                <p class="card-text">{zerokb_tables}</p>
              </div>
            </div>
          </div>
        </div>
        <div class="row">
          <div class="col-sm-12">
            <div class="card">
              <div class="card-body">
                <h5 class="card-title title-text">{db_name} Database with Tables in Text Format</h5>
                <p class="card-text">{text_formatted_tables}</p>
              </div>
            </div>
          </div>
        </div>
        <footer class="blue page-footer font-small">

        <div class="footer-copyright text-center py-3">© 2019 ZS:
          <a href="mailto:jatin.chauhan@zs.com"> NEED SUPPORT?</a>
        </div>
        <!-- Copyright -->

        </footer>
        <!-- Footer -->
        '''.format(**input_props)
    return html_buffer

def get_common_location(dataframe):
    arr = dataframe['Location'].unique().tolist()
    n = len(arr)
    s = arr[0]
    l = len(s)
    res = ""
    for i in range( l) :
        for j in range( i + 1, l + 1) :
            stem = s[i:j]
            k = 1
            for k in range(1, n):
                if stem not in arr[k]:
                    break
            if (k + 1 == n and len(res) < len(stem)):
                res = stem
    return res

#Reads JSON File
def read_from_json(file_path):
    with open(file_path) as json_file:
        data = json.load(json_file)
    return data

def get_html(dataframe_all_stats, dataframe, franchise):
    df_temp = get_plot(dataframe, 'Table Name', 'totalSize')
    encoded, franchise = fig_to_base64(df_temp), franchise
    my_html = get_html_header(dataframe_all_stats, dataframe, franchise)
    my_html = my_html + '<img src="data:image/png;base64, {}" height="100%" >'.format(encoded.decode('utf-8'))
    my_html = my_html + get_html_footer(dataframe_all_stats, dataframe, franchise)
    return my_html

#Converts JSON Object into a list with only values
def convert_json_to_list(json_obj, typ):
    json_val_list = []
    if typ.lower() == "key":
        for items in json_obj.keys():
            json_val_list.append(items)
    if typ.lower() == "value":
        for items in json_obj.values():
            json_val_list.append(items)
    return json_val_list

# Returns a db_tbl_name<list> with table names with
# respect to the database name -> db_name
def find_db_tbl_name(db_name, spark):
    input_list, db_tbl_name = spark.sql("show tables in {0}".format(db_name)).collect(), []
    for index in range(len(input_list)):
        db_tbl_name.append("" + input_list[index].__getitem__("database") + "." + input_list[index].__getitem__("tableName"))
    return db_tbl_name # Format -> <db_name>.<tbl_name>

def filter_tbl_list(db_name, tbl_name, spark):
    input_list = spark.sql("describe formatted {0}.{1}".format(db_name, tbl_name)).collect()
    for index in range(len(input_list)):
        index_val = input_list[index].__getitem__("col_name")
        if index_val == 'Type':
            value = input_list[index].__getitem__("data_type")
            if value == "VIEW":
                return False
    return True

#Takes DB_Name and TBL_Name as input and populates the <stats> dictionary
# with the collected Metadata of Individual Tables
def find_tbl_stats(db_name, tbl_name, spark): # Takes one TBL at a time
    analyse = spark.sql("analyze table {0}.{1} compute statistics noscan".format(db_name, tbl_name)).collect()
    input_list = spark.sql("describe formatted {0}.{1}".format(db_name, tbl_name)).collect()
    params_to_check = ['Owner','Location', 'InputFormat']

    for index in range(len(input_list)):
        index_val = input_list[index].__getitem__("col_name")
        if index_val in params_to_check:
            prop, val = index_val, input_list[index].__getitem__("data_type")
            stats[db_name][tbl_name].update({prop : val})

        if input_list[index].__getitem__("col_name") == 'Table Properties':
            val = input_list[index].__getitem__("data_type")
            val = string_to_dict(val) #val is now a <dict>
            if 'totalSize' in val.keys():
                val = val['totalSize'] #vak is now validated according to 'Table Properties' requirement
                stats[db_name][tbl_name].update({'totalSize' : val})
            else:
                val = 0
                stats[db_name][tbl_name].update({'totalSize' : val})
        if input_list[index].__getitem__("col_name") == 'Statistics':
            val = input_list[index].__getitem__("data_type")
            val = int(val.split(' ')[0])
            stats[db_name][tbl_name].update({'totalSize' : val})


# Input -> db_names_hdfs<list> of all the database obtained from the input
def collect_all_stats(db_names_hdfs, spark):
    for index in range(len(db_names_hdfs)): # Iterate db wise from the db_names_hdfs<list>
        db_name = db_names_hdfs[index]

        # Returns a db_meta_list<list> with table_names in the db_name<string>
        # one at a time
        db_meta_list = find_db_tbl_name(db_name, spark)

        # Generates the <db_name>.<tbl_name> key-value pairs in the stats<dict>
        for stat_itr in db_meta_list:
            db_name_meta_list = stat_itr.split('.')[0]
            tbl_name_meta_list = stat_itr.split('.')[1]
            if filter_tbl_list(db_name_meta_list, tbl_name_meta_list, spark):
                if db_name_meta_list not in stats:
                    stats[db_name_meta_list] = {}
                else:
                    stats[db_name_meta_list].update({tbl_name_meta_list : {} })

        for tbls_names_meta_list in stats[db_name].keys():
            find_tbl_stats(db_name, tbls_names_meta_list, spark)

        out_dict = {}
        for db_index in stats.keys():
            collect_db_stat(db_name)

# Input -> parent<string> which is the DataBase name
def collect_db_stat(parent):
    parent_key, out_list = parent, []
    for tb_ind in stats[parent_key].keys():
        for meta in stats[parent_key][tb_ind].keys():
            out_key, out_list = parent_key + "." + tb_ind, []
            out_list.append(parent_key)
            out_list.append(tb_ind)

            param_list = ['Owner','Location','totalSize', 'InputFormat']

            for param_list_item in param_list:
                out_list.append(stats[parent_key][tb_ind][param_list_item])

            out_dict.update({out_key:out_list})

def clean_indexes(db_name_dict_seg, db_index):
    for idx in db_name_dict_seg.index:
        tbl = idx.split('.')[1]
        db_name_dict_seg = db_name_dict_seg.rename(index={idx: tbl})
    return db_name_dict_seg

def segregate_df_on_db(db_name_dict, df_all_stats): #Input as list
    db_name_dict_seg = {}
    for db_index in db_name_dict:
        print("Creating an Empty <dict> for: " + str(db_index))
        db_name_dict_seg[db_index] = {}
        db_name_dict_seg[db_index] = df_all_stats[df_all_stats['DB Name'] == db_index]
        print("Cleaning Indexes of the Dataframe: <db_name>.<tb_name> to <tb_name>")
        db_df_temp = db_name_dict_seg[db_index]
        db_df_temp = clean_indexes(db_df_temp, db_index)
        db_name_dict_seg[db_index] = db_df_temp
        print("Segregated for: " + str(db_index))
    return db_name_dict_seg

def string_to_dict(str_list):
    str_out_dict = {}
    str_out_list = str_list.replace(' ', '').replace('[', '').replace(']', '')
    str_out_list = str_out_list.split(',')
    for list_item in str_out_list:
        list_key_val = list_item.split('=')
        list_key, list_val = list_key_val[0], list_key_val[1]
        str_out_dict.update({list_key : list_val})
    return str_out_dict

def convert_clm_to_numeric(dataframe):
    dataframe['totalSize'] = dataframe['totalSize'].apply(pd.to_numeric)
    return dataframe

def generate_mail(db_json, db_names_hdfs, df_all_stats, df_db_name_dict_seg_stats, admin):
    html_dump = ""
    for db in db_names_hdfs:
        # Function fingerprint: franchise_name, send_from, send_to, subject, email_content, files, server_host, server_port
        if admin == False:
            html_dump = get_html(df_all_stats, df_db_name_dict_seg_stats[db], db_json[db]["name"])
            send_mail(db_json[db]["name"], db_json["admin"]["email"], db_json[db]["email"], db_json["mail_format"]["subject"], db_json["mail_format"]["body"], html_dump, db_json["admin"]["server_host"], db_json["admin"]["server_port"])
        if admin == True:
            html_dump = html_dump + get_html(df_all_stats, df_db_name_dict_seg_stats[db], db_json[db]["name"])
    if admin == True:
        send_mail("HDFS Report", db_json["admin"]["email"], db_json["admin"]["all_stats_to_email"], db_json["mail_format"]["subject"], db_json["mail_format"]["body"], html_dump, db_json["admin"]["server_host"], db_json["admin"]["server_port"])

if __name__ == '__main__':

    file_name = main()
    css_file = args.cssfile

    try:
        spark = SparkSession.builder.enableHiveSupport().appName("HDFS Stats Generator").getOrCreate()
        print(spark.version)
        log=spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger(__name__)

        stats, out_dict = {}, {}
        with open(css_file, 'r') as myfile:
            style = myfile.read()

        db_names_file_path_to_json = file_name

        db_names_hdfs = convert_json_to_list(read_from_json(db_names_file_path_to_json), "key")
        db_json = read_from_json(db_names_file_path_to_json)

        db_names_hdfs.remove("admin")
        db_names_hdfs.remove("mail_format")

        print("Databases Used:")
        print("\n".join(db_names_hdfs))

        # Function populates the stat<dict> with all the info obtained from the
        # db_names_hdfs<list> database names
        print("Collecting all stats...")
        collect_all_stats(db_names_hdfs, spark)

        print("Creating the Dataframe...")
        # Creates a df_all_stats<DataFrame> with all the stats in the databases specified
        print("Creating DL_ALL_STATS <dict>")
        df_all_stats = pd.DataFrame.from_dict(out_dict, orient='index', columns=['DB Name', 'Table Name', 'Owner', 'Location', 'totalSize', 'InputFormat'])
        df_all_stats.rename_axis('Table Name')
        # Converts the column 'totalSize' to numeric
        df_all_stats = convert_clm_to_numeric(df_all_stats)
        # Converts the NaN values to to ZERO
        df_all_stats = get_convert_nan_to_zero(df_all_stats)
        # Converts the sizes from KB to GB
        df_all_stats = convert_kb_to_gb(df_all_stats, "totalSize")
        print("DL_ALL_STATS: " + str(df_all_stats.head(1)))
        print("Segregating dataframe database wise..")
        # Segregates the df_all_stats<DataFrame> into database wise <DataFrame> in df_db_name_dict_seg_stats<dict>
        df_db_name_dict_seg_stats = segregate_df_on_db(db_names_hdfs, df_all_stats)
        print("Sending mail to Franchise Leads...")
        #Send mail to the franchise leads
        generate_mail(db_json, db_names_hdfs, df_all_stats, df_db_name_dict_seg_stats, admin=False)
        print("Sending mail to Admin Leads...")
        #Send mail to the Admin
        generate_mail(db_json, db_names_hdfs, df_all_stats, df_db_name_dict_seg_stats, admin=True)

        print("Emailed all the reports")

    except Exception as e:
        msg = "Module execution interrupted, following error occured : " + str(e)
        raise Exception(msg)
