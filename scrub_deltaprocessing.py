#!/usr/bin/env python
# -*- coding: utf-8 -*-

#depends on bootstrap action to install this package
import boto3

import sys  # required for fetching command line arguments
import os
import time
import datetime
import hashlib
import json
import re
from babel import numbers
from pyspark import SparkConf,  SparkContext
from pyspark.sql import Row
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from time import gmtime, strftime
from py4j.protocol import Py4JJavaError



class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)
#### End Class ############################################################################


def __build_dataframe_schema(_sqlContext, filename):

    try:

        print __get_dataframe_schema(_sqlContext, filename)

        _schema_string = __get_dataframe_schema(_sqlContext, filename)

        _schema_json = json.loads(_schema_string)

        print json.dumps(_schema_json["schema"])
        # fields = [StructField(field_name, StringType(), True) for field_name in _df_columns.split(",")]
        fields = [StructField(field["name"], __get_dataframe_field_type(field["type"]), True) for field in _schema_json["schema"]["fields"]]

        return StructType(fields)

    except IOError as err:
        print "Get DataFrame Field Type: I/O error({0}): {1}".format(err.errno, err.strerror)
        sys.exit(1)
    except ValueError as _value_err:
        print("Get DataFrame Field Type: An value error occurred: {0}".format(_value_err))
        sys.exit(1)
    except Exception as e:
        print("Get DataFrame Field Type: Unexpected error:", str(e))
        sys.exit(1)
#### End Method ###########################################################################

def concatenate_csv_columns(_rowList, _outputDelimiter):
    ###########################################################################################
    # this method replace source delimiter with new delimiter and concatenate csv columns
    # last nodified by samuel davison on 03/01/2017
    ###########################################################################################

    try:

        # --{Define Local Variable}
        _data = ""
        _tmpStr = ""

        # --{Loop through each column}
        _iCol=0
        for column in _rowList:
            # --{Concatenate CSV columns}
            # print str(column)
            if column is None:
                print "column is None"
                _tmpStr = ""
            else:
                _ascii_column = ''.join(char for char in column if ord(char) < 128)
                _tmpStr = str(_ascii_column)
                #_tmpStr = str(column).encode('utf-8').strip()

            if _iCol > 0:
                _data = _data + __delimiter_get(_outputDelimiter) + str(_tmpStr.replace("\n", "").replace(__delimiter_get(_outputDelimiter), ";").replace("\t", " "))
            else:
                _data = _data + str(_tmpStr.replace("\n", "").replace(__delimiter_get(_outputDelimiter), ";").replace("\t", " "))

            _iCol += 1

        return _data

    except IOError as err:
        print "concatenate_csv_columns Error: I/O error({0}): {1}".format(err.errno, err.strerror)
        sys.exit(1)
    except ValueError as _value_err:
        print("concatenate_csv_columns: A Value error occurred: {0}".format(_value_err))
        sys.exit(1)
    except Exception as e:
        print("concatenate_csv_columns Data Unexpected error:", str(e))
        sys.exit(1)
#### End Method ###########################################################################

def __create_hash_data(_data):
    ###########################################################################################
    # this method replace source delimiter with new delimiter
    # last nodified by samuel davison on 03/01/2017
    ###########################################################################################

    try:

        return __hashdata(_data)

    except IOError as err:
        print "Create Hash Data Error: I/O error({0}): {1}".format(err.errno, err.strerror)
        sys.exit(1)
    except ValueError as _value_err:
        print("Create Hash Data: A Value error occurred: {0}".format(_value_err))
        sys.exit(1)
    except Exception as e:
        print("Create Hash Data Unexpected error:", str(e))
        sys.exit(1)
#### End Method ###########################################################################


def __create_hash_rdd(_raw_file_rdd, _flag_validate_records, _flag_regex_validation, _regex_validation, _validation_json, _source_delimiter):
    ###########################################################################################
    # This method is the script starting point
    # Last Nodified by Samuel Davison on 03/01/2017
    ###########################################################################################

    try:

        __hashRDD = _raw_file_rdd.map(lambda p: Row(record=p, hash=__create_hash_data(p), bad=__validating_data(p, _flag_validate_records, _flag_regex_validation, _regex_validation, _validation_json, _source_delimiter)))

        return __hashRDD

    except IOError as err:
        print "Create Hash RDD Error: I/O error({0}): {1}".format(err.errno, err.strerror)
        sys.exit(1)
    except ValueError as _value_err:
        print("Create Hash RDD A Value error occurred: {0}".format(_value_err))
        sys.exit(1)
    except Exception as e:
        print("Create Hash RDD Unexpected error:", str(e))
        sys.exit(1)
#### End Method ###########################################################################


def __currency_format(_data, _locale, _currency, _format_type):
    ###########################################################################################
    # This method loads the json validation file that specifies which fields will be validated
    # Last Nodified by Samuel Davison on 04/05/2017
    ###########################################################################################

    try:

        return numbers.format_currency(numbers.parse_decimal(_data.replace("$", ""), locale=_locale), _currency,
                                       locale=_locale, format_type=_format_type)

    except IOError as err:
        print "__currency_format: I/O error({0}): {1}".format(err.errno, err.strerror)
        return False
    except ValueError as _value_err:
        print("__currency_format: A Value error occurred: {0}".format(_value_err))
        return False
    except Exception as e:
        print("__currency_format: Unexpected error:", str(e))
        return False
#### End Method ###########################################################################


def __date_format(_data, _input__expression, _output_expression):
    ###########################################################################################
    # This method loads the json validation file that specifies which fields will be validated
    # Last Nodified by Samuel Davison on 04/05/2017
    ###########################################################################################

    try:

        if len(_data.strip()) > 0 and _data.strip() != "?":
            if _input__expression == "%m/%d/%y" and int(_data[-2:]) >= 50 and int(_data[-2:]) <= 70:
                return datetime.datetime.strptime(_data, _input__expression).strftime('19%y-%m-%d')
            else:
                return datetime.datetime.strptime(_data, _input__expression).strftime(_output_expression)
        else:
            return _data

    except IOError as err:
        print "__date_format Error: I/O error({0}): {1}".format(err.errno, err.strerror)
        return False
    except ValueError as _value_err:
        print("__date_format A Value error occurred: {0}".format(_value_err))
        return False
    except Exception as e:
        print("__date_format Unexpected error:", str(e))
        return False
#### End Method ###########################################################################


def __delimiter_get(keyword):
    ###########################################################################################
    # This method returns a character based on keyword
    # Returns '|' if no match
    # Last Nodified by serge declama on 11.08.2017
    ###########################################################################################

    return {
        'PIPE': '|',
        'TAB': '\t',
        'COMMA': ',',
        'COLON': ':',
        'SEMICOLON': ';',
        'TILDE': '~',
        'CARET': '^'
    }.get(keyword.upper(), '|')
#### End Method ###########################################################################


def __format_data(_data, _formatjson, _delimiter):
    ###########################################################################################
    # This method downloads the latest trackus changes from their RESTful API
    # Last Nodified by Samuel Davison on 4/02/2016
    ###########################################################################################

    __fields = _data.encode('utf-8').split(_delimiter)

    try:

        __errors = False
        __format_fields = json.loads(_formatjson)

        for __format in __format_fields["formats"]:
            if __fields[int(__format["column_index"])] != "?" and __fields[int(__format["column_index"])] != "":
                if __format["format_type"].lower() == "date":
                   __fields[int(__format["column_index"])] = __date_format(__fields[int(__format["column_index"])], __format["in_expression"], __format["out_expression"])
                elif __format["format_type"].lower() == "decimal":
                    if __format["function"].lower() == "parse":
                        __fields[int(__format["column_index"])] = numbers.parse_decimal(__fields[int(__format["column_index"])].replace("$", ""), locale='en_US')
                    else:
                        __fields[int(__format["column_index"])] = numbers.format_decimal(__fields[int(__format["column_index"])].replace("$", ""), locale='en_US')
                elif __format["format_type"].lower() == "number":
                    if __format["function"].lower() == "parse":
                        __fields[int(__format["column_index"])] = numbers.parse_number(__fields[int(__format["column_index"])].replace("$", ""), locale='en_US')
                    else:
                        __fields[int(__format["column_index"])] = numbers.format_number(__fields[int(__format["column_index"])].replace("$", ""), locale='en_US')
                elif __format["format_type"].lower() == "currency":
                    __fields[int(__format["column_index"])] = numbers.format_currency(__fields[int(__format["column_index"])].replace("$", ""), 'USD', locale='en_US')
                else:
                    __data_result = "None"
            else:
                __data_result = "None"

        return _delimiter.join(map(str, __fields))

    except IOError as err:
        print "Format Data Error: I/O error({0}): {1}".format(err.errno, err.strerror)
        return _delimiter.join(map(str, __fields))
    except ValueError as _value_err:
        print("Format Data A Value error occurred: {0}".format(_value_err))
        return _delimiter.join(map(str, __fields))
    except Exception as e:
        print("Format Data Unexpected error:", str(e))
        return _delimiter.join(map(str, __fields))
# ### End Method ###########################################################################


def __get_dataframe_field_type(_type):

    try:

        if _type == "integer":
            return pyspark.sql.types.IntegerType()
        elif _type == "string":
            return pyspark.sql.types.StringType()
        elif _type == "long":
            return pyspark.sql.types.LongType()
        elif _type== "timestamp":
            return pyspark.sql.types.TimestampType()
        elif _type == "decimal":
            return pyspark.sql.types.DecimalType()
        elif _type == "double":
            return pyspark.sql.types.DoubleType()
        elif _type == "float":
            return pyspark.sql.types.FloatType()
        elif _type == "date":
            return pyspark.sql.types.DateType()
        else:
            return pyspark.sql.types.StringType()

    except IOError as err:
        print "Get DataFrame Field Type: I/O error({0}): {1}".format(err.errno, err.strerror)
        sys.exit(1)
    except ValueError as _value_err:
        print("Get DataFrame Field Type: A value error occurred: {0}".format(_value_err))
        sys.exit(1)
    except Exception as e:
        print("Get DataFrame Field Type: Unexpected error:", str(e))
        sys.exit(1)
#### End Method ###########################################################################


def __get_dataframe_schema(sqlContext, filename):

    try:

        _query_json_df = sqlContext.read.json("s3://cmgt-integration-services/ncscirculation/config/ncscirculation_dataframe_schema.json")

        _query_json_df.registerTempTable("schema_json")

        _records_json = sqlContext.sql("select schema from schema_json where filename = '{0}'".format(filename))

        return _records_json.toJSON().first()

    except IOError as err:
        print "Get DataFrame Schema: I/O error({0}): {1}".format(err.errno, err.strerror)
        sys.exit(1)
    except ValueError as _value_err:
        print("Get DataFrame Schema: A value error occurred: {0}".format(_value_err))
        sys.exit(1)
    except Exception as e:
        print("Get DataFrame Schema: Unexpected error:", str(e))
        sys.exit(1)
#### End Method ###########################################################################


def __get_deltas_df(_first_df, _second_df, _bad_status):
    ###########################################################################################
    # This method is the script starting point
    # Last Nodified by Samuel Davison on 03/01/2017
    ###########################################################################################

    try:

        __deltasDF = _first_df.join(_second_df, _first_df.hash == _second_df.hash, "left_outer").where(_second_df.hash.isNull()).where(_first_df.hash.isNotNull()).where(_first_df.bad == _bad_status).select(_first_df.record, _first_df.hash, _first_df.bad)

        return __deltasDF

    except IOError as err:
        print "Get Deltas DataFrame Error: I/O error({0}): {1}".format(err.errno, err.strerror)
        sys.exit(1)
    except ValueError as _value_err:
        print("Get Deltas DataFrame A Value error occurred: {0}".format(_value_err))
        sys.exit(1)
    except Exception as e:
        print("Get Deltas DataFrame Unexpected error:", str(e))
        sys.exit(1)
#### End Method ###########################################################################


def __get_json_format(_sqlContext, _json_path, _file_lookup_name):
    ###########################################################################################
    # this method process gets the query for the table being processed
    # last nodified by samuel davison on 03/01/2017
    ###########################################################################################

    try:

        _query_json_df = _sqlContext.read.json(_json_path)

        _query_json_df.registerTempTable("format_json")

        _records_json = _sqlContext.sql(("select formats from format_json where filename = '{0}'").format(_file_lookup_name))

        return _records_json.toJSON().first()

    except IOError as err:
        print "Get Json Format Error: I/O error({0}): {1}".format(err.errno, err.strerror)
        sys.exit(1)
    except ValueError as _value_err:
        print("Get Json Format A Value error occurred: {0}".format(_value_err))
        sys.exit(1)
    except Exception as e:
        print("Get Json Format Unexpected error:", str(e))
        sys.exit(1)
#### End Method ###########################################################################


def __get_json_validation(_sqlContext, _json_path, _file_lookup_name):
    ###########################################################################################
    # this method process gets the query for the table being processed
    # last nodified by samuel davison on 03/01/2017
    ###########################################################################################

    try:

        _query_json_df = _sqlContext.read.json(_json_path)

        _query_json_df.registerTempTable("config_json")

        _records_json = _sqlContext.sql(("select validates from config_json where filename = '{0}'").format(_file_lookup_name))

        return _records_json.toJSON().first()

    except IOError as err:
        print "Get Json Config Error: I/O error({0}): {1}".format(err.errno, err.strerror)
        sys.exit(1)
    except ValueError as _value_err:
        print("Get Json Config A Value error occurred: {0}".format(_value_err))
        sys.exit(1)
    except Exception as e:
        print("Get Json Config Unexpected error:", str(e))
        sys.exit(1)
#### End Method ###########################################################################


def __hashdata(_data):
    ###########################################################################################
    # This method creates and return an MD5 Hash of the data specified
    # Last Nodified by Samuel Davison on 04/05/2017
    ###########################################################################################

    try:

        # -{Create MD5 Hash}
        __hash_object = hashlib.md5(_data.encode('utf-8'))

        # -{Return Hash}
        return __hash_object.hexdigest()

    except IOError as err:
        print "Create MD5 Hash Error: I/O error({0}): {1}".format(err.errno, err.strerror)
        return False
    except ValueError as _value_err:
        print("Create MD5 Hash Error A Value error occurred: {0}".format(_value_err))
        return False
    except Exception as e:
        print("Create MD5 Hash Unexpected error:", str(e))
        return False
#### End Method ###########################################################################


def __isDate(_data, _expression):
    ###########################################################################################
    # This method determine if specified data is a validate date
    # Last Nodified by Samuel Davison on 4/10/2017
    ###########################################################################################

    try:

        __data_result = datetime.datetime.strptime(_data, _expression)

        return True

    except IOError as err:
        print "__isDate Validate Date Error: I/O error({0}): {1}".format(err.errno, err.strerror)
        return False
    except ValueError as _value_err:
        print("__isDate Validate Date A Value error occurred: {0}".format(_value_err))
        return False
    except Exception as e:
        print("__isDate Validate Date Unexpected error:", str(e))
        return False
#### End Method ###########################################################################


def __isDecimal(self, _data):
    ###########################################################################################
    # This method determine if specified data is a validate integer
    # Last Nodified by Samuel Davison on 4/10/2017
    ###########################################################################################

    try:

        __data_result = float(_data)

        return True

    except IOError as err:
        print "__isDecimal Validate Integer Error: I/O error({0}): {1}".format(err.errno, err.strerror)
        return False
    except ValueError as _value_err:
        print("__isDecimal Validate Integer A Value error occurred: {0}".format(_value_err))
        return False
    except Exception as e:
        print("__isDecimal Validate Integer Unexpected error:", str(e))
        return False
#### End Method ###########################################################################


def __isInteger(_data):
    ###########################################################################################
    # This method determine if specified data is a validate integer
    # Last Nodified by Samuel Davison on 4/10/2017
    ###########################################################################################

    try:

        __data_result = int(_data)

        return True

    except IOError as err:
        print "__isInteger Validate Integer Error: I/O error({0}): {1}".format(err.errno, err.strerror)
        return False
    except ValueError as _value_err:
        print("__isInteger Validate Integer A Value error occurred: {0}".format(_value_err))
        return False
    except Exception as e:
        print("__isInteger Validate Integer Unexpected error:", str(e))
        return False
#### End Method ###########################################################################


def __regex_replace(_data, _replacement_value, _regex_expression):
    ###########################################################################################
    # This method loads the json validation file that specifies which fields will be validated
    # Last Nodified by Samuel Davison on 04/05/2017
    ###########################################################################################

    try:

        return re.sub(_regex_expression, _replacement_value, _data.rstrip())

    except IOError as err:
        print "Regex Replace Error: I/O error({0}): {1}".format(err.errno, err.strerror)
        return False
    except ValueError as _value_err:
        print("Regex Replace A Value error occurred: {0}".format(_value_err))
        return False
    except Exception as e:
        print("Regex Replace Unexpected error:", str(e))
        return False
#### End Method ###########################################################################


def __regex_extract(_data, _regex_expression):
    ###########################################################################################
    # This method loads the json validation file that specifies which fields will be validated
    # Last Nodified by Samuel Davison on 04/05/2017
    ###########################################################################################

    try:

        _found = ""

        for match in re.finditer(_regex_expression, _data):
            _found += match.group(0)

        return _found

    except IOError as err:
        print "Regex Extract Error: I/O error({0}): {1}".format(err.errno, err.strerror)
        return False
    except ValueError as _value_err:
        print("Regex Extract A Value error occurred: {0}".format(_value_err))
        return False
    except Exception as e:
        print("Regex Extract Unexpected error:", str(e))
        return False
#### End Method ###########################################################################


def __regex_match(_data, _regex_expression):
    ###########################################################################################
    # This method loads the json validation file that specifies which fields will be validated
    # Last Nodified by Samuel Davison on 04/05/2017
    ###########################################################################################

    try:

        if re.search(_regex_expression, _data):
            return True
        else:
            return False

    except IOError as err:
        print "Regex Match Error: I/O error({0}): {1}".format(err.errno, err.strerror)
        return False
    except ValueError as _value_err:
        print("Regex Match A Value error occurred: {0}".format(_value_err))
        return False
    except Exception as e:
        print("Regex Match Unexpected error:", str(e))
        return False
#### End Method ###########################################################################


def __replacing_data(_data, _flag_replace_data, _replacement_json, _flag_format_data, _source_delimiter, _formatting_json):
    ###########################################################################################
    # This method replaces values specified within the replacement data json
    # Last Nodified by Samuel Davison on 4/04/2017
    ###########################################################################################

    try:

        if _flag_replace_data:
            # --{Load the JSON string}
            print _replacement_json
            __replacements = json.loads(_replacement_json)
            print __replacements

            # --{Load through each JSON replacement list items}
            for __replacement in __replacements["replacements"]:
                _data = _data.encode('ascii', 'ignore').replace(__replacement["find"], __replacement["replace"])

        if _flag_format_data:
            # format_data.delimiter = self.source_delimiter
            _data = __format_data(_data.encode('ascii', 'ignore'), _formatting_json, _source_delimiter)

        # --{Return modified data}
        return _data

    except IOError as err:
        print "Replacing Data Error: I/O error({0}): {1}".format(err.errno, err.strerror)
        sys.exit(1)
    except ValueError as _value_err:
        print("Replacing Data A Value error occurred: {0}".format(_value_err))
        sys.exit(1)
    except Exception as e:
        print("Replacing Data Unexpected error:", str(e))
        sys.exit(1)
#### End Method ###########################################################################


def __s3_folder_delete(_target_bucket, _path_to_delete):
    ###########################################################################################
    # This method returns a character based on keyword
    # Returns '|' if no match
    # Last Nodified by serge declama on 11.08.2017
    ###########################################################################################

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(_target_bucket)
    bucket.objects.filter(Prefix=_path_to_delete).delete()
#### End Method ###########################################################################


def __validate_data(_data, _validationjson, _delimiter):
    ###########################################################################################
    # This method downloads the latest trackus changes from their RESTful API
    # Last Nodified by Samuel Davison on 4/02/2016
    ###########################################################################################

    __failedField = ""

    try:

        __errors = False


        __validate_fields = json.loads(_validationjson)
        # __validate_fields = json.dumps(_validationjson)

        # __fields = _data.split(self.delimiter)
        __fields = _data.split(__delimiter_get(_delimiter))

        for __validation in __validate_fields["validates"]:
            __failedField = ""
            if __validation["validation_type"].lower() == "record":
                if len(__fields) != int(__validation["column_count"]):
                    __errors = True
            else:
                if __fields[int(__validation["column_index"])] != "?":
                    if __validation["data_type"].lower() == "date":
                        __failedField = __validation["column_index"]
                        if __fields[int(__validation["column_index"])].strip() != "":
                            __data_result = datetime.datetime.strptime(__fields[int(__validation["column_index"])],
                                                                       __validation["expression"])
                        else:
                            __data_result = "None"
                    elif __validation["data_type"].lower() == "decimal":
                        __failedField = __validation["column_index"]
                        if __fields[int(__validation["column_index"])].strip() != "":
                            __data_result = float(__fields[int(__validation["column_index"])].replace(",", ""))
                        else:
                            __data_result = "None"
                    elif __validation["data_type"].lower() == "integer":
                        __failedField = __validation["column_index"]
                        if __fields[int(__validation["column_index"])].strip() != "":
                            __data_result = int(__fields[int(__validation["column_index"])].replace(",", ""))
                        else:
                            __data_result = "None"
                    elif __validation["data_type"].lower() == "string":
                        __failedField = __validation["column_index"]
                        if __fields[int(__validation["column_index"])].strip() != "":
                            if len(__fields[int(__validation["column_index"])]) > int(__validation["length"]):
                                __errors = True
                        else:
                            __data_result = "None"
                    elif __validation["data_type"].lower() == "regex":
                        __failedField = __validation["column_index"]
                        if __fields[int(__validation["column_index"])].strip() != "":
                            if re.match(__validation["expression"], __fields[int(__validation["column_index"])]):
                                __data_result = "Match"
                            else:
                                __errors = True
                        else:
                            __data_result = "None"
                    else:
                        __data_result = "None"

        return __errors

    except IOError as err:
        print "Validate Data Error: I/O error({0}): {1} {2}".format(err.errno, err.strerror, __failedField)
        return True
    except ValueError as _value_err:
        print("Validate Data A Value error occurred: {0} {1}".format(_value_err, __failedField))
        return True
    except Exception as e:
        print("Validate Data Unexpected error:", str(e) + __failedField)
        return True
#### End Method ###########################################################################


def __validating_data(_data, _flag_validate_records, _flag_regex_validation, _regex_validation, _validation_json, _source_delimiter):
    ###########################################################################################
    # This method replace source delimiter with new delimiter
    # Last Nodified by Samuel Davison on 03/01/2017
    ###########################################################################################

    try:

        if _flag_validate_records:
            if _flag_regex_validation:
                # data_format = formatting.Format()
                if __regex_match(_data, _regex_validation):
                    return "False"
                else:
                    return "True"
            else:
                return str(__validate_data(_data, _validation_json, _source_delimiter))
        else:
            return "False"

    except IOError as err:
        print "__validating_data Format Data Error: I/O error({0}): {1}".format(err.errno, err.strerror)
        sys.exit(1)
    except ValueError as _value_err:
        print("__validating_data Format Data A Value error occurred: {0}".format(_value_err))
        sys.exit(1)
    except Exception as e:
        print("__validating_data Format Data Unexpected error:", str(e))
        sys.exit(1)
#### End Method ###########################################################################



def main():
    ###########################################################################################
    # This method is the script starting point
    # Last Nodified by Samuel Davison on 03/01/2017
    ###########################################################################################

    try:

        # --{ Get commmand line parameters }
        print "{CHECK POINT 1} - set command line parameters for s3 paths"
        _spark_app_name = sys.argv[1] #"NCSCirculationGraypay_20180910"
        _additional_output = sys.argv[11] #"AJC|20180910" #sys.argv[8]
        _path_previous_raw_file = sys.argv[2] #       "s3://cmgt-integration-services/ncscirculation/raw/gracepay/2018/09/09/AJC/"
        _path_current_raw_file = sys.argv[3] #        "s3://cmgt-integration-services/ncscirculation/raw/gracepay/2018/09/10/AJC/"
        _path_insert_deltas_file = sys.argv[4] #      "s3://cmgt-integration-services/ncscirculation/processed/gracepay/2018/09/10/AJC/inserts/"
        _path_delete_deltas_file = sys.argv[5] #      "s3://cmgt-integration-services/ncscirculation/processed/gracepay/2018/09/10/AJC/deletes/"
        _path_current_bad_records_file = sys.argv[6] #"s3://cmgt-integration-services/ncscirculation/bad/gracepay/2018/09/10/AJC/"
        _path_audit_records_file = sys.argv[7] #      "s3://cmgt-integration-services/ncscirculation/audit/2018/09/10/AJC/gracepay/"
        _path_config_json_file = sys.argv[8] #        "s3://cmgt-integration-services/ncscirculation/config/"

        print "{CHECK POINT 2} - set command line parameters for delimiters"
        _delimiter_output = sys.argv[9] #"PIPE"
        _delimiter_source = sys.argv[10] #"PIPE"
        _lookup_table_name = sys.argv[12] #""

        print "{CHECK POINT 3} - set command line parameters flags"
        _flag_reseed = bool(int(sys.argv[13])) #bool(int("0"))
        _flag_format_data = bool(int(sys.argv[14])) #bool(int("0"))
        _flag_json_string_formatting = bool(int(sys.argv[15])) #bool(int("0"))
        _flag_validate_records = bool(int(sys.argv[16])) #bool(int("1"))
        _flag_regex_validation = bool(int(sys.argv[17])) #bool(int("1"))
        _flag_replace_data = bool(int(sys.argv[18])) #bool(int("0"))

        print "{CHECK POINT 4} - set command line parameters for config json"
        _regex_validation = sys.argv[19]  # "^-?(\d+)?\??\|-?(\d+)?\??\|-?(\d+)?\??\|(-?\d*\.?\d+)\|-?(\d+)?\??\|(-?\d*\.?\d+)\|(-?\d*\.?\d+)\|(-?\d*\.?\d+)\|(-?\d*\.?\d+)\|(-?\d*\.?\d+)\|(-?\d*\.?\d+)\|(-?\d*\.?\d+)$"
        _replacement_json = sys.argv[20] #"{"replacements":[{"find":"|","replace":";"}]}"
        _formatting_json = sys.argv[21] #""
        _validation_json = sys.argv[22] #""
        _sourceType = sys.argv[23] #""
        _csv_Has_Header = sys.argv[24] #"1"
        _partition_year = sys.argv[25] #yyyy
        _partition_month  = sys.argv[26] #mm
        _partition_day  = sys.argv[27] #dd
        _filename = sys.argv[28] #FDW_{0}-{1}-{2}
        _cmgtDSAWSBucketFolder = sys.argv[29] #s3://cmgt-dataservices-o2c/pio_fdw/{0}/{1}/{2}/
        _delete_before_write = sys.argv[30] #1
        _delete_delta_path = sys.argv[31] #'enterprisedata/pio_fdw/2018/06/02/'
        _delete_bad_path = sys.argv[32] #'pio_fdw/bad/pio_fdw/2018/06/02/'
        _output_bucket = sys.argv[33] #'cmgt-dataservices-o2c'
        _bad_bucket = sys.argv[34] #'cmgt-dataservices-o2c'

        #_filename = _filename.format(_partition_year, _partition_month, _partition_day)
        print _filename
        #_cmgtDSAWSBucketFolder = _cmgtDSAWSBucketFolder.format(_partition_year, _partition_month, _partition_day)
        print _cmgtDSAWSBucketFolder
        #_extension='.csv'
        #_staging_directory = './'

        if (_delete_before_write == '1'):
            print "{CHECK POINT 4.1} - S3 folders cleared before update"
            __s3_folder_delete(_output_bucket, _delete_delta_path)
            __s3_folder_delete(_bad_bucket, _delete_bad_path)

        # --{ Set Spark configuration }
        conf = SparkConf().setAppName("delta processing - " + _spark_app_name + ": " + strftime("%Y-%m-%d %H:%M:%S", gmtime()))
        sc = SparkContext(conf=conf)
        sqlContext = SQLContext(sc)
        sqlContext.setConf("spark.sql.shuffle.partitions", "10")
        # sqlContext.setConf("spark.shuffle.memoryFraction", "0")
        sqlContext.setConf("spark.yarn.executor.memoryOverhead", "1600")
        sqlContext.setConf("spark.network.timeout", "800")

        currentDateTimeStamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        __previous_records = 0
        __current_records = 0
        __raw_previous_records = 0
        __raw_current_records = 0
        __inserted_records = 0
        __deleted_records = 0

        AWS_BUCKET_NAME = 'placementsio-export'
        AWS_PREFIX = 'b447c655e981f5/Cox_production/'
        AWS_ACCESS_KEY_ID = 'AKIAJSOC7GIYDZY4XDUA'
        AWS_SECRET_ACCESS_KEY = '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'

        session = boto3.Session(
            aws_access_key_id='AKIAJSOC7GIYDZY4XDUA',
            aws_secret_access_key='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',
            region_name = 'us-east-1'
            )

        s3_src = boto3.resource(
            's3',
            aws_access_key_id='AKIAJSOC7GIYDZY4XDUA',
            aws_secret_access_key='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'
            )

        #src_bucket = s3_src.Bucket(Bucket=AWS_BUCKET_NAME, Prefix=AWS_PREFIX)
        src_bucket = s3_src.Bucket(AWS_BUCKET_NAME)
        objs = src_bucket.objects.filter(Prefix=AWS_PREFIX)
        print objs
        #for obj in objs.list(delimeter='/'):
        #for obj in objs:
        #    print(obj.name)
       
        #obj = s3_src.get_object(Bucket='placementsio-export', Key='/b447c655e981f5/Cox_production/FDW_2018-06-02.csv')

        print "{CHECK POINT 5} - get validation json from config json"
        if ((_flag_validate_records == True) and (_flag_regex_validation == False)):
            _validation_json = __get_json_validation(sqlContext, _path_config_json_file, _lookup_table_name)

        print "{CHECK POINT 6} - get formatting json from config json"
        if ((_flag_format_data == True) and (_flag_json_string_formatting == False)):
            _formatting_json = __get_json_format(sqlContext, _path_config_json_file, _lookup_table_name)

        if _sourceType == "CSV":
            print "{CHECK POINT 7} - load previous raw CSV file"
            if _flag_reseed == True:
                print "{CHECK POINT 8} - create empty rdd previous into RDD"
                __previousRawFileRDD = sc.parallelize([])
            else:
                if _csv_Has_Header == "1":
                    #__previousRawFileRDD = sqlContext.read.option("header","true").csv(_path_previous_raw_file, multiLine=True).rdd.map(lambda p: concatenate_csv_columns(list(p), _delimiter_output))
                    __previousRawFileRDD = sqlContext.read.option("header","true").option("charset", "utf-8").csv(_path_previous_raw_file, multiLine=True).rdd.map(lambda p: concatenate_csv_columns(list(p), _delimiter_output))
                else:
                    __previousRawFileRDD = sqlContext.read.csv(_path_previous_raw_file, multiLine=True).rdd.map(lambda p: concatenate_csv_columns(list(p), _delimiter_output))

            print "{CHECK POINT 9} - load current raw CSV file into RDD"
            if _csv_Has_Header == "1":
                #__currentRawFileRDD = sqlContext.read.option("header","true").csv(_path_current_raw_file, multiLine=True).rdd.map(lambda p: concatenate_csv_columns(list(p), _delimiter_output))
                __currentRawFileRDD = sqlContext.read.option("header","true").option("charset", "utf-8").csv(_path_current_raw_file, multiLine=True).rdd.map(lambda p: concatenate_csv_columns(list(p), _delimiter_output))
            else:
                __currentRawFileRDD = sqlContext.read.csv(_path_current_raw_file, multiLine=True).rdd.map(lambda p: concatenate_csv_columns(list(p), _delimiter_output))

            print __currentRawFileRDD.take(3)
        else:
            print "{CHECK POINT 7} - load previous raw file"
            if _flag_reseed == True:
                print "{CHECK POINT 8} - create empty rdd previous into RDD"
                __previousRawFileRDD = sc.parallelize([])
            else:
                __previousRawFileRDD = sc.textFile(_path_previous_raw_file)

            print "{CHECK POINT 9} - load current raw file into RDD"
            __currentRawFileRDD = sc.textFile(_path_current_raw_file)

        print "{CHECK POINT 10} - cache raw RDDs"
        __previousRawFileRDD.cache()
        __currentRawFileRDD.cache()

        print "{CHECK POINT 11} - count raw RDDs records"
        __previous_records = __previousRawFileRDD.count()
        print "{__previous_records} - " + str(__previous_records)
        __current_records = __currentRawFileRDD.count()
        print "{__current_records} - " + str(__current_records)

        print "{CHECK POINT 12} - hashing raw records"
        # --{ Hash records and tag records as bad status (True=bad, False=not bad)
        __previousHashRDD = __create_hash_rdd(__previousRawFileRDD, _flag_validate_records, _flag_regex_validation, _regex_validation, _validation_json, _delimiter_source)
        __currentHashRDD = __create_hash_rdd(__currentRawFileRDD, _flag_validate_records, _flag_regex_validation, _regex_validation, _validation_json, _delimiter_source)

        schema = StructType([StructField("bad", StringType(), True), StructField("hash", StringType(), True),
                             StructField("record", StringType(), True)])

        print "{CHECK POINT 13} - create raw records DataFrames"
        # --{ Convert RDDs into DataFrames}
        __previousHashDF = sqlContext.createDataFrame(__previousHashRDD, schema)
        __currentHashDF = sqlContext.createDataFrame(__currentHashRDD, schema)

        print "{CHECK POINT 14} - get distinct raw records from DataFrames"
        __previousDistinctHashDF = __previousHashDF.distinct()
        __currentDistinctHashDF = __currentHashDF.distinct()

        print "{CHECK POINT 15} - cache raw DataFrames"
        __previousDistinctHashDF.cache()
        __currentDistinctHashDF.cache()

        print "{CHECK POINT 16} - count raw DataFrames records"
        __raw_previous_records = __previousDistinctHashDF.count()
        __raw_current_records = __currentDistinctHashDF.count()

        print "{CHECK POINT 17} - getting DataFrames deltas"
        # --{ Query DF for deltas records }
        __insertDeltasDF = __get_deltas_df(__currentDistinctHashDF, __previousDistinctHashDF, "False")
        __deleteDeltasDF = __get_deltas_df(__previousDistinctHashDF, __currentDistinctHashDF, "False")

        print "{CHECK POINT 18} - cache delta DataFrames"
        __insertDeltasDF.cache()
        __deleteDeltasDF.cache()

        print "{CHECK POINT 19} - count delta DataFrame records"
        __inserted_records = __insertDeltasDF.count()
        __deleted_records = __deleteDeltasDF.count()

        print "{CHECK POINT 20} - convert insert deltas DF back to RDD"
        __insertDeltasRDDMapped = __insertDeltasDF.rdd.map(lambda p: __replacing_data(p.record.replace("\\", ""), _flag_replace_data, _replacement_json, _flag_format_data, _delimiter_source,  _formatting_json) + __delimiter_get(_delimiter_output) + p.hash + __delimiter_get(_delimiter_output) + p.bad + __delimiter_get(_delimiter_output) + "insert" + __delimiter_get(_delimiter_output) + currentDateTimeStamp + __delimiter_get(_delimiter_output) + _additional_output)
        __insertDeltasRDDFinal = __insertDeltasRDDMapped.coalesce(1, False)
        __insertDeltasRDDFinal.cache()

        print "{CHECK POINT 21} - convert delete deltas DF back to RDD"
        __deleteDeltasRDDMapped = __deleteDeltasDF.rdd.map(lambda p: __replacing_data(p.record.replace("\\", ""), _flag_replace_data, _replacement_json, _flag_format_data, _delimiter_source,  _formatting_json) + __delimiter_get(_delimiter_output) + p.hash + __delimiter_get(_delimiter_output) + p.bad + __delimiter_get(_delimiter_output) + "delete" + __delimiter_get(_delimiter_output) + currentDateTimeStamp + __delimiter_get(_delimiter_output) + _additional_output)
        __deleteDeltasRDDFinal = __deleteDeltasRDDMapped.coalesce(1, False)
        __deleteDeltasRDDFinal.cache()

        print "{CHECK POINT 22} - count insert delta and delete delta RDDs records"
        __final_inserts = __insertDeltasRDDFinal.count()
        __final_deletes = __deleteDeltasRDDFinal.count()

        # --{ Save the RDDs to HDFS}
        print "{CHECK POINT 23} - save delta RDDs to HDFS"
        __insertDeltasRDDFinal.saveAsTextFile(_path_insert_deltas_file)
        __deleteDeltasRDDFinal.saveAsTextFile(_path_delete_deltas_file)

        print "{CHECK POINT 24} - save bad records to HDFS"
        # --{ Save bad records to HDFS }
        __bad_records = "0"
        if __currentDistinctHashDF.where(__currentDistinctHashDF.bad == "True").count() != 0:
            __currentBadRecordsRDD = __currentDistinctHashDF.where(__currentDistinctHashDF.bad == "True").rdd.repartition(1).map(lambda p: p.record + __delimiter_get(_delimiter_output) + p.hash + __delimiter_get(_delimiter_output) + p.bad)
            __currentBadRecordsRDD.saveAsTextFile(_path_current_bad_records_file)
            __bad_records = str(__currentBadRecordsRDD.count())

        # print "save audit record to HDFS"
        # # --{ Save Audit Information}
        # __deltaAuditRDD = sc.parallelize([str(deltaData.validate_records) + "|" + str(__raw_current_records) + "|" + str(__inserted_records) + "|" + str(__deleted_records) + "|" + __bad_records + "|" + str(__inserted_records + __deleted_records) + "|" + __currentRawFilePath + "|" + __insertDeltasFilePath + "|" + __deleteDeltasFilePath + "|" + __currentBadRecordsFilePath + "|" + currentDateTimeStamp])
        # __deltaAuditRDD = __deltaAuditRDD.repartition(1)
        # __deltaAuditRDD.saveAsTextFile(__sparkAuditRecordsFilePath)


        #sc.stop()

    except Py4JJavaError as e:
        print "Main Error: I/O error({0}): {1}".format(e.java_exception, e.errmsg)
        sc.stop()
        sys.exit(1)
    except IOError as e:
        print "Main Error: I/O error({0}): {1}".format(e.errno, e.strerror)
        sc.stop()
        sys.exit(1)
    except:
        print "Main Error: Unexpected error:", sys.exc_info()[0]
        sc.stop()
        sys.exit(1)

    finally:
        sc.stop()

if __name__ == '__main__':
    main()
