"""
code to execute request on postgresql database

this code is using pginterface.py to connect to the database
and execute the requests from a json file

in order to be dockerized, 
it is using either or env var or the following command line.

From command line:
  python pgexecutor.py -d postgresql://user:password@localhost:5432/postgres -c /code/atlog/requests/req1.json
or
  python /home/renaud/code/AtlanticLog/python/pgexecutor.py -c /home/renaud/code/AtlanticLog/python/select_version.yml

env var are:
PG_URI:         string, uri for database, ie postgresql://postgres:postgres@localhost:5432/postgres 
PG_CONFIG:      string, path for config file, ie /home/renaud/code/atlog/python/requests/req1.yaml

yaml file like:
    process: 100
    subject: Update all FOREIGN DATA WRAPPERS
    deadline: 1 day
    listRequests:
    - select version()
    - select now()
    documentation: "https://www.postgresql.org/docs/10/static/sql-alterforeigndatawrapper.html"


in order to write traces in the database, we need this table:
CREATE TABLE cron_process (
  id SERIAL PRIMARY KEY,
  i_process int,
  s_platform VARCHAR(100),
  d_start TIMESTAMP,
  d_end TIMESTAMP,
  s_subject VARCHAR(100),
  s_message VARCHAR(500),
  s_deadline VARCHAR(10),
  s_doc VARCHAR(200)
);

    
"""
import argparse
import platform
import json
import yaml
from pginterface import PG
import os
from sys import exit
from datetime import datetime

class PGExecutor:
    """
    This class to manage:
        a list of requests
        a list of servers from foreign data wrapper
        and execute the requests on the servers

    v0.2.0: 2023-19-05 write on cron_table
    v0.1.0: 2023-18-05 1st version
    - read the yaml file and execute the commands accross postgresql, yes!

    
    """
    def __init__(self, databaseUri: str, configFile: str) -> None:
        """
        firing the conn
        using databse_uri

        params:
            databaseUri:    string, uri for database, ie postgresql://postgres:postgres@localhost:5432/postgres
            configFile:     string, path for config file, ie /home/renaud/code/atlog/python/requests/req1.json

        """
        self.__version__ = '0.2.0'
        self.myPG = None    # My Postgres class
        self.error = False
        self.configFile = configFile

        print("PG Executor version " + self.__version__ + " is starting ", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        try:
            self.myPG = PG(databaseUri)
        except Exception as e:
            print(f'Error: databse connexion has failed: {e}')
            self.error = True
        if self.error is not True:    
            print("PG Executor version " + self.__version__ + " is ready")
        
        self.process = 0                        # job process number, 1 to 99 ar for test, 100 to 999 are for production
        self.listRequests = []                  # list of requests to execute
        self.documentation = "doc not found"    # documentation for the job, should be the grafana panel link
        self.deadline = 0                       # deadline in days
        self.subject = "subject not found"      # subject of the job, ie "Update all FOREIGN DATA WRAPPERS"
        self.id = 0                             # id sequence of the process in the cron_process table

        self.message = None                     # message to write in the cron_process table
    
    def clsExecute(self) -> None:
        """
        execute a request from a json file
        """
        print("go to executeRequest", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        # read the yaml file
        self._readYaml(self.configFile)

        self._pgWriteCronProcessTable() # update the cron_process table
        self._executeRequests()         # execute the requests 
        self._pgWriteCronProcessTable(self.message[:499])   # update the cron_process table
        return None
    
    def _executeRequests(self) -> None:
        """
        executamos mucha requestas got from the json file
        olééééé
        """
        print("executeRequest...")
        self.message = "Results" 
        self.reqCt = 0

        # execute the listExecute
        for request in self.listRequests:
            if "{schema}" in request:
                self.__clsSchematize(request)
            else:
                self.reqCt += 1
                print("------------- request: " + str(self.reqCt) + "/" + str(len(self.listRequests)), datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                print(request)
                # print(self.myPG.clsExecute(request))
                myResponse = self.myPG.clsSelect(request)
                self.__addMessage(myResponse[0][0])
                print("response: ", self.message )
                print("done at: ", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        print('-----------------')
        return None
    
    def __clsSchematize(self, request) -> None:
        """
        execute a request with schema name, special ROMAIN request
        """
        # get the list of FDW schemas...
        myResponse = self.myPG.clsSelect("SELECT srv_name FROM fdw_get_pg_foreign_servers_info()")
        fdw_servers = [myResponse[0] for myResponse in myResponse]
        reqCt = 0
        for server in fdw_servers:
            reqCt += 1
            print("------------- request schema: " + str(reqCt) + "/" + str(len(fdw_servers)), datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            schemaRequest = request.replace("{schema}", server)
            print(schemaRequest)
            # print(self.myPG.clsExecute(request))
            myResponse = self.myPG.clsSelect(schemaRequest)
            if len (myResponse) > 0:
                self.__addMessage(myResponse[0][0])
                print("response: ", myResponse[0][0])
            else:
                self.__addMessage("no result")
                print("response: no result")
            print("done at: ", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        return None

    def __addMessage(self, message) -> None:
        """
        add a message to self.message
        """
        if self.message is None:
            self.message = message
        else:
            if type(message) is int or type(message) is datetime:
                self.message += ' / ' + str(message)
            else:
                self.message += ' / ' + message
            
        return None

    def _pgWriteCronProcessTable(self, message:str = None) -> None:
        """
        write in the cron_process table
        
        """
        myReq = "select count(1) from cron_process where id = " + str(self.id)
        myResponse = self.myPG.clsSelect(myReq)
        if myResponse[0][0] == 0:
            # this is an insert
            if message is None:
                message = "process " + str(self.process) + " is starting"
            myReq = "insert into cron_process (i_process, s_platform, d_start, s_subject, s_message, s_deadline, s_doc) values "
            myReq += "(" + str(self.process) + ", '" + str(platform.uname()[1])  +"', "
            myReq += " now(), '" + self.subject + "', '" + message + "', '" + self.deadline + "', '" + self.documentation + "')"
            self.myPG.clsExecute(myReq)
            # Grab the id of the process
            myReq = "select max(id) from cron_process where i_process = " 
            myReq += str(self.process) + " and s_platform = '" + str(platform.uname()[1]) + "'"
            myResponse = self.myPG.clsSelect(myReq)
            self.id = myResponse[0][0]
        else:
            # update
            if message is None:
                message = "process " + str(self.process) + " is ending"
            myReq = "update cron_process set d_end = now(), s_message = '" + message + "' where id = " + str(self.id)
            self.myPG.clsExecute(myReq)
        return None
    
    def _readYaml(self, filename: str) -> None:
        """
        read a Yaml file
        
        params:
            filename: string, ie /code/atlog/requests/req1.yaml
        return None
        """
        print("trying to read yaml file " + filename)
        try:
            with open(filename, 'r') as f:
                data = yaml.load(f)
                print("found process: " + str(data['process']))
                print("found subject: " + data['subject'])
                self.process = data["process"]
                self.listRequests = data["listRequests"]
                self.documentation = data["documentation"]
                self.deadline = data["deadline"]
                self.subject = data["subject"]


        except Exception as e:
            self.error = True
            print("error while reading yaml file: " + str(e))
            raise e

        print("read yaml done")
        return None


    def _readJson(self, filename: str) -> None:
        """
        read a json file
        deprecatd, use yaml instead
        params:
            filename: string, ie /code/atlog/requests/req1.json
        return None
        """
        print("trying to read readJson " + filename)
        try:
            with open(filename, 'r') as f:
                jsonData = json.load(f)
                print("found process: " + str(jsonData['process']))
                print("found subject: " + jsonData['subject'])
                self.process = jsonData["process"]
                self.listRequests = jsonData["listRequests"]
                self.listExecuteBefore = jsonData["listExecuteBefore"]
                self.listExecuteAfter = jsonData["listExecuteAfter"]

        except Exception as e:
            self.error = True
            print("error while reading json file: " + str(e))
            raise e

        print("readJson done")
        return None


if __name__ == "__main__":
    print("╔═╗╔═╗    ╔═╗═╗ ╦╔═╗╔═╗╦ ╦╔╦╗╔═╗╦═╗")
    print("╠═╝║ ╦    ║╣ ╔╩╦╝║╣ ║  ║ ║ ║ ║ ║╠╦╝")
    print("╩  ╚═╝────╚═╝╩ ╚═╚═╝╚═╝╚═╝ ╩ ╚═╝╩╚═")

    description = "Sql automation tool"

    # select either or var env or command line
    if os.getenv("PG_URI") is not None:
        defaultURI = os.getenv("PG_URI")  
    if os.getenv("PG_CONFIG") is not None:
        defaultConfigFile = os.getenv("PG_CONFIG")
    if os.getenv("PG_URI") is None and os.getenv("PG_CONFIG") is None: 
        parser = argparse.ArgumentParser(description=description)
        defaultConfigFile = "/home/renaud/code/AtlanticLog/python/select_version.yml"
        defaultURI = "postgresql://postgres:postgres@localhost:5432/postgres"
        parser.add_argument('-d','--databaseUri',type=str, help='database uri', default=defaultURI)
        parser.add_argument('-c', "--configFile", type=str, help='file configuration', default=defaultConfigFile)
        args = parser.parse_args()
        defaultConfigFile = args.configFile
        defaultURI = args.databaseUri

    myExecutor = PGExecutor(args.databaseUri, args.configFile)
    if myExecutor.error:
        print("error while creating PGExecutor. Exiting")
        exit(1)
    myExecutor.clsExecute()
