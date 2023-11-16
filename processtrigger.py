from imap_tools import MailBox, AND
import requests
import json
import pandas as pd
import cx_Oracle
import keyring
from exchangelib import DELEGATE, Account, Identity, Credentials, Version, Configuration, OAuth2Credentials, OAUTH2, \
    HTMLBody, Message
from exchangelib.autodiscover import clear_cache
from datetime import datetime
import os

clear_cache()

now = datetime.now()
log_date = now.strftime("%Y-%m-%d")
# The getAuthentication function will derive the orchestrator api token
def getAuthentication(granttype, clientid, clientsecret, clientscope, OrchestratorURL):
    url = OrchestratorURL
    data = "grant_type=client_credentials&client_id=" + clientid + "&client_secret=" + clientsecret + "&scope=" + clientscope
    header = {"content-type": "application/x-www-form-urlencoded"}

    response = requests.post(url, data=data, headers=header, verify=False)
    r_json = response.json()

    key = r_json["access_token"]
    return key


# The getFolderID function will derive the orchestrator folder id
def getFolderID(key, foldername, OrchestratorUrl):
    global releaseKey
    url = "" + OrchestratorUrl + "/Folders"
    header = {"content-type": "application/json",
              "Authorization": "Bearer " + str(key)}
    response = requests.get(url, headers=header, verify=False)
    r_json = response.json()
    val = json.dumps(r_json['value'])
    resp_dict = json.loads(val)
    for i in resp_dict:
        if i['DisplayName'] == foldername:
            folderId = str(i['Id'])
    return folderId


# The getReleaseKey function will derive the release key
def getReleaseKey(key, inputProcessKey, folderid, OrchestratorUrl):
    global releaseKey
    url = "" + OrchestratorUrl + "/Releases"
    header = {"content-type": "application/json",
              "Authorization": "Bearer " + str(key),
              "X-UIPATH-OrganizationUnitId": "" + folderid + ""}
    response = requests.get(url, headers=header, verify=False)
    r_json = response.json()
    val = json.dumps(r_json['value'])
    resp_dict = json.loads(val)
    for i in resp_dict:
        if i['ProcessKey'] == inputProcessKey:
            releaseKey = str(i['Key'])
            processKey = str(i['ProcessKey'])
    return releaseKey


# The startJob function will trigger the robot
def startJob(key, releaseKey, OrchestratorUrl, folderid, Trigger_Point, Process_Name, Instance_Name, mail_id):
    url = "" + OrchestratorUrl + "/Jobs/UiPath.Server.Configuration.OData.StartJobs"
    data = {
        "startInfo": {
            "ReleaseKey": str(releaseKey),
            "JobsCount": 1,
            "Strategy": "ModernJobsCount",
            "InputArguments": "{\"Trigger_Point\":\"" + Trigger_Point + "\",\"Process_Name\":\"" + Process_Name + "\",\"Application_Name\":\"" + Instance_Name + "\",\"mail_id\":\"" + mail_id + "\"}"
        }
    }
    header = {"content-type": "application/json",
              "Authorization": "Bearer " + str(key),
              "X-UIPATH-OrganizationUnitId": "" + folderid + ""}
    response = requests.post(url, data=json.dumps(data), headers=header, verify=False)
    r_json = response.json()
    jobId = r_json['value'][0]['Id']


# The PWDKEYRING function will retrieve the password from windows credentials
def PWDKEYRING(NetworkAddress, user_name):
    return keyring.get_password(NetworkAddress, user_name)


def GetSharePointToken(sharepointtenantname, sharepointusername, sharepointpassword, sharepointclientid,
                       sharepointclientsecret, sharepointscope):
    url_token = "https://login.microsoftonline.com/" + str(sharepointtenantname) + "/oauth2/v2.0/token"
    data_token = "grant_type=client_credentials&username=" + str(sharepointusername) + "&password=" + str(
        sharepointpassword) + "&client_id=" + str(sharepointclientid) + "&client_secret=" + str(
        sharepointclientsecret) + "&scope=" + str(sharepointscope)
    header_token = {"content-type": "application/x-www-form-urlencoded", "SdkVersion": "postman-graph/v1.0"}
    response_token = requests.post(url_token, data=data_token, headers=header_token, verify=False)
    AccessToken_json = response_token.json()
    Sharepoint_AccessToken = AccessToken_json["access_token"]
    return Sharepoint_AccessToken


def GetSiteID(sharepointtenant, sharepointsitename, SharePointToken):
    url_site = "https://graph.microsoft.com/v1.0/sites/" + sharepointtenant + ":/sites/" + site_name
    header_site = {"Authorization": "Bearer " + str(Sharepoint_AccessToken)}
    response_site = requests.get(url_site, headers=header_site, verify=False)
    s_json = response_site.json()
    site_id = s_json['id']
    return site_id


def GetDriveID(site_id, sharepointrootfolder, Sharepoint_AccessToken):
    url_drive_id = "https://graph.microsoft.com/v1.0/sites/" + site_id + "/drive?filter = name eq '" + sharepointrootfolder + "'"
    header_drive_id = {"Authorization": "Bearer " + str(Sharepoint_AccessToken)}
    response_drive_id = requests.get(url_drive_id, headers=header_drive_id,
                                     verify=False)
    d_json = response_drive_id.json()
    drive_id = d_json['id']


def GetParentFolderID(folder_value, Sharepoint_AccessToken, drive_id):
    folder_value_arr = folder_value.split("/")
    count = 0
    parent_folder_id = ""
    for folder_name in folder_value_arr:
        if count == 0:
            url = "https://graph.microsoft.com/v1.0/drives/" + drive_id + "/root:/" + folder_name
            header = {"Authorization": "Bearer " + str(Sharepoint_AccessToken)}
            response = requests.get(url, headers=header, verify=False)
            p_json = response.json()
            parent_folder_id = p_json['id']
        else:
            url = "https://graph.microsoft.com/v1.0/drives/" + drive_id + "/items/" + parent_folder_id + "/children?filter=name eq '" + folder_name + "'"
            header = {"Authorization": "Bearer " + str(Sharepoint_AccessToken)}
            response = requests.get(url, headers=header, verify=False)
            p_json = response.json()
            val = json.dumps(p_json['value'])
            resp_dict_p = json.loads(val)
            parent_folder_id = resp_dict_p[0]['id']

        count = count + 1
    return parent_folder_id


def GetFilesCount(Sharepoint_AccessToken, drive_id, parent_folder_id):
    headers = {'Authorization': f'Bearer {Sharepoint_AccessToken}'}
    api_url = "https://graph.microsoft.com/v1.0/drives/" + drive_id + "/items/" + parent_folder_id + "/children"
    response = requests.get(api_url, headers=headers)
    data = response.json()
    folders_with_child_count_1 = [item["name"] for item in data.get("value", []) if
                                  item.get("folder", {}).get("childCount", 0) == 1]
    return folders_with_child_count_1


def SendMail(servicemailclientid, servicemailclientsecret, servicetenantid, serviceemailaddress, server, log_message,
             recipient_email):
    credentials = OAuth2Credentials(client_id=servicemailclientid, client_secret=servicemailclientsecret,
                                    tenant_id=servicetenantid, identity=Identity(smtp_address=serviceemailaddress))
    config = Configuration(server=server, credentials=credentials, auth_type=OAUTH2)
    account = Account(serviceemailaddress, access_type=DELEGATE, config=config)
    email = Message(account=account, subject='Process Trigger Exception', body=HTMLBody(f'<p>{log_message}</p>'),
                    to_recipients=[recipient_email])
    email.send()


def UpdateLogstoDB(log_message, logstablename, max_log_id, c, conn):
    log_message_db = log_message.strip(';')
    log_message_db_arr = log_message_db.split(';')
    now = datetime.now()
    current_time = now.strftime("%Y-%m-%d")
    line_id = 1
    for log in log_message_db_arr:
        log_arr = log.split("/")
        log_message_val = log_arr[0]
        log_date_val = log_arr[1]
        log_time_val = log_arr[2]
        print("log msg val ", log_message_val)
        sql_query = "insert into " + logstablename + " (log_id,log_line_id) values (:max_log_id,:line_id)"
        c.execute(sql_query, max_log_id=max_log_id, line_id=line_id)
        conn.commit()
        sql_query = "update " + logstablename + " set log_message = :log_message,log_date = TO_DATE(:log_date_val,'yyyy-MM-dd') ,log_time =:log_time_val ,process_name = 'Process_Trigger' where log_line_id = " + str(
            line_id) + " and log_id =" + str(max_log_id) + ""
        c.execute(sql_query, log_message=log_message_val, log_date_val=log_date_val, log_time_val=log_time_val)
        conn.commit()
        line_id = line_id + 1
    sql_query = "DELETE FROM " + logstablename + " WHERE log_line_id IS NULL AND log_id =" + str(max_log_id)
    c.execute(sql_query)
    conn.commit()


def ValuesCheck(configurations,headerlinedata,exception_message):
    values_missing_list = []
    config_names_list = []
    values_dict = {}
    for config_name, var_name in configurations.items():
        try:
            value = headerlinedata['CONFIGURATION_VALUE'][headerlinedata['CONFIGURATION_NAME'] == config_name].values[0]
            try:
                values_dict[var_name] = value[0]
            except:
                values_missing_list.append(config_name)
        except:
            config_names_list.append(config_name)
    values_missing = ','.join(values_missing_list)
    config_names = ','.join(config_names_list)
    if len(values_missing_list) == 0 and len(config_names_list) > 0:
        log_time = now.strftime("%I:%M %p")
        raise ("configuration names wrong for ",config_names,"while ",exception_message ,"/" + log_date , "/" + log_time)
    elif len(values_missing_list) > 0 and len(config_names_list) == 0:
        log_time = now.strftime("%I:%M %p")
        raise ("Input Missing for ", values_missing,values_missing,"while ",exception_message, "/" + log_date , "/" + log_time)
    elif len(values_missing_list) > 0 and len(config_names_list) > 0:
        log_time = now.strftime("%I:%M %p")
        raise ("configuration names wrong for ",config_names , " and "," Input Missing for ", values_missing,"while ",exception_message, "/" + log_date , "/" + log_time)
    else:
        return values_dict




def MailCheck():
    log_message = clientId = clientSecret = clientScope = folderName = orchestratorURL = orchestratorTokenURL = value =""
    fullPath = os.path.abspath(os.path.join(os.path.dirname(__file__), "ProcessTrigger\\Config.txt"))
    with open(fullPath) as config:
        jsonData = json.load(config)
        databasePort = jsonData["Port"]
        databaseHostName = jsonData["HostName"]
        databaseServiceName = jsonData["ServiceName"]
        databaseUsername = jsonData["User"]
        dbNetworkAddress = jsonData["DBNetworkAddress"]
        databasePassword = PWDKEYRING(dbNetworkAddress, databaseUsername)
        serviceMailClientId = jsonData["ClientID"]
        serviceMailClientSecret = jsonData["ClientSecret"]
        serviceTenantID = jsonData["TenantID"]
        serviceEmailAddress = jsonData["EmailAddress"]
        serviceMailFolder = jsonData["MailFolder"]
        server = jsonData["Server"]
        recipientEmail = jsonData["RecipientEmail"]
        logsTableName = jsonData["LogsTablename"]
    try:
        try:
            dsn = cx_Oracle.makedsn(databaseHostName, databasePort, service_name=databaseServiceName)
            conn = cx_Oracle.connect(user=databaseUsername, password=databasePassword, dsn=dsn)
            c = conn.cursor()
        except Exception as e:
            log_time = now.strftime("%I:%M %p")
            raise Exception("Oracle Connection Failed " + str(e) + "/" + log_date + "/" + log_time)

        sqlQuery = "insert into sales_order_creation_logs (log_id) values(sales_order_creation_logs_seq.nextval)"
        c.execute(sqlQuery)
        conn.commit()
        sqlQuery = "select max(log_id) from sales_order_creation_logs"
        c.execute(sqlQuery)
        max_log_id = c.fetchone()[0]
        c.execute(sqlQuery)
        sqlQuery = 'SELECT * FROM WB_CONFIG_GROUP'
        c.execute(sqlQuery)
        headercolumns = [x[0] for x in c.description]
        headerrows = c.fetchall()
        if not headerrows:
            log_time = now.strftime("%I:%M %p")
            log_date_time = log_date + "/" + log_time
            raise Exception("no rows are fetched from wb_config_group table" + "/" + log_date + "/" + log_time)
        else:
            headerdata = pd.DataFrame(headerrows, columns=headercolumns)

            values_missing_a = []
            for header_row in headerdata.itertuples():
                if header_row.CONFIG_GROUP_NAME == 'UiPath Orchestrator':
                    sqlQuery = 'SELECT * FROM WB_CONFIG_LINES where config_group_id=' + str(
                        header_row.CONFIG_GROUP_ID)
                    c.execute(sqlQuery)
                    headercolumnslines = [x[0] for x in c.description]
                    headerlinesrows = c.fetchall()
                    headerlinedata = pd.DataFrame(headerlinesrows, columns=headercolumnslines)
                    granttype = ''
                    configurations = {'Orchestrator Client Details': 'clientid','Orchestrator Client Details': 'clientsecret','Orchestrator Scope': 'clientscope','Orchestrator Token URL': 'OrchestratorTokenURL','Orchestrator URL': 'OrchestratorURL','Orchestrator Folder Name': 'foldername','Orchestrator Folder Id': 'folderid'}
                    exception_message = "fetching orchestrator api detais"
                    values_dict = ValuesCheck(configurations,headerlinedata,exception_message)
                    clientid = values_dict.get('clientid')
                    clientsecret = values_dict.get('clientsecret')
                    clientscope = values_dict.get('clientscope')
                    OrchestratorTokenURL = values_dict.get('OrchestratorTokenURL')
                    OrchestratorURL = values_dict.get('OrchestratorURL')
                    foldername = values_dict.get('foldername')
                    folderid = values_dict.get('folderid')

            log_time = now.strftime("%I:%M %p")
            log_message = log_message + ";" + "Established database connection, obtained Orchestrator and SharePoint credentials." + "/" + log_date + "/" + log_time
            token = getAuthentication(granttype, clientid, clientsecret, clientscope, OrchestratorTokenURL)
            # The code (167-602)  database connection, obtained Orchestrator and SharePoint credentials to run the robot
            value = False
            sqlQuery = 'SELECT * FROM PROCESS_ADMINISTRATION where enable = \'Yes\''
            c.execute(sqlQuery)
            linecolumns = [x[0] for x in c.description]
            linerows = c.fetchall()
            if not linerows:
                log_time = now.strftime("%I:%M %p")
                raise Exception(
                    "no rows are fetched from PROCESS_ADMINISTRATION table" + "/" + log_date + "/" + log_time)
            else:
                linedata = pd.DataFrame(linerows, columns=linecolumns)
            # The Code (258-315) will trigger the robot either the file is present in sharepoint or mail or in both.
            for i in linedata.itertuples():
                trigger_source_value = i.TRG_SOURCE.lower()
                sourceval = i.TRG_SOURCE
                processnameval = i.WB_PROCESS_NAME
                targetapplication = i.TARGET_APPLICATION
                folder_value = i.DOC_REP_INPUT_FOLDER
                config_group_name = i.config_group_name.lower()
                config_group_name_arr = config_group_name.split(";")
                if trigger_source_value == 'mail':
                    for header_row in headerdata.itertuples():
                        if header_row.CONFIG_GROUP_NAME.lower() in config_group_name_arr:
                            sql_query = 'SELECT * FROM SO_CONFIG_LINES_EBS where config_group_id=' + str(
                                header_row.CONFIG_GROUP_ID)
                            c.execute(sql_query)
                            headercolumnslines = [x[0] for x in c.description]
                            headerlinesrows = c.fetchall()
                            granttype = 'client_credentials'
                            headerlinedata = pd.DataFrame(headerlinesrows, columns=headercolumnslines)
                            configurations = {'Mail Client Details': 'mailclientsecret_1','Mail Client Details': 'mailclientid_1','Mail Login Credentials': 'mailusername_1','Mail Tenant': 'mailtenant_1','Mail Server': 'mailServer_1','Mail Folder': 'mailfolder1'}
                            ValuesCheck(configurations, headerlinedata)
                            try:
                                exception_message = "fetching mail api detais"
                                values_dict = ValuesCheck(configurations, headerlinedata)
                            except Exception as e:
                                log_message = log_message + "," + str(e)
                            mailclientid_1 = values_dict.get('mailclientid_1')
                            mailclientsecret_1 = values_dict.get('mailclientsecret_1')
                            mailtenant_1 = values_dict.get('mailtenant_1')
                            mailusername_1 = values_dict.get('mailusername_1')
                            mailServer_1 = values_dict.get('mailServer_1')
                            mailfolder1 = values_dict.get('mailfolder1')
                            try:
                                credentials = OAuth2Credentials(client_id=mailclientid_1,
                                                                client_secret=mailclientsecret_1,
                                                                tenant_id=mailtenant_1,
                                                                identity=Identity(smtp_address=mailusername_1))
                                config = Configuration(server=mailServer_1, credentials=credentials, auth_type=OAUTH2)
                                account = Account(mailusername_1, access_type=DELEGATE, config=config)
                                unread_messages = account.root / 'Top of Information Store' / mailfolder1
                                unread_mail_list = unread_messages.filter(is_read=False)
                                breakFor = False
                                mailExists = False
                                mail_count = 0
                                count_attachments = 0
                                mail_address = mailusername_1
                                for msg in unread_mail_list:
                                    mailExists = True
                                    for attachment in msg.attachments:
                                        count_attachments = count_attachments + 1
                                if mailExists == True and count_attachments > 0:
                                    value = True
                                    startJob(token, releaseKey, OrchestratorURL, folderid, sourceval,
                                             processnameval, targetapplication, mail_address)
                                    log_time = now.strftime("%I:%M %p")
                                    log_message = log_message + ";" + "Triggered robot to process po's in " + mail_id + "mail for " + processnameval + "/" + log_date + "/" + log_time
                            except Exception as e:
                                print(
                                    "Mail check for " + mailusername_1 + " to trigger robot for " + processnameval + "is completed with error " + str(
                                        e))
                    log_time = now.strftime("%I:%M %p")
                    log_message = log_message + ";" + "Mail check to trigger robot for " + processnameval + " is completed" + "/" + log_date + "/" + log_time

                elif trigger_source_value == 'sharepoint':
                    for header_row in headerdata.itertuples():
                        if header_row.CONFIG_GROUP_NAME.lower() in config_group_name_arr:
                            sql_query = 'SELECT * FROM WB_CONFIG_LINES where config_group_id=' + str(header_row.CONFIG_GROUP_ID)
                            c.execute(sql_query)
                            headercolumnslines = [x[0] for x in c.description]
                            headerlinesrows = c.fetchall()
                            granttype = 'client_credentials'
                            headerlinedata = pd.DataFrame(headerlinesrows, columns=headercolumnslines)
                            configurations = {'Mail Client Details': 'mailclientsecret_1',
                                              'Mail Client Details': 'mailclientid_1',
                                              'Mail Login Credentials': 'mailusername_1', 'Mail Tenant': 'mailtenant_1',
                                              'Mail Server': 'mailServer_1', 'Mail Folder': 'mailfolder1'}
                            try:
                                exception_message = "fetching sharepoint api detais"
                                values_dict = ValuesCheck(configurations, headerlinedata,exception_message)
                            except Exception as e:
                                log_message = log_message + "," + str(e)
                            sharepointclientsecret = values_dict.get('Sharepoint Client Details')
                            sharepointclientid = values_dict.get('Sharepoint Client Details')
                            sharepointusername = values_dict.get('harepoint Login Credentials')
                            sharepointpassword = values_dict.get('harepoint Login Credentials')
                            sharepointscope = values_dict.get('Sharepoint Scope')
                            sharepointtenantname = values_dict.get('Sharepoint Tenant Name')
                            sharepointtenant = values_dict.get('Sharepoint Tenant')
                            sharepointrootfolder = values_dict.get('Sharepoint Root Folder')
                            sharepointsitename = values_dict.get('Sharepoint Site Name')

                            try:
                                mail_id = ""
                                Sharepoint_AccessToken = GetSharePointToken(sharepointtenantname, sharepointusername,
                                                                            sharepointpassword, sharepointclientid,
                                                                            sharepointclientsecret,
                                                                            sharepointclientsecret)
                                site_id = GetSiteID(sharepointtenant, sharepointsitename, Sharepoint_AccessToken)
                                drive_id = GetDriveID(site_id, sharepointrootfolder, Sharepoint_AccessToken)
                                parent_folder_id = GetParentFolderID(folder_value, Sharepoint_AccessToken, drive_id)
                                count_files_present = GetFilesCount(Sharepoint_AccessToken, drive_id, parent_folder_id)
                                if int(count_files_present) > 0:
                                    value = True
                                    token = getAuthentication(granttype, clientid, clientsecret, clientscope,
                                                              OrchestratorTokenURL)
                                    startJob(token, releaseKey, OrchestratorURL, folderid, sourceval, processnameval,
                                             targetapplication, mail_id)
                                    log_time = now.strftime("%I:%M %p")
                                    log_message = log_message + ";" + "Triggered robot to process po's in Sharepoint" + "/" + log_date + "/" + log_time

                            except:
                                print(
                                    "Sharepoint check for " + sharepointsitename + " to trigger robot for " + processnameval + "is completed with error " + str(
                                        e))

                    log_time = now.strftime("%I:%M %p")
                    log_message = log_message + ";" + "Sharepoint check to trigger robot is completed" + "/" + log_date + "/" + log_time



    except Exception as e:
        log_message = log_message + ";" + str(e)
        UpdateLogstoDB(log_message, logstablename, max_log_id, c, conn)
        try:
            SendMail(servicemailclientid, servicemailclientsecret, servicetenantid, serviceemailaddress, server, str(e),
                     recipient_email)
        except Exception as e:
            log_time = now.strftime("%I:%M %p")
            log_message = log_message + ";" + "Failed to send exception email " + str(e) + "/" + log_date + "/" + log_time
            UpdateLogstoDB(log_message, logstablename, max_log_id, c, conn)
    finally:
        UpdateLogstoDB(log_message, logstablename, max_log_id, c, conn)
        if conn:
            conn.close()


if __name__ == '__main__':
    MailCheck()



