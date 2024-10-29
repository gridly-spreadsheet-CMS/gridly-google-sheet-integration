import random
import re
import string
import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder
import json
import os
import time
import urllib



viewID = ""


def importCSV(mycsv, sheetHeaders, _viewId, _gridlyApiKEy, synchColumns, _ExcludedColumnName):
    ExcludedColumnName = _ExcludedColumnName
    global viewId
    global gridlyApiKEy
    gridlyApiKEy = _gridlyApiKEy
    viewId = _viewId

    refreshView()

    while(view["gridStatus"] != "active"):
        time.sleep(30)
        refreshView()

    if synchColumns == "true":
        synchHeaders(sheetHeaders, ExcludedColumnName)
        
    url = "https://api.gridly.com/v1/views/" + viewId + "/import"

    

    mp_encoder = MultipartEncoder(
        fields={
        'file': ('addresses.csv',mycsv,'text/csv')
        }
    )

    headers = {
    'Authorization': 'ApiKey ' + gridlyApiKEy,
    'Content-Type': mp_encoder.content_type
    }
    importResponse = requests.request("POST", url, headers=headers, data=mp_encoder)
    #print(importResponse.text)
    time.sleep(5)


def refreshView():
    url = "https://api.gridly.com/v1/views/" + viewId

    payload={}
    headers = {
    'Authorization': 'ApiKey ' + gridlyApiKEy
    }
    global view
    view = json.loads(requests.request("GET", url, headers=headers, data=payload).content)

def getGridlyHeaders():
    columnNames = []
    try:
        for column in view["columns"]:
            if "name" in column:
                columnNames.append(column["name"])
    except Exception as e:
        return None
    return columnNames

def getGridlyColmnData(_viewId, _gridlyApiKEy):
    url = "https://api.gridly.com/v1/views/" + _viewId

    payload={}
    headers = {
    'Authorization': 'ApiKey ' + _gridlyApiKEy
    }

    data = json.loads(requests.request("GET", url, headers=headers, data=payload).content)
    return {column["id"]: column["name"] for column in data["columns"]}


def synchHeaders(sheetHeaders, ExcludedColumnName):
    #print(sheetHeaders)
    gridlyHeaders = getGridlyHeaders()
    for sheetheader in sheetHeaders:
        if sheetheader not in gridlyHeaders and sheetheader != "_recordId" and sheetheader != "_pathTag" and sheetheader != ExcludedColumnName:
            createGridlyHeader(sheetheader)
    refreshView()


def createGridlyHeader(headerName):
    #print(f"Try create new column for: {headerName}")
    refreshView()
    if "gridStatus" in view:
        while view["gridStatus"] != "active":
            time.sleep(30)
            refreshView()

    url = f"https://api.gridly.com/v1/views/{viewId}/columns"
    
    # Check if headerName contains only valid characters; if not, generate a random ID
    if re.search(r'[^a-zA-Z0-9_]', headerName):
        id = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    else:
        id = headerName

    payload = {
        "name": headerName,   # UTF-8 compatible name
        "type": "multipleLines",
        "id": id              # Valid alphanumeric or generated ID
    }
    #print("Payload to send:", payload)
    
    headers = {
        'Authorization': f'ApiKey {gridlyApiKEy}',
        'Content-Type': 'application/json; charset=utf-8'
    }

    response = requests.post(url, headers=headers, json=payload)
    #print(response.text)  # Log response to check for any API-related errors