import io
from locale import normalize

import urllib
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json, requests, csv
import gridly_api_handler

# Define the scope and authorization
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('cred.json', scope)
client = gspread.authorize(creds)


import urllib.parse
import json

def updateCells(viewId, sheetUniqueIdColumn, gridlyApiKey, spreadSheetName):
    column_mapping = gridly_api_handler.getGridlyColmnData(viewId, gridlyApiKey)
    #print("Column mapping:", column_mapping)
    sheetUniqueIdColumn = int(sheetUniqueIdColumn)
    limit = 1000
    offset = 0
    records = []

    headers = {'Authorization': f'ApiKey {gridlyApiKey}'}
    url = f"https://api.gridly.com/v1/views/{viewId}/records"

    # Get total count from headers and fetch records in pages
    first_page_params = urllib.parse.quote(json.dumps({"offset": offset, "limit": limit}))
    response = requests.get(f"{url}?page={first_page_params}", headers=headers)
    total_count = int(response.headers.get('X-Total-Count', 0))
    records.extend(response.json())

    while len(records) < total_count:
        offset += limit
        next_page_params = urllib.parse.quote(json.dumps({"offset": offset, "limit": limit}))
        response = requests.get(f"{url}?page={next_page_params}", headers=headers)
        records.extend(response.json())

    #print(len(records))  # Should now match X-Total-Count exactly

    sheet = client.open(spreadSheetName)
    sheetTabs = {s['properties']['title']: s['properties']['index'] for s in sheet._spreadsheets_get()["sheets"]}

    updates = []
    current_sheet = None

    for record in records:
        path_name = record["path"]
        if path_name not in sheetTabs:
            continue

        if not current_sheet or current_sheet.title != path_name:
            if updates:
                send_batch_updates(current_sheet, updates)
                updates = []
            current_sheet = sheet.get_worksheet(sheetTabs[path_name])

            # Get headers and unique IDs for the current sheet
            sheet_data = current_sheet.get_all_records()
            if not sheet_data:
                continue
            sheet_headers = [key for key in sheet_data[0].keys()]
            record_ids = [str(row[sheet_headers[sheetUniqueIdColumn]]) for row in sheet_data]

        try:
            row = record_ids.index(record["id"]) + 2  # +2 to account for header row
            row_values = [""] * len(sheet_headers)  # Initialize row values

            for cell in record["cells"]:
                
                col_id = column_mapping[cell["columnId"]]
                value = cell.get("value", "")
                col = sheet_headers.index(col_id)
                
                
                # Skip updating the unique ID column
                if col != sheetUniqueIdColumn:
                    row_values[col] = value
                

            # Adjust the range to exclude the unique ID column
            update_range = f"{chr(65 + (sheetUniqueIdColumn + 1))}{row}:{chr(65 + len(sheet_headers) - 1)}{row}"
            updates.append({
                'range': update_range,
                'values': [row_values[sheetUniqueIdColumn + 1:]]
            })
            

        except ValueError:
            continue

    if updates:
        send_batch_updates(current_sheet, updates)


def send_batch_updates(sheet, updates, max_batch_size=1000):
    """Batch updates by row to Google Sheets, with rate limiting if necessary."""
    results = []
    for i in range(0, len(updates), max_batch_size):
        batch = updates[i:i + max_batch_size]
        result = sheet.batch_update(batch)
        results.append(result)
        #print("Batch update result:", result)

    return results


def pullSheet(event, context):
    #event = json.loads(event) uncomment if you want to trigger manuall from your pc

    sheetUniqueIdColumn = event["sheetUniqueIdColumn"]
    synchColumns = event["synchColumns"]
    spreadSheetName = event["spreadSheetName"]
    viewId = event["viewId"]
    gridlyApiKey = event["gridlyApiKey"]
    getSheetAsCSV(spreadSheetName, viewId, gridlyApiKey, synchColumns, sheetUniqueIdColumn)


def pushSheet(event, context):
    #event = json.loads(event) uncomment if you want to trigger manuall from your pc
    
    sheetUniqueIdColumn = event["sheetUniqueIdColumn"]
    synchColumns = event["synchColumns"]
    spreadSheetName = event["spreadSheetName"]
    viewId = event["viewId"]
    gridlyApiKey = event["gridlyApiKey"]
    updateCells(viewId, sheetUniqueIdColumn, gridlyApiKey, spreadSheetName)


def getSheetAsCSV(spreadSheetName, viewId, gridlyApiKey, synchColumns, sheetUniqueIdColumn):
    sheet = client.open(spreadSheetName)
    sheets = sheet._spreadsheets_get()["sheets"]
    for i in range(len(sheets)):
        data = sheet.get_worksheet(i).get_all_records()
        headers = list(data[0].keys())
        for item in data:
            item.update({"_pathTag": sheets[i]["properties"]["title"]})

        gridly_api_handler.importCSV(json_to_csv(data, int(sheetUniqueIdColumn)), headers, viewId, gridlyApiKey, synchColumns, ExcludedColumnName)


def json_to_csv(jsonFile, sheetUniqueIdColumn):
    keys = list(jsonFile[0].keys())
    global ExcludedColumnName
    ExcludedColumnName = keys[sheetUniqueIdColumn]
    
    for rec in jsonFile:
        rec["_recordId"] = rec.pop(keys[sheetUniqueIdColumn])
    
    # Create an in-memory string buffer
    output = io.StringIO()
    
    # Create CSV writer for the string buffer
    writer = csv.DictWriter(output, fieldnames=jsonFile[0].keys(), delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()
    writer.writerows(jsonFile)
    
    # Get the CSV content as a string
    csv_content = output.getvalue()

    return csv_content


# Example call for manual testing
#pushSheet("{\r\n\t\"gridlyApiKey\":\"YOURAPIKEY\",\r\n\r\n\t\"spreadSheetName\":\"YOURSPREADSHEETNAME\",\r\n\r\n\t\"viewId\":\"YOURVIEWID\",\r\n\r\n\t\"synchColumns\":\"true\",\r\n\r\n\t\"sheetUniqueIdColumn\":0\r\n}", "")
#pullSheet("{\r\n\t\"gridlyApiKey\":\"YOURAPIKEY\",\r\n\t\"spreadSheetName\":\"YOURSPREADSHEETNAME\",\r\n\t\"viewId\":\"YOURVIEWID\",\r\n\t\"synchColumns\":\"true\",\r\n\t\"sheetUniqueIdColumn\":0\r\n}", "")
