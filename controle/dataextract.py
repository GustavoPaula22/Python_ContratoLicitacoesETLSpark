import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pyspark.sql import SparkSession


def planilha():
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
    SAMPLE_SPREADSHEET_ID = "1Y44bOR67JH_Oei9EbtnsByEZFJI9Uh0EF3SKoqGHSKA"
    SAMPLE_RANGE_NAME = "Licitacao!A4:DS"

    token_path = os.path.join("modelo", "token.json")
    credentials_path = os.path.join("modelo", "client_secret.json")
    creds = None

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_path, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, "w") as token:
            token.write(creds.to_json())

    try:
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId=SAMPLE_SPREADSHEET_ID, range=SAMPLE_RANGE_NAME).execute()
        values = result.get("values", [])

        if not values:
            print("Planilha não localizada...")
        else:
            spark = SparkSession.builder.getOrCreate()
            columns = values[0]
            data = values[1:]
            df = spark.createDataFrame(data, schema=columns)
            return df

    except HttpError as err:
        print(f"Erro ao acessar a planilha: {err}")
