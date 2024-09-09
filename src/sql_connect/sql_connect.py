from typing import Any
import os
import pandas as pd
import mysql.connector
import json
from ensure import ensure_annotations
from mysql.connector import Error

class MySQLOperation:
    __connection = None  # 私有/保护变量，存储数据库连接
    __cursor = None
    
    def __init__(self, host: str, user: str, password: str, database: str):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
    
    def create_mysql_connection(self):
        """ 创建 MySQL 数据库连接 """
        if MySQLOperation.__connection is None:
            try:
                MySQLOperation.__connection = mysql.connector.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    database=self.database
                )
                MySQLOperation.__cursor = MySQLOperation.__connection.cursor()
                print(f"Connected to MySQL database: {self.database}")
            except Error as e:
                print(f"Error occurred: {e}")
        return MySQLOperation.__connection

    def execute_query(self, query: str):
        """ 执行 SQL 查询 """
        connection = self.create_mysql_connection()
        cursor = MySQLOperation.__cursor
        try:
            cursor.execute(query)
            connection.commit()
            print("Query executed successfully")
        except Error as e:
            print(f"Error occurred: {e}")

    def insert_record(self, record: dict, table_name: str) -> Any:
        """ 插入一条记录 """
        connection = self.create_mysql_connection()
        cursor = MySQLOperation.__cursor
        columns = ', '.join(record.keys())
        values = ', '.join(['%s'] * len(record))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
        try:
            cursor.execute(query, tuple(record.values()))
            connection.commit()
            print("Record inserted successfully")
        except Error as e:
            print(f"Error occurred: {e}")

    def bulk_insert(self, datafile: str, table_name: str):
        """ 批量插入数据到表中 """
        if datafile.endswith('.csv'):
            dataframe = pd.read_csv(datafile, encoding='utf-8')
        elif datafile.endswith(".xlsx"):
            dataframe = pd.read_excel(datafile, encoding='utf-8')
        else:
            raise ValueError("Unsupported file format. Only .csv or .xlsx files are allowed.")
        
        records = dataframe.to_dict(orient='records')  # 将 DataFrame 转换为记录列表
        for record in records:
            self.insert_record(record, table_name)

    def fetch_records(self, query: str) -> Any:
        """ 执行 SELECT 查询并返回结果 """
        cursor = MySQLOperation.__cursor
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except Error as e:
            print(f"Error occurred: {e}")
            return None
