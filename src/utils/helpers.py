import os
import re
import unicodedata
import logging
from pathlib import Path
from typing import List
import pandas as pd

class ColumnNameProcessor:
    """Procesa y sanitiza nombres de columnas."""
    MAX_COLUMN_NAME_LENGTH = 63
    
    @staticmethod
    def sanitize_column_name(raw_name: str) -> str:
        if not isinstance(raw_name, str):
            return ""
            
        name = raw_name.lower()
        name = re.sub(r'\[.*?\]', '', name)
        name = unicodedata.normalize('NFD', name)
        name = ''.join(char for char in name if unicodedata.category(char) != 'Mn')
        
        replacements = {'ñ': 'n', '°': 'deg', '²': '2', '³': '3', 'º': 'deg'}
        for old_char, new_char in replacements.items():
            name = name.replace(old_char, new_char)
        
        name = re.sub(r'[^a-z0-9_]+', '_', name)
        name = re.sub(r'_+', '_', name)
        name = name.strip('_')
        
        if not name:
             name = f"col_raw_{str(raw_name)[:10].replace(' ','')}"

        if name and name[0].isdigit():
            name = 'col_' + name
            
        if len(name) > ColumnNameProcessor.MAX_COLUMN_NAME_LENGTH:
            name = name[:ColumnNameProcessor.MAX_COLUMN_NAME_LENGTH].rstrip('_')
        
        return name

def enforce_pg_numeric_constraints(series: pd.Series, precision: int = 26, scale: int = 13) -> pd.Series:
    """
    Ajusta una serie numérica para cumplir con PostgreSQL NUMERIC(p, s).
    Estrategia: Round & Clip (Evita overflow cortando al máximo permitido).
    """
    limit = (10 ** (precision - scale)) - 1.0
    series_adj = series.round(scale)
    series_adj = series_adj.clip(lower=-limit, upper=limit)
    return series_adj

class SmartCsvReader:
    """Helper para manejar la lógica compleja de lectura de CSVs con distintos formatos."""
    CSV_ENCODINGS = ['latin1', 'cp1252', 'iso-8859-1', 'utf-8']
    
    @staticmethod
    def read_csv_robust(file_path: str, db_num_columns: int) -> pd.DataFrame:
        filename = os.path.basename(file_path)
        detected_sep = ';' 
        
        try:
            with open(file_path, 'r', encoding='latin1') as f:
                line = f.readline()
                if line.count(',') > line.count(';'):
                    detected_sep = ','
        except Exception:
            pass 

        df = None
        for enc in SmartCsvReader.CSV_ENCODINGS:
            try:
                df = pd.read_csv(file_path, sep=detected_sep, encoding=enc, header=0, index_col=False, decimal=',')
                if len(df.columns) >= db_num_columns:
                    break
                df = None 
            except Exception:
                continue
        
        if df is None:
            alt_sep = ',' if detected_sep == ';' else ';'
            try:
                df = pd.read_csv(file_path, sep=alt_sep, encoding='latin1', header=0, index_col=False, decimal=',')
            except:
                pass

        if df is None:
            raise ValueError("No se pudo leer el archivo con ninguna combinación de encoding/separador.")

        df.dropna(how='all', axis=1, inplace=True)
        
        if len(df.columns) > db_num_columns:
            df = df.iloc[:, :db_num_columns]
            
        return df