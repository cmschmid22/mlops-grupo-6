from fastapi import FastAPI
import psycopg2
import json
from contextlib import contextmanager

app = FastAPI()

def get_db_connection():
    return psycopg2.connect(
        database="postgres",
        user="postgres",
        password="abcde12345",
        host="grupo-6-rds.cf4i6e6cwv74.us-east-1.rds.amazonaws.com",
        port='5432'
    )

@contextmanager
def get_cursor():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        yield cursor
        conn.commit()
    finally:
        cursor.close()
        conn.close()

def read_table(modelo, adv):
    with get_cursor() as cursor:
        try:
            if modelo == 'top-ctr':
                tabla = "top_ctr"
                score = "ctr"
            elif modelo == 'top-product':
                tabla = "top_product"
                score = "views"
            else:
                return None

            cursor.execute("""
                SELECT product_id 
                FROM {} 
                WHERE advertiser_id = %s
                ORDER BY DATE(fecha) DESC, {} DESC
                LIMIT 1;
            """.format(tabla, score), (adv,))

            rows = cursor.fetchall()
            return rows[0][0] if rows else "Sin resultados"
            
        except Exception as e:
            print(f"Error: {e}")
            return None

def count_adv():
    with get_cursor() as cursor:
        try:
            cursor.execute("""
                SELECT COUNT(DISTINCT advertiser_id) 
                FROM top_ctr;
            """)
            rows = cursor.fetchall()
            return rows[0][0] if rows else "Sin resultados"
        except Exception as e:
            print(f"Error: {e}")
            return None

def top_5_clicks(): 
    with get_cursor() as cursor:
        cursor.execute("""
                    SELECT advertiser_id, SUM(clicks) as clicks
                    FROM top_ctr
                    WHERE DATE(fecha) = (
                            SELECT DATE(MAX(fecha))
                            FROM top_ctr
                        )
                    GROUP BY advertiser_id
                    ORDER BY clicks DESC
                    LIMIT 5;
        """)
        rows = cursor.fetchall()
        return rows
    
def top_5_views(): 
    with get_cursor() as cursor:
        cursor.execute("""
                    SELECT advertiser_id, SUM(views) as views
                    FROM top_product
                    WHERE DATE(fecha) = (
                            SELECT DATE(MAX(fecha))
                            FROM top_product
                        )
                    GROUP BY advertiser_id
                    ORDER BY views DESC
                    LIMIT 5;
        """)
        rows = cursor.fetchall()
        return rows

def rec7d(modelo, adv):
    with get_cursor() as cursor:
        try:
            if modelo == 'top-ctr':
                tabla = "top_ctr"
                score = "ctr" 
            elif modelo == 'top-product':
                tabla = "top_product"
                score = "views"
            else:
                return None  # Return None for invalid modelo

            cursor.execute("""
                WITH RankedProducts AS (
                    SELECT 
                        DATE(fecha) as fecha,
                        product_id,
                        {},
                        ROW_NUMBER() OVER (PARTITION BY DATE(fecha) ORDER BY {} DESC) as rank
                    FROM {}
                    WHERE advertiser_id = %s
                    AND DATE(fecha) >= CURRENT_DATE - INTERVAL '8 day'
                )
                SELECT product_id
                FROM RankedProducts 
                WHERE rank = 1
                ORDER BY fecha DESC;
            """.format(score, score, tabla), (adv,))            

            rows = cursor.fetchall()
            return [row[0] for row in rows] if rows else []
            
        except Exception as e:
            print(f"Error: {e}")
            return []

# Devuelve un producto recomendado para un advertiser y un modelo específico
@app.get("/recommendations/{ADV}/{Modelo}")
def recommendations(ADV: str, Modelo: str):     
    result = {"adv": ADV, "modelo": Modelo, "product_id": read_table(Modelo, ADV)}
    return result  

# Devuelve stats para el último día: cantidad de advertisers, top 5 advertisers por clicks y top 5 advertisers por views
@app.get("/stats/")
def stats():     
    result = {"Cantidad de advertisers": count_adv(), "Top 5 advertisers por clicks": top_5_clicks(), "Top 5 advertisers por views": top_5_views()}
    return result 

# Devuelve las recomendaciones de los últimos 7 días para un advertiser y un modelo específico
@app.get("/history/{ADV}/{Modelo}")
def history(ADV: str, Modelo: str):
    result = {"adv": ADV, "Recomendaciones 7 dias": rec7d(Modelo, ADV)}
    return result