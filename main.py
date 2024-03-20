from flask import Flask, render_template
from google.cloud import bigquery



bigQuery_client = bigquery.Client()


app = Flask(__name__)

@app.route("/", methods=['GET', 'POST'])
def index():
    
    query1 = """
        SELECT time_ref, CAST(SUM(value) as int) as total
        FROM `s2008156-cca1-2.country.gsquarterlySeptember20` 
        GROUP BY time_ref
        ORDER BY total DESC
        LIMIT 10;
        """
    job = bigQuery_client.query_and_wait(query1) 

    results2_1 = list(job)
    
    query2 ="""
        SELECT countries.country_label AS country,
        extras.product_type AS product_type,
        CAST((import_total-export_total) as int) AS trade_deficit,
        extras.status AS status
        FROM  (SELECT country_code,
                Sum(CASE
                        WHEN time_ref BETWEEN 201301 AND 201512
                                AND account = 'Exports' THEN value
                        ELSE 0
                        end) AS export_total
        FROM   `s2008156-cca1-2.country.gsquarterlySeptember20`
        WHERE  status = 'F'
                AND product_type = 'Goods'
        GROUP  BY country_code) AS export
        JOIN (SELECT country_code,
                        Sum(CASE
                                WHEN time_ref BETWEEN 201301 AND 201512
                                AND account = 'Imports' THEN value
                                ELSE 0
                        end) AS import_total
                FROM   `s2008156-cca1-2.country.gsquarterlySeptember20`
                WHERE  status = 'F'
                        AND product_type = 'Goods'
                GROUP  BY country_code) AS import
                ON export.country_code = import.country_code
        JOIN `s2008156-cca1-2.country.country_codes` AS countries
                ON export.country_code = countries.country_code
        JOIN (SELECT DISTINCT country_code,
                                product_type,
                                status
                FROM   `s2008156-cca1-2.country.gsquarterlySeptember20`
                WHERE  product_type = 'Goods'
                        AND status = 'F') AS extras
                ON export.country_code = extras.country_code
        ORDER  BY trade_deficit DESC
        LIMIT  40;
    """
    
    job2 = bigQuery_client.query_and_wait(query2) 

    results2_2 = list(job2)
    
    query3 = """
        SELECT services.service_label AS service, 
        CAST(SUM(  CASE
        WHEN account = 'Exports'  THEN value
                                        ELSE 0
        END) -  SUM(  CASE
                        WHEN account = 'Imports'  THEN value
                                                ELSE 0
                        END) AS int) AS surplus
        FROM `s2008156-cca1-2.country.gsquarterlySeptember20` AS base
        JOIN (
        SELECT time_ref
                FROM `s2008156-cca1-2.country.gsquarterlySeptember20` 
                GROUP BY time_ref
                ORDER BY SUM(value) DESC
                LIMIT 10
        ) AS top10
        ON base.time_ref = top10.time_ref

        JOIN (
        SELECT export.country_code AS country_code,
                (import_total-export_total) AS trade_deficit,
        FROM  (
                SELECT country_code,
                SUM (CASE
                        WHEN time_ref BETWEEN 201301 AND 201512
                                        AND account = 'Exports' THEN value
                        ELSE 0
                END) AS export_total
                FROM   `s2008156-cca1-2.country.gsquarterlySeptember20`
                WHERE  status = 'F'
                AND product_type = 'Goods'
                GROUP  BY country_code) AS export
                JOIN  (
                SELECT country_code,
                SUM (CASE
                        WHEN time_ref BETWEEN 201301 AND 201512
                                        AND account = 'Imports' THEN value
                                        ELSE 0
                        END) AS import_total
                FROM   `s2008156-cca1-2.country.gsquarterlySeptember20`
                WHERE  status = 'F'
                AND product_type = 'Goods'
                GROUP  BY country_code) AS import
                ON export.country_code = import.country_code
        ORDER  BY trade_deficit DESC
        LIMIT  40
        ) AS top40
        ON base.country_code = top40.country_code
        JOIN `s2008156-cca1-2.country.services_codes` AS services
        ON base.code = services.service_code
        WHERE code LIKE 'A%'
        GROUP BY service
        ORDER BY surplus DESC
        LIMIT 25
        """
    job3 = bigQuery_client.query_and_wait(query3) 
    
    results2_3 = list(job3)
    
    return render_template("index.html", results2_1 = results2_1, results2_2 = results2_2, results2_3 = results2_3)

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True)

    
    
   

    

    
    
    
    
    


  
