
import datetime
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
    
    return render_template("index.html", results2_1 = results2_1, results2_2 = results2_2)

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True)

    
    
   

    

    
    
    
    
    


  
