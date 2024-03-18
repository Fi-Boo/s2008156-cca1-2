
import datetime
from flask import Flask, render_template
from google.cloud import bigquery



bigQuery_client = bigquery.Client()


app = Flask(__name__)

@app.route("/", methods=['GET', 'POST'])
def index():
    
    query = """
    SELECT time_ref, SUM(value) as total
    FROM `s2008156-cca1-2.country.gsquarterlySeptember20` 
    GROUP BY time_ref
    ORDER BY total DESC
    LIMIT 10;
    """
    job = bigQuery_client.query_and_wait(query) 

    results7_1 = list(job)

    
    query1 ="""
            WITH tradevalue AS
            (
                    SELECT   Substr(Cast(time_ref AS STRING), 1, 6) AS year_month,
                            Sum(value)                             AS trade_value
                    FROM     `s2008156-cca1-2.country.gsquarterlySeptember20`
                    GROUP BY year_month )
            SELECT   year_month AS time_ref,
                    trade_value
            FROM     tradevalue
            ORDER BY trade_value DESC limit 10;
    
    """
    


    query2 ="""
            WITH totaltrade AS
            (
                    SELECT   gs.country_code,
                            cc.country_label,
                            Sum(
                            CASE
                                    WHEN gs.time_ref BETWEEN 201301 AND      201512 THEN gs.value
                                    ELSE 0
                            END)                                            AS total_trade_value
                    FROM     `s2008156-cca1-2.country.gsquarterlySeptember20`       AS gs
                    JOIN     `s2008156-cca1-2.country.country_codes`  AS cc
                    ON       gs.country_code = cc.country_code
                    WHERE    gs.status = 'F'
                    AND      gs.product_type = 'Goods'
                    GROUP BY gs.country_code,
                            cc.country_label ), imports AS
            (
                    SELECT   country_code,
                            sum(
                            CASE
                                    WHEN time_ref BETWEEN 201301 AND      201512
                                    AND      account = 'Imports' THEN value
                                    ELSE 0
                            END) AS import_value
                    FROM     `s2008156-cca1-2.country.gsquarterlySeptember20` 
                    WHERE    status = 'F'
                    AND      product_type = 'Goods'
                    GROUP BY country_code ), exports AS
            (
                    SELECT   country_code,
                            sum(
                            CASE
                                    WHEN time_ref BETWEEN 201301 AND      201512
                                    AND      account = 'Exports' THEN value
                                    ELSE 0
                            END) AS export_value
                    FROM     `s2008156-cca1-2.country.gsquarterlySeptember20` 
                    WHERE    status = 'F'
                    AND      product_type = 'Goods'
                    GROUP BY country_code )
            SELECT   tt.country_label,
                    'Goods'                           AS product_type,
                    (i.import_value - e.export_value) AS trade_deficit,
                    'F'                               AS status
            FROM     imports i
            JOIN     exports e
            ON       i.country_code = e.country_code
            JOIN     totaltrade tt
            ON       i.country_code = tt.country_code
            ORDER BY trade_deficit DESC limit 40;
    """
    
    query3 ="""
            WITH toptimeslots AS
            (
                    -- Query Result 1
                    SELECT   Substr(Cast(time_ref AS STRING), 1, 6) AS year_month
                    FROM     `s2008156-cca1-2.country.gsquarterlySeptember20`
                    GROUP BY year_month
                    ORDER BY sum(value) DESC limit 10 ), topcountries AS
            (
                    -- Query Result 2
                    SELECT   gs.country_code
                    FROM     `s2008156-cca1-2.country.gsquarterlySeptember20`      AS gs
                    JOIN     `s2008156-cca1-2.country.country_codes` AS cc
                    ON       gs.country_code = cc.country_code
                    WHERE    gs.status = 'F'
                    AND      gs.product_type = 'Goods'
                    GROUP BY gs.country_code
                    ORDER BY sum(value) DESC limit 40 ), tradesurplus AS
            (
                    SELECT   service_code,
                            sum(
                            CASE
                                    WHEN gs.account = 'Exports' THEN value
                                    ELSE -value
                            END)                                            AS trade_surplus
                    FROM     `s2008156-cca1-2.country.gsquarterlySeptember20`      AS gs
                    JOIN     `s2008156-cca1-2.country.services_codes` AS sc
                    ON       gs.code = sc.service_code
                    JOIN     toptimeslots tts
                    ON       substr(cast(gs.time_ref AS string), 1, 6) = tts.year_month
                    JOIN     topcountries tc
                    ON       gs.country_code = tc.country_code
                    GROUP BY service_code )
            SELECT   sc.service_label,
                    trade_surplus
            FROM     tradesurplus ts
            JOIN     `s2008156-cca1-2.country.services_codes` AS sc
            ON       ts.service_code = sc.service_code
            ORDER BY trade_surplus DESC limit 25;

    """
    

    return render_template("index.html", results7_1 = results7_1)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True)
    

  
    
