
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

    results = list(job)
    
    for result in results:
        
        print(result)
    
    return render_template("index.html", results = results)

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True)
    

  
    
