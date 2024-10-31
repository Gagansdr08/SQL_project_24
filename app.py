from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
from contextlib import closing

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

@app.route('/top-paying-jobs', methods=['GET'])
def get_top_paying_jobs():
    job_title = request.args.get('job_title', default='Data Analyst')
    location = request.args.get('location', default='India')

    try:
        with closing(psycopg2.connect(
            dbname="sql_course",
            user="postgres",
            password="GaganPOSTGRE8+",
            host="localhost",
            port="5432"
        )) as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT job_id, job_title, job_location, job_schedule_type, 
                           salary_year_avg, job_posted_date, company_dim.name AS company_name
                    FROM job_postings_fact
                    LEFT JOIN company_dim ON job_postings_fact.company_id = company_dim.company_id
                    WHERE job_title_short = %s AND job_location = %s AND salary_year_avg IS NOT NULL
                    ORDER BY salary_year_avg DESC
                    LIMIT 10;
                """
                cur.execute(query, (job_title, location))
                result = cur.fetchall()

                # Format the result as a list of dictionaries
                jobs = [
                    {
                        "job_id": row[0],
                        "job_title": row[1],
                        "job_location": row[2],
                        "job_schedule_type": row[3],
                        "salary_year_avg": row[4],
                        "job_posted_date": row[5],
                        "company_name": row[6],
                    }
                    for row in result
                ]

                return jsonify(jobs)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)