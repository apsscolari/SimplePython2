import re
from flask import Flask, request, jsonify
import pandas as pd
import pymysql

app = Flask(__name__)

@app.route("/run_query", methods=["POST"])
def run_query():
    # Get parameters from JSON POST body or query string
    data = request.get_json() or request.values
    target_db = data.get('target_db', 'curated')
    target_table = data.get('target_table', 'client_communication_preferences_journal')
    as_of = data.get('as_of')

    # Validate as_of parameter
    if not as_of or not re.match(r'^\d{8}$', as_of):
        return jsonify({"error": "Invalid or missing as_of. Expected format: YYYYMMDD."}), 400

    # Dummy DB connection details (replace as needed)
    user = 'myuser'
    password = 'mypassword'
    host = 'mysql1.mycorp.io'
    port = 3306
    source_db = 'blueshift'

    try:
        # Connect to source DB
        source_conn = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=source_db,
            port=port,
            cursorclass=pymysql.cursors.DictCursor
        )

        qry = f"""
            WITH blueshift_active_email_client_agg AS (
                SELECT client_id, 
                       MAX(last_opened_at) AS last_opened_at,
                       MIN(first_opened_at) AS first_opened_at
                FROM campaign_activity_kpis
                WHERE (DATE(last_opened_at) <= STR_TO_DATE(%s, '%%Y%%m%%d')
                   OR last_opened_at IS NULL
                   OR DATE(first_opened_at) <= STR_TO_DATE(%s, '%%Y%%m%%d'))
                GROUP BY client_id
            )
            SELECT * FROM blueshift_active_email_client_agg
        """

        # Use parameterized queries!
        df = pd.read_sql(qry, source_conn, params=[as_of, as_of])

        source_conn.close()
    except Exception as e:
        return jsonify({"error": f"Failed to query source: {str(e)}"}), 500

    # Now insert into target
    try:
        target_conn = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=target_db,
            port=port,
            cursorclass=pymysql.cursors.DictCursor
        )

        # Only allow specific table names (simple whitelist for demo)
        allowed_tables = ["client_communication_preferences_journal"]
        if target_table not in allowed_tables:
            return jsonify({"error": "Target table is not allowed."}), 400

        if not df.empty:
            cols = list(df.columns)
            values = [tuple(row) for row in df.values]

            insert_stmt = f"INSERT INTO {target_table} ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(cols))})"

            with target_conn.cursor() as cursor:
                cursor.executemany(insert_stmt, values)
            target_conn.commit()
            target_conn.close()
            return jsonify({"status": f"Inserted {len(df)} rows into {target_db}.{target_table}"}), 200
        else:
            return jsonify({"status": "No data to insert."}), 200

    except Exception as e:
        return jsonify({"error": f"Insert failed: {str(e)}"}), 500


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)
