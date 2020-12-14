import os
import subprocess
from flask import Flask, request, make_response
import json
from app import main

print("Started...")
app = Flask(__name__)

@app.route('/')
def func1():
    print("Running")
    return "Ok"

@app.route('/exec', methods=['POST'])
def route_exec():
    params_table = request.data.decode('utf-8')
    print(params_table)
    main.run(params_table)
    

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get('PORT', 8080)))
