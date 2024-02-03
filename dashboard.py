from flask import Flask, render_template, request
from influx import q_data as queue_data

app = Flask(__name__)

@app.route('/')
def index():
    
    data = queue_data()

    return render_template('index.html', data=data)

if __name__ == '__main__':
    app.run(debug=True)