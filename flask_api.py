from flask import Flask, render_template, request
from user_aggregation import *

app = Flask(__name__)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/button1")
def first_button():
    users_hotspots = merge_datasets(*data_download())[0]
    print(count_users_hotspots(users_hotspots).head(10))
    return "Nothing"


@app.route("/button2")
def second_button():
    users_hotspots = merge_datasets(*data_download())[0]
    print(count_users_hotspots_geo(users_hotspots).head(10))
    return "Nothing"


@app.route("/button3")
def third_button():
    users_hotspots = merge_datasets(*data_download())[0]
    for i in range(3):
        print(count_users_hotspots_over_time(users_hotspots)[i].head(10))
    return "Nothing"


@app.route("/button4")
def fourth_button():
    users_hotspots = merge_datasets(*data_download())[0]
    print(count_users_hotspots_score(users_hotspots).head(10))
    return "Nothing"


@app.route("/button5")
def fifth_button():
    users_hotspots, users_conns = merge_datasets(*data_download())
    for i in range(5):
        print(count_users_unique_hotspots(users_conns, users_hotspots)[i].head(10))
    return "Nothing"


if __name__ == "__main__":
    app.run()
