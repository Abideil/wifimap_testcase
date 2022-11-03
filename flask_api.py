from flask import Flask, render_template
from user_aggregation import *

app = Flask(__name__)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/button1")
def first_button():
    titles = ("Count the total number of created by user hotspots",)
    users_hotspots = merge_datasets(*data_download())[0]
    df = count_users_hotspots(users_hotspots).head(10)
    plot_url = df_visualisation(df)
    return render_template(
        "button.html",
        tables=[df.to_html(classes="data")],
        titles=titles,
        title="Button 1",
        plot_url=plot_url,
    )


@app.route("/button2")
def second_button():
    titles = ("Count the total number of created by user hotspots with geoposition",)
    users_hotspots = merge_datasets(*data_download())[0]
    df = count_users_hotspots_geo(users_hotspots).head(10)
    plot_url = df_visualisation(df)
    return render_template(
        "button.html",
        tables=[df.to_html(classes="data")],
        titles=titles,
        title="Button 2",
        plot_url=plot_url,
    )


@app.route("/button3")
def third_button():
    titles = [
        "За все время:",
        "За последний месяц:",
        "За последнюю неделю:",
        "За последнюю доступную неделю:",
    ]
    users_hotspots = merge_datasets(*data_download())[0]
    df = [
        i.apply(lambda x: x.head(10))
        for i in count_users_hotspots_over_time(users_hotspots)
    ]
    plot_url = df_visualisation(df)
    return render_template(
        "button.html",
        tables=[i.to_html(classes="data") for i in df],
        titles=titles,
        title="Button 3",
        plot_url=plot_url,
    )


@app.route("/button4")
def fourth_button():
    titles = (
        "Count the total number of created by user hotspots with desired score values",
    )
    users_hotspots = merge_datasets(*data_download())[0]
    df = count_users_hotspots_score(users_hotspots).head(10)
    plot_url = df_visualisation(df)
    return render_template(
        "button.html",
        tables=[df.to_html(classes="data")],
        titles=titles,
        title="Button 4",
        plot_url=plot_url,
    )


@app.route("/button5")
def fifth_button():
    titles = [
        "За все время:",
        "За последний год:",
        "За последний месяц:",
        "За последнюю неделю:",
        "За последнюю доступную неделю:",
    ]
    users_hotspots, users_conns = merge_datasets(*data_download())
    df = [
        i.apply(lambda x: x.head(10))
        for i in count_users_unique_hotspots(users_conns, users_hotspots)
    ]
    plot_url = df_visualisation(df)
    return render_template(
        "button.html",
        tables=[i.to_html(classes="data") for i in df],
        titles=titles,
        title="Button 5",
        plot_url=plot_url,
    )


if __name__ == "__main__":
    app.run(debug=True)
