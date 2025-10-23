from flask import Flask, render_template, request
import pandas as pd

app = Flask(__name__)

# Charger les données CSV
def get_data():
    return pd.read_csv("joueurs.csv")

@app.route("/", methods=["GET"])
def index():
    df = get_data()

    # Recherche
    search = request.args.get("search", "")
    if search:
        df = df[df["Nom"].str.contains(search, case=False, na=False)]

    # Tri
    sort_col = request.args.get("sort", "")
    order = request.args.get("order", "asc")
    if sort_col in df.columns:
        df = df.sort_values(by=sort_col, ascending=(order == "asc"))

    # Convertir en liste de dictionnaires pour le template
    data = df.to_dict(orient="records")

    return render_template("index.html", data=data, columns=df.columns, search=search, sort_col=sort_col, order=order)

if __name__ == "__main__":
    app.run(debug=True)
