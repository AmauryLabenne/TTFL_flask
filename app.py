from flask import Flask, render_template, request
import pandas as pd
import os

app = Flask(__name__)

CSV_FILE = "joueurs.csv"

def load_data():
    if not os.path.exists(CSV_FILE):
        return pd.DataFrame()
    try:
        return pd.read_csv(CSV_FILE)
    except Exception:
        return pd.DataFrame()

@app.route("/", methods=["GET"])
def index():
    df = load_data()

    # Recherche serveur par nom
    search = request.args.get("search", "").strip()
    if search and not df.empty:
        col_candidates = [c for c in df.columns if c.lower() == "nom" or c.lower() == "name"]
        if not col_candidates:
            # fallback: première colonne de type texte
            for c in df.columns:
                if df[c].dtype == object:
                    col_candidates = [c]
                    break
        if col_candidates:
            col = col_candidates[0]
            df = df[df[col].astype(str).str.contains(search, case=False, na=False)]

    data = df.to_dict(orient="records")
    columns = list(df.columns)

    return render_template("index.html", data=data, columns=columns, search=search)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
