import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
from sqlalchemy import create_engine, MetaData
import os
import requests

# Créer une chaîne de connexion SQLAlchemy
url = "http://job-fastapi:8888/located_jobs"
data = requests.get(url).json()
df = pd.DataFrame(data)

# Vérifier que les données sont chargées correctement
print(df.head())

# Agréger les données par pays
country_jobs = df.groupby('pays').size().reset_index(name='nombre_jobs')
country_coords = df.groupby('pays').agg({'latitude': 'mean', 'longitude': 'mean'}).reset_index()
country_data = pd.merge(country_jobs, country_coords, on='pays')
category_jobs = df.groupby('categorie')['id'].nunique().nlargest(5).reset_index().rename(columns={"id": "unique_count"})

# Créer l'application Dash
app = dash.Dash(__name__)

# Layout avec un menu déroulant
app.layout = html.Div([
    html.H1("Carte des Postes en Europe"),
    dcc.Dropdown(
        id='map-selection',
        options=[
            {'label': 'Carte agrégée par pays', 'value': 'country'},
            {'label': 'Carte des postes individuels', 'value': 'individual'},
            {'label': 'Graphes', 'value': 'graph'}
        ],
        value='country',  # Valeur par défaut
        clearable=False
    ),
    dcc.Graph(id='map-graph')  # Graph dynamique
])

# Callback pour mettre à jour la carte en fonction du choix de l'utilisateur
@app.callback(
    Output('map-graph', 'figure'),
    [Input('map-selection', 'value')]
)
def update_map(selected_map):
    if selected_map == 'country':
        # Carte agrégée par pays
        fig = px.scatter_mapbox(
            country_data,
            lat="latitude",
            lon="longitude",
            size="nombre_jobs",
            color="nombre_jobs",
            hover_name="pays",
            hover_data={"nombre_jobs": True, "latitude": False, "longitude": False},
            color_continuous_scale=px.colors.sequential.Oranges,
            zoom=3,
            height=600,
            size_max=50
        )
    elif selected_map == 'individual':
        # Carte des postes individuels
        fig = px.scatter_mapbox(
            df,
            lat="latitude",
            lon="longitude",
            hover_name="nom_poste",
            hover_data=["nom_entreprise", "ville", "pays"],
            color_discrete_sequence=["fuchsia"],
            zoom=3,
            height=600
        )
    else:
        # Graphique en camembert des catégories
        fig = px.pie(category_jobs, values='unique_count', names='categorie', title="Top 5 des Catégories d'Emploi")

    fig.update_layout(mapbox_style="open-street-map")
    return fig

# Lancer le serveur
if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=5000)
