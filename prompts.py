import streamlit as st
from sqlalchemy import create_engine
import pandas as pd

SCHEMA_PATH = st.secrets.get("SCHEMA_PATH", "dwh")
TABLE_DESCRIPTIONS = {
    "ecole_primaire_lyon": {
        "description": "Table contenant les informations des écoles primaires de Lyon",
        "primary_key":"uai",
        "foreign_keys": {},
        "columns": {
            "uai": "Identifiant unique de l'école (UAI)",
            "nom": "Nom de l'école primaire",
            "statut_public_prive": "Statut de l'école (public ou privé)",
            "adresse": "Adresse de l'école primaire",
            "code_postal": "Code postal de l'école primaire",
            "commune": "Commune / arrondissement où se trouve l'école primaire",
            "nombre_d_eleves": "Nombre d'élèves inscrits à l'école primaire",
            "etat": "État de l'établissement (OUVERT ou A FERMER)",
            "datemaj": "Date de la dernière mise à jour des informations",
            "lon": "Longitude de l'emplacement de l'école primaire",
            "lat": "Latitude de l'emplacement de l'école primaire"
        }
    },
    "dataset_marche_immobilier_lyon": {
        "description": "Table contenant les informations des transactions immobilières et plus généralement le marché immobilier de Lyon. C'est la table principal qui contient des informations exogènes reliées aux autre tables de la BDD_LYON",
        "primary_key":"id_mutation",
        "foreign_keys": {
            "id_ecole_primaire_a_moins_de_1000m": "ecole_primaire_lyon.uai",
            "id_velov_a_moins_de_200m": "transport_velov.idstation",
            "id_parc_a_moins_de_500m": "parcjardin_lyon.identifiant",   
            "id_interet_touristique_a_moins_de_500m": "interet_touristique_lyon.id",  
            "id_metro_a_moins_de_500m": "metrostation.id",
            "id_travaux_a_moins_de_1000m": "prevtravaux_lyon.numero", 
            "id_hopital_a_moins_de_1000m": "_structurehospitalier_lyon.id",
            "id_centre_commercial_a_moins_de_1000m": "ctrecommercial_lyon.nom",
            "id_producteur_local_a_moins_de_1000m": "producteurlocaux_lyon.identifiant",    
            "id_lycee_a_moins_de_1000m": "lycee_lyon.uai",  
            "id_college_a_moins_de_1000m": "college_lyon.uai",            
        },    
        "columns": {
            "id_mutation": "Identifiant unique de la mutation",
            "date_mutation": "Date de la mutation",
            "nature_mutation": "Nature de la mutation",
            "valeur_fonciere": "Valeur foncière de la mutation",
            "adresse_numero": "Numéro de l'adresse",
            "adresse_suffixe": "Suffixe de l'adresse",
            "adresse_nom_voie": "Nom de la voie de l'adresse",
            "adresse_code_voie": "Code de la voie de l'adresse",
            "code_postal": "Code postal de l'adresse",
            "code_commune": "Code de la commune",
            "nom_commune": "Nom de la commune",
            "code_departement": "Code du département",
            "id_parcelle": "Identifiant de la parcelle",
            "ancien_id_parcelle": "Ancien identifiant de la parcelle",
            "nombre_lots": "Nombre de lots",
            "code_type_local": "Code du type de local",
            "type_local": "Type de local",
            "surface_reelle_bati": "Surface réelle bâtie",
            "nombre_pieces_principales": "Nombre de pièces principales",
            "code_nature_culture": "Code de la nature de la culture",
            "nature_culture": "Nature de la culture",
            "code_nature_culture_speciale": "Code de la nature spéciale de la culture",
            "nature_culture_speciale": "Nature spéciale de la culture",
            "surface_terrain": "Surface du terrain",
            "longitude": "Longitude de l'emplacement",
            "latitude": "Latitude de l'emplacement",
            "annee": "Année de la mutation",
            "concat_field": "Champ concaténé pour les recherches",
            
            "velov_a_moins_de_200m_oui_1_non_0": "Fournit un indicateur binaire indiquant si une station de velov est à moins de 200m. Cela aide à déterminer rapidement la présence de station de velov à proximité.",
            "id_velov_a_moins_de_200m": "Contient une liste d'identifiants de stations de velov situés à moins de 200m, séparés par des points-virgules. Cela permet de lister toutes les stations de velov proches. Nécessite une conversion en bigint pour les opérations SQL.",
            "nombre_de_velov_a_moins_de_200m": "Indique le nombre total de stations de velov situés à moins de 200m. Cette colonne est utilisée pour quantifier les stations de velov à proximité d'un emplacement donné. Doit être filtré pour exclure les valeurs nulles.",
            "velov_le_plus_proche_en_metres": "Spécifie la distance en mètres de la station de velov la plus proche. Cela permet de savoir quelle station de velov est la plus rapidement accessible et proche depuis l'emplacement de la mutation.",
            "id_velov_le_plus_proche_en_metres": "Donne l'identifiant de la station de velov la plus proche en mètres. Cette colonne est utilisée pour obtenir des informations détaillées sur la station de velov la plus proche.",
            
            "parc_a_moins_de_500m_oui_1_non_0": "Fournit un indicateur binaire indiquant si un parc est à moins de 500m. Cela aide à déterminer rapidement la présence de parc à proximité.",
            "id_parc_a_moins_de_500m": "Contient une liste d'identifiants de parcs situés à moins de 500m, séparés par des points-virgules. Cela permet de lister tous les parcs proches. Nécessite une conversion en bigint pour les opérations SQL.",
            "nombre_de_parc_a_moins_de_500m": "Indique le nombre total de parcs situés à moins de 500m. Cette colonne est utilisée pour quantifier les parcs à proximité d'un emplacement donné. Doit être filtré pour exclure les valeurs nulles.",
            "parc_le_plus_proche_en_metres": "Spécifie la distance en mètres du parc le plus proche. Cela permet de savoir quel parc est le plus rapidement accessible et proche depuis l'emplacement de la mutation.",
            "id_parc_le_plus_proche_en_metres": "Donne l'identifiant du parc le plus proche en mètres. Cette colonne est utilisée pour obtenir des informations détaillées sur le parc le plus proche.",
            
            "interet_touristique_a_moins_de_500m_oui_1_non_0": "Fournit un indicateur binaire indiquant si un intérêt touristique est à moins de 500m. Cela aide à déterminer rapidement la présence d'intérêt touristiques à proximité.",
            "id_interet_touristique_a_moins_de_500m": "Contient une liste d'identifiants d'intérêts touristiques situés à moins de 500m, séparés par des points-virgules. Cela permet de lister toutes les intérêts touristiques proches. Nécessite une conversion en bigint pour les opérations SQL.",
            "nombre_de_interet_touristique_a_moins_de_500m": "Indique le nombre total d'intérêts touristiques situés à moins de 500m. Cette colonne est utilisée pour quantifier les intérêts touristiques à proximité d'un emplacement donné. Doit être filtré pour exclure les valeurs nulles.",
            "interet_touristique_le_plus_proche_en_metres": "Spécifie la distance en mètres de l'intérêt touristique le plus proche. Cela permet de savoir quel intérêt touristique est le plus rapidement accessible et proche depuis l'emplacement de la mutation.",
            "id_interet_touristique_le_plus_proche_en_metres": "Donne l'identifiant de l'intérêt touristique le plus proche en mètres. Cette colonne est utilisée pour obtenir des informations détaillées sur l'intérêt touristique le plus proche.",
            
            "metro_a_moins_de_500m_oui_1_non_0": "Fournit un indicateur binaire indiquant si une station de métro est à moins de 500m. Cela aide à déterminer rapidement la présence de station de métro à proximité.",
            "id_metro_a_moins_de_500m": "Contient une liste d'identifiants de stations de métro situés à moins de 500m, séparés par des points-virgules. Cela permet de lister toutes les stations de métro proches. Nécessite une conversion en bigint pour les opérations SQL.",
            "nombre_de_metro_a_moins_de_500m": "Indique le nombre total de stations de métro situés à moins de 500m. Cette colonne est utilisée pour quantifier les stations de métro à proximité d'un emplacement donné. Doit être filtré pour exclure les valeurs nulles.",
            "metro_le_plus_proche_en_metres": "Spécifie la distance en mètres de la station de métro la plus proche. Cela permet de savoir quelle station de métro est la plus rapidement accessible et proche depuis l'emplacement de la mutation.",
            "id_metro_le_plus_proche_en_metres": "Donne l'identifiant de la station de métro la plus proche en mètres. Cette colonne est utilisée pour obtenir des informations détaillées sur la station de métro la plus proche.",
            
            "travaux_a_moins_de_1000m_oui_1_non_0": "Fournit un indicateur binaire indiquant si un chantiers de travaux est à moins de 1000m. Cela aide à déterminer rapidement la présence de chantiers de travaux à proximité.",
            "id_travaux_a_moins_de_1000m": "Contient une liste d'identifiants de chantiers de travaux situés à moins de 1000m, séparés par des points-virgules. Cela permet de lister tous les chantiers de travaux proches. Nécessite une conversion en bigint pour les opérations SQL.",
            "nombre_de_travaux_a_moins_de_1000m": "Indique le nombre total de chantiers de travaux situés à moins de 1000m. Cette colonne est utilisée pour quantifier les chantiers de travaux à proximité d'un emplacement donné. Doit être filtré pour exclure les valeurs nulles.",
            "travaux_le_plus_proche_en_metres": "Spécifie la distance en mètres de chantiers de travaux le plus proche. Cela permet de savoir quel chantier de travaux est le plus rapidement accessible et proche depuis l'emplacement de la mutation.",
            "id_travaux_le_plus_proche_en_metres": "Donne l'identifiant du chantiers de travaux le plus proche en mètres. Cette colonne est utilisée pour obtenir des informations détaillées sur le chantiers de travaux le plus proche.",
            
            "concat_field.1": "Champ concaténé pour les recherches supplémentaires",
            
            "hopital_a_moins_de_1000m_oui_1_non_0": "Fournit un indicateur binaire indiquant si un hôpital est à moins de 1000m. Cela aide à déterminer rapidement la présence d'hôpital à proximité.",
            "id_hopital_a_moins_de_1000m": "Contient une liste d'identifiants d'hôpitaux situés à moins de 1000m, séparés par des points-virgules. Cela permet de lister tous les hôpitaux proches. Nécessite une conversion en bigint pour les opérations SQL.",
            "nombre_de_hopitaux_a_moins_de_1000m": "Indique le nombre total d'hôpitaux situés à moins de 1000m. Cette colonne est utilisée pour quantifier les hôpitaux à proximité d'un emplacement donné. Doit être filtré pour exclure les valeurs nulles.",
            "hopital_le_plus_proche_en_metres": "Spécifie la distance en mètres d'hôpital le plus proche. Cela permet de savoir quel hôpital est le plus rapidement accessible et proche depuis l'emplacement de la mutation.",
            "id_hopital_le_plus_proche_en_metres": "Donne l'identifiant de l'hôpital le plus proche en mètres. Cette colonne est utilisée pour obtenir des informations détaillées sur l'hôpital le plus proche.",
            
            "centre_commercial_a_moins_de_1000m_oui_1_non_0": "Fournit un indicateur binaire indiquant si un centre commercial est à moins de 1000m. Cela aide à déterminer rapidement la présence de centre commerciaux à proximité.",
            "id_centre_commercial_a_moins_de_1000m": "Contient une liste d'identifiants de centres commerciaux situés à moins de 1000m, séparés par des points-virgules. Cela permet de lister tous les centres commerciaux proches. Nécessite une conversion en bigint pour les opérations SQL.",
            "nombre_de_centre_commerciaux_a_moins_de_1000m": "Indique le nombre total de centres commerciaux situés à moins de 1000m. Cette colonne est utilisée pour quantifier les centres commerciaux à proximité d'un emplacement donné. Doit être filtré pour exclure les valeurs nulles.",
            "centre_commercial_le_plus_proche_en_metres": "Spécifie la distance en mètres de centre commercial le plus proche. Cela permet de savoir quel centre commercial est le plus rapidement accessible et proche depuis l'emplacement de la mutation.",
            "id_centre_commercial_le_plus_proche_en_metres": "Donne l'identifiant du centre commercial le plus proche en mètres. Cette colonne est utilisée pour obtenir des informations détaillées sur le centre commercial le plus proche.",
            
            "producteur_local_a_moins_de_1000m_oui_1_non_0": "Fournit un indicateur binaire indiquant si un producteur local est à moins de 1000m. Cela aide à déterminer rapidement la présence de producteur local à proximité.",
            "id_producteur_local_a_moins_de_1000m": "Contient une liste d'identifiants de producteurs locaux situés à moins de 1000m, séparés par des points-virgules. Cela permet de lister tous les producteurs locaux proches. Nécessite une conversion en bigint pour les opérations SQL.",
            "nombre_de_producteur_locaux_a_moins_de_1000m": "Indique le nombre total de producteurs locaux situés à moins de 1000m. Cette colonne est utilisée pour quantifier les producteurs locaux à proximité d'un emplacement donné. Doit être filtré pour exclure les valeurs nulles.",
            "producteur_local_le_plus_proche_en_metres": "Spécifie la distance en mètres du producteur local le plus proche. Cela permet de savoir quel producteur local est le plus rapidement accessible et proche depuis l'emplacement de la mutation.",
            "id_producteur_local_le_plus_proche_en_metres": "Donne l'identifiant du producteur local le plus proche en mètres. Cette colonne est utilisée pour obtenir des informations détaillées sur le producteur local le plus proche.",
            
            "lycee_a_moins_de_1000m_oui_1_non_0": "Fournit un indicateur binaire indiquant si un lycée est à moins de 1000m. Cela aide à déterminer rapidement la présence de lycée à proximité.",
            "id_lycee_a_moins_de_1000m": "Contient une liste d'identifiants de lycées situés à moins de 1000m, séparés par des points-virgules. Cela permet de lister tous les lycées proches. Nécessite une conversion en bigint pour les opérations SQL.",
            "nombre_de_lycee_a_moins_de_1000m": "Indique le nombre total de lycées situés à moins de 1000m. Cette colonne est utilisée pour quantifier les lycées à proximité d'un emplacement donné. Doit être filtré pour exclure les valeurs nulles.",
            "lycee_le_plus_proche_en_metres": "Spécifie la distance en mètres du lycée le plus proche. Cela permet de savoir quel lycée est le plus rapidement accessible et proche depuis l'emplacement de la mutation.",
            "id_lycee_le_plus_proche_en_metres": "Donne l'identifiant du lycée le plus proche en mètres. Cette colonne est utilisée pour obtenir des informations détaillées sur le lycée le plus proche.",
            
            "college_a_moins_de_1000m_oui_1_non_0": "Fournit un indicateur binaire indiquant si un collège est à moins de 1000m. Cela aide à déterminer rapidement la présence de collège à proximité.",
            "id_college_a_moins_de_1000m": "Contient une liste d'identifiants de collèges situés à moins de 1000m, séparés par des points-virgules. Cela permet de lister tous les collèges proches. Nécessite une conversion en bigint pour les opérations SQL.",
            "nombre_de_college_a_moins_de_1000m": "Indique le nombre total de collèges situés à moins de 1000m. Cette colonne est utilisée pour quantifier les collèges à proximité d'un emplacement donné. Doit être filtré pour exclure les valeurs nulles.",
            "college_le_plus_proche_en_metres": "Spécifie la distance en mètres du collège le plus proche. Cela permet de savoir quel collège est le plus rapidement accessible et proche depuis l'emplacement de la mutation.",
            "id_college_le_plus_proche_en_metres": "Donne l'identifiant du collège le plus proche en mètres. Cette colonne est utilisée pour obtenir des informations détaillées sur le collège le plus proche.",
            
            "ecole_primaire_a_moins_de_1000m_oui_1_non_0": "Fournit un indicateur binaire indiquant si une école est à moins de 1000m. Cela aide à déterminer rapidement la présence de l'école à proximité.",
            "id_ecole_primaire_a_moins_de_1000m": "Contient une liste d'identifiants d'écoles situés à moins de 1000m, séparés par des points-virgules. Cela permet de lister toutes les écoles proches. Nécessite une conversion en bigint pour les opérations SQL.",
            "nombre_de_ecole_primaire_a_moins_de_1000m": "Indique le nombre total d'écoles situés à moins de 1000m. Cette colonne est utilisée pour quantifier les écoles à proximité d'un emplacement donné. Doit être filtré pour exclure les valeurs nulles.",
            "ecole_primaire_le_plus_proche_en_metres": "Spécifie la distance en mètres de l'école la plus proche. Cela permet de savoir quelle école est la plus rapidement accessible et proche depuis l'emplacement de la mutation.",
            "id_ecole_primaire_le_plus_proche_en_metres": "Donne l'identifiant de l'école la plus proche en mètres. Cette colonne est utilisée pour obtenir des informations détaillées sur l'école la plus proche.",
            
            "valeur_fonciere_2023": "Valeur foncière de l'année 2023",
            "valeur_fonciere_2019": "Valeur foncière de l'année 2019",
            "valeur_fonciere_2020": "Valeur foncière de l'année 2020",
            "valeur_fonciere_2021": "Valeur foncière de l'année 2021",
            "valeur_fonciere_2022": "Valeur foncière de l'année 2022"
        }
    },
    "metrostation": {
        "description": "Table contenant les informations des stations de métro de Lyon",
        "primary_key":"id",
        "foreign_keys": {},
        "columns": {
            "id": "Identifiant unique de la station de métro",
            "id_station": "Identifiant de la station",
            "nom": "Nom de la station de métro",
            "longitude": "Longitude de l'emplacement de la station de métro",
            "latitude": "Latitude de l'emplacement de la station de métro"
        }
    },
    "ctrecommercial_lyon": {
        "description": "Table contenant les informations des centres commerciaux de Lyon",
        "primary_key":"nom",
        "foreign_keys": {},
        "columns": {
            "nom": "Nom du centre commercial",
            "commune": "Commune où se trouve le centre commercial",
            "adresse": "Adresse du centre commercial",
            "latitude": "Latitude de l'emplacement du centre commercial",
            "longitude": "Longitude de l'emplacement du centre commercial"
        }
    },
    "information_schema": {
        "description": "Table contenant les descriptions et informations de toutes les colonnes de chaque table de la base de donnée BDD_LYON. Quand on pose une question sur les informations des tables, c'est la table de référence",
        "columns": {
            "table_name": "Nom de la table",
            "column_name": "Nom de la colonne",
            "description": "information sur les colonnes"
        }
    },
    "_structurehospitalier_lyon": {
        "description": "Table contenant les informations des structures hospitalières de Lyon",
        "primary_key":"id",
        "foreign_keys": {},
        "columns": {
            "id": "Identifiant unique de la structure hospitalière",
            "nom": "Nom de la structure hospitalière",
            "theme": "Thème de la structure hospitalière",
            "soustheme": "Sous-thème de la structure hospitalière",
            "identifiant": "Identifiant de la structure hospitalière",
            "datecreation": "Date de création de la structure hospitalière",
            "longitude": "Longitude de l'emplacement de la structure hospitalière",
            "latitude": "Latitude de l'emplacement de la structure hospitalière"
        }
    },
    "college_lyon": {
        "description": "Table contenant les informations des collèges de Lyon",
        "primary_key":"uai",
        "foreign_keys": {},
        "columns": {
            "uai": "Identifiant unique de l'établissement (UAI)",
            "nom": "Nom du collège",
            "statut_public_prive": "Statut de l'établissement (public ou privé)",
            "adresse": "Adresse du collège",
            "code_postal": "Code postal du collège",
            "commune": "Commune / arrondissement où se trouve le collège",
            "nombre_d_eleves": "Nombre d'élèves inscrits au collège",
            "etat": "État de l'établissement (OUVERT ou A FERMER)",
            "datemaj": "Date de la dernière mise à jour des informations",
            "lon": "Longitude de l'emplacement du collège",
            "lat": "Latitude de l'emplacement du collège"
        }
    },
    "interet_touristique_lyon": {
        "description": "Table contenant les informations des intérêts touristiques de Lyon",
        "primary_key":"id",
        "foreign_keys": {},
        "columns": {
            "id": "Identifiant unique de l'intérêt touristique",
            "idsitra": "Identifiant SITRA de l'intérêt touristique",
            "nom": "Nom de l'intérêt touristique",
            "theme": "Thème de l'intérêt touristique. Permet de voir si l'intérêt touristique est un restaurant, un hébergements, etc",
            "type": "Type de l'intérêt touristique. Permet de voir si l'intérêt touristique est de la RESTAURATION, HOTELLERIE, etc",
            "address": "Adresse de l'intérêt touristique",
            "datemaj": "Date de la dernière mise à jour des informations",
            "lon": "Longitude de l'emplacement de l'intérêt touristique",
            "lat": "Latitude de l'emplacement de l'intérêt touristique"
        }
    },
    "lycee_lyon": {
        "description": "Table contenant les informations des lycées de Lyon",
        "primary_key":"uai",
        "foreign_keys": {},
        "columns": {
            "uai": "Identifiant unique de l'établissement (UAI)",
            "nom": "Nom du lycée",
            "statut_public_prive": "Statut de l'établissement (public ou privé)",
            "adresse": "Adresse du lycée",
            "code_postal": "Code postal du lycée",
            "commune": "Commune / arrondissement où se trouve le lycée",
            "nombre_d_eleves": "Nombre d'élèves inscrits au lycée",
            "etat": "État de l'établissement (OUVERT ou A FERMER)",
            "datemaj": "Date de la dernière mise à jour des informations",
            "lon": "Longitude de l'emplacement du lycée",
            "lat": "Latitude de l'emplacement du lycée"
        }
    },
    "parcjardin_lyon": {
        "description": "Table contenant les informations des parcs et jardins de Lyon",
        "primary_key":"identifiant",
        "foreign_keys": {},
        "columns": {
            "identifiant": "Identifiant unique du parc ou jardin",
            "nom": "Nom du parc ou jardin",
            "adresse": "Adresse du parc ou jardin",
            "commune": "Commune où se trouve le parc ou jardin",
            "ann_ouvert": "Année d'ouverture du parc ou jardin",
            "clos": "Indique si le parc ou jardin est clos",
            "type_equip": "Type d'équipement disponible dans le parc ou jardin",
            "lon": "Longitude de l'emplacement du parc ou jardin",
            "lat": "Latitude de l'emplacement du parc ou jardin"
        }
    },
    "prevtravaux_lyon": {
        "description": "Table contenant les informations des prévisions de travaux à Lyon",
        "primary_key":"numero",
        "foreign_keys": {},
        "columns": {
            "numero": "Numéro unique du chantier de travaux",
            "intervenant": "Intervenant responsable des travaux",
            "nature_chantier": "Nature du chantier de travaux",
            "etat": "État actuel du chantier de travaux",
            "localisation": "Localisation du chantier de travaux",
            "date_debut": "Date de début des travaux",
            "date_fin": "Date de fin prévue des travaux",
            "rue_concernee": "Rue concernée par les travaux",
            "latitude": "Latitude de l'emplacement du chantier de travaux",
            "longitude": "Longitude de l'emplacement du chantier de travaux"
        }
    },
    "producteurlocaux_lyon": {
        "description": "Table contenant les informations des producteurs locaux de Lyon",
        "primary_key":"identifiant",
        "foreign_keys": {},
        "columns": {
            "identifiant": "Identifiant unique du producteur local",
            "nom": "Nom du producteur local",
            "adresse": "Adresse du producteur local",
            "commune": "Commune où se trouve le producteur local",
            "type": "Type de produit ou de service fourni par le producteur local",
            "lon": "Longitude de l'emplacement du producteur local",
            "lat": "Latitude de l'emplacement du producteur local"
        }
    },
    "transport_velov": {
        "description": "Table contenant les informations des stations Vélo'v de Lyon",
        "primary_key":"idstation",
        "foreign_keys": {},
        "columns": {
            "idstation": "Identifiant unique de la station Vélo'v",
            "nom": "Nom de la station Vélo'v",
            "adresse1": "Première ligne d'adresse de la station Vélo'v",
            "adresse2": "Deuxième ligne d'adresse de la station Vélo'v",
            "commune": "Commune où se trouve la station Vélo'v",
            "numdansarrondisse": "Numéro dans l'arrondissement",
            "nbbornettes": "Nombre de bornettes disponibles à la station",
            "ouverte": "Indique si la station est ouverte",
            "lon": "Longitude de l'emplacement de la station Vélo'v",
            "lat": "Latitude de l'emplacement de la station Vélo'v"
        }
    }
}


GEN_SQL = """
YOU MUST AND SHOULD EXCLUSIVELY ANSWER IN FRENCH, you are not allowed to speak or answer in English.
You will be acting as an IA Expert on the Lyon real estate market named 🏠 LyonImmoBot.
Your goal is to give correct, executable SQL queries to users.
You will be replying to users who will be confused if you don't respond in the character of 🏠 LyonImmoBot.
You have access to SEVERAL TABLES containing information about various aspects of the city of Lyon, including education, transport, health, parks, and more.
The user will ask questions, for each question you should respond and include a SQL query based on the question and the table.

Here are the tables you can refer to and their descriptions:
- **dataset_marche_immobilier_lyon** : Table containing information on real estate transactions and the Lyon real estate market in general. This is the main table containing exogenous information linked to the other tables in BDD_LYON.
- **ecole_primaire_lyon**: Table containing information on primary school in Lyon.
- **metrostations**: Table containing information on Lyon metro stations.
- **ctrecommercial_lyon**: Table containing information about shopping centers in Lyon.
- **_structurehospitalier_lyon**: Table containing information about Lyon hospitals.
- **college_lyon**: Table containing information on Lyon secondary schools.
- **interet_touristique_lyon**: Table containing information about Lyon's tourist attractions.
- **lycee_lyon**: Table containing information on Lyon high schools.
- **parcjardin_lyon**: Table containing information about Lyon's parks and gardens.
- **prevtravaux_lyon**: Table containing information about work forecasts in Lyon.
- **producteurlocaux_lyon**: Table containing information about local producers in Lyon.
- **transport_velov**: Table containing information about Lyon's Vélo'v stations.

For each table, here are the columns available:
- **dataset_marche_immobilier_lyon**: id_mutation, date_mutation, nature_mutation, valeur_fonciere, adresse_numero, adresse_suffixe, adresse_nom_voie, adresse_code_voie, code_postal, code_commune, nom_commune, code_departement, id_parcelle, ancien_id_parcelle, nombre_lots, code_type_local, type_local, surface_reelle_bati, nombre_pieces_principales, code_nature_culture, nature_culture, code_nature_culture_speciale, nature_culture_speciale, surface_terrain, longitude, latitude, annee, concat_field, velov_a_moins_de_200m_oui_1_non_0, id_velov_a_moins_de_200m, nombre_de_velov_a_moins_de_200m, velov_le_plus_proche_en_metres, id_velov_le_plus_proche_en_metres, parc_a_moins_de_500m_oui_1_non_0, id_parc_a_moins_de_500m, nombre_de_parc_a_moins_de_500m, parc_le_plus_proche_en_metres, id_parc_le_plus_proche_en_metres, interet_touristique_a_moins_de_500m_oui_1_non_0, id_interet_touristique_a_moins_de_500m, nombre_de_interet_touristique_a_moins_de_500m, interet_touristique_le_plus_proche_en_metres, id_interet_touristique_le_plus_proche_en_metres, metro_a_moins_de_500m_oui_1_non_0, id_metro_a_moins_de_500m, nombre_de_metro_a_moins_de_500m, metro_le_plus_proche_en_metres, id_metro_le_plus_proche_en_metres, travaux_a_moins_de_1000m_oui_1_non_0, id_travaux_a_moins_de_1000m, nombre_de_travaux_a_moins_de_1000m, travaux_le_plus_proche_en_metres, id_travaux_le_plus_proche_en_metres, concat_field.1, hopital_a_moins_de_1000m_oui_1_non_0, id_hopital_a_moins_de_1000m, nombre_de_hopitaux_a_moins_de_1000m, hopital_le_plus_proche_en_metres, id_hopital_le_plus_proche_en_metres, centre_commercial_a_moins_de_1000m_oui_1_non_0, id_centre_commercial_a_moins_de_1000m, nombre_de_centre_commerciaux_a_moins_de_1000m, centre_commercial_le_plus_proche_en_metres, id_centre_commercial_le_plus_proche_en_metres, producteur_local_a_moins_de_1000m_oui_1_non_0, id_producteur_local_a_moins_de_1000m, nombre_de_producteur_locaux_a_moins_de_1000m, producteur_local_le_plus_proche_en_metres, id_producteur_local_le_plus_proche_en_metres, lycee_a_moins_de_1000m_oui_1_non_0, id_lycee_a_moins_de_1000m, nombre_de_lycee_a_moins_de_1000m, lycee_le_plus_proche_en_metres, id_lycee_le_plus_proche_en_metres, college_a_moins_de_1000m_oui_1_non_0, id_college_a_moins_de_1000m, nombre_de_college_a_moins_de_1000m, college_le_plus_proche_en_metres, id_college_le_plus_proche_en_metres, ecole_primaire_a_moins_de_1000m_oui_1_non_0, id_ecole_primaire_a_moins_de_1000m, nombre_de_ecole_primaire_a_moins_de_1000m, ecole_primaire_le_plus_proche_en_metres, id_ecole_primaire_le_plus_proche_en_metres, valeur_fonciere_2023, valeur_fonciere_2019, valeur_fonciere_2020, valeur_fonciere_2021, valeur_fonciere_2022
- **ecole_primaire_lyon**: uai, nom, statut_public_prive, adresse, code_postal, commune, nombre_d_eleves, etat, datemaj, lon, lat
- **metrostation**: id, id_station, nom, longitude, latitude
- **ctrecommercial_lyon**: nom, commune, adresse, latitude, longitude
- **_structurehospitalier_lyon**: id, nom, theme, soustheme, identifiant, datecreation, longitude, latitude
- **college_lyon**: uai, nom, statut_public_prive, adresse, code_postal, commune, nombre_d_eleves, etat, datemaj, lon, lat
- **interet_touristique_lyon**: id, idsitra, nom, theme, type, address, datemaj, lon, lat
- **lycee_lyon**: uai, nom, statut_public_prive, adresse, code_postal, commune, nombre_d_eleves, etat, datemaj, lon, lat
- **parcjardin_lyon**: identifiant, nom, adresse, commune, ann_ouvert, clos, type_equip, lon, lat
- **prevtravaux_lyon**: numero, intervenant, nature_chantier, etat, localisation, date_debut, date_fin, rue_concernee, latitude, longitude
- **producteurlocaux_lyon**: identifiant, nom, adresse, commune, type, lon, lat
- **transport_velov**: idstation, nom, adresse1, adresse2, commune, numdansarrondisse, nbbornettes, ouverte, lon, lat


{context}

Here are 9 critical rules for the interaction you must abide:
<rules>
1. You MUST MUST wrap the generated sql code within ``` sql code markdown in this format e.g
```sql
(select 1) union (select 2)
```
2. If I don't tell you to find a limited set of results in the sql query or question, you MUST limit the number of responses to 100. The syntax is "SELECT TOP 100"
3. Text / string where clauses must be fuzzy match e.g like %keyword%
4. Make sure to generate a single sql server code, not multiple. 
5. You should only use the table columns given in <columns>, and the table given in <tableName>, you MUST NOT hallucinate about the table names
6. DO NOT put numerical at the very front of sql variable.
7. When you are asked a question relating to a place or arrondissement, such as: which secondary schools are in the 5th arrondissement, you must ALWAYS use the postcode in your SQL query to ensure that the information is reliable.
8. When you need to perform joins between the main dataset and other tables. You must ALWAYS base your joins on primary/foreign key define on TABLE_DESCRIPTIONS.
9. You MUST ALWAYS use the dwh. on each FROM.

example: idstation with id_metro_a_moins_de_500m or id_metro_le_plus_proche_en_metres if you are only asked for the nearest id.

</rules>

Don't forget to use "like %keyword%" for fuzzy match queries (especially for variable_name column)
and wrap the generated sql code with ``` sql code markdown in this format e.g:
```sql
(select 1) union (select 2)
```

For each question from the user, make sure to include a query in your response.

Now to get started, please briefly introduce yourself, describe the further table you know at a high level, and share the available metrics in 2-3 sentences.
Then provide 3 example questions using bullet points.
"""


@st.cache_data(show_spinner="Loading 🏠 LyonImmoBot's context...")
def get_table_context(table_name: str, table_description: str):
    conn_str = (
        f"mssql+pyodbc://{st.secrets['connections']['sqlserver']['user']}:"
        f"{st.secrets['connections']['sqlserver']['password']}@"
        f"{st.secrets['connections']['sqlserver']['host']}:1433/"
        f"{st.secrets['connections']['sqlserver']['database']}?"
        "driver=ODBC+Driver+17+for+SQL+Server"
    )
    engine = create_engine(conn_str)
    with engine.connect() as connection:
        columns_query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{SCHEMA_PATH}' AND table_name = '{table_name}'
        """
        columns = pd.read_sql_query(columns_query, connection)
    columns_str = "\n".join(
        [
            f"- {columns['column_name'][i]}: {columns['data_type'][i]}"
            for i in range(len(columns["column_name"]))
        ]
    )
    context = f"""
Here is the table {SCHEMA_PATH}.{table_name}

<tableDescription>{table_description}</tableDescription>

Here are the columns of the table {SCHEMA_PATH}.{table_name}

<columns>\n\n{columns_str}\n\n</columns>
    """
    return context

def generate_query(table_name, column_name, conditions=None):
    try:
        # Liste des colonnes nécessitant une exclusion des valeurs nulles
        exclude_null_columns = [
            "nombre_de_interet_touristique_a_moins_de_500m",
            "nombre_de_parc_a_moins_de_500m",
            "nombre_de_velov_a_moins_de_200m",
            "nombre_de_metro_a_moins_de_500m",
            "nombre_de_travaux_a_moins_de_1000m",
            "nombre_de_hopitaux_a_moins_de_1000m",
            "nombre_de_centre_commerciaux_a_moins_de_1000m",
            "nombre_de_producteur_locaux_a_moins_de_1000m",
            "nombre_de_lycee_a_moins_de_1000m",
            "nombre_de_college_a_moins_de_1000m",
            "nombre_de_ecole_primaire_a_moins_de_1000m"
        ]
        
        # Liste des colonnes nécessitant une conversion de type
        convert_to_bigint_columns = [
            "id_interet_touristique_a_moins_de_500m",
            "id_parc_a_moins_de_500m",
            "id_velov_a_moins_de_200m",
            "id_metro_a_moins_de_500m",
            "id_travaux_a_moins_de_1000m",
            "id_hopital_a_moins_de_1000m",
            "id_centre_commercial_a_moins_de_1000m",
            "id_producteur_local_a_moins_de_1000m",
            "id_lycee_a_moins_de_1000m",
            "id_college_a_moins_de_1000m",
            "id_ecole_primaire_a_moins_de_1000m" 
        ]

        # Base de la requête
        base_query = f"SELECT {column_name} FROM {SCHEMA_PATH}.{table_name}"
        
        # Ajout des conditions s'il y en a
        if conditions:
            base_query += f" WHERE {conditions}"
        
        # Condition pour exclure les valeurs nulles
        if column_name in exclude_null_columns:
            if "WHERE" in base_query:
                base_query += f" AND {column_name} IS NOT NULL"
            else:
                base_query += f" WHERE {column_name} IS NOT NULL"
        
        # Condition pour gérer les conversions de type
        if column_name in convert_to_bigint_columns:
            # Spécifique pour les colonnes nécessitant une conversion en bigint
            base_query = f"""
            SELECT *
            FROM dwh.{table_name}
            WHERE id IN (
                SELECT CAST(regexp_split_to_table({column_name}, ';') AS bigint)
                FROM {SCHEMA_PATH}.{table_name}
                WHERE {conditions} AND {column_name} IS NOT NULL
            )
            """
        
        return base_query

    except Exception as e:
        # Gestion de l'erreur et renvoi d'un message convivial
        return "Désolé, une erreur est survenue, veuillez ré-écrire la question. Si l’erreur persiste, reformulez avec plus de contexte."

# Exemple d'utilisation
query = generate_query('dataset_marche_immobilier_lyon', 'id_velov_a_moins_de_200m', "id_mutation = '2020-958162' AND nombre_de_velov_a_moins_de_200m = 5")
print(query)





def execute_query(query: str):
    try:
        conn_str = (
            f"mssql+pyodbc://{st.secrets['connections']['sqlserver']['user']}:"
            f"{st.secrets['connections']['sqlserver']['password']}@"
            f"{st.secrets['connections']['sqlserver']['host']}:1433/"
            f"{st.secrets['connections']['sqlserver']['database']}?"
            "driver=ODBC+Driver+17+for+SQL+Server"
        )
        engine = create_engine(conn_str)
        with engine.connect() as connection:
            result = pd.read_sql_query(query, connection)
        return result
    except Exception as e:
        # Afficher l'erreur pour le débogage
        print(f"Erreur lors de l'exécution de la requête : {e}")
        # Renvoie un message convivial
        return "Désolé, une erreur est survenue, veuillez ré-écrire la question. Si l’erreur persiste, reformulez avec plus de contexte."

# Exemple d'utilisation
query = generate_query('dataset_marche_immobilier_lyon', 'id_velov_a_moins_de_200m', "id_mutation = '2020-958162' AND nombre_de_velov_a_moins_de_200m = 5")
result = execute_query(query)
if isinstance(result, str):
    st.error(result)
else:
    st.dataframe(result)

# Example usage of the execute_query function
def get_schools_in_lyon(arrondissement: str):
    query = f"""
    SELECT TOP 10 nom, adresse, code_postal, commune
    FROM {SCHEMA_PATH}.ecole_primaire_lyon
    WHERE code_postal LIKE '%{arrondissement}%' OR commune LIKE '%{arrondissement}%'
    """
    result = execute_query(query)
    if result.empty:
        return f"No schools found in the specified arrondissement {arrondissement}."
    else:
        schools = ", ".join(result['nom'])
        return f"The primary schools in {arrondissement} are: {schools}."

def get_system_prompt():
    table_contexts = []
    for table_name, table_description in TABLE_DESCRIPTIONS.items():
        table_context = get_table_context(table_name, table_description)
        table_contexts.append(table_context)
    return GEN_SQL.format(context="\n\n".join(table_contexts))

# Streamlit App
st.title("LyonImmoBot")
st.write("""
    🏠 LyonImmoBot at your service! I am an expert in SQL for real estate data in Lyon. I have access to several tables containing essential information about the city, such as primary schools, metro stations, shopping centers, hospitals, parks and gardens, planned construction works, local producers, and Vélov bike stations. Each table has specific columns providing important details like names, addresses, geographical coordinates, and statuses of different establishments in Lyon.
""")

# Example user query
user_query = st.text_input("Ask me a question about Lyon's real estate data:", "")

if user_query:
    response = ""
    if "schools" in user_query.lower():
        arrondissement = user_query.split()[-1]
        response = get_schools_in_lyon(arrondissement)
    else:
        response = "Sorry, I can only answer questions about schools for now. More functionalities coming soon!"
    st.write(response)