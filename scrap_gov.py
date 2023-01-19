import requests
import re
from bs4 import BeautifulSoup
from fastapi import APIRouter, FastAPI
from dotenv import dotenv_values
from pymongo import MongoClient

app = FastAPI()

config = dotenv_values(".env")

router = APIRouter()


@app.on_event("startup")
def stratup_db_client():
    app.mongo_client = MongoClient(config["MONGODB_URI"])
    app.database = app.mongo_client[config["DB_NAME"]]
    print("Connection to the mongodb database!")


@app.on_event("shutdown")
def shutdow_db_client():
    app.mongodb_client.close()


# Creating a pymongo client
client = MongoClient('localhost', 27017)

# Getting the database instance
db = client['streetAPI']

# Creating a collection
#italia_data = db['italia_data']
italia_data = db['ita_data_v1']
scraper_osm = db['scraper_osm']


def scraper_gov_dati():
    url_origin = "dait.interno.gov.it"

    cod_url = "https://dait.interno.gov.it/territorio-e-autonomie-locali/sut/elenco_codici_comuni.php"
    contact_url = "https://dait.interno.gov.it/territorio-e-autonomie-locali/sut/elenco_contatti_comuni_italiani.php"

    headers = {
        'Host': url_origin,
        'User-Agent': 'Mozilla/5.0'
    }

    cod_response = requests.get(cod_url, headers=headers)
    contact_response = requests.get(contact_url, headers=headers)

    cod_soup = BeautifulSoup(cod_response.text, 'html.parser')
    contact_soup = BeautifulSoup(contact_response.text, 'html.parser')

    # la size la so di default
    cod_size = len(cod_soup.find("thead").find_all("th"))
    contact_size = len(contact_soup.find("thead").find_all("th"))

    print('LUNGHEZZA TABELLA CODICE ' + cod_size.__str__())
    print('LUNGHEZZA TABELLA CONTATTI ' + contact_size.__str__())

    content = []

    cod_tbody = cod_soup.tbody
    contact_tbody = contact_soup.tbody

    # per entrambi le tabelle ci sono lo stesso numero di righe
    # hanno la stessa size, gioco con l'indice

    cod_trs = cod_tbody.find_all('tr')
    contact_trs = contact_tbody.find_all('tr')

    for idx, row in enumerate(cod_trs):
        # -- inizio tabella codici --

        cod_columns = row.find_all('td')

        num = cod_columns[0].text
        nome = cod_columns[1].text
        prov = cod_columns[2].text
        cod_elet = cod_columns[3].text
        cod_istat = cod_columns[4].text
        cod_amm = cod_columns[5].text

        cod = {
            "cod_ente": cod_elet,
            "cod_istat": cod_istat,
            "cod_catastale": cod_amm
        }
        # -- fine tabella codici --

        # -- inizio tabella contatti --

        contact_colums = contact_trs[idx].find_all('td')

        email = contact_colums[3].text
        pec = contact_colums[4].text
        tel = contact_colums[5].text
        fax = contact_colums[6].text

        contacts = {
            "email": email,
            "pec": pec,
            "tel": tel,
            "fax": fax
        }

        # -- fine tabella contatti --

        comune = {
            "id": num,
            "prov": prov,  # DATO CHE SPARISCE
            "nome": nome,
            "cod": cod,
            "contact": contacts
        }
        content.append(comune)

    italia = dict()

    for comune in content:
        # print(comune['prov'])
        if comune['prov'] not in italia:
            italia[comune['prov']] = []

        key = comune['prov']
        del comune['prov']
        italia[key].append(comune)

    # next((x for x in content if x.key == prov), None)
    print(len(italia.keys()))
    return italia


def scraper_wiki_ita():
    url = "https://it.wikipedia.org/wiki/Province_d'Italia"
    wiki_url = "https://it.wikipedia.org"

    wiki_response = requests.get(url)
    wiki_soup = BeautifulSoup(wiki_response.text, 'html.parser')

    wiki_table = wiki_soup.find('table', {"class": "wikitable sortable"})

    wiki_tbody = wiki_table.tbody
    # print(wiki_tbody)

    wiki_trs = wiki_tbody.find_all('tr')

    content = []
    italia = dict()

    for idx, row in enumerate(wiki_trs):
        td = row.find_all('td')
        if td:
            prov_name = td[0].find_all('a', href=True)[1].text.upper()
            prov_wiki = wiki_url + td[0].find_all('a', href=True)[1]['href']

            region_wiki = wiki_url + td[2].find_all('a', href=True)[1]['href']
            region_name = ''.join(td[2].text.split()).upper()

            info_province = {
                "population": ''.join(td[3].text.split()),
                "area_km": ''.join(td[4].text.split()),
                "density": ''.join(td[5].text.split()),
                # "year_establishment": ''.join(td[9].text.split()),
                "wiki": prov_wiki
                # totali comuni
                # densita
            }

            sigla = ''.join(td[1].text.split())

            if sigla == 'ROMA':
                sigla = 'RM'

            province = {
                "id": sigla,  # sigla
                "name": prov_name,
                "info": info_province
            }

            region = {
                "name": region_name,
                "wiki": region_wiki,
                "province": []
            }

            if region_name not in italia:
                italia[region_name] = region

            italia[region_name]["province"].append(province)

    return italia


def merge_data(gov_data, wiki_data):
    region_id = list(wiki_data.keys())

    italia_obj = []

    for key in region_id:
        print(wiki_data[key])
        for province in wiki_data[key]['province']:
            if province['id'] in gov_data:
                province['comuni'] = (gov_data[province['id']])
        italia_obj.append(wiki_data[key])

    return italia_obj


def insert_regione(italia):
    res = italia_data.insert_many(italia)
    print(res)


@app.get("/scrap_gov")
def index():
    gov_data = scraper_gov_dati()
    ita_data = scraping_cap(gov_data)  # con il CAP

    wiki_data = scraper_wiki_ita()

    italia_obj = merge_data(ita_data, wiki_data)

    insert_regione(italia_obj)

    return italia_obj


def scraping_cap(ita):
    it_key = list(ita.keys())

    with requests.session() as session:
        for po in it_key:
            [cap_by_comune_name(comune, po, session) for comune in ita[po]]

    return ita


def cap_by_comune_name(comune, key, session):
    cap_url = "https://www.paginebianche.it/cap?dv="

    cap_request = session.get(cap_url + comune['nome'])
    cap_soup = BeautifulSoup(cap_request.text, 'html.parser')  # row box-dis-cont__text

    cap_res = cap_soup.findAll("span", {"class": "text-bold result-cap mr-12 text-primary"})
    cap_name = cap_soup.findAll("span", {"class": "text-bold result-localita text-primary"})

    # lo so e' brutto!! ma e' tardi
    i = 0
    for cap_po in cap_name:
        prov = re.findall("[A-Z]{2}", cap_po.text)
        if prov[0] == key:
            comune['cap'] = ''.join(cap_res[i].text.split())
            print(comune['cap'])

        i += 1

# def n_pref_by_comune_name(comune, key, session):
#     return None
