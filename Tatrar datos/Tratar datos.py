import requests
import bs4 as bs
import boto3
import datetime
import io
import csv

def estructura_publimetro(html_publimetro):
  publimetro = bs.BeautifulSoup(html_publimetro)
  final = [["Categoria", "Titular", "Enlace"]]
  pubs = publimetro.find_all("article")
  for articulo in pubs:
    pub = articulo.find("a", string=True)
    final.append([None, pub.text, pub.get("href")])
  return final
  
def estructura_el_tiempo(html_tiempo):
  el_tiempo = bs.BeautifulSoup(html_tiempo)
  articulos = el_tiempo.find_all("article")
  final = [["Categoria", "Titular", "Enlace"]]
  for articulo in articulos:
    enlace = articulo.find('meta', attrs={'itemid': True})
    if enlace: enlace =  enlace.get('itemid')
    final.append([articulo.get("data-category"), articulo.get("data-name"), enlace])
  return final


def guarda(data, periodico):
    s3 = boto3.client('s3')
    temp_file = io.StringIO()
    csv_writer = csv.writer(temp_file)
    for row in data:
        csv_writer.writerow(row)
    bucket_name = 'bucket-datos-periodico'
    key =f"headlines/final/periodico={periodico}/year={fecha_hoy[-10:-6]}/month={fecha_hoy[-5:-3]}/day={fecha_hoy[-2:]}"
    s3.put_object(Bucket=bucket_name, Key=key, Body=temp_file.getvalue().encode('utf-8'))
    print(f"Guarde  {periodico}")
 
fecha_hoy = datetime.date.today().strftime('%Y-%m-%d')
name_tiempo =f"headlines/raw/eltiempo-{fecha_hoy}.html"
name_publimetro =f"headlines/raw/publimetro-{fecha_hoy}.html"
s3 = boto3.resource('s3')
bucket = s3.Bucket('bucket-parcial')
tiempo = bucket.Object(name_tiempo)
publimetro = bucket.Object(name_publimetro)
per_tiempo = tiempo.get()['Body'].read()
per_publimetro = publimetro.get()['Body'].read()
html_tiempo = per_tiempo.decode('utf-8')
html_publimetro = per_publimetro.decode('utf-8')   
datos_publimetro = estructura_publimetro(html_publimetro)
datos_tiempo = estructura_el_tiempo(html_tiempo)
guarda(datos_publimetro, "publimetro")
guarda(datos_tiempo, "eltiempo")
