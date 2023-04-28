# Parcial 2 BigData

En esta carpeta, verá un programa que descarga las páginas principales de https://www.eltiempo.com/ y https://www.publimetro.co/ y otro que realiza scraping de esas páginas y las guarda en un bucket de s3. Por otro lado, en AWS Glue se incluirán en 2 trabajos para trabajar simultáneamente y finalmente se utilizará un crawler que permita ver los resultados en AWS Athena.

Para usar localmente, debe configurar:

pip install -r requirements.txt
Para cargar las bibliotecas utilizadas en este elemento, debe generar un archivo .whl. Todo lo necesario se encuentra en: https://aws.amazon.com/premiumsupport/knowledge-center/glue-import-error-no-module-named/
