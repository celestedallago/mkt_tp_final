# Trabajo Práctico Final — Introducción al Marketing Online y los Negocios Digitales
El objetivo es diseñar e implementar un mini-ecosistema de datos comercial (online + offline) para la empresa ficticia EcoBottle AR. El pipeline utiliza datos transaccionales (RAW) para construir un Data Warehouse dimensional (Esquema Estrella) y alimentar un Dashboard de Looker Studio con KPIs clave para el área comercial.

## Instrucciones de Ejecución Local:
Para reproducir el pipeline de datos, sigue los siguientes pasos desde la terminal.

*1- Clonar el Repositorio*
*Cambia 'TU_USUARIO' por tu nombre de usuario de GitHub*
git clone https://github.com/TU_USUARIO/mkt_tp_final.git
cd mkt_tp_final

*2- Crear y Activar un Entorno Visual*
*Crear entorno virtual*
python3 -m venv .venv

*Activar en macOS/Linux*
source .venv/bin/activate

*Activar en Windows (PowerShell)*
.\.venv\Scripts\Activate

*3- Instalar dependencias*
pip install -r requirements.txt

*4- Ejecutar el Pipeline de transformación*
El script main.py leerá los datos de data/raw/, construirá el esquema estrella y guardará los resultados en warehouse/.
python main.py

*5- Verificar la salida*
Tras la ejecución exitosa, la carpeta warehouse/ contendrá las subcarpetas dim/ y fact/ con los datos transformados listos para ser usados en Looker Studio.
Repositorio del trabajo práctico final de la materia.

---

## Stack Tecnologico 
|compoente | herramienta | uso principal |
|---|---|---|
|Fuente de datos | Archivos .CSV | Simulacion de la base de datos transaccional(OLTP)|
|Transformación (ETL) | Python (Pandas) | Limpieza, denormalización y construcción del esquema estrella|
|Control de Versiones | Git / GitHub | Gestión del código fuente y del proyecto (se evaluará el uso de conventional commits)|
|Visualización (Dashboard) | Looker Studio | Construcción del tablero de reporte final|

---

## Diccionarios de datos y supuestos 
El **Data Warehouse (DW)** está modelado siguiendo el estándar de **Esquema Estrella**, según la metodología Kimball. Esta arquitectura fue seleccionada para **maximizar la velocidad de consulta** y **simplificar el análisis de las métricas clave (KPIs)**, asegurando la trazabilidad de los datos a través de las dimensiones de negocio.

**A tablas de hechos**
Las tablas almacenan las metricas claves y las claves foraneas que conectan con las dimensiones 


|Hecho | Grano | Claves Foraneas | Medidas/metricas clave | 
|---|---|---|---|
|FACT_SALES_ORDER | Una fila por pedido (cabecera) | customer_key, channel_key, store_key, date_key (order_date), shipping_address_key | total_amount (KPI: Ventas, Ticket Promedio), subtotal, shipping_fee|
|FACT_SALES_ORDER_ITEM | Una fila por línea/producto dentro de un pedido | order_key (a Fact), product_key | line_total (KPI: Ranking por Producto), quantity, unit_price, discount_amount|
|FACT_NPS_RESPONSE | Una fila por respuesta de encuesta NPS | customer_key (NULLable), channel_key, date_key (responded_at) | score (0-10) (KPI: NPS), comment|
| FACT_WEB_SESSION | Una fila por sesión web/app. | customer_key (NULLable), date_key (started_at) | session_id (KPI: Usuarios Activos), source, device |


**B tablas de dimensiones**
Las dimensiones contienen los atributos descriptivos, desnormalizando los catálogos de origen


|Dimensión | Clave Primaria (PK) | Atributos Clave / Desnormalizados | Propósito / KPI|
|---|---|---|---|
|DIM_CALENDAR | date_key (INT) | date, month_name, year, is_weekend |	
Series temporales (Ventas, Usuarios Activos, Ranking)|
|DIM_CUSTOMER | customer_key (INT) | customer_id, email, full_name, status | Filtrado de Pedidos y Usuarios Activos | 
|DIM_PRODUCT | product_key (INT) | product_id, sku, name, list_price, category_name | Ranking Mensual por Producto |
|DIM_ADDRESS | address_key (INT) | address_id, city, province_name, country_code | Ventas por Provincia (shipping_address) |
|DIM_CHANNEL | channel_key (INT) | channel_id, code (ONLINE/OFFLINE), name | Filtro clave para el tablero |
|DIM_STORE | store_key (INT) | store_id, name, province_name | Localización de ventas OFFLINE | 

**C supuestos del modelo**

**Ventas y Ticket Promedio** Solo se consideran pedidos con estado 'PAID' o 'FULFILLED'26. El campo utilizado para el monto es sales_order.total_amount2727.
**Usuarios Activos** Se calculará mediante el conteo de DISTINCT customer_id en la tabla web_session. Si no hay customer_id (sesión anónima), se usará session_id como alternativa para capturar el tráfico total28.
**Ventas por Provincia** La geolocalización de una venta se basa en la dirección de envío (shipping_address_id) del pedido, que se mapea a address.province_id29.
**NPS** Se calcula usando la fórmula $((\%\text{Promotores}) - (\%\text{Detractores})) \times 100$30. Promotores: score 9-10. Detractores: score 0-631.

---

## Dashboard y Criterios de Evaluación
El tablero, que se desarrollará en Looker Studio, debe incluir los filtros de Fecha, Canal, Provincia y Producto  y las siguientes vistas mínimas:

| KPI | Logica de agregacion | Base de Datos (DW)|
|---|---|---|
Total Ventas ($M) | SUM(total_amount_order)	| fact_sales (filtrando status = PAID / FULFILLED)
| Ticket Promedio ($K) | SUM(total_amount_order) / COUNT(DISTINCT order_id)	 | fact_sales (filtrando status = PAID / FULFILLED) | 
| Usuarios activos (nK)	| COUNT(DISTINCT customer_id) | fact_web_session |
| NPS | ((%Promotores - %Detractores)) x 100 | fact_nps_response (Score 9-10 Promotor, 0-6 Detractor) | 
| Ranking por producto | SUM(line_total) agrupado por product_id y date.month | fact_sales y dim_calendar | 

---

## Dashboard Final 


