# Guía paso a paso: ejecutar y desarrollar el proyecto por tu cuenta

Esta guía te lleva desde cero hasta tener el radar funcionando en Azure, con datos reales y un tablero en Power BI. Cada etapa dice **qué hacés vos**, **cómo verificás que salió bien** y **en qué te puedo ayudar yo** (Claude) si algo falla.

Conviene seguirla en orden. Las etapas 1 y 2 no cuestan plata y no necesitan Azure. Desde la etapa 3 empieza a haber costo.

| Etapa | Qué lográs | Costo | Tiempo estimado |
|------:|-----------|-------|-----------------|
| 0 | Cuentas y herramientas listas | Gratis | 1-2 h |
| 1 | El proyecto corre en tu máquina | Gratis | 30 min |
| 2 | El repo está en tu GitHub con CI en verde | Gratis | 30 min |
| 3 | Infraestructura creada en Azure | Bajo (ver etapa 3) | 1-2 h |
| 4 | Databricks configurado y el código desplegado | Bajo | 1 h |
| 5 | Primera corrida con datos reales | Unos USD por corrida | 1 h |
| 6 | Archivos de INDEC llegando solos con Data Factory | Muy bajo | 1 h |
| 7 | Vistas SQL y permisos | Bajo | 30 min |
| 8 | Tablero en Power BI | Gratis (Desktop) | 2-4 h |
| 9 | Despliegue automático desde GitHub | Gratis | 1 h |
| 10 | Seguir desarrollando | — | — |

---

## Etapa 0. Cuentas y herramientas

### Cuentas que necesitás

| Cuenta | Para qué | Cómo conseguirla |
|--------|----------|------------------|
| **Azure** con una suscripción propia | Toda la infraestructura | [azure.microsoft.com/free](https://azure.microsoft.com/free). La cuenta gratuita trae crédito por 30 días. Si usás una suscripción de tu organización, necesitás rol **Owner** (o Contributor + User Access Administrator) sobre ella. |
| **GitHub** | Repositorio y CI/CD | [github.com](https://github.com) |
| **UN Comtrade** (API key gratuita, opcional al principio) | Datos bilaterales de comercio | Para empezar no hace falta: la **API pública** (`https://comtradeapi.un.org/public/v1`) no pide clave y alcanza para probar. Para la carga completa, registrate en [comtradedeveloper.un.org](https://comtradedeveloper.un.org), entrá a *Products*, suscribite a **comtrade - v1** (plan free) y copiá la *Primary key* desde tu perfil. |
| **Power BI Desktop** | Tablero | Gratis, se descarga desde Microsoft Store (sólo Windows). |

BCRA e INDEC no piden registro.

### Herramientas en tu computadora

Si usás Windows, te recomiendo instalar **WSL2 con Ubuntu** (`wsl --install` en PowerShell como administrador) y hacer todo lo que sigue dentro de Ubuntu. Spark corre mucho mejor ahí que en Windows directo. Power BI Desktop sí va en Windows.

| Herramienta | Versión | Verificar con |
|-------------|---------|---------------|
| Git | cualquiera reciente | `git --version` |
| Python | 3.11 o 3.12 | `python3 --version` |
| Java (JDK) | 17 | `java -version` |
| Azure CLI | 2.60+ | `az version` |
| Terraform | 1.9.x | `terraform version` |
| Databricks CLI | 0.230+ | `databricks version` |
| VS Code (opcional) | — | — |

En Ubuntu / WSL:

```bash
sudo apt update && sudo apt install -y git python3 python3-venv python3-pip openjdk-17-jdk unzip make
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh
# Terraform: https://developer.hashicorp.com/terraform/install (sección Linux / Ubuntu)
```

**Verificación:** los seis comandos de la tabla devuelven una versión.

**En qué te ayudo:** si una instalación falla, pegame el error y te digo cómo seguir.

---

## Etapa 1. Correr el proyecto en tu máquina

Esto prueba que todo el código funciona antes de gastar un peso en Azure.

```bash
unzip trade-intelligence-platform.zip && cd trade-intelligence-platform
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]" build
pytest                     # tienen que pasar 35 tests
python scripts/run_local_demo.py
```

**Verificación:**
- `pytest` termina con `35 passed`.
- La demo imprime una tabla *Top 10 oportunidades*, una alerta espejo para `NLD 080550` (limones a Países Bajos) y un outlier de precio para `CHN 220421 FRA` (vino).

**Para entender el código**, leé en este orden:
1. `conf/base.yml`: qué productos, países y parámetros usa.
2. `src/trade_intel/silver/trade_flows.py`: cómo se unifican Comtrade e INDEC.
3. `src/trade_intel/gold/metrics.py`: las fórmulas (con `docs/metrics.md` al lado).
4. `tests/unit/test_metrics.py`: ejemplos chicos que muestran qué devuelve cada métrica.

**En qué te ayudo:** si falla un test o la demo, pegame la salida. Si no se entiende alguna parte del código, preguntame por el archivo.

### 1.1 Probar con datos reales de Comtrade, sin clave

La demo anterior usa datos sintéticos. Para ver el pipeline con datos oficiales, sin registrarte en ningún lado, usá la API pública de Comtrade:

```bash
python scripts/fetch_comtrade_public.py --year 2023
python scripts/run_local_demo.py --landing data/public_sample
```

El primer comando baja Argentina más 5 mercados (España, Países Bajos, EE.UU., China, Alemania) para limones, maní y vino: son 42 pedidos y tarda unos 4 minutos, porque va despacio a propósito. El segundo corre bronze, silver y gold sobre esos datos.

Podés cambiar el alcance con `--reporters` (códigos M49), `--hs` (códigos HS6) y `--year` (acepta varios años).

**Límites de la API pública:**
- Devuelve como máximo **500 registros por pedido**. Por eso el script pide un producto por vez: así cada respuesta trae una fila por país socio, bastante menos de 500. Si alguna respuesta llega justo a 500, el script lo avisa, porque significa que faltan datos.
- No publica un límite de pedidos por minuto. El script hace 12 por minuto. Si empieza a dar errores 429, bajalo con `--rpm 6`.
- Con un solo año no hay CAGR (necesita 5) y con pocos países el RCA es aproximado. Para el radar completo conviene la clave gratuita (etapa 4.1).

**Verificación:** el script termina con `Listo: N filas` y `truncados: 0`, y la demo muestra la tabla de oportunidades con nombres reales de mercados.

---

## Etapa 2. Subir el repo a GitHub

1. En GitHub, creá un repositorio **vacío** (sin README ni .gitignore), por ejemplo `radar-comex-argentina`.
2. Desde la carpeta del proyecto (el zip ya trae el historial de git):

```bash
git remote add origin https://github.com/<tu-usuario>/radar-comex-argentina.git
git push -u origin main
```

3. Entrá a la pestaña **Actions** del repo. El workflow `ci` corre solo.

**Verificación:** `ci` en verde. Los jobs `python`, `terraform` (fmt + validate), `bundle` y `adf` pasan. El workflow `cd` va a fallar hasta la etapa 9, y eso es esperable.

**En qué te ayudo:** si conectás el repo a este proyecto de Claude, lo puedo empujar yo, abrir PRs con cambios y arreglar el CI si se pone rojo.

---

## Etapa 3. Crear la infraestructura en Azure

### 3.1 Iniciar sesión y preparar la suscripción

```bash
az login
az account set --subscription "<ID de tu suscripción>"
az account show --query id -o tsv          # anotá este ID

# Registrar los servicios que se van a usar (una sola vez por suscripción)
for p in Microsoft.Storage Microsoft.KeyVault Microsoft.Databricks Microsoft.DataFactory Microsoft.OperationalInsights Microsoft.Insights; do
  az provider register --namespace $p
done
```

**Poné un tope de gasto:** en el portal, *Cost Management → Budgets*, creá un presupuesto mensual (por ejemplo USD 50) con alerta por mail al 50% y al 90%.

### 3.2 Crear dónde se guarda el estado de Terraform

Terraform necesita un lugar para recordar qué creó. Esto se hace una sola vez, a mano:

```bash
az group create -n rg-tfstate -l brazilsouth
az storage account create -n sttfstatetradeintel -g rg-tfstate -l brazilsouth --sku Standard_LRS --allow-blob-public-access false
az storage container create -n tfstate --account-name sttfstatetradeintel --auth-mode login
```

Si `sttfstatetradeintel` ya existe en Azure (los nombres de storage son únicos en todo el mundo), usá otro y cambialo también en `infra/terraform/envs/dev.backend.hcl` y `prod.backend.hcl`.

Para correr Terraform desde tu máquina, abrí `infra/terraform/envs/dev.backend.hcl` y cambiá `use_oidc = true` por `use_azuread_auth = true`. OIDC es sólo para GitHub Actions (etapa 9). Para acceder con tu usuario necesitás además este rol sobre el storage del estado:

```bash
az role assignment create --role "Storage Blob Data Contributor" \
  --assignee "$(az ad signed-in-user show --query id -o tsv)" \
  --scope "$(az storage account show -n sttfstatetradeintel -g rg-tfstate --query id -o tsv)"
```

### 3.3 Completar las variables

Editá `infra/terraform/envs/dev.tfvars`:

```hcl
env              = "dev"
subscription_id  = "<el ID que anotaste>"
assign_metastore = false
metastore_id     = ""
```

**Nombres únicos:** el storage del lago se va a llamar `sttradeinteldev`. Si alguien ya usa ese nombre, `terraform apply` falla con `StorageAccountAlreadyTaken`. En ese caso agregá `project = "tradeintelab"` (o cualquier sufijo tuyo) en el `.tfvars` y cambiá `storage_account` en `conf/dev.yml` para que coincidan.

### 3.4 Aplicar

```bash
cd infra/terraform
terraform init -backend-config=envs/dev.backend.hcl
terraform plan  -var-file=envs/dev.tfvars -out=tfplan     # leé el plan: unos 30 recursos
terraform apply tfplan
terraform output
```

**Qué se crea:** resource group, ADLS Gen2 con contenedores `landing`, `lakehouse` y `checkpoints`, Key Vault, workspace de Databricks Premium con Access Connector, Data Factory, Log Analytics con alertas, y en Unity Catalog el catálogo `trade_dev` con los schemas `bronze`, `silver`, `gold` y `ops`, el volume `ops.reference`, el secret scope y un SQL Warehouse.

**Si falla la parte de Unity Catalog** (los recursos `databricks_*`): pasa cuando el workspace todavía no tiene Unity Catalog habilitado. Dos caminos:
- Esperar unos minutos y volver a correr `terraform apply` (a veces la habilitación automática tarda).
- Entrar a [accounts.azuredatabricks.net](https://accounts.azuredatabricks.net) → *Catalog*, copiar el ID del metastore de la región `brazilsouth` (o crearlo si no hay), poner `assign_metastore = true` y `metastore_id = "<id>"` en el `.tfvars`, y volver a aplicar.

**Costo:** con todo apagado, el costo fijo es casi cero (storage con pocos datos, Key Vault y Log Analytics se cobran por uso). Lo que cuesta es el cómputo de Databricks mientras corre un job o el SQL Warehouse está prendido (se apaga solo a los 10 minutos).

**Verificación:** `terraform output` muestra `databricks_workspace_url`. Al abrir esa URL en el navegador ves el workspace y, en *Catalog*, el catálogo `trade_dev`.

**En qué te ayudo:** pegame el error de `plan` o `apply` y te digo qué cambiar. Los errores típicos son nombres repetidos, permisos insuficientes en la suscripción y servicios que todavía no están registrados.

---

## Etapa 4. Configurar Databricks y desplegar el código

### 4.1 Cargar la API key de Comtrade en Key Vault

Este paso es opcional para empezar. El job de Comtrade tiene `api_mode: auto` en `conf/base.yml`: si en Key Vault sigue el valor de ejemplo (`CHANGE_ME`), usa la API pública sin clave y pide un producto por vez. Funciona, pero hace muchos más pedidos y va más lento, así que para la carga completa conviene cargar la clave:

```bash
KV=$(terraform output -raw key_vault_uri | sed 's#https://##; s#\.vault.*##')
az keyvault secret set --vault-name "$KV" --name comtrade-subscription-key --value "<tu primary key de Comtrade>"
```

### 4.2 Crear los grupos de usuarios

En el workspace: *Settings → Identity and access → Groups*. Creá `data-engineers` y `trade-analysts`, y sumate al primero. Los permisos de Unity Catalog ya están asignados a esos nombres.

Si el `terraform apply` de la etapa 3 falló en los grants porque los grupos no existían, volvé a correrlo después de crearlos.

### 4.3 Conectar la CLI de Databricks

```bash
databricks auth login --host <databricks_workspace_url>
databricks current-user me      # verifica que funciona
```

### 4.4 Ajustar el bundle

En `databricks.yml`:
- `targets.dev.workspace.host`: tu URL del workspace.
- `variables.alert_email.default`: tu mail, para que te avisen si falla un job.
- Si cambiaste el nombre del storage en la etapa 3, también `landing_url` en el target `dev`.

### 4.5 Subir los datos de referencia y desplegar

Desde la raíz del repo:

```bash
databricks fs cp data/reference/countries.csv dbfs:/Volumes/trade_dev/ops/reference/countries.csv --overwrite
databricks fs cp data/reference/products.csv  dbfs:/Volumes/trade_dev/ops/reference/products.csv  --overwrite

databricks bundle validate -t dev
databricks bundle deploy   -t dev
```

**Verificación:** en *Workflows* aparecen `[dev tu_usuario] trade-intel-annual-dev` y `trade-intel-daily-dev`. En el modo dev los schedules quedan pausados, así que no se dispara nada solo.

**En qué te ayudo:** si `bundle validate` o `deploy` da error, pegámelo. Es lo más probable que necesite ajuste la primera vez, porque cada workspace tiene detalles propios (tipos de nodo disponibles, versión de runtime).

---

## Etapa 5. Primera corrida con datos reales

### 5.1 Empezar chico

La primera vez conviene bajar un solo año para ver que todo funciona.

- **Con clave:** el plan gratuito permite unas 500 llamadas por día. Un año son unas 60 llamadas y la historia completa (6 años, lo necesario para el CAGR de 5 años) unas 360, así que no conviene repetir la carga completa el mismo día.
- **Sin clave (API pública):** un año son unas 620 llamadas, porque se pide un producto por vez, y a 12 por minuto tarda cerca de una hora. Para probar alcanza. Para la historia completa, cargá la clave.

Para forzar un modo, agregá `--api-mode,public` o `--api-mode,keyed` a los parámetros. Con `keyed`, el job falla si no encuentra la clave en vez de pasar a la pública.

```bash
databricks bundle run -t dev trade_intel_annual --python-params="--year,2023"
```

El comando muestra el link a la corrida. Las tareas van en este orden: `ingest_comtrade → bronze_comtrade → silver → gold → optimize`.

### 5.2 Revisar qué llegó

En el workspace, *SQL Editor*, con el catálogo `trade_dev`:

```sql
-- ¿Cuántas filas por capa?
SELECT 'bronze' capa, count(*) FROM trade_dev.bronze.comtrade_raw
UNION ALL SELECT 'silver', count(*) FROM trade_dev.silver.trade_flows
UNION ALL SELECT 'cuarentena', count(*) FROM trade_dev.ops.quarantine_trade_flows;

-- ¿Por qué se rechazaron filas?
SELECT _dq_failed, count(*) FROM trade_dev.ops.quarantine_trade_flows GROUP BY 1;

-- ¿Respuestas truncadas? (habría que partir más el pedido)
SELECT count(*) FROM trade_dev.bronze.comtrade_raw WHERE _truncated;

-- El radar
SELECT * FROM trade_dev.gold.fact_export_opportunity ORDER BY opportunity_score DESC LIMIT 20;
```

**Qué es normal ver:**
- Filas en cuarentena por `known_partner`: Comtrade usa códigos de agregados regionales (por ejemplo "Areas, nes") que no están en `countries.csv`. Está bien que queden afuera del análisis bilateral.
- RCA y CAGR nulos con un solo año: el CAGR necesita 5 años de historia. Cuando funcione con uno, corré el job sin `--year` y baja los 6 años configurados en `years_back`.

### 5.3 Completar la historia

```bash
databricks bundle run -t dev trade_intel_annual
```

**Verificación:** `fact_export_opportunity` tiene valores de `cagr` y `home_rca`, y los productos argentinos fuertes (limones, maní, carne) aparecen con RCA mayor a 1.

**En qué te ayudo:** si una tarea falla, copiá el error del run (pestaña *Output* de la tarea) y te lo resuelvo. Si los resultados no te cierran desde el negocio, mandame la consulta y lo revisamos juntos.

---

## Etapa 6. INDEC con Data Factory

### 6.1 Confirmar las URLs reales

Este es el punto que no pude validar desde acá. Entrá a [comex.indec.gob.ar](https://comex.indec.gob.ar), buscá la sección de descarga de bases (exportaciones por NCM y país de destino) y copiá el link de descarga del zip de un año. Anotá también:
- la ruta del link (lo que va después de `https://comex.indec.gob.ar`),
- los nombres de columna del CSV de adentro,
- el separador (`;` o `,`) y si los decimales usan coma,
- la tabla de códigos de país de INDEC (suele venir en el mismo zip o en la documentación).

Con eso:
1. Actualizá `relativeUrl` en `adf/pipeline/pl_ingest_indec.json`.
2. Si las columnas son distintas a `Año;Mes;NCM;Pdes;Pnet(kg);FOB(u$s)`, actualizá el diccionario `rename` en `src/trade_intel/bronze/ingest.py` (función `parse_indec`).
3. Completá o corregí la columna `indec_code` en `data/reference/countries.csv`.

### 6.2 Publicar y correr el pipeline

```bash
RG=rg-tradeintel-dev; ADF=adf-tradeintel-dev
az extension add --name datafactory
az datafactory linked-service create -g $RG --factory-name $ADF -n ls_http_indec --properties @adf/linkedService/ls_http_indec.json
az datafactory linked-service create -g $RG --factory-name $ADF -n ls_adls_lake  --properties @adf/linkedService/ls_adls_lake.json
az datafactory dataset create -g $RG --factory-name $ADF -n ds_indec_zip      --properties @adf/dataset/ds_indec_zip.json
az datafactory dataset create -g $RG --factory-name $ADF -n ds_landing_binary --properties @adf/dataset/ds_landing_binary.json
az datafactory pipeline create -g $RG --factory-name $ADF -n pl_ingest_indec --pipeline @adf/pipeline/pl_ingest_indec.json
az datafactory pipeline create-run -g $RG --factory-name $ADF -n pl_ingest_indec
```

También podés abrir Data Factory Studio desde el portal y apretar *Debug* sobre el pipeline, que es más visual para la primera vez.

**Verificación:**
- En el storage, contenedor `landing`, aparece `indec/exports/year=2024/` con CSV adentro.
- El job `trade-intel-daily-dev` arranca solo por el *file arrival trigger* (en modo dev puede estar pausado: activalo desde la UI del job o corrélo a mano con `databricks bundle run -t dev trade_intel_daily`).
- `SELECT count(*) FROM trade_dev.gold.fact_ar_exports_monthly` devuelve filas y `fob_ars` no es nulo.

**En qué te ayudo:** pasame los encabezados del CSV real y dos o tres filas de ejemplo, y te ajusto el parser, el mapeo de países y los tests en un cambio.

---

## Etapa 7. Vistas SQL y permisos

En el *SQL Editor*, con el SQL Warehouse `wh-trade-bi-dev` seleccionado:

```sql
USE CATALOG trade_dev;
```

Después pegá y ejecutá el contenido de:
1. `sql/01_semantic_views.sql`: crea las 4 vistas que usa Power BI.
2. `sql/02_governance.sql`: tags, comentarios y el row filter por sector. El row filter menciona un grupo `camara-citricos` de ejemplo. Si no lo vas a usar, borrá esa línea de la función antes de ejecutar.
3. `sql/03_observability.sql`: son consultas de monitoreo. Guardalas como queries y armá con ellas un dashboard operativo en *Dashboards*.

**Verificación:** `SELECT * FROM gold.vw_opportunity_radar LIMIT 10` devuelve nombres de países y productos en español.

---

## Etapa 8. Tablero en Power BI

1. En Databricks: *SQL Warehouses → wh-trade-bi-dev → Connection details*. Copiá **Server hostname** y **HTTP path**.
2. En Power BI Desktop: *Obtener datos → Azure Databricks*. Pegá los dos valores y elegí **DirectQuery**. Autenticación: *Microsoft Entra ID* con tu usuario.
3. Navegá a `trade_dev → gold` y cargá: `vw_opportunity_radar`, `vw_competitor_landscape`, `vw_mirror_alerts`, `vw_ar_exports_monthly`, `dim_country`, `dim_product` y `dim_date`.
4. Armá relaciones y medidas siguiendo `powerbi/README.md`.
5. Construí las 4 páginas, empezando por la más útil:
   - **Radar**: dispersión con CAGR en X, share ARG en Y, tamaño = `market_value_usd`, color = `strategic_quadrant`. Al lado, una tabla top 20 por `opportunity_score`.
   - **Competidores**: segmentadores de mercado y producto, barras de share por proveedor y una columna de precio contra la mediana.
   - **Alertas espejo**: matriz socio × producto coloreada por `gap_ratio`.
   - **Mensual**: líneas de USD y ARS por mes, con segmentador por campaña agrícola.
6. Guardá el `.pbix` en `powerbi/` y hacé commit.

**En qué te ayudo:** te puedo escribir más medidas DAX, revisar el modelo con vos o proponerte el diseño de cada página si me contás a quién va dirigido el tablero.

---

## Etapa 9. Despliegue automático desde GitHub

El objetivo es que cada push a `main` despliegue a dev, y cada tag `v*` despliegue a prod con tu aprobación.

### 9.1 Identidad para GitHub en Azure

```bash
APP_ID=$(az ad app create --display-name gh-trade-intel --query appId -o tsv)
az ad sp create --id $APP_ID
SUB=$(az account show --query id -o tsv)
az role assignment create --assignee $APP_ID --role Contributor --scope /subscriptions/$SUB
az role assignment create --assignee $APP_ID --role "User Access Administrator" --scope /subscriptions/$SUB

# Una credencial federada por entorno de GitHub (sin contraseñas)
for ENV in dev prod plan; do
az ad app federated-credential create --id $APP_ID --parameters "{
  \"name\": \"gh-$ENV\",
  \"issuer\": \"https://token.actions.githubusercontent.com\",
  \"subject\": \"repo:<tu-usuario>/radar-comex-argentina:environment:$ENV\",
  \"audiences\": [\"api://AzureADTokenExchange\"]
}"
done
echo "AZURE_CLIENT_ID=$APP_ID"; az account show --query "{tenant:tenantId, subscription:id}"
```

Dale también a esa identidad el rol *Storage Blob Data Contributor* sobre `sttfstatetradeintel` (como en 3.2) y agregala como usuario admin del workspace de Databricks (*Settings → Identity and access → Service principals → Add*, con el `APP_ID`).

### 9.2 Configurar GitHub

En el repo: *Settings → Environments*. Creá `dev`, `prod` y `plan`. En `prod`, activá **Required reviewers** y ponete a vos.

En *Settings → Secrets and variables → Actions → Variables*, agregá:

| Variable | Valor |
|----------|-------|
| `AZURE_CLIENT_ID` | el `APP_ID` |
| `AZURE_TENANT_ID` | tu tenant |
| `AZURE_SUBSCRIPTION_ID` | tu suscripción |
| `DATABRICKS_HOST` | la URL del workspace dev |

En `envs/*.backend.hcl`, volvé a dejar `use_oidc = true` para que el CI lo use.

### 9.3 Probar

Hacé un cambio chico (por ejemplo agregar un producto a `conf/base.yml` y `data/reference/products.csv`), commit y push a `main`.

**Verificación:** el workflow `cd` termina en verde y el job en Databricks refleja el cambio.

**Para prod:** repetí las etapas 3 y 4 con `envs/prod.tfvars` y el target `prod` (completá `run_as.service_principal_name` en `databricks.yml` con el `APP_ID`). Después `git tag v0.1.0 && git push --tags` dispara el deploy a prod, que espera tu aprobación.

---

## Etapa 10. Seguir desarrollando

### Flujo de trabajo

1. Creá una rama: `git checkout -b feat/aranceles`.
2. Escribí primero el test en `tests/unit/` con un DataFrame chico y el resultado esperado.
3. Implementá la función en `src/trade_intel/...` hasta que pase `pytest`.
4. Probá con `python scripts/run_local_demo.py`.
5. Abrí un PR. El CI corre los tests. Cuando se mergea, el CD despliega a dev.
6. Probá en dev con datos reales y, cuando esté bien, sacá un tag para prod.

### Cambios frecuentes

| Quiero... | Dónde |
|-----------|-------|
| Agregar un producto | `conf/base.yml` (`product_universe`) + `data/reference/products.csv` |
| Agregar un mercado | `conf/base.yml` (`reporters_m49`, con el código M49 de Comtrade) + `data/reference/countries.csv` |
| Cambiar los pesos del score | `conf/base.yml` (`gold.opportunity_weights`, deben sumar 1) |
| Agregar una regla de calidad | `src/trade_intel/quality/expectations.py` + un test en `tests/unit/test_quality.py` |
| Agregar una métrica | función en `gold/metrics.py` + test + agregarla a `outputs` en `gold/build.py` |
| Re-procesar sin llamar a las APIs | Repair run de las tareas `silver` y `gold` |

### Próximas mejoras sugeridas (en `docs/roadmap.md`)

1. Aranceles de WITS en el score.
2. Distancia y costo logístico (CEPII).
3. Nowcasting con Comtrade mensual.
4. Alertas cuando un competidor pierde share en un mercado donde Argentina tiene ventaja.

### Cuidar los costos

- Los jobs usan clusters que se apagan solos al terminar.
- El SQL Warehouse se apaga a los 10 minutos sin uso.
- Si no vas a usar el entorno por un tiempo: `terraform destroy -var-file=envs/dev.tfvars`. Se puede recrear igual con `apply`, y los datos se recuperan volviendo a correr los jobs, porque la landing de Comtrade se vuelve a bajar.
- Revisá el gasto con la consulta 4 de `sql/03_observability.sql` y en *Cost Management* del portal.

---

## Checklist resumida

- [ ] Etapa 0: cuentas de Azure, GitHub y Comtrade; herramientas instaladas
- [ ] Etapa 1: `pytest` con 35 tests en verde, la demo local funcionando y una muestra real bajada con la API pública
- [ ] Etapa 2: repo en GitHub y CI en verde
- [ ] Etapa 3: `terraform apply` en dev y catálogo `trade_dev` visible
- [ ] Etapa 4: key de Comtrade en Key Vault, grupos creados, `bundle deploy` hecho
- [ ] Etapa 5: corrida de un año y después la historia completa; radar con datos
- [ ] Etapa 6: URL de INDEC confirmada, pipeline de ADF corriendo, serie mensual en gold
- [ ] Etapa 7: vistas y gobierno aplicados
- [ ] Etapa 8: tablero de Power BI con las 4 páginas
- [ ] Etapa 9: CD automático a dev y prod con aprobación

Cuando termines cada etapa, o si te trabás en alguna, escribime en este hilo con lo que ves (error, captura o resultado de una consulta) y seguimos desde ahí.
