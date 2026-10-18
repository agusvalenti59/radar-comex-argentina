# Runbook operativo

## Puesta en marcha (una vez por entorno)

1. Crear el backend de estado: RG `rg-tfstate`, storage `sttfstatetradeintel`, container `tfstate`.
2. Crear una app de Entra ID para GitHub con **federated credentials** (`repo:<org>/<repo>:environment:dev|prod`) y roles `Contributor` + `User Access Administrator` sobre la suscripción (necesario para los role assignments).
3. Cargar en GitHub (Settings → Environments) las variables `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_ID`, `DATABRICKS_HOST`. En `prod`, requerir aprobación.
4. `terraform apply` (o merge a `main` con cambios en `infra/`).
5. Cargar el secreto: `az keyvault secret set --vault-name kv-tradeintel-dev --name comtrade-subscription-key --value <key>`.
6. Crear los grupos `data-engineers` y `trade-analysts` en la cuenta de Databricks (SCIM desde Entra ID).
7. Merge a `main` → CD despliega el bundle, sube seeds y publica ADF.
8. Ejecutar `sql/01_semantic_views.sql` y `sql/02_governance.sql`.

## Operación diaria

| Qué mirar | Dónde |
|-----------|-------|
| Corridas fallidas | Mail de Workflows, `system.lakeflow.job_run_timeline`, alerta ADF en Log Analytics |
| Calidad | `ops.dq_metrics`; alerta si cuarentena > 2% |
| Frescura | Tarea `freshness` (falla el job si se incumple el SLA), `ops.freshness` |
| Costos | `sql/03_observability.sql` query 4 |

## Procedimientos

**Backfill de un año de Comtrade**
```bash
databricks bundle run -t prod trade_intel_annual --python-params="--year,2019"
```
Seguro de repetir: landing sobreescribe la misma ruta y silver hace MERGE.

**Re-procesar silver/gold sin llamar a las APIs**: correr sólo las tareas `silver` y `gold` (Repair run en la UI o `bundle run --only silver,gold`).

**Filas en cuarentena**
```sql
SELECT _dq_failed, count(*) FROM trade_prod.ops.quarantine_trade_flows
WHERE _quarantined_at > current_date() - 1 GROUP BY 1;
```
- `known_partner` → falta un código de país en `countries.csv`: agregarlo por PR y re-procesar silver.
- `non_negative_value` → revisión del origen; Comtrade publica correcciones.

**Falla `valid_flow` (regla crítica)**: la fuente cambió de semántica. No forzar; revisar bronze y ajustar `normalize_*`.

**Rotar la key de Comtrade**: actualizar el secreto en Key Vault. No requiere deploy (el secret scope lee en vivo).

**Time travel ante un error en gold**
```sql
RESTORE TABLE trade_prod.gold.fact_export_opportunity TO VERSION AS OF <n>;
```
