# Restricciones de la suscripción y decisiones de diseño

## Contexto
La suscripción utilizada limita las regiones por política
(West US 2, North Central US, Mexico Central, Brazil South y Chile Central)
y tiene cuotas bajas de vCPU. Antes de desplegar Databricks había que
averiguar dónde se podía encender un cluster.

## Investigación
Los comandos se ejecutaron en Azure Cloud Shell (Bash).

**1. Cuotas con límite distinto de cero en West US 2**

    az vm list-usage --location westus2 --query "[?limit!='0'].{Familia:name.localizedValue, Uso:currentValue, Limite:limit}" -o table

Resultado: 6 vCPU regionales en total; familias antiguas (Dv2, DSv3, DSv4,
etc.) con 4 vCPU; familias modernas (v5 a v7) sin cuota.
Nota: la CLI devuelve `limit` como texto, por lo que comparar con `>0` falla.

**2. Máquinas sin restricciones por región**

    for r in westus2 northcentralus brazilsouth mexicocentral chilecentral; do echo "== $r"; az vm list-skus --location $r --resource-type virtualMachines --all --query "[].{n:name,r:length(restrictions)}" -o tsv | awk '$2==0 {print $1}' | grep -E "D4s_v3|DS3_v2|D4s_v4|D4_v3|D4_v4|F4s_v2|E4s_v3|E4_v3|DS12_v2"; done

Resultado: en West US 2, North Central US y Mexico Central ninguna de las
máquinas de 4 vCPU evaluadas estaba disponible (por ejemplo, Standard_D4s_v3
aparece como NotAvailableForSubscription en West US 2). Brazil South y
Chile Central sí tenían opciones.

**3. Cuota en las regiones viables**

    for r in brazilsouth chilecentral; do echo "== $r"; az vm list-usage --location $r --query "[?limit!='0'].{Familia:name.localizedValue, Uso:currentValue, Limite:limit}" -o table | grep -E "Total Regional vCPUs|DSv4|Dv4|DSv2"; done

Resultado: ambas regiones tienen 6 vCPU regionales y 4 vCPU en las familias
DSv4, Dv4 y DSv2.

## Decisión
- Región: **Brazil South** (Chile Central queda como respaldo).
- Workspace: Premium, modo Híbrido (permite cluster policies y control del
  tipo de nodo, como en un cluster administrado).
- Nodo: **Standard_D4ds_v4** (4 vCPU, 16 GB), propuesto por Databricks al
  ajustar el tamaño a la cuota.
- Validación: el cluster de nodo único arrancó y ejecutó
  `spark.range(1000000).count()`. Se apagó manualmente.

## Consecuencias
- Con 6 vCPU regionales solo puede haber un cluster de 4 vCPU encendido a la vez.
- No es posible demostrar clusters multinodo; el dimensionamiento de
  cores y memoria se explica con base en la experiencia previa con YARN.
- Autoterminación a los 10 minutos para controlar costos.